/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! VECTOR-951: how fast CDC inserts reach an index while closed-loop ANN
//! searches hit the same index. Every stage but Scylla is the real one:
//! monitor_items and its table lock, the index actor, the worker pool and the
//! HTTP server; the usearch simulator spins for the cost of each operation.
//! It prints a table and asserts nothing, so it is ignored. Run it with
//!
//! ```sh
//! cargo test -p vector-store --release --test integration \
//!     ingest_under_search -- --ignored --nocapture
//! ```
//!
//! and tune it with the `INGEST_*` variables read in `Params::from_env`.

use crate::db_basic;
use crate::db_basic::ScanFn;
use crate::db_basic::TxIndexedRow;
use crate::vs_index::setup_store_with_quantization;
use crate::vs_index::usearch_test_config;
use futures::FutureExt;
use httpclient::HttpClient;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;
use vector_store::AsyncInProgress;
use vector_store::Config;
use vector_store::DbIndexPartitioning;
use vector_store::DbIndexedOperation;
use vector_store::DbIndexedRow;
use vector_store::DbIndexedValue;
use vector_store::NonemptyBox;
use vector_store::PrimaryKey;
use vector_store::Quantization;
use vector_store::Timestamp;
use vector_store::Timestamped;

const RESERVE: Duration = Duration::from_millis(10);
const SAMPLE: Duration = Duration::from_millis(250);

struct Params {
    workers: usize,
    client_threads: usize,
    search: Duration,
    add: Duration,
    dimensions: usize,
    base_rows: usize,
    rate: f64,
    clients: Vec<usize>,
    phase: Duration,
    drain_timeout: Duration,
}

fn env<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

impl Params {
    /// The defaults model cohere-1m on an r8g.4xlarge: a search costs about
    /// 1 ms of CPU, an add about 2.5 ms.
    fn from_env() -> Self {
        let workers = env("INGEST_WORKERS", 8);
        let add = Duration::from_micros(env("INGEST_ADD_US", 2500));
        let clients = std::env::var("INGEST_CLIENTS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(|c| c.trim().parse().unwrap())
                    .collect()
            })
            .unwrap_or_else(|| vec![workers, 4 * workers, 16 * workers]);
        Self {
            workers,
            client_threads: env("INGEST_CLIENT_THREADS", 4),
            search: Duration::from_micros(env("INGEST_SEARCH_US", 1000)),
            add,
            dimensions: env("INGEST_DIMENSIONS", 768),
            base_rows: env("INGEST_BASE_ROWS", 5000),
            // A quarter of what the workers add with no searches running.
            rate: env("INGEST_RATE", 0.25 * workers as f64 / add.as_secs_f64()),
            clients,
            phase: Duration::from_secs(env("INGEST_PHASE_SECS", 15)),
            drain_timeout: Duration::from_secs(env("INGEST_DRAIN_TIMEOUT_SECS", 120)),
        }
    }
}

fn primary_key(id: usize) -> PrimaryKey {
    [CqlValue::BigInt(id as i64)].into()
}

fn vector(dimensions: usize, id: usize) -> Vec<f32> {
    vec![id as f32; dimensions]
}

/// A CDC reader that writes rows of new keys on a cumulative schedule of
/// `rate` rows/s from `start`, waiting on the index's backpressure. Once
/// `stop_at` names a row count it sends the rows due until then and ends.
fn spawn_producer(
    tx: TxIndexedRow,
    params: &Params,
    start: Instant,
    stop_at: watch::Receiver<Option<u64>>,
    sent: Arc<AtomicU64>,
) -> JoinHandle<()> {
    let (first_id, rate, dimensions) = (params.base_rows, params.rate, params.dimensions);
    tokio::spawn(async move {
        let mut next = 0;
        loop {
            let stop = *stop_at.borrow();
            let due = stop.unwrap_or_else(|| (start.elapsed().as_secs_f64() * rate) as u64);
            if next >= due {
                if stop.is_some() {
                    break;
                }
                time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            let id = first_id + next as usize;
            let row = DbIndexedRow {
                primary_key: primary_key(id),
                operation: DbIndexedOperation::Upsert(
                    NonemptyBox::new([Timestamped::new(
                        Timestamp::from_millis(2 + next),
                        Some(DbIndexedValue::Vector(vector(dimensions, id).into())),
                    )])
                    .unwrap(),
                ),
            };
            if tx.send((row, AsyncInProgress::None)).await.is_err() {
                break;
            }
            next += 1;
            sent.store(next, Ordering::Relaxed);
        }
    })
}

#[derive(Default)]
struct SearchStats {
    done: AtomicU64,
    failed: AtomicU64,
    nanos: AtomicU64,
}

impl SearchStats {
    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.done.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.nanos.load(Ordering::Relaxed),
        )
    }
}

/// `count` clients, each sending an ANN request and waiting for its answer
/// before the next one, on their own runtime so that they do not take the
/// Vector Store's workers.
fn spawn_searchers(
    runtime: &Handle,
    count: usize,
    addr: SocketAddr,
    dimensions: usize,
    stop: Arc<AtomicBool>,
    stats: Arc<SearchStats>,
) -> Vec<JoinHandle<()>> {
    (0..count)
        .map(|id| {
            let (stop, stats) = (Arc::clone(&stop), Arc::clone(&stats));
            runtime.spawn(async move {
                let client = HttpClient::new(addr);
                let (keyspace, index) = ("vector".to_string().into(), "ann".to_string().into());
                let limit = NonZeroUsize::new(10).unwrap().into();
                while !stop.load(Ordering::Relaxed) {
                    let start = Instant::now();
                    let response = client
                        .post_ann(
                            &keyspace,
                            &index,
                            vector(dimensions, id).into(),
                            None,
                            limit,
                        )
                        .await;
                    let ok = response.status().is_success();
                    _ = response.bytes().await;
                    let counter = if ok { &stats.done } else { &stats.failed };
                    counter.fetch_add(1, Ordering::Relaxed);
                    stats
                        .nanos
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
            })
        })
        .collect()
}

async fn count(client: &HttpClient) -> usize {
    let (keyspace, index) = ("vector".to_string().into(), "ann".to_string().into());
    client
        .index_status(&keyspace, &index)
        .await
        .map(|status| status.count)
        .unwrap_or(0)
}

async fn wait_for_count(
    client: &HttpClient,
    expected: usize,
    timeout: Duration,
) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if count(client).await >= expected {
            return Some(start.elapsed());
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    None
}

#[test]
#[ignore = "VECTOR-951: a measurement that prints a table, run by hand"]
fn ingest_under_search() {
    let params = Params::from_env();
    let clients = Builder::new_multi_thread()
        .worker_threads(params.client_threads)
        .enable_all()
        .build()
        .unwrap();
    Builder::new_multi_thread()
        .worker_threads(params.workers)
        .enable_all()
        .build()
        .unwrap()
        .block_on(measure(params, clients.handle().clone()));
}

async fn measure(params: Params, clients: Handle) {
    let (tx_cdc, rx_cdc) = oneshot::channel();
    let cdc_fn: ScanFn = Box::new(move |tx| {
        async move {
            _ = tx_cdc.send(tx);
            std::future::pending::<()>().await
        }
        .boxed()
    });
    let base_rows = (0..params.base_rows).map({
        let dimensions = params.dimensions;
        move |id| {
            (
                primary_key(id),
                Some(vector(dimensions, id).into()),
                [].into(),
                Timestamp::from_millis(1),
            )
        }
    });
    let config = Config {
        usearch_simulator: Some(vec![params.search, params.add, RESERVE]),
        ..usearch_test_config()
    };
    let (run, _index, _db, _node_state) = setup_store_with_quantization(
        config,
        DbIndexPartitioning::Global,
        ["pk".into()],
        1,
        [("pk".to_string().into(), NativeType::BigInt)],
        Some(db_basic::scan_fn_vectors(base_rows)),
        Some(cdc_fn),
        Quantization::default(),
        NonZeroUsize::new(params.dimensions).unwrap().into(),
    )
    .await;
    let (client, _server, _config_tx) = run.await;
    let addr: SocketAddr = client.url()["http://".len()..]
        .split('/')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    wait_for_count(&client, params.base_rows, Duration::from_secs(120))
        .await
        .expect("the full scan should index the base rows");
    let tx_cdc = rx_cdc.await.unwrap();

    println!(
        "workers {}, search {:?}, add {:?}, {} base rows, inserts {:.0} rows/s",
        params.workers, params.search, params.add, params.base_rows, params.rate
    );
    println!(
        "{:>8} {:>9} {:>10} {:>10} {:>9} {:>9} {:>8} {:>9}",
        "clients", "sent/s", "applied/s", "searches/s", "mean_ms", "failed/s", "backlog", "stall_s"
    );

    let start = Instant::now();
    let (stop_at_tx, stop_at) = watch::channel(None);
    let sent = Arc::new(AtomicU64::new(0));
    let producer = spawn_producer(tx_cdc, &params, start, stop_at, Arc::clone(&sent));
    let stats = Arc::new(SearchStats::default());
    let applied = |count: usize| count - params.base_rows;

    for searchers in std::iter::once(0).chain(params.clients.iter().copied()) {
        let stop = Arc::new(AtomicBool::new(false));
        let tasks = spawn_searchers(
            &clients,
            searchers,
            addr,
            params.dimensions,
            Arc::clone(&stop),
            Arc::clone(&stats),
        );
        let (phase_start, sent0, searches0) = (
            Instant::now(),
            sent.load(Ordering::Relaxed),
            stats.snapshot(),
        );
        let applied0 = applied(count(&client).await);
        let (mut last, mut last_change, mut stall) = (applied0, phase_start, Duration::ZERO);
        while phase_start.elapsed() < params.phase {
            time::sleep(SAMPLE).await;
            let now = applied(count(&client).await);
            if now != last {
                (last, last_change) = (now, Instant::now());
            }
            stall = stall.max(last_change.elapsed());
        }
        let secs = phase_start.elapsed().as_secs_f64();
        let (done, failed, nanos) = stats.snapshot();
        let (done, failed, nanos) = (
            done - searches0.0,
            failed - searches0.1,
            nanos - searches0.2,
        );
        let scheduled = (start.elapsed().as_secs_f64() * params.rate) as usize;
        println!(
            "{searchers:>8} {:>9.0} {:>10.0} {:>10.0} {:>9.2} {:>9.0} {:>8} {:>9.1}",
            (sent.load(Ordering::Relaxed) - sent0) as f64 / secs,
            (last - applied0) as f64 / secs,
            done as f64 / secs,
            nanos as f64 / (done + failed).max(1) as f64 / 1e6,
            failed as f64 / secs,
            scheduled.saturating_sub(last),
            stall.as_secs_f64(),
        );
        stop.store(true, Ordering::Relaxed);
        for task in tasks {
            task.await.unwrap();
        }
    }

    let scheduled = (start.elapsed().as_secs_f64() * params.rate) as u64;
    stop_at_tx.send(Some(scheduled)).unwrap();
    let backlog = scheduled as usize - applied(count(&client).await);
    match wait_for_count(
        &client,
        params.base_rows + scheduled as usize,
        params.drain_timeout,
    )
    .await
    {
        Some(took) => println!(
            "drained a backlog of {backlog} rows in {:.1} s",
            took.as_secs_f64()
        ),
        None => println!(
            "did not drain in {:?}: {} of {scheduled} rows applied",
            params.drain_timeout,
            applied(count(&client).await)
        ),
    }
    producer.await.unwrap();
}
