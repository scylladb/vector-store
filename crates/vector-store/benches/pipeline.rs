/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#[path = "../tests/integration/db_basic.rs"]
#[allow(dead_code)]
mod db_basic;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use db_basic::DbBasic;
use db_basic::ScanFn;
use db_basic::Table;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream;
use httpapi::IndexStatus;
use httpclient::HttpClient;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Once;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use tap::Pipe;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use uuid::Uuid;
use vector_store::AsyncInProgress;
use vector_store::ColumnName;
use vector_store::Config;
use vector_store::ConfigReceivers;
use vector_store::Connectivity;
use vector_store::DbEmbedding;
use vector_store::DbIndexType;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::HttpServerConfig;
use vector_store::IndexMetadata;
use vector_store::PrimaryKey;
use vector_store::Quantization;
use vector_store::SpaceType;
use vector_store::Timestamp;
use vector_store::Vector;
use vector_store::db::Db;
use vector_store::node_state::NodeState;

const ENV_CONCURRENCY: &str = "BENCHES_CONCURRENCY";

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        _ = dotenvy::dotenv();

        tracing_subscriber::registry()
            .with(EnvFilter::try_new("none").unwrap())
            .with(fmt::layer().with_target(false))
            .init();
    });
}

static INIT_RAYON: Once = Once::new();

fn default_runtime() -> Runtime {
    let threads = dotenvy::var("VECTOR_STORE_THREADS")
        .ok()
        .and_then(|s| s.parse().ok());
    INIT_RAYON.call_once(|| {
        rayon::ThreadPoolBuilder::new()
            .pipe(|b| {
                if let Some(threads) = threads {
                    b.num_threads(threads)
                } else {
                    b
                }
            })
            .build_global()
            .unwrap();
    });

    tokio::runtime::Builder::new_multi_thread()
        .pipe(|mut b| {
            if let Some(threads) = threads {
                b.worker_threads(threads);
            }
            b
        })
        .enable_all()
        .build()
        .unwrap()
}

fn default_index_metadata(dimensions: usize) -> IndexMetadata {
    IndexMetadata {
        keyspace_name: "vector".into(),
        table_name: "items".into(),
        index_name: "ann".into(),
        target_column: "embedding".into(),
        index_type: DbIndexType::Global,
        filtering_columns: Arc::new(vec![]),
        dimensions: NonZeroUsize::new(dimensions).unwrap().into(),
        connectivity: Connectivity::default(),
        expansion_add: ExpansionAdd::default(),
        expansion_search: ExpansionSearch::default(),
        space_type: SpaceType::Euclidean,
        version: Uuid::new_v4().into(),
        quantization: Quantization::default(),
    }
}

async fn default_config() -> Arc<Config> {
    fn env(key: &str) -> anyhow::Result<String> {
        Ok(dotenvy::var(key)?)
    }
    Arc::new(vector_store::load_config(env).await.unwrap())
}

fn default_concurrency() -> usize {
    dotenvy::var(ENV_CONCURRENCY)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}
fn setup_table(
    db: &DbBasic,
    index: &IndexMetadata,
    primary_keys: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
) {
    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: Arc::new(primary_keys.into_iter().collect()),
            partition_key_count,
            columns: Arc::new(columns.into_iter().collect()),
            dimensions: [(index.target_column.clone(), index.dimensions)]
                .into_iter()
                .collect(),
        },
    )
    .unwrap();
}

fn setup_index(
    db: &DbBasic,
    index: IndexMetadata,
    fullscan_fn: Option<ScanFn>,
    cdc_fn: Option<ScanFn>,
) {
    db.add_index(index, fullscan_fn, cdc_fn).unwrap();
}

fn delete_index(db: &DbBasic, index: &IndexMetadata) {
    db.del_index(&index.keyspace_name, &index.index_name)
        .unwrap();
}

async fn run_vector_store(
    config: watch::Receiver<Arc<Config>>,
    node_state: mpsc::Sender<NodeState>,
    db: mpsc::Sender<Db>,
) -> (impl Sized, HttpClient) {
    let internals = vector_store::new_internals();
    let index_factory = vector_store::new_index_factory_usearch(config.clone()).unwrap();

    let addr = config.borrow().vector_store_addr;
    let (http_tx, http_rx) = watch::channel(Arc::new(HttpServerConfig { addr, tls: None }));
    let receivers = ConfigReceivers {
        config,
        http: http_rx,
    };

    let (server, addr) = vector_store::run(node_state, db, internals, index_factory, receivers)
        .await
        .unwrap();

    ((http_tx, server), HttpClient::new(addr))
}

async fn wait_until_index_is_created(
    client: &HttpClient,
    keyspace_name: &httpapi::KeyspaceName,
    index_name: &httpapi::IndexName,
) {
    while client
        .index_status(keyspace_name, index_name)
        .await
        .is_err()
    {
        task::yield_now().await;
    }
}

async fn wait_until_index_is_ready(
    client: &HttpClient,
    keyspace_name: &httpapi::KeyspaceName,
    index_name: &httpapi::IndexName,
) {
    while client
        .index_status(keyspace_name, index_name)
        .await
        .map(|status| status.status != IndexStatus::Serving)
        .unwrap_or(true)
    {
        task::yield_now().await;
    }
}

async fn wait_until_index_is_removed(
    client: &HttpClient,
    keyspace_name: &httpapi::KeyspaceName,
    index_name: &httpapi::IndexName,
) {
    while client.index_status(keyspace_name, index_name).await.is_ok() {
        task::yield_now().await;
    }
}

fn wait_until_all_tasks_finished(runtime: &Runtime) {
    while runtime.metrics().num_alive_tasks() > 1 {
        std::thread::yield_now();
    }
}

fn scan_fn_mpsc(
    mut items: mpsc::Receiver<(
        PrimaryKey,
        Option<Vector>,
        Timestamp,
        Option<AsyncInProgress>,
    )>,
) -> ScanFn {
    Box::new(move |tx| {
        async move {
            while let Some((primary_key, embedding, timestamp, in_progress)) = items.recv().await {
                let _ = tx
                    .send((
                        DbEmbedding {
                            primary_key,
                            embedding,
                            timestamp,
                        },
                        in_progress,
                    ))
                    .await;
            }
        }
        .boxed()
    })
}

fn fullscan_add(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 128;
    let index_metadata = default_index_metadata(DIMENSIONS);
    let concurrency = default_concurrency();

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_db_client, rx_db_client) = oneshot::channel();
        runtime.spawn({
            let index_metadata = index_metadata.clone();
            let notify_stop = notify_stop.clone();
            async move {
                let node_state = vector_store::new_node_state().await;
                let (db_actor, db) = db_basic::new(node_state.clone());
                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                let (_config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, node_state, db_actor).await;

                let (tx, rx) = mpsc::channel(concurrency);

                setup_index(&db, index_metadata.clone(), Some(scan_fn_mpsc(rx)), None);
                let keyspace_name = index_metadata.keyspace_name.clone().into();
                let index_name = index_metadata.index_name.clone().into();
                wait_until_index_is_created(&client, &keyspace_name, &index_name).await;

                tx_db_client.send((db, client, tx)).unwrap();
                notify_stop.notified().await;
            }
        });
        let (db, client, tx) = rx_db_client.blocking_recv().unwrap();
        (runtime, notify_stop, db, client, tx)
    });

    let next_pk = Arc::new(AtomicI64::new(0));
    group.bench_with_input(
        BenchmarkId::new("fullscan-add", concurrency),
        &concurrency,
        |b, concurrency| {
            let (runtime, _, _, _, tx) = &*fixture;
            b.to_async(runtime).iter_custom(|iters| {
                let tx = tx.clone();
                let next_pk = Arc::clone(&next_pk);
                async move {
                    run_with_concurrency(*concurrency, iters, move |it| {
                        let tx = tx.clone();
                        let pk = next_pk.fetch_add(1, Ordering::Relaxed);
                        async move {
                            let (tx_in_progress, mut rx_in_progress) = mpsc::channel(1);
                            let request = (
                                [(CqlValue::BigInt(pk))].into_iter().collect(),
                                Some(vec![it as f32; DIMENSIONS].into()),
                                Timestamp::from_unix_timestamp(0),
                                Some(tx_in_progress.into()),
                            );
                            let start = Instant::now();
                            tx.send(request).await.unwrap();
                            // wait until in-progress marker is dropped
                            while rx_in_progress.recv().await.is_some() {}
                            start.elapsed()
                        }
                    })
                    .await
                }
            });
        },
    );

    if let Some((runtime, notify_stop, db, client, _)) = LazyLock::get(&fixture) {
        delete_index(db, &index_metadata);
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            client,
            &keyspace_name,
            &index_name,
        ));
        notify_stop.notify_one();
        wait_until_all_tasks_finished(runtime);
    }
}

fn search(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 128;
    let concurrency = default_concurrency();
    let index_metadata = default_index_metadata(DIMENSIONS);
    let limit = NonZeroUsize::new(1).unwrap().into();

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_client, rx_client) = oneshot::channel();
        let (tx, rx) = mpsc::channel(runtime.metrics().num_workers() * 2);
        runtime.spawn({
            let notify_stop = notify_stop.clone();
            let index_metadata = index_metadata.clone();
            async move {
                let node_state = vector_store::new_node_state().await;
                let (db_actor, db) = db_basic::new(node_state.clone());
                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                setup_index(&db, index_metadata.clone(), Some(scan_fn_mpsc(rx)), None);
                let (_config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, node_state, db_actor).await;

                let (tx_in_progress, mut rx_in_progress) = mpsc::channel(1);
                for it in 0..100000 {
                    tx.send((
                        [(CqlValue::BigInt(it))].into_iter().collect(),
                        Some(vec![it as f32; DIMENSIONS].into()),
                        Timestamp::from_unix_timestamp(0),
                        Some(tx_in_progress.clone().into()),
                    ))
                    .await
                    .unwrap();
                }
                // wait until all in-progress markers are dropped
                drop(tx_in_progress);
                while rx_in_progress.recv().await.is_some() {}

                drop(tx);
                let keyspace_name = index_metadata.keyspace_name.clone().into();
                let index_name = index_metadata.index_name.clone().into();
                wait_until_index_is_ready(&client, &keyspace_name, &index_name).await;

                tx_client.send((db, client)).unwrap();
                notify_stop.notified().await;
            }
        });
        let (db, client) = rx_client.blocking_recv().unwrap();
        (runtime, notify_stop, db, client)
    });

    group.bench_with_input(
        BenchmarkId::new("search", concurrency),
        &concurrency,
        |b, concurrency| {
            let (runtime, _, _, client) = &*fixture;
            let index_metadata = index_metadata.clone();
            let client = client.clone();
            b.to_async(runtime).iter_custom(|iters| {
                let index_metadata = index_metadata.clone();
                let client = client.clone();
                async move {
                    run_with_concurrency(*concurrency, iters, move |it| {
                        let keyspace_name = index_metadata.keyspace_name.clone().into();
                        let index_name = index_metadata.index_name.clone().into();
                        let client = client.clone();
                        async move {
                            let vector = vec![it as f32; DIMENSIONS];
                            let start = Instant::now();
                            _ = client
                                .ann(&keyspace_name, &index_name, vector.into(), None, limit)
                                .await;
                            start.elapsed()
                        }
                    })
                    .await
                }
            })
        },
    );

    if let Some((runtime, notify_stop, db, client)) = LazyLock::get(&fixture) {
        delete_index(db, &index_metadata);
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            client,
            &keyspace_name,
            &index_name,
        ));
        notify_stop.notify_one();
        wait_until_all_tasks_finished(runtime);
    }
}

async fn run_with_concurrency<F, Fut>(concurrency: usize, iters: u64, f: F) -> Duration
where
    F: Fn(u64) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Duration> + Send + 'static,
{
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut tasks = Vec::new();
    for it in 0..iters {
        let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
        let f = f.clone();
        tasks.push(tokio::spawn(async move {
            let duration = f(it).await;
            drop(permit);
            duration
        }));
    }
    _ = semaphore.acquire_many(concurrency as u32).await.unwrap();
    stream::iter(tasks.into_iter())
        .then(|task| async move { task.await.unwrap() })
        .fold(Duration::ZERO, |acc, x| async move { acc + x })
        .await
}

criterion_group!(benches, fullscan_add, search);
criterion_main!(benches);
