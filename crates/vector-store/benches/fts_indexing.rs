/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! In-process FTS indexing benchmark.
//!
//! Feeds a synthetic corpus to the FTS indexes through a mocked full scan and reports indexing
//! throughput, commit latency, segments and memory per commit for several runtime thread counts
//! and index counts. Each configuration runs in its own process, so that the memory numbers of one
//! do not leak into the next. Without `--bench` (as under `cargo test`) it runs a small smoke
//! configuration instead.

#[path = "../tests/integration/db_basic.rs"]
#[allow(dead_code)]
mod db_basic;

use axum::http::StatusCode;
use db_basic::DbBasic;
use db_basic::ScanFn;
use db_basic::Table;
use futures::FutureExt;
use httpapi::IndexInfo;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;
use testclient::TestClient;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task;
use uuid::Uuid;
use vector_store::AsyncInProgress;
use vector_store::Config;
use vector_store::ConfigReceivers;
use vector_store::DbIndexPartitioning;
use vector_store::DbIndexedOperation;
use vector_store::DbIndexedRow;
use vector_store::DbIndexedValue;
use vector_store::HttpServerConfig;
use vector_store::HttpServerExt;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsFts;
use vector_store::NonemptyArc;
use vector_store::NonemptyBox;
use vector_store::PrimaryKey;
use vector_store::Timestamp;
use vector_store::Timestamped;
use vector_store::bench_hooks;
use vector_store::db::Db;
use vector_store::node_state::NodeState;

const ENV_THREADS: &str = "FTS_BENCH_THREADS";
const ENV_INDEXES: &str = "FTS_BENCH_INDEXES";
const ENV_DOCS: &str = "FTS_BENCH_DOCS";
const ENV_SEED: &str = "FTS_BENCH_SEED";

const DEFAULT_THREADS: &[usize] = &[1, 2, 4, 8];
const DEFAULT_INDEXES: &[usize] = &[1, 100];
const DEFAULT_SEED: u64 = 42;
// Enough documents per index for several threshold commits, so the timer commits do not dominate.
const DEFAULT_DOCS_PER_INDEX: usize = 40_000;
const DEFAULT_MIN_DOCS: usize = 1_000_000;

const VOCABULARY: usize = 100_000;
const WORD_LEN: std::ops::RangeInclusive<usize> = 3..=10;
const DOC_LEN: std::ops::RangeInclusive<usize> = 50..=150;
// Distinct documents generated up front and cycled, to keep the corpus out of the timed path.
const POOL: usize = 100_000;
const FEED_CHANNEL: usize = 64;

const SMOKE: Params = Params {
    threads: 2,
    indexes: 2,
    docs: 2_000,
    seed: DEFAULT_SEED,
};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone, Copy)]
struct Params {
    threads: usize,
    indexes: usize,
    docs: usize,
    seed: u64,
}

impl Params {
    fn args(&self) -> [String; 4] {
        [
            self.threads.to_string(),
            self.indexes.to_string(),
            self.docs.to_string(),
            self.seed.to_string(),
        ]
    }

    fn parse(args: &[String]) -> Self {
        let arg = |i: usize| -> u64 { args[i].parse().expect("invalid benchmark parameter") };
        Self {
            threads: arg(0) as usize,
            indexes: arg(1) as usize,
            docs: arg(2) as usize,
            seed: arg(3),
        }
    }
}

struct Sample {
    lock_wait: Duration,
    duration: Duration,
    operations: usize,
    segments: usize,
    rss_kib: Option<u64>,
}

struct Report {
    params: Params,
    elapsed: Duration,
    samples: Vec<Sample>,
    rss_base_kib: Option<u64>,
    rss_hwm_kib: Option<u64>,
}

fn main() {
    init_tracing();
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|arg| arg == "--child") {
        let report = run(Params::parse(&args[pos + 1..]));
        println!("{}", report.row());
        return;
    }
    if args.iter().any(|arg| arg == "--bench") {
        bench();
        return;
    }
    smoke();
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    _ = dotenvy::dotenv();
    tracing_subscriber::registry()
        .with(EnvFilter::try_new("none").unwrap())
        .with(fmt::layer().with_target(false))
        .init();
}

fn smoke() {
    let report = run(SMOKE);
    println!("{}", Report::header());
    println!("{}", report.row());
    assert!(!report.samples.is_empty(), "no commit observed");
}

fn bench() {
    let threads = env_list(ENV_THREADS, DEFAULT_THREADS);
    let indexes = env_list(ENV_INDEXES, DEFAULT_INDEXES);
    let docs: Option<usize> = env_parse(ENV_DOCS);
    let seed = env_parse(ENV_SEED).unwrap_or(DEFAULT_SEED);

    let sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let cpus = std::thread::available_parallelism().map_or(0, NonZeroUsize::get);
    println!(
        "fts_indexing: git {sha}, {cpus} cpus, seed {seed}, vocabulary {VOCABULARY} (zipf 1.0), \
         {}-{} tokens per document",
        DOC_LEN.start(),
        DOC_LEN.end()
    );
    println!(
        "docs/s: documents until all are searchable; commit: writer commit after taking its lock; \
         segments: searchable segments right after a commit, racing with merges"
    );
    println!("{}", Report::header());

    let exe = std::env::current_exe().unwrap();
    for &threads in &threads {
        for &indexes in &indexes {
            let params = Params {
                threads,
                indexes,
                docs: docs.unwrap_or((indexes * DEFAULT_DOCS_PER_INDEX).max(DEFAULT_MIN_DOCS)),
                seed,
            };
            let status = Command::new(&exe)
                .arg("--child")
                .args(params.args())
                .status()
                .unwrap();
            assert!(status.success(), "benchmark configuration failed");
        }
    }
}

fn env_parse<T: std::str::FromStr>(key: &str) -> Option<T> {
    dotenvy::var(key).ok().and_then(|value| value.parse().ok())
}

fn env_list(key: &str, default: &[usize]) -> Vec<usize> {
    dotenvy::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|item| item.trim().parse().expect("invalid list item"))
                .collect()
        })
        .unwrap_or_else(|| default.to_vec())
}

fn run(params: Params) -> Report {
    vector_store::block_on(Some(params.threads), async move || run_async(params).await)
}

async fn run_async(params: Params) -> Report {
    let samples = Arc::new(Mutex::new(Vec::new()));
    let observer_set = bench_hooks::observe_commits({
        let samples = Arc::clone(&samples);
        move |commit| {
            let sample = Sample {
                lock_wait: commit.lock_wait,
                duration: commit.duration,
                operations: commit.operations,
                segments: commit.segments,
                rss_kib: proc_status_kib("VmRSS:"),
            };
            samples.lock().unwrap().push(sample);
        }
    });
    assert!(observer_set, "the commit observer is set once per process");

    let per_index = params.docs / params.indexes;
    let pool = Arc::new(Corpus::new(params.seed).documents(per_index.min(POOL)));

    let node_state = vector_store::new_node_state().await;
    let (db_actor, db) = db_basic::new(node_state.clone());
    let feeders: Vec<_> = (0..params.indexes).map(|i| setup_index(&db, i)).collect();
    let config = Config {
        vector_store_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (_server, client) = run_vector_store(config, node_state, db_actor).await;
    for (index, _) in &feeders {
        wait_until_index_is_created(&client, index).await;
    }

    let rss_base_kib = proc_status_kib("VmRSS:");
    let (done_tx, mut done_rx) = mpsc::channel::<()>(1);
    let start = Instant::now();
    let mut indexes = Vec::new();
    for (i, (index, tx)) in feeders.into_iter().enumerate() {
        indexes.push(index);
        let pool = Arc::clone(&pool);
        let done_tx = done_tx.clone();
        tokio::spawn(async move {
            for n in 0..per_index {
                let id = i * per_index + n;
                let item = (
                    [CqlValue::BigInt(id as i64)].into_iter().collect(),
                    pool[id % pool.len()].clone(),
                    AsyncInProgress::fullscan(done_tx.clone()),
                );
                tx.send(item).await.unwrap();
            }
        });
    }
    drop(done_tx);
    // Every guard is dropped once its document is searchable.
    while done_rx.recv().await.is_some() {}
    let elapsed = start.elapsed();

    wait_until_counted(&client, &indexes, per_index * params.indexes).await;

    let samples = std::mem::take(&mut *samples.lock().unwrap());
    Report {
        params,
        elapsed,
        samples,
        rss_base_kib,
        rss_hwm_kib: proc_status_kib("VmHWM:"),
    }
}

type Feed = (PrimaryKey, String, AsyncInProgress);

fn setup_index(db: &DbBasic, i: usize) -> (IndexMetadata, mpsc::Sender<Feed>) {
    let index = IndexMetadata {
        keyspace_name: "fts_bench".into(),
        table_name: format!("docs_{i}").into(),
        index_name: format!("fts_{i}").into(),
        primary_key_columns: NonemptyArc::new(["pk"]).unwrap(),
        partition_key_count: NonZeroUsize::new(1).unwrap(),
        target_columns: NonemptyArc::new(["content"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new([]),
        alternator_attribute_types: Default::default(),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Fts(IndexOptionsFts::default()),
    };
    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: index.primary_key_columns.clone(),
            partition_key_count: 1,
            columns: Arc::new(
                [
                    ("pk".into(), NativeType::BigInt),
                    ("content".into(), NativeType::Text),
                ]
                .into_iter()
                .collect(),
            ),
            dimensions: HashMap::new(),
        },
    )
    .unwrap();
    let (tx, rx) = mpsc::channel(FEED_CHANNEL);
    db.add_index(index.clone(), Some(scan_fn_feed(rx)), None)
        .unwrap();
    (index, tx)
}

fn scan_fn_feed(mut items: mpsc::Receiver<Feed>) -> ScanFn {
    Box::new(move |tx| {
        async move {
            while let Some((primary_key, document, in_progress)) = items.recv().await {
                let row = DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Upsert(
                        NonemptyBox::new([Timestamped::new(
                            Timestamp::from_millis(1),
                            Some(DbIndexedValue::Document(document)),
                        )])
                        .unwrap(),
                    ),
                };
                _ = tx.send((row, in_progress)).await;
            }
        }
        .boxed()
    })
}

async fn run_vector_store(
    config: Config,
    node_state: mpsc::Sender<NodeState>,
    db: mpsc::Sender<Db>,
) -> (impl Sized, TestClient) {
    let addr = config.vector_store_addr;
    let (_config_tx, config_rx) = watch::channel(Arc::new(config));
    let (http_tx, http_rx) = watch::channel(Some(Arc::new(HttpServerConfig { addr, tls: None })));
    let (mtls_tx, mtls_http_rx) = watch::channel(None);
    let receivers = ConfigReceivers {
        config: config_rx,
        http: http_rx,
        mtls_http: mtls_http_rx,
    };
    let (server, _mtls) = vector_store::run(Some(node_state), Some(db), receivers)
        .await
        .unwrap();
    let client = TestClient::new(server.router().await.unwrap());
    ((_config_tx, http_tx, mtls_tx, server), client)
}

async fn wait_until_index_is_created(client: &TestClient, index: &IndexMetadata) {
    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();
    while client
        .index_status(&keyspace_name, &index_name)
        .await
        .status_code()
        != StatusCode::OK
    {
        task::yield_now().await;
    }
}

/// Fails the run if any document was dropped instead of indexed, e.g. under memory pressure.
async fn wait_until_counted(client: &TestClient, indexes: &[IndexMetadata], expected: usize) {
    const TIMEOUT: Duration = Duration::from_secs(10);
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let mut count = 0;
        for index in indexes {
            let response = client
                .index_status(
                    &index.keyspace_name.clone().into(),
                    &index.index_name.clone().into(),
                )
                .await;
            count += response.json::<IndexInfo>().count;
        }
        if count == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "indexed {count} documents, expected {expected}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Reads a `kB` field of `/proc/self/status`; `None` outside Linux.
fn proc_status_kib(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .ok()
}

/// A deterministic corpus: Zipf-distributed words over a fixed vocabulary of pseudo-words.
struct Corpus {
    rng: SplitMix64,
    words: Vec<String>,
    // Cumulative Zipf weights, for sampling by binary search.
    cdf: Vec<f64>,
}

impl Corpus {
    fn new(seed: u64) -> Self {
        let mut rng = SplitMix64(seed);
        let words = (0..VOCABULARY)
            .map(|_| {
                let len = rng.in_range(&WORD_LEN);
                (0..len)
                    .map(|_| char::from(b'a' + rng.below(26) as u8))
                    .collect()
            })
            .collect();
        let cdf = (1..=VOCABULARY)
            .scan(0.0, |sum, rank| {
                *sum += 1.0 / rank as f64;
                Some(*sum)
            })
            .collect();
        Self { rng, words, cdf }
    }

    fn documents(mut self, count: usize) -> Vec<String> {
        (0..count).map(|_| self.document()).collect()
    }

    fn document(&mut self) -> String {
        let total = *self.cdf.last().unwrap();
        let len = self.rng.in_range(&DOC_LEN);
        let mut document = String::new();
        for _ in 0..len {
            let target = self.rng.unit() * total;
            let rank = self
                .cdf
                .partition_point(|&sum| sum < target)
                .min(VOCABULARY - 1);
            if !document.is_empty() {
                document.push(' ');
            }
            document.push_str(&self.words[rank]);
        }
        document
    }
}

/// SplitMix64, to keep the corpus independent of the `rand` version.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn in_range(&mut self, range: &std::ops::RangeInclusive<usize>) -> usize {
        range.start() + self.below((range.end() - range.start() + 1) as u64) as usize
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

impl Report {
    fn header() -> String {
        format!(
            "{:>7} {:>7} {:>9} {:>9} {:>7} {:>10} {:>27} {:>13} {:>11} {:>23}",
            "threads",
            "indexes",
            "docs",
            "docs/s",
            "commits",
            "ops/commit",
            "commit ms avg/p50/p99/max",
            "lock ms avg/max",
            "segs avg/max",
            "rss MiB base/avg/max/hwm",
        )
    }

    fn row(&self) -> String {
        let docs = self.params.docs / self.params.indexes * self.params.indexes;
        let docs_per_sec = docs as f64 / self.elapsed.as_secs_f64();
        let commits = self.samples.len();
        let avg = |sum: f64| {
            if commits == 0 {
                0.0
            } else {
                sum / commits as f64
            }
        };
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;

        let mut durations: Vec<f64> = self.samples.iter().map(|s| ms(s.duration)).collect();
        durations.sort_by(f64::total_cmp);
        let percentile = |p: f64| {
            if durations.is_empty() {
                0.0
            } else {
                durations[((durations.len() - 1) as f64 * p).round() as usize]
            }
        };
        let commit = format!(
            "{:.1}/{:.1}/{:.1}/{:.1}",
            avg(durations.iter().sum()),
            percentile(0.5),
            percentile(0.99),
            percentile(1.0),
        );
        let lock = format!(
            "{:.1}/{:.1}",
            avg(self.samples.iter().map(|s| ms(s.lock_wait)).sum()),
            self.samples
                .iter()
                .map(|s| ms(s.lock_wait))
                .fold(0.0, f64::max),
        );
        let ops = avg(self.samples.iter().map(|s| s.operations as f64).sum());
        let segments = format!(
            "{:.1}/{}",
            avg(self.samples.iter().map(|s| s.segments as f64).sum()),
            self.samples.iter().map(|s| s.segments).max().unwrap_or(0),
        );

        let mib = |kib: Option<u64>| kib.map_or("n/a".to_string(), |kib| (kib / 1024).to_string());
        let rss: Vec<u64> = self.samples.iter().filter_map(|s| s.rss_kib).collect();
        let rss_avg = (!rss.is_empty()).then(|| rss.iter().sum::<u64>() / rss.len() as u64);
        let rss = format!(
            "{}/{}/{}/{}",
            mib(self.rss_base_kib),
            mib(rss_avg),
            mib(rss.iter().copied().max()),
            mib(self.rss_hwm_kib),
        );

        format!(
            "{:>7} {:>7} {:>9} {:>9.0} {:>7} {:>10.0} {:>27} {:>13} {:>11} {:>23}",
            self.params.threads,
            self.params.indexes,
            docs,
            docs_per_sec,
            commits,
            ops,
            commit,
            lock,
            segments,
            rss,
        )
    }
}
