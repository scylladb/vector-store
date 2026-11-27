/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! FTS scenarios of the pipeline benchmark: real Tantivy indexes fed through the range scan and
//! CDC paths, measured until the documents are searchable.

use super::corpus;
use super::corpus::Corpus;
use super::db_basic;
use super::db_basic::CdcRow;
use super::db_basic::DbBasic;
use super::db_basic::Index;
use super::db_basic::RangeScanFn;
use super::db_basic::TOKEN_MIN;
use super::db_basic::Table;
use super::default_concurrency;
use super::default_config;
use super::default_runtime;
use super::init;
use super::run_vector_store;
use super::wait_until_all_tasks_finished;
use super::wait_until_index_is_created;
use super::wait_until_index_is_ready;
use super::wait_until_index_is_removed;
use axum::http::StatusCode;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::Throughput;
use httpapi::IndexInfo;
use httpapi::PostIndexBm25Response;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::routing::Token;
use scylla::value::CqlValue;
use scylla_cdc::consumer::OperationType;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Once;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use testclient::TestClient;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;
use vector_store::Config;
use vector_store::DbIndexPartitioning;
use vector_store::DbRow;
use vector_store::DbValue;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsFts;
use vector_store::NonemptyArc;

/// Upper bound for any wait on the pipeline, so a lost document fails the run instead of hanging it.
const WAIT_TIMEOUT: Duration = Duration::from_secs(120);

const SEARCH_LIMIT: usize = 10;

/// Zipf ranks of the query terms: a frequent word, a word present in a few documents out of
/// a thousand, and a pair of mid-frequency words.
const COMMON_RANK: usize = 0;
const RARE_RANK: usize = 1_000;
const PAIR_RANKS: (usize, usize) = (10, 100);

struct Sizes {
    build_docs: usize,
    search_docs_per_index: usize,
    index_counts: Vec<usize>,
}

const ENV_FTS_INDEXES: &str = "BENCHES_FTS_INDEXES";
const ENV_FTS_INSERT_RATE: &str = "BENCHES_FTS_INSERT_RATE";

/// Documents per second sent through CDC while searching; unbounded when unset,
/// but then a faster version also searches a bigger index.
fn insert_rate() -> Option<f64> {
    dotenvy::var(ENV_FTS_INSERT_RATE).ok().map(|rate| {
        rate.parse()
            .unwrap_or_else(|_| panic!("{ENV_FTS_INSERT_RATE}: bad rate {rate}"))
    })
}

/// The index counts to run, a comma-separated list; larger counts are opt-in because an
/// unmodified Tantivy writer needs over 20 GB to build a hundred indexes.
fn index_counts() -> Vec<usize> {
    dotenvy::var(ENV_FTS_INDEXES)
        .ok()
        .map(|counts| {
            counts
                .split(',')
                .map(|count| {
                    count
                        .trim()
                        .parse()
                        .unwrap_or_else(|_| panic!("{ENV_FTS_INDEXES}: bad index count {count}"))
                })
                .collect()
        })
        .unwrap_or_else(|| vec![1, 10])
}

/// Debug builds only run as the CI smoke test, so they get small sizes.
fn sizes() -> Sizes {
    Sizes {
        build_docs: if cfg!(debug_assertions) {
            1_000
        } else {
            500_000
        },
        search_docs_per_index: 1_000,
        index_counts: index_counts(),
    }
}

fn print_setup() {
    static PRINTED: Once = Once::new();
    PRINTED.call_once(|| {
        eprintln!(
            "fts: VECTOR_STORE_THREADS={} BENCHES_CONCURRENCY={} {ENV_FTS_INDEXES}={:?} \
             {ENV_FTS_INSERT_RATE}={:?}",
            dotenvy::var("VECTOR_STORE_THREADS").unwrap_or_else(|_| "default".into()),
            default_concurrency(),
            index_counts(),
            insert_rate(),
        );
    });
}

fn status_field(key: &str) -> Option<String> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .map(|value| value.trim().to_string())
}

/// The thread count of the process, without the sampler's own thread.
fn thread_count() -> Option<usize> {
    status_field("Threads:")?
        .parse::<usize>()
        .ok()
        .map(|threads| threads - 1)
}

/// Samples the thread count of the process until dropped, so threads that live only while
/// documents are indexed are seen too.
struct ThreadSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ThreadSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicUsize::new(0));
        let thread = std::thread::spawn({
            let stop = Arc::clone(&stop);
            let peak = Arc::clone(&peak);
            move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Some(threads) = thread_count() {
                        peak.fetch_max(threads, Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        });
        Self {
            stop,
            peak,
            thread: Some(thread),
        }
    }

    /// The peak thread count since the start or the previous call.
    fn take_peak(&self) -> usize {
        self.peak.swap(0, Ordering::Relaxed)
    }
}

impl Drop for ThreadSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

/// Prints the thread count and memory of the process.
fn print_footprint(label: &str, sampler: &ThreadSampler) {
    let field = |key: &str| status_field(key).unwrap_or_else(|| "n/a".into());
    eprintln!(
        "fts footprint [{label}]: threads={} peak_threads={} rss={} peak={}",
        thread_count().map_or_else(|| "n/a".into(), |threads| threads.to_string()),
        sampler.take_peak(),
        field("VmRSS:"),
        field("VmHWM:"),
    );
}

fn fts_index_metadata(id: usize) -> IndexMetadata {
    IndexMetadata {
        keyspace_name: "fts".into(),
        table_name: format!("docs_{id}").into(),
        index_name: format!("fts_{id}").into(),
        primary_key_columns: NonemptyArc::new(["id"]).unwrap(),
        partition_key_count: NonZeroUsize::new(1).unwrap(),
        target_columns: NonemptyArc::new(["content"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new([]),
        alternator_attribute_types: Default::default(),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Fts(IndexOptionsFts::default()),
    }
}

fn add_fts_table(db: &DbBasic, index: &IndexMetadata) {
    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: index.primary_key_columns.clone(),
            partition_key_count: 1,
            columns: Arc::new(
                [
                    ("id".into(), ColumnType::Native(NativeType::BigInt)),
                    ("content".into(), ColumnType::Native(NativeType::Text)),
                ]
                .into_iter()
                .collect(),
            ),
            dimensions: Default::default(),
        },
    )
    .unwrap();
}

/// A range scan that streams the documents sent to the returned sender and ends when it is
/// dropped.
fn range_scan_fn_documents_mpsc() -> (RangeScanFn, mpsc::Sender<(i64, String)>) {
    let (tx, rx) = mpsc::channel(1024);
    let mut rx = Some(rx);
    let scan: RangeScanFn = Box::new(move || {
        let (tx_rows, rx_rows) = mpsc::channel(1);
        let Some(mut rx) = rx.take() else {
            return rx_rows;
        };
        tokio::spawn(async move {
            while let Some((pk, document)) = rx.recv().await {
                let row = DbRow {
                    columns: vec![
                        Some(DbValue::Value(CqlValue::BigInt(pk))),
                        Some(DbValue::Value(CqlValue::Text(document))),
                        Some(DbValue::Value(CqlValue::BigInt(1))),
                    ],
                };
                if tx_rows.send(Ok(row)).await.is_err() {
                    break;
                }
            }
        });
        rx_rows
    });
    (scan, tx)
}

/// An FTS index whose full scan streams the documents sent through `tx` until it is dropped.
struct FtsIndex {
    metadata: IndexMetadata,
    tx: Option<mpsc::Sender<(i64, String)>>,
}

impl FtsIndex {
    fn keyspace_name(&self) -> httpapi::KeyspaceName {
        self.metadata.keyspace_name.clone().into()
    }

    fn index_name(&self) -> httpapi::IndexName {
        self.metadata.index_name.clone().into()
    }

    /// Ends the full scan, after which the index serves searches.
    async fn finish_scan(&mut self, client: &TestClient) {
        self.tx = None;
        wait_until_index_is_ready(client, &self.keyspace_name(), &self.index_name()).await;
    }
}

/// Hands out unique index ids and primary keys across iterations.
#[derive(Default)]
struct Ids {
    index: AtomicUsize,
    pk: AtomicI64,
}

impl Ids {
    fn next_pk(&self) -> i64 {
        self.pk.fetch_add(1, Ordering::Relaxed)
    }
}

/// Creates `count` FTS indexes fed through range scans and waits until Vector Store knows them.
async fn create_fts_indexes(
    db: &DbBasic,
    client: &TestClient,
    ids: &Ids,
    count: usize,
) -> Vec<FtsIndex> {
    let indexes = (0..count)
        .map(|_| {
            let metadata = fts_index_metadata(ids.index.fetch_add(1, Ordering::Relaxed));
            add_fts_table(db, &metadata);
            let (scan, tx) = range_scan_fn_documents_mpsc();
            db.add_index(Index::new(metadata.clone()).with_range_scan(Token::new(TOKEN_MIN), scan))
                .unwrap();
            FtsIndex {
                metadata,
                tx: Some(tx),
            }
        })
        .collect::<Vec<_>>();
    for index in &indexes {
        wait_until_index_is_created(client, &index.keyspace_name(), &index.index_name()).await;
    }
    indexes
}

/// Deletes the indexes together and only then waits for them, as Vector Store notices a removed
/// index only on its next periodic check.
async fn delete_fts_indexes(db: &DbBasic, client: &TestClient, indexes: Vec<FtsIndex>) {
    for index in &indexes {
        db.del_index(&index.metadata.keyspace_name, &index.metadata.index_name)
            .unwrap();
    }
    for index in &indexes {
        wait_until_index_is_removed(client, &index.keyspace_name(), &index.index_name()).await;
    }
}

/// Sends the documents round-robin over the indexes from `concurrency` tasks and returns how many
/// documents each index got.
async fn send_documents(
    indexes: &[FtsIndex],
    documents: Vec<(i64, String)>,
    concurrency: usize,
) -> Vec<usize> {
    let mut sent = vec![0; indexes.len()];
    let mut batches = vec![Vec::new(); concurrency];
    for (i, document) in documents.into_iter().enumerate() {
        sent[i % indexes.len()] += 1;
        let tx = indexes[i % indexes.len()].tx.clone().unwrap();
        batches[i % concurrency].push((tx, document));
    }
    let tasks = batches
        .into_iter()
        .map(|batch| {
            tokio::spawn(async move {
                for (tx, document) in batch {
                    tx.send(document).await.unwrap();
                }
            })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        task.await.unwrap();
    }
    sent
}

/// Waits until the documents of a batch are searchable: every row the pipeline takes gets a clone
/// of one sender, which is dropped with the in-progress marker of that row, so the receiver closes
/// once the last marker of the batch is gone.
struct BatchGuard {
    tx: Arc<Mutex<Option<mpsc::Sender<()>>>>,
    taken: Arc<AtomicUsize>,
    rx: mpsc::Receiver<()>,
}

impl BatchGuard {
    fn new(db: &DbBasic) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let tx = Arc::new(Mutex::new(Some(tx)));
        let taken = Arc::new(AtomicUsize::new(0));
        db.set_make_in_progress_guard(Some(Box::new({
            let tx = Arc::clone(&tx);
            let taken = Arc::clone(&taken);
            move |_| {
                let tx = tx.lock().unwrap().clone();
                if tx.is_some() {
                    taken.fetch_add(1, Ordering::Relaxed);
                }
                tx
            }
        })));
        Self { tx, taken, rx }
    }

    /// Waits until `expected` rows have entered the pipeline and all of them are searchable.
    async fn wait(mut self, db: &DbBasic, expected: usize) {
        tokio::time::timeout(WAIT_TIMEOUT, async {
            while self.taken.load(Ordering::Relaxed) < expected {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            db.set_make_in_progress_guard(None);
            self.tx.lock().unwrap().take();
            while self.rx.recv().await.is_some() {}
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "fts: {} of {expected} documents entered the pipeline, not all became searchable in {WAIT_TIMEOUT:?}",
                self.taken.load(Ordering::Relaxed)
            )
        });
    }
}

/// Asserts that the index holds `expected` searchable documents, waiting for late reloads.
async fn assert_count(client: &TestClient, index: &FtsIndex, expected: usize) {
    let mut count = 0;
    let result = tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let response = client
                .index_status(&index.keyspace_name(), &index.index_name())
                .await;
            if response.status_code() == StatusCode::OK {
                count = response.json::<IndexInfo>().count;
                if count == expected {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "fts: index {} holds {count} documents, expected {expected}",
        index.metadata.index_name
    );
}

fn with_pks(ids: &Ids, documents: &[String]) -> Vec<(i64, String)> {
    documents
        .iter()
        .map(|document| (ids.next_pk(), document.clone()))
        .collect()
}

/// A running Vector Store over `DbBasic`, kept alive for one benchmark.
struct Fixture {
    runtime: tokio::runtime::Runtime,
    db: DbBasic,
    client: Arc<TestClient>,
    _server: Box<dyn Send>,
    _config_tx: watch::Sender<Arc<Config>>,
    sampler: ThreadSampler,
}

impl Fixture {
    fn new() -> Self {
        let sampler = ThreadSampler::start();
        let runtime = default_runtime();
        let (db, client, server, config_tx) = runtime.block_on(async {
            let db = db_basic::new();
            let (config_tx, config_rx) = watch::channel(default_config().await);
            let (server, client) = run_vector_store(config_rx, db.clone()).await;
            (db, client, Box::new(server) as Box<dyn Send>, config_tx)
        });
        Self {
            runtime,
            db,
            client,
            _server: server,
            _config_tx: config_tx,
            sampler,
        }
    }

    /// Creates `count` indexes, fills each with `docs_per_index` documents and lets them serve.
    fn serving_indexes(&self, ids: &Ids, count: usize, documents: &[String]) -> Vec<FtsIndex> {
        self.runtime.block_on(async {
            let mut indexes = create_fts_indexes(&self.db, &self.client, ids, count).await;
            let guard = BatchGuard::new(&self.db);
            // Each document once per index, in an order that the round robin hands every index
            // the whole set.
            let documents = documents
                .iter()
                .flat_map(|document| std::iter::repeat_n(document, count))
                .map(|document| (ids.next_pk(), document.clone()))
                .collect::<Vec<_>>();
            let total = documents.len();
            send_documents(&indexes, documents, default_concurrency()).await;
            guard.wait(&self.db, total).await;
            for index in &mut indexes {
                index.finish_scan(&self.client).await;
                assert_count(&self.client, index, total / count).await;
            }
            indexes
        })
    }

    fn teardown(self, indexes: Vec<FtsIndex>) {
        let Self {
            runtime,
            db,
            client,
            _server,
            _config_tx,
            sampler,
        } = self;
        runtime.block_on(delete_fts_indexes(&db, &client, indexes));
        drop(client);
        drop(_server);
        drop(_config_tx);
        wait_until_all_tasks_finished(&runtime);
        drop(sampler);
    }
}

/// Builds fresh indexes from a full scan of `build_docs` documents spread over N indexes and
/// reports documents per second until all of them are searchable.
pub(crate) fn fts_fullscan_build(c: &mut Criterion) {
    init();
    print_setup();

    let sizes = sizes();
    let concurrency = default_concurrency();
    let documents = Arc::new(Corpus::new(corpus::SEED).documents(sizes.build_docs));

    let mut group = c.benchmark_group("pipeline");
    group
        .sample_size(10)
        .sampling_mode(SamplingMode::Flat)
        .throughput(Throughput::Elements(sizes.build_docs as u64));

    for &count in &sizes.index_counts {
        // Created on the first call, so a benchmark left out by the filter costs nothing.
        let mut fixture = None;
        let ids = Ids::default();
        let mut footprint_printed = false;
        group.bench_with_input(
            BenchmarkId::new(format!("fts-fullscan-build-{count}"), concurrency),
            &concurrency,
            |b, &concurrency| {
                let fixture = fixture.get_or_insert_with(Fixture::new);
                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        elapsed += fixture.runtime.block_on(async {
                            let indexes =
                                create_fts_indexes(&fixture.db, &fixture.client, &ids, count).await;
                            let batch = with_pks(&ids, &documents);
                            let guard = BatchGuard::new(&fixture.db);

                            let start = Instant::now();
                            let sent = send_documents(&indexes, batch, concurrency).await;
                            guard.wait(&fixture.db, documents.len()).await;
                            let elapsed = start.elapsed();

                            for (index, sent) in indexes.iter().zip(sent) {
                                assert_count(&fixture.client, index, sent).await;
                            }
                            if !footprint_printed {
                                print_footprint(
                                    &format!("after a build of {count} indexes"),
                                    &fixture.sampler,
                                );
                                footprint_printed = true;
                            }
                            delete_fts_indexes(&fixture.db, &fixture.client, indexes).await;
                            elapsed
                        });
                    }
                    elapsed
                });
            },
        );
        if let Some(fixture) = fixture {
            fixture.teardown(Vec::new());
        }
    }
}

/// Calls slower than this count as stalls, which the 99th percentile hides.
const SLOW_CALL: Duration = Duration::from_millis(50);

/// Latencies of single calls, for the percentiles Criterion does not report.
#[derive(Clone, Default)]
struct Latencies(Arc<Mutex<Vec<Duration>>>);

impl Latencies {
    fn push(&self, latency: Duration) {
        self.0.lock().unwrap().push(latency);
    }

    fn print(&self, name: &str) {
        let mut latencies = std::mem::take(&mut *self.0.lock().unwrap());
        if latencies.is_empty() {
            return;
        }
        latencies.sort();
        let percentile = |p: usize| latencies[(latencies.len() - 1) * p / 100];
        let slow = latencies.len() - latencies.partition_point(|&l| l <= SLOW_CALL);
        eprintln!(
            "fts latency [{name}]: p50={:?} p99={:?} max={:?} over {SLOW_CALL:?}={slow} calls={}",
            percentile(50),
            percentile(99),
            latencies[latencies.len() - 1],
            latencies.len()
        );
    }
}

/// The three query kinds the search scenarios rotate through.
fn queries() -> [String; 3] {
    let corpus = Corpus::new(corpus::SEED);
    [
        corpus.word(COMMON_RANK).to_string(),
        corpus.word(RARE_RANK).to_string(),
        format!(
            "{} {}",
            corpus.word(PAIR_RANKS.0),
            corpus.word(PAIR_RANKS.1)
        ),
    ]
}

/// Asserts once that every query finds something, so no query measures a dictionary miss.
fn assert_queries_hit(fixture: &Fixture, index: &FtsIndex, queries: &[String]) {
    fixture.runtime.block_on(async {
        for query in queries {
            let response = fixture
                .client
                .bm25(
                    &index.keyspace_name(),
                    &index.index_name(),
                    query.clone(),
                    NonZeroUsize::new(SEARCH_LIMIT).unwrap().into(),
                )
                .await;
            assert_eq!(response.status_code(), StatusCode::OK, "fts: query {query}");
            assert!(
                !response.json::<PostIndexBm25Response>().scores.is_empty(),
                "fts: query {query} finds nothing"
            );
        }
    });
}

/// What a search scenario needs: a running fixture, the indexes to query, and the work left to
/// do once the measurement ends.
struct SearchSetup {
    fixture: Fixture,
    targets: Targets,
    finish: Box<dyn FnOnce(Fixture)>,
}

/// The indexes a search scenario queries, as keyspace and index names.
type Targets = Arc<Vec<(httpapi::KeyspaceName, httpapi::IndexName)>>;

/// Measures BM25 calls with `concurrency` parallel callers, the query and the index picked by
/// the iteration number. The setup runs only when the filter selects the benchmark.
fn bench_search(
    c: &mut Criterion,
    name: String,
    queries: [String; 3],
    setup: impl FnOnce(&[String; 3]) -> SearchSetup,
) {
    let concurrency = default_concurrency();
    let latencies = Latencies::default();
    let queries = Arc::new(queries);
    let mut setup = Some(setup);
    let mut state: Option<SearchSetup> = None;

    let mut group = c.benchmark_group("pipeline");
    group.throughput(Throughput::Elements(concurrency as u64));
    group.bench_with_input(
        BenchmarkId::new(&name, concurrency),
        &concurrency,
        |b, &concurrency| {
            let SearchSetup {
                fixture, targets, ..
            } = state.get_or_insert_with(|| setup.take().unwrap()(&queries));
            b.to_async(&fixture.runtime).iter_custom(|iters| {
                let client = Arc::clone(&fixture.client);
                let targets = Arc::clone(targets);
                let queries = Arc::clone(&queries);
                let latencies = latencies.clone();
                async move {
                    super::run_with_concurrency(concurrency, iters, move |_, it| {
                        let client = Arc::clone(&client);
                        let targets = Arc::clone(&targets);
                        let queries = Arc::clone(&queries);
                        let latencies = latencies.clone();
                        async move {
                            let (keyspace_name, index_name) = &targets[it as usize % targets.len()];
                            let query = queries[it as usize % queries.len()].clone();
                            let start = Instant::now();
                            let response = client
                                .bm25(
                                    keyspace_name,
                                    index_name,
                                    query,
                                    NonZeroUsize::new(SEARCH_LIMIT).unwrap().into(),
                                )
                                .await;
                            let elapsed = start.elapsed();
                            assert_eq!(response.status_code(), StatusCode::OK);
                            latencies.push(elapsed);
                            elapsed
                        }
                    })
                    .await
                }
            });
        },
    );
    group.finish();
    if let Some(SearchSetup {
        fixture, finish, ..
    }) = state
    {
        latencies.print(&name);
        finish(fixture);
    }
}

fn targets(indexes: &[FtsIndex]) -> Targets {
    Arc::new(
        indexes
            .iter()
            .map(|index| (index.keyspace_name(), index.index_name()))
            .collect(),
    )
}

/// BM25 latency over N serving indexes, each call to one of them.
pub(crate) fn fts_search(c: &mut Criterion) {
    init();
    print_setup();

    let sizes = sizes();
    let documents = Corpus::new(corpus::SEED).documents(sizes.search_docs_per_index);

    for &count in &sizes.index_counts {
        bench_search(c, format!("fts-search-{count}"), queries(), |queries| {
            let fixture = Fixture::new();
            let ids = Ids::default();
            let indexes = fixture.serving_indexes(&ids, count, &documents);
            print_footprint(&format!("{count} idle indexes"), &fixture.sampler);
            assert_queries_hit(&fixture, &indexes[0], queries);
            SearchSetup {
                targets: targets(&indexes),
                fixture,
                finish: Box::new(move |fixture| fixture.teardown(indexes)),
            }
        });
    }
}

/// A background task that writes documents until stopped and reports how many it wrote.
struct Writer {
    stop: Arc<AtomicBool>,
    task: JoinHandle<usize>,
}

impl Writer {
    fn spawn<F, Fut>(
        runtime: &tokio::runtime::Runtime,
        documents: Arc<Vec<String>>,
        write: F,
    ) -> Self
    where
        F: Fn(usize, String) -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let task = runtime.spawn({
            let stop = Arc::clone(&stop);
            async move {
                let mut written = 0;
                while !stop.load(Ordering::Relaxed) {
                    let document = documents[written % documents.len()].clone();
                    if !write(written, document).await {
                        break;
                    }
                    written += 1;
                }
                written
            }
        });
        Self { stop, task }
    }

    fn stop(self, runtime: &tokio::runtime::Runtime) -> usize {
        self.stop.store(true, Ordering::Relaxed);
        runtime.block_on(self.task).unwrap()
    }
}

/// BM25 latency on one index while CDC inserts stream into the same index.
pub(crate) fn fts_search_while_inserting(c: &mut Criterion) {
    init();
    print_setup();

    let sizes = sizes();
    bench_search(
        c,
        "fts-search-while-inserting".into(),
        queries(),
        |queries| {
            let mut corpus = Corpus::new(corpus::SEED);
            let documents = corpus.documents(sizes.search_docs_per_index);
            let cdc_documents = Arc::new(corpus.documents(sizes.search_docs_per_index));
            let fixture = Fixture::new();
            let ids = Ids::default();

            // The CDC rows carry only the primary key; the text is fetched by key, as from ScyllaDB.
            let first_cdc_pk = 1_000_000_000;
            let (tx_cdc, rx_cdc) = mpsc::channel::<CdcRow>(1024);
            let index = fixture.runtime.block_on(async {
                let metadata = fts_index_metadata(ids.index.fetch_add(1, Ordering::Relaxed));
                add_fts_table(&fixture.db, &metadata);
                let (scan, tx) = range_scan_fn_documents_mpsc();
                let mut rx_cdc = Some(rx_cdc);
                let cdc_scan: db_basic::CdcScanFn = Box::new(move || {
                    let (tx_rows, rx_rows) = mpsc::channel(1);
                    if let Some(mut rx_cdc) = rx_cdc.take() {
                        tokio::spawn(async move {
                            while let Some(row) = rx_cdc.recv().await {
                                if tx_rows.send(row).await.is_err() {
                                    break;
                                }
                            }
                        });
                    }
                    rx_rows
                });
                let fetch_documents = Arc::clone(&cdc_documents);
                let writetime = AtomicI64::new(1);
                fixture
                    .db
                    .add_index(
                        Index::new(metadata.clone())
                            .with_range_scan(Token::new(TOKEN_MIN), scan)
                            .with_cdc_scan(
                                super::CDC_SAFETY_INTERVAL,
                                cdc_scan,
                                db_basic::cdc_column_specs(
                                    &metadata.keyspace_name,
                                    &metadata.table_name,
                                    [("id", NativeType::BigInt)],
                                    "content",
                                    ColumnType::Native(NativeType::Text),
                                ),
                            )
                            .with_fetch_row(db_basic::fetch_row(move |pk| {
                                let pk = pk.get(0).unwrap().as_bigint().unwrap();
                                let document = fetch_documents
                                    [(pk - first_cdc_pk) as usize % fetch_documents.len()]
                                .clone();
                                Ok(Some(DbRow {
                                    columns: vec![
                                        Some(DbValue::Value(CqlValue::Text(document))),
                                        Some(DbValue::Value(CqlValue::BigInt(
                                            writetime.fetch_add(1, Ordering::Relaxed) + 1,
                                        ))),
                                    ],
                                }))
                            })),
                    )
                    .unwrap();
                let mut index = FtsIndex {
                    metadata,
                    tx: Some(tx),
                };
                wait_until_index_is_created(
                    &fixture.client,
                    &index.keyspace_name(),
                    &index.index_name(),
                )
                .await;
                let guard = BatchGuard::new(&fixture.db);
                send_documents(
                    std::slice::from_ref(&index),
                    with_pks(&ids, &documents),
                    default_concurrency(),
                )
                .await;
                guard.wait(&fixture.db, documents.len()).await;
                index.finish_scan(&fixture.client).await;
                index
            });
            assert_queries_hit(&fixture, &index, queries);

            let insert_rate = insert_rate();
            let insert_start = Instant::now();
            let writer = Writer::spawn(&fixture.runtime, cdc_documents, move |written, _| {
                let tx_cdc = tx_cdc.clone();
                async move {
                    if let Some(rate) = insert_rate {
                        let due = Duration::from_secs_f64(written as f64 / rate);
                        let ahead = due.saturating_sub(insert_start.elapsed());
                        if !ahead.is_zero() {
                            tokio::time::sleep(ahead).await;
                        }
                    }
                    tx_cdc
                        .send(CdcRow {
                            primary_key: vec![CqlValue::BigInt(first_cdc_pk + written as i64)],
                            operation: OperationType::RowUpdate,
                            timestamp_us: 1,
                        })
                        .await
                        .is_ok()
                }
            });
            SearchSetup {
                targets: targets(std::slice::from_ref(&index)),
                fixture,
                finish: Box::new(move |fixture| {
                    let written = writer.stop(&fixture.runtime);
                    let secs = insert_start.elapsed().as_secs_f64();
                    eprintln!(
                        "fts: {written} documents inserted through CDC during the search \
                         in {secs:.1}s ({:.1} Kdocs/s)",
                        written as f64 / secs / 1000.0
                    );
                    fixture.runtime.block_on(assert_count(
                        &fixture.client,
                        &index,
                        documents.len() + written,
                    ));
                    fixture.teardown(vec![index]);
                }),
            }
        },
    );
}

/// BM25 latency on one index while another index takes a full-scan build.
pub(crate) fn fts_search_while_other_building(c: &mut Criterion) {
    init();
    print_setup();

    let sizes = sizes();
    bench_search(
        c,
        "fts-search-while-other-building".into(),
        queries(),
        |queries| {
            let mut corpus = Corpus::new(corpus::SEED);
            let documents = corpus.documents(sizes.search_docs_per_index);
            let build_documents = Arc::new(corpus.documents(sizes.search_docs_per_index));
            let fixture = Fixture::new();
            let ids = Arc::new(Ids::default());
            let searched = fixture.serving_indexes(&ids, 1, &documents);
            assert_queries_hit(&fixture, &searched[0], queries);

            let building = fixture
                .runtime
                .block_on(create_fts_indexes(&fixture.db, &fixture.client, &ids, 1))
                .pop()
                .unwrap();
            let tx = building.tx.clone().unwrap();
            let writer = Writer::spawn(&fixture.runtime, build_documents, {
                let ids = Arc::clone(&ids);
                move |_, document| {
                    let tx = tx.clone();
                    let pk = ids.next_pk();
                    async move { tx.send((pk, document)).await.is_ok() }
                }
            });
            SearchSetup {
                targets: targets(&searched),
                fixture,
                finish: Box::new(move |fixture| {
                    let written = writer.stop(&fixture.runtime);
                    eprintln!(
                        "fts: {written} documents built into the other index during the search"
                    );
                    fixture
                        .runtime
                        .block_on(assert_count(&fixture.client, &building, written));
                    let mut indexes = searched;
                    indexes.push(building);
                    fixture.teardown(indexes);
                }),
            }
        },
    );
}
