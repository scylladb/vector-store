/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! FTS scenarios of the pipeline benchmark: real Tantivy indexes fed through the range scan
//! path, measured until the documents are searchable.

use super::corpus;
use super::corpus::Corpus;
use super::db_basic;
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
use super::wait_until_index_is_removed;
use axum::http::StatusCode;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::Throughput;
use httpapi::IndexInfo;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::routing::Token;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Once;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use testclient::TestClient;
use tokio::sync::mpsc;
use tokio::sync::watch;
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

struct Sizes {
    build_docs: usize,
    index_counts: Vec<usize>,
}

const ENV_FTS_INDEXES: &str = "BENCHES_FTS_INDEXES";

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
        index_counts: index_counts(),
    }
}

fn print_setup() {
    static PRINTED: Once = Once::new();
    PRINTED.call_once(|| {
        eprintln!(
            "fts: VECTOR_STORE_THREADS={} BENCHES_CONCURRENCY={} {ENV_FTS_INDEXES}={:?}",
            dotenvy::var("VECTOR_STORE_THREADS").unwrap_or_else(|_| "default".into()),
            default_concurrency(),
            index_counts(),
        );
    });
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
}

impl Fixture {
    fn new() -> Self {
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
        }
    }

    fn teardown(self) {
        let Self {
            runtime,
            client,
            _server,
            _config_tx,
            ..
        } = self;
        drop(client);
        drop(_server);
        drop(_config_tx);
        wait_until_all_tasks_finished(&runtime);
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
                            delete_fts_indexes(&fixture.db, &fixture.client, indexes).await;
                            elapsed
                        });
                    }
                    elapsed
                });
            },
        );
        if let Some(fixture) = fixture {
            fixture.teardown();
        }
    }
}
