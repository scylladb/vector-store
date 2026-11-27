/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use super::db_basic::CdcRow;
use super::db_basic::CdcScanFn;
use super::db_basic::DbBasic;
use super::db_basic::Index;
use super::db_basic::RangeScanFn;
use super::db_basic::TOKEN_MIN;
use super::db_basic::Table;
use criterion::BenchmarkId;
use criterion::Criterion;
use futures::FutureExt;
use itertools::Itertools;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::frame::response::result::ColumnSpec;
use scylla::routing::Token;
use scylla::value::CqlValue;
use scylla_cdc::consumer::OperationType;
use std::cell::RefCell;
use std::collections::HashMap;
use std::iter;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tap::Pipe;
use testclient::TestClient;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;
use vector_store::ColumnName;
use vector_store::Connectivity;
use vector_store::DbIndexPartitioning;
use vector_store::DbRow;
use vector_store::DbValue;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsVs;
use vector_store::NonemptyArc;
use vector_store::NonemptyIteratorExt;
use vector_store::Quantization;
use vector_store::SpaceType;
use vector_store::Vector;

use super::CDC_SAFETY_INTERVAL;
use super::db_basic;
use super::default_concurrency;
use super::default_config;
use super::default_runtime;
use super::init;
use super::run_vector_store;
use super::run_with_concurrency;
use super::wait_until_all_tasks_finished;
use super::wait_until_index_is_created;
use super::wait_until_index_is_ready;
use super::wait_until_index_is_removed;

fn default_index_metadata(
    primary_key_columns: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    dimensions: usize,
) -> IndexMetadata {
    IndexMetadata {
        keyspace_name: "vector".into(),
        table_name: "items".into(),
        index_name: "ann".into(),
        primary_key_columns: NonemptyArc::new(primary_key_columns).unwrap(),
        partition_key_count: NonZeroUsize::new(partition_key_count).unwrap(),
        target_columns: NonemptyArc::new(["embedding"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new([]),
        alternator_attribute_types: Default::default(),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Vs(IndexOptionsVs {
            dimensions: NonZeroUsize::new(dimensions).unwrap().into(),
            connectivity: Connectivity::default(),
            expansion_add: ExpansionAdd::default(),
            expansion_search: ExpansionSearch::default(),
            space_type: SpaceType::Euclidean,
            quantization: Quantization::default(),
        }),
    }
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
            primary_keys: primary_keys.into_iter().collect_nonempty_arc().unwrap(),
            partition_key_count,
            columns: Arc::new(
                columns
                    .into_iter()
                    .map(|(name, column)| (name, ColumnType::Native(column)))
                    .collect(),
            ),
            dimensions: [(
                index.target_columns.first().clone(),
                index.vs().unwrap().dimensions,
            )]
            .into_iter()
            .collect(),
        },
    )
    .unwrap();
}

fn cdc_column_specs(index: &IndexMetadata) -> Vec<ColumnSpec<'static>> {
    db_basic::cdc_column_specs_vector(
        &index.keyspace_name,
        &index.table_name,
        index
            .primary_key_columns
            .iter()
            .map(|column| (column, NativeType::BigInt)),
        index.target_columns.first(),
        *index.vs().unwrap().dimensions.as_ref(),
    )
}

fn setup_index(
    db: &DbBasic,
    index_metadata: &IndexMetadata,
    range_scan_fn: Option<RangeScanFn>,
    cdc: Option<(CdcScanFn, usize)>,
) {
    db.add_index(
        Index::new(index_metadata.clone())
            .pipe(|index| {
                if let Some(range_scan_fn) = range_scan_fn {
                    index.with_range_scan(Token::new(TOKEN_MIN), range_scan_fn)
                } else {
                    index
                }
            })
            .pipe(|index| {
                if let Some((cdc_scan_fn, dimensions)) = cdc {
                    index
                        .with_cdc_scan(
                            CDC_SAFETY_INTERVAL,
                            cdc_scan_fn,
                            cdc_column_specs(index_metadata),
                        )
                        .with_fetch_row(db_basic::fetch_row({
                            let mut timestamp = 1;
                            move |pk| {
                                let pk = pk.get(0).unwrap().as_bigint().unwrap();
                                timestamp += 1;
                                Ok(Some(DbRow {
                                    columns: vec![
                                        Some(DbValue::Vector(vec![pk as f32; dimensions].into())),
                                        Some(DbValue::Value(CqlValue::BigInt(timestamp))),
                                    ],
                                }))
                            }
                        }))
                } else {
                    index
                }
            }),
    )
    .unwrap();
}

fn delete_index(db: &DbBasic, index: &IndexMetadata) {
    db.del_index(&index.keyspace_name, &index.index_name)
        .unwrap();
}

fn range_scan_fn_mpsc(items: mpsc::Receiver<(Vec<CqlValue>, Option<Vector>, i64)>) -> RangeScanFn {
    let mut items = Some(items);
    Box::new(move || {
        let (tx, rx) = mpsc::channel(1);
        let Some(mut items) = items.take() else {
            return rx;
        };
        tokio::spawn(async move {
            while let Some((primary_key, embedding, timestamp)) = items.recv().await {
                let _ = tx
                    .send(Ok(DbRow {
                        columns: primary_key
                            .into_iter()
                            .map(|v| Some(DbValue::Value(v)))
                            .chain(iter::once(embedding.map(DbValue::Vector)))
                            .chain(iter::once(Some(DbValue::Value(CqlValue::BigInt(
                                timestamp,
                            )))))
                            .collect(),
                    }))
                    .await;
            }
        });
        rx
    })
}

fn cdc_scan_fn_mpsc(items: mpsc::Receiver<(Vec<CqlValue>, OperationType, u64)>) -> CdcScanFn {
    let mut items = Some(items);
    Box::new(move || {
        let (tx, rx) = mpsc::channel(1);
        let Some(mut items) = items.take() else {
            return rx;
        };
        tokio::spawn(async move {
            while let Some((primary_key, operation, timestamp_us)) = items.recv().await {
                let _ = tx
                    .send(CdcRow {
                        primary_key,
                        operation,
                        timestamp_us,
                    })
                    .await;
            }
        });
        rx
    })
}

#[derive(Clone)]
struct Guards(Arc<Mutex<HashMap<i64, mpsc::Sender<()>>>>);

impl Guards {
    fn new(db: &DbBasic) -> Self {
        let guards = Self(Arc::new(Mutex::new(HashMap::new())));
        db.set_make_in_progress_guard({
            let guards = guards.clone();
            Some(Box::new(move |pk| {
                guards.take(pk.get(0).unwrap().as_bigint().unwrap())
            }))
        });
        guards
    }

    fn insert(&self, key: i64) -> mpsc::Receiver<()> {
        let (tx, rx) = mpsc::channel(1);
        self.0.lock().unwrap().insert(key, tx);
        rx
    }

    fn take(&self, key: i64) -> Option<mpsc::Sender<()>> {
        self.0.lock().unwrap().remove(&key)
    }
}

struct Guard(mpsc::WeakSender<()>);

impl Guard {
    fn receiver(db: &DbBasic) -> (mpsc::Sender<()>, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel(1);
        let guard = Self(tx.clone().downgrade());
        db.set_make_in_progress_guard(Some(Box::new(move |_| guard.0.upgrade())));
        (tx, rx)
    }
}

pub(crate) fn fullscan_insert(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 128;
    let index_metadata = default_index_metadata(["id".into()], 1, DIMENSIONS);
    let concurrency = default_concurrency();

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_fixture, rx_fixture) = oneshot::channel();
        runtime.spawn({
            let index_metadata = index_metadata.clone();
            let notify_stop = notify_stop.clone();
            async move {
                let db = db_basic::new();
                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                let (_config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, db.clone()).await;

                let (tx, rx) = mpsc::channel(concurrency);

                setup_index(&db, &index_metadata, Some(range_scan_fn_mpsc(rx)), None);
                let keyspace_name = index_metadata.keyspace_name.clone().into();
                let index_name = index_metadata.index_name.clone().into();
                wait_until_index_is_created(&client, &keyspace_name, &index_name).await;

                tx_fixture.send((db, client, tx)).unwrap();
                notify_stop.notified().await;
            }
        });
        let (db, client, tx) = rx_fixture.blocking_recv().unwrap();
        RefCell::new(Some((runtime, notify_stop, db, client, tx)))
    });

    let next_pk = Arc::new(AtomicI64::new(0));
    group.bench_with_input(
        BenchmarkId::new("ann-fullscan-insert", concurrency),
        &concurrency,
        |b, concurrency| {
            let fixture = fixture.borrow();
            let (runtime, _, db, _, tx) = fixture.as_ref().unwrap();
            let guards = Guards::new(db);
            b.to_async(runtime).iter_custom(|iters| {
                let guards = guards.clone();
                let tx = tx.clone();
                let next_pk = Arc::clone(&next_pk);
                async move {
                    run_with_concurrency(*concurrency, iters, move |_, it| {
                        let guards = guards.clone();
                        let tx = tx.clone();
                        let pk = next_pk.fetch_add(1, Ordering::Relaxed);
                        async move {
                            let mut rx_in_progress = guards.insert(pk);
                            let request = (
                                vec![(CqlValue::BigInt(pk))],
                                Some(vec![it as f32; DIMENSIONS].into()),
                                1,
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

    if let Some(fixture) = LazyLock::get(&fixture) {
        let (runtime, notify_stop, db, client, _) = fixture.take().unwrap();
        delete_index(&db, &index_metadata);
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            &client,
            &keyspace_name,
            &index_name,
        ));
        notify_stop.notify_one();
        drop(client);
        wait_until_all_tasks_finished(&runtime);
    }
}

pub(crate) fn search(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 128;
    let concurrency = default_concurrency();
    let index_metadata = default_index_metadata(["id".into()], 1, DIMENSIONS);
    let limit = NonZeroUsize::new(1).unwrap().into();

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_fixture, rx_fixture) = oneshot::channel();
        let (tx, rx) = mpsc::channel(runtime.metrics().num_workers() * 2);
        runtime.spawn({
            let notify_stop = notify_stop.clone();
            let index_metadata = index_metadata.clone();
            async move {
                let db = db_basic::new();
                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                setup_index(&db, &index_metadata, Some(range_scan_fn_mpsc(rx)), None);
                let (_config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, db.clone()).await;

                let (tx_in_progress, mut rx_in_progress) = Guard::receiver(&db);
                for it in 0..100000 {
                    tx.send((
                        vec![(CqlValue::BigInt(it))],
                        Some(vec![it as f32; DIMENSIONS].into()),
                        1,
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

                tx_fixture.send((db, client)).unwrap();
                notify_stop.notified().await;
            }
        });
        let (db, client) = rx_fixture.blocking_recv().unwrap();
        RefCell::new(Some((runtime, notify_stop, db, client)))
    });

    group.bench_with_input(
        BenchmarkId::new("ann-search", concurrency),
        &concurrency,
        |b, concurrency| {
            let fixture = fixture.borrow();
            let (runtime, _, _, client) = fixture.as_ref().unwrap();
            let index_metadata = index_metadata.clone();
            b.to_async(runtime).iter_custom(|iters| {
                let index_metadata = index_metadata.clone();
                let client = client.clone();
                async move {
                    run_with_concurrency(*concurrency, iters, move |_, it| {
                        let keyspace_name = index_metadata.keyspace_name.clone().into();
                        let index_name = index_metadata.index_name.clone().into();
                        let client = client.clone();
                        async move {
                            let vector = vec![it as f32; DIMENSIONS];
                            let start = Instant::now();
                            let response = client
                                .ann(&keyspace_name, &index_name, vector.into(), None, limit)
                                .await;
                            let elapsed = start.elapsed();
                            response.assert_status_ok();
                            elapsed
                        }
                    })
                    .await
                }
            })
        },
    );

    if let Some(fixture) = LazyLock::get(&fixture) {
        let (runtime, notify_stop, db, client) = fixture.take().unwrap();
        delete_index(&db, &index_metadata);
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            &client,
            &keyspace_name,
            &index_name,
        ));
        notify_stop.notify_one();
        drop(client);
        wait_until_all_tasks_finished(&runtime);
    }
}

pub(crate) fn cdc_insert(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 128;
    let concurrency = default_concurrency();
    let index_metadata = default_index_metadata(["id".into()], 1, DIMENSIONS);

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_fixture, rx_fixture) = oneshot::channel();
        runtime.spawn({
            let index_metadata = index_metadata.clone();
            let notify_stop = notify_stop.clone();
            async move {
                let db = db_basic::new();
                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                let (_config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, db.clone()).await;

                let (tx, rx) = mpsc::channel(concurrency);

                setup_index(
                    &db,
                    &index_metadata,
                    None,
                    Some((cdc_scan_fn_mpsc(rx), DIMENSIONS)),
                );
                let keyspace_name = index_metadata.keyspace_name.clone().into();
                let index_name = index_metadata.index_name.clone().into();
                wait_until_index_is_ready(&client, &keyspace_name, &index_name).await;

                tx_fixture.send((db, client, tx)).unwrap();
                notify_stop.notified().await;
            }
        });
        let (db, client, tx) = rx_fixture.blocking_recv().unwrap();
        RefCell::new(Some((runtime, notify_stop, db, client, tx)))
    });

    let next_pk = Arc::new(AtomicI64::new(0));
    group.bench_with_input(
        BenchmarkId::new("ann-cdc-insert", concurrency),
        &concurrency,
        |b, concurrency| {
            let fixture = fixture.borrow();
            let (runtime, _, db, _, tx) = fixture.as_ref().unwrap();
            let guards = Guards::new(db);
            b.to_async(runtime).iter_custom(|iters| {
                let guards = guards.clone();
                let tx = tx.clone();
                let next_pk = Arc::clone(&next_pk);
                async move {
                    run_with_concurrency(*concurrency, iters, move |_, _| {
                        let guards = guards.clone();
                        let tx = tx.clone();
                        let pk = next_pk.fetch_add(1, Ordering::Relaxed);
                        async move {
                            let mut rx_in_progress = guards.insert(pk);
                            let request =
                                (vec![(CqlValue::BigInt(pk))], OperationType::RowUpdate, 1);
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

    if let Some(fixture) = LazyLock::get(&fixture) {
        let (runtime, notify_stop, db, client, _) = fixture.take().unwrap();
        delete_index(&db, &index_metadata);
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            &client,
            &keyspace_name,
            &index_name,
        ));
        notify_stop.notify_one();
        drop(client);
        wait_until_all_tasks_finished(&runtime);
    }
}

pub(crate) fn cdc_update(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 1536;
    const INDEX_SIZE: usize = 100000;
    let concurrency = default_concurrency();
    let index_metadata = default_index_metadata(["id".into()], 1, DIMENSIONS);

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_fixture, rx_fixture) = oneshot::channel();
        let (tx_fullscan, rx_fullscan) = mpsc::channel(runtime.metrics().num_workers() * 3);
        let (tx_cdc, rx_cdc) = mpsc::channel(concurrency);
        runtime.spawn({
            let notify_stop = notify_stop.clone();
            let index_metadata = index_metadata.clone();
            async move {
                let db = db_basic::new();
                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                setup_index(
                    &db,
                    &index_metadata,
                    Some(range_scan_fn_mpsc(rx_fullscan)),
                    Some((cdc_scan_fn_mpsc(rx_cdc), DIMENSIONS)),
                );
                let (_config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, db.clone()).await;

                let (tx_in_progress, mut rx_in_progress) = Guard::receiver(&db);
                for it in 0..INDEX_SIZE {
                    tx_fullscan
                        .send((
                            vec![(CqlValue::BigInt(it as i64))],
                            Some(vec![it as f32; DIMENSIONS].into()),
                            1,
                        ))
                        .await
                        .unwrap();
                }
                // wait until all in-progress markers are dropped
                drop(tx_in_progress);
                while rx_in_progress.recv().await.is_some() {}

                drop(tx_fullscan);
                let keyspace_name = index_metadata.keyspace_name.clone().into();
                let index_name = index_metadata.index_name.clone().into();
                wait_until_index_is_ready(&client, &keyspace_name, &index_name).await;

                tx_fixture.send((db, client)).unwrap();
                notify_stop.notified().await;
            }
        });
        let (db, client) = rx_fixture.blocking_recv().unwrap();
        RefCell::new(Some((runtime, notify_stop, db, client, tx_cdc)))
    });

    // We start the next timestamp at 2, because the fullscan inserts used timestamp 1.
    let next_timestamp = Arc::new(AtomicU64::new(2));
    group.bench_with_input(
        BenchmarkId::new("ann-cdc-update", concurrency),
        &concurrency,
        |b, concurrency| {
            let fixture = fixture.borrow();
            let (runtime, _, db, _, tx_cdc) = fixture.as_ref().unwrap();
            let next_timestamp = Arc::clone(&next_timestamp);
            let guards = Guards::new(db);
            b.to_async(runtime).iter_custom(|iters| {
                let guards = guards.clone();
                let next_timestamp = Arc::clone(&next_timestamp);
                let tx_cdc = tx_cdc.clone();
                let concurrency = *concurrency;
                async move {
                    run_with_concurrency(concurrency, iters, move |concurrency_no, _| {
                        let guards = guards.clone();
                        let next_timestamp = Arc::clone(&next_timestamp);
                        let tx_cdc = tx_cdc.clone();
                        let min_pk = concurrency_no * INDEX_SIZE / concurrency;
                        let max_pk = (concurrency_no + 1) * INDEX_SIZE / concurrency;
                        async move {
                            let id = rand::random_range(min_pk..max_pk) as i64;
                            let mut rx_in_progress = guards.insert(id);
                            let timestamp = next_timestamp.fetch_add(1, Ordering::Relaxed);
                            let start = Instant::now();
                            tx_cdc
                                .send((
                                    vec![(CqlValue::BigInt(id))],
                                    OperationType::RowUpdate,
                                    timestamp,
                                ))
                                .await
                                .unwrap();
                            // wait until the in-progress marker is dropped
                            while rx_in_progress.recv().await.is_some() {}
                            start.elapsed()
                        }
                    })
                    .await
                }
            })
        },
    );

    if let Some(fixture) = LazyLock::get(&fixture) {
        let (runtime, notify_stop, db, client, _) = fixture.take().unwrap();
        delete_index(&db, &index_metadata);
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            &client,
            &keyspace_name,
            &index_name,
        ));
        notify_stop.notify_one();
        drop(client);
        wait_until_all_tasks_finished(&runtime);
    }
}

pub(crate) fn cdc_delete(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 1536;
    const INDEX_SIZE: usize = 1000000;
    let concurrency = default_concurrency();
    let index_metadata = default_index_metadata(["id".into()], 1, DIMENSIONS);

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_fixture, rx_fixture) = oneshot::channel();
        let (tx_fullscan, rx_fullscan) = mpsc::channel(runtime.metrics().num_workers() * 3);
        let (tx_cdc, rx_cdc) = mpsc::channel(concurrency);
        runtime.spawn({
            let notify_stop = notify_stop.clone();
            let index_metadata = index_metadata.clone();
            async move {
                let db = db_basic::new();
                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                setup_index(
                    &db,
                    &index_metadata,
                    Some(range_scan_fn_mpsc(rx_fullscan)),
                    Some((cdc_scan_fn_mpsc(rx_cdc), DIMENSIONS)),
                );
                let (_config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, db.clone()).await;

                let (tx_in_progress, mut rx_in_progress) = Guard::receiver(&db);
                for it in 0..INDEX_SIZE {
                    tx_fullscan
                        .send((
                            vec![(CqlValue::BigInt(it as i64))],
                            Some(vec![it as f32; DIMENSIONS].into()),
                            1,
                        ))
                        .await
                        .unwrap();
                }
                // wait until all in-progress markers are dropped
                drop(tx_in_progress);
                while rx_in_progress.recv().await.is_some() {}

                drop(tx_fullscan);
                let keyspace_name = index_metadata.keyspace_name.clone().into();
                let index_name = index_metadata.index_name.clone().into();
                wait_until_index_is_ready(&client, &keyspace_name, &index_name).await;

                tx_fixture.send((db, client)).unwrap();
                notify_stop.notified().await;
            }
        });
        let (db, client) = rx_fixture.blocking_recv().unwrap();
        RefCell::new(Some((runtime, notify_stop, db, client, tx_cdc)))
    });

    // We start the next timestamp at 2, because the fullscan inserts used timestamp 1.
    let next_timestamp = Arc::new(AtomicU64::new(2));
    group.bench_with_input(
        BenchmarkId::new("ann-cdc-delete", concurrency),
        &concurrency,
        |b, concurrency| {
            let fixture = fixture.borrow();
            let (runtime, _, db, _, tx_cdc) = fixture.as_ref().unwrap();
            let next_timestamp = Arc::clone(&next_timestamp);
            let guards = Guards::new(db);
            b.to_async(runtime).iter_custom(|iters| {
                let guards = guards.clone();
                let next_timestamp = Arc::clone(&next_timestamp);
                let tx_cdc = tx_cdc.clone();
                async move {
                    run_with_concurrency(*concurrency, iters, move |_, _| {
                        let guards = guards.clone();
                        let next_timestamp = Arc::clone(&next_timestamp);
                        let tx_cdc = tx_cdc.clone();
                        async move {
                            let timestamp = next_timestamp.fetch_add(1, Ordering::Relaxed);
                            let id = timestamp as i64;
                            let mut rx_in_progress = guards.insert(id);
                            let start = Instant::now();
                            tx_cdc
                                .send((
                                    vec![(CqlValue::BigInt(id))],
                                    OperationType::RowDelete,
                                    timestamp,
                                ))
                                .await
                                .unwrap();
                            // wait until the in-progress marker is dropped
                            while rx_in_progress.recv().await.is_some() {}
                            start.elapsed()
                        }
                    })
                    .await
                }
            })
        },
    );

    if let Some(fixture) = LazyLock::get(&fixture) {
        let (runtime, notify_stop, db, client, _) = fixture.take().unwrap();
        delete_index(&db, &index_metadata);
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            &client,
            &keyspace_name,
            &index_name,
        ));
        notify_stop.notify_one();
        drop(client);
        wait_until_all_tasks_finished(&runtime);
    }
}

pub(crate) fn search_while_updating(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 1536;
    const INDEX_SIZE: usize = 100000;
    let concurrency = default_concurrency();
    let index_metadata = default_index_metadata(["id".into()], 1, DIMENSIONS);
    let limit = NonZeroUsize::new(1).unwrap().into();

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_fixture, rx_fixture) = oneshot::channel();
        let (tx_fullscan, rx_fullscan) = mpsc::channel(runtime.metrics().num_workers() * 3);
        let (tx_cdc, rx_cdc) = mpsc::channel(runtime.metrics().num_workers() * 3);
        let bg_concurrency = runtime.metrics().num_workers() * 3;
        runtime.spawn({
            let notify_stop = notify_stop.clone();
            let index_metadata = index_metadata.clone();
            async move {
                let db = db_basic::new();

                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                setup_index(
                    &db,
                    &index_metadata,
                    Some(range_scan_fn_mpsc(rx_fullscan)),
                    Some((cdc_scan_fn_mpsc(rx_cdc), DIMENSIONS)),
                );

                let (config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, db.clone()).await;

                let (tx_in_progress, mut rx_in_progress) = Guard::receiver(&db);
                for it in 0..INDEX_SIZE {
                    tx_fullscan
                        .send((
                            vec![(CqlValue::BigInt(it as i64))],
                            Some(vec![it as f32; DIMENSIONS].into()),
                            1,
                        ))
                        .await
                        .unwrap();
                }
                // wait until all in-progress markers are dropped
                drop(tx_in_progress);
                while rx_in_progress.recv().await.is_some() {}

                let keyspace_name = index_metadata.keyspace_name.clone().into();
                let index_name = index_metadata.index_name.clone().into();

                drop(tx_fullscan);
                wait_until_index_is_ready(&client, &keyspace_name, &index_name).await;

                // run updates in the background while searching
                let (tx_in_progress, mut rx_in_progress) = Guard::receiver(&db);
                let cdc_task = tokio::spawn(async move {
                    let mut next_timestamp = 0;
                    while Arc::clone(&notify_stop).notified().now_or_never().is_none() {
                        let id = rand::random_range(0..INDEX_SIZE) as i64;
                        let timestamp = next_timestamp;
                        next_timestamp += 1;
                        if tx_cdc
                            .send((
                                vec![(CqlValue::BigInt(id))],
                                OperationType::RowUpdate,
                                timestamp,
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    // wait until all in-progress markers are dropped
                    drop(tx_in_progress);
                    while rx_in_progress.recv().await.is_some() {}
                });
                let (bg_task, bg_stop) =
                    run_search_in_background(db.clone(), client.clone(), bg_concurrency).await;
                tx_fixture.send((client, config_tx)).unwrap();

                cdc_task.await.unwrap();
                bg_stop.store(true, Ordering::Relaxed);
                bg_task.await.unwrap();
                delete_index(&db, &index_metadata);
            }
        });
        let (client, config_tx) = rx_fixture.blocking_recv().unwrap();
        RefCell::new(Some((runtime, notify_stop, client, config_tx)))
    });

    group.bench_with_input(
        BenchmarkId::new("ann-search-while-updating", concurrency),
        &concurrency,
        |b, concurrency| {
            let fixture = fixture.borrow();
            let (runtime, _, client, _) = fixture.as_ref().unwrap();
            let index_metadata = index_metadata.clone();
            let client = client.clone();
            b.to_async(runtime).iter_custom(|iters| {
                let index_metadata = index_metadata.clone();
                let client = client.clone();
                async move {
                    run_with_concurrency(*concurrency, iters, move |_, it| {
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
            });
        },
    );

    if let Some(fixture) = LazyLock::get(&fixture) {
        let (runtime, notify_stop, client, config_tx) = fixture.take().unwrap();
        notify_stop.notify_one();
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            &client,
            &keyspace_name,
            &index_name,
        ));
        drop(client);
        drop(config_tx);
        wait_until_all_tasks_finished(&runtime);
    }
}

pub(crate) fn search_while_inserting(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 1536;
    let concurrency = default_concurrency();
    let index_metadata = default_index_metadata(["id".into()], 1, DIMENSIONS);
    let limit = NonZeroUsize::new(1).unwrap().into();

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_fixture, rx_fixture) = oneshot::channel();
        let (tx_cdc, rx_cdc) = mpsc::channel(runtime.metrics().num_workers() * 3);
        let bg_concurrency = runtime.metrics().num_workers() * 3;
        runtime.spawn({
            let notify_stop = notify_stop.clone();
            let index_metadata = index_metadata.clone();
            async move {
                let db = db_basic::new();

                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                setup_index(
                    &db,
                    &index_metadata,
                    None,
                    Some((cdc_scan_fn_mpsc(rx_cdc), DIMENSIONS)),
                );

                let (config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, db.clone()).await;

                let keyspace_name = index_metadata.keyspace_name.clone().into();
                let index_name = index_metadata.index_name.clone().into();

                wait_until_index_is_ready(&client, &keyspace_name, &index_name).await;

                // run inserts in the background while searching
                let (tx_in_progress, mut rx_in_progress) = Guard::receiver(&db);
                let cdc_task = tokio::spawn(async move {
                    let pk = Arc::new(AtomicI64::new(0));
                    while Arc::clone(&notify_stop).notified().now_or_never().is_none() {
                        let pk = pk.fetch_add(1, Ordering::Relaxed);
                        if tx_cdc
                            .send((vec![(CqlValue::BigInt(pk))], OperationType::RowUpdate, 1))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    // wait until all in-progress markers are dropped
                    drop(tx_in_progress);
                    while rx_in_progress.recv().await.is_some() {}
                });
                let (bg_task, bg_stop) =
                    run_search_in_background(db.clone(), client.clone(), bg_concurrency).await;
                tx_fixture.send((client, config_tx)).unwrap();

                cdc_task.await.unwrap();
                bg_stop.store(true, Ordering::Relaxed);
                bg_task.await.unwrap();
                delete_index(&db, &index_metadata);
            }
        });
        let (client, config_tx) = rx_fixture.blocking_recv().unwrap();
        RefCell::new(Some((runtime, notify_stop, client, config_tx)))
    });

    group.bench_with_input(
        BenchmarkId::new("ann-search-while-inserting", concurrency),
        &concurrency,
        |b, concurrency| {
            let fixture = fixture.borrow();
            let (runtime, _, client, _) = fixture.as_ref().unwrap();
            let index_metadata = index_metadata.clone();
            let client = client.clone();
            b.to_async(runtime).iter_custom(|iters| {
                let index_metadata = index_metadata.clone();
                let client = client.clone();
                async move {
                    run_with_concurrency(*concurrency, iters, move |_, it| {
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
            });
        },
    );

    if let Some(fixture) = LazyLock::get(&fixture) {
        let (runtime, notify_stop, client, config_tx) = fixture.take().unwrap();
        notify_stop.notify_one();
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            &client,
            &keyspace_name,
            &index_name,
        ));
        drop(client);
        drop(config_tx);
        wait_until_all_tasks_finished(&runtime);
    }
}

pub(crate) fn search_while_deleting(c: &mut Criterion) {
    init();

    const DIMENSIONS: usize = 1536;
    const INDEX_SIZE: usize = 1000000;
    let concurrency = default_concurrency();
    let index_metadata = default_index_metadata(["id".into()], 1, DIMENSIONS);
    let limit = NonZeroUsize::new(1).unwrap().into();

    let mut group = c.benchmark_group("pipeline");
    group.throughput(criterion::Throughput::Elements(concurrency as u64));

    let fixture = LazyLock::new(|| {
        let runtime = default_runtime();
        let notify_stop = Arc::new(Notify::new());
        let (tx_fixture, rx_fixture) = oneshot::channel();
        let (tx_fullscan, rx_fullscan) = mpsc::channel(runtime.metrics().num_workers() * 3);
        let (tx_cdc, rx_cdc) = mpsc::channel(runtime.metrics().num_workers() * 3);
        let bg_concurrency = runtime.metrics().num_workers() * 3;
        runtime.spawn({
            let notify_stop = notify_stop.clone();
            let index_metadata = index_metadata.clone();
            async move {
                let db = db_basic::new();

                setup_table(
                    &db,
                    &index_metadata,
                    ["id".into()],
                    1,
                    [("id".into(), NativeType::BigInt)],
                );
                setup_index(
                    &db,
                    &index_metadata,
                    Some(range_scan_fn_mpsc(rx_fullscan)),
                    Some((cdc_scan_fn_mpsc(rx_cdc), DIMENSIONS)),
                );

                let (config_tx, config_rx) = watch::channel(default_config().await);
                let (_server, client) = run_vector_store(config_rx, db.clone()).await;

                // build the index with a fullscan before starting the benchmark
                let (tx_in_progress, mut rx_in_progress) = Guard::receiver(&db);
                for it in 0..INDEX_SIZE {
                    tx_fullscan
                        .send((
                            vec![(CqlValue::BigInt(it as i64))],
                            Some(vec![it as f32; DIMENSIONS].into()),
                            1,
                        ))
                        .await
                        .unwrap();
                }
                // wait until all in-progress markers are dropped
                drop(tx_in_progress);
                while rx_in_progress.recv().await.is_some() {}

                let keyspace_name = index_metadata.keyspace_name.clone().into();
                let index_name = index_metadata.index_name.clone().into();

                drop(tx_fullscan);
                wait_until_index_is_ready(&client, &keyspace_name, &index_name).await;

                // run removes in the background while searching
                let (tx_in_progress, mut rx_in_progress) = Guard::receiver(&db);
                let cdc_task = tokio::spawn(async move {
                    let mut next_timestamp = 2;
                    while Arc::clone(&notify_stop).notified().now_or_never().is_none() {
                        let id = rand::random_range(0..INDEX_SIZE) as i64;
                        let timestamp = next_timestamp;
                        next_timestamp += 1;
                        if tx_cdc
                            .send((
                                vec![(CqlValue::BigInt(id))],
                                OperationType::RowDelete,
                                timestamp,
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    // wait until all in-progress markers are dropped
                    drop(tx_in_progress);
                    while rx_in_progress.recv().await.is_some() {}
                });
                let (bg_task, bg_stop) =
                    run_search_in_background(db.clone(), client.clone(), bg_concurrency).await;
                tx_fixture.send((client, config_tx)).unwrap();

                cdc_task.await.unwrap();
                bg_stop.store(true, Ordering::Relaxed);
                bg_task.await.unwrap();
                delete_index(&db, &index_metadata);
            }
        });
        let (client, config_tx) = rx_fixture.blocking_recv().unwrap();
        RefCell::new(Some((runtime, notify_stop, client, config_tx)))
    });

    group.bench_with_input(
        BenchmarkId::new("ann-search-while-deleting", concurrency),
        &concurrency,
        |b, concurrency| {
            let fixture = fixture.borrow();
            let (runtime, _, client, _) = fixture.as_ref().unwrap();
            let index_metadata = index_metadata.clone();
            let client = client.clone();
            b.to_async(runtime).iter_custom(|iters| {
                let index_metadata = index_metadata.clone();
                let client = client.clone();
                async move {
                    run_with_concurrency(*concurrency, iters, move |_, it| {
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
            });
        },
    );

    if let Some(fixture) = LazyLock::get(&fixture) {
        let (runtime, notify_stop, client, config_tx) = fixture.take().unwrap();
        notify_stop.notify_one();
        let keyspace_name = index_metadata.keyspace_name.clone().into();
        let index_name = index_metadata.index_name.clone().into();
        runtime.block_on(wait_until_index_is_removed(
            &client,
            &keyspace_name,
            &index_name,
        ));
        drop(client);
        drop(config_tx);
        wait_until_all_tasks_finished(&runtime);
    }
}

async fn run_search_in_background(
    db: DbBasic,
    client: Arc<TestClient>,
    concurrency: usize,
) -> (JoinHandle<()>, Arc<AtomicBool>) {
    const DIMENSIONS: usize = 1536;
    let index_metadata = IndexMetadata {
        table_name: "tbl_bg".into(),
        index_name: "idx_bg".into(),
        ..default_index_metadata(["id".into()], 1, DIMENSIONS)
    };
    let (tx_fullscan, rx_fullscan) = mpsc::channel(concurrency);

    setup_table(
        &db,
        &index_metadata,
        ["id".into()],
        1,
        [("id".into(), NativeType::BigInt)],
    );
    setup_index(
        &db,
        &index_metadata,
        Some(range_scan_fn_mpsc(rx_fullscan)),
        None,
    );

    let (tx_in_progress, mut rx_in_progress) = Guard::receiver(&db);
    tx_fullscan
        .send((
            vec![(CqlValue::BigInt(1i64))],
            Some(vec![1.0f32; DIMENSIONS].into()),
            1,
        ))
        .await
        .unwrap();
    // wait until all in-progress markers are dropped
    drop(tx_in_progress);
    while rx_in_progress.recv().await.is_some() {}

    let keyspace_name = index_metadata.keyspace_name.clone().into();
    let index_name = index_metadata.index_name.clone().into();
    drop(tx_fullscan);
    wait_until_index_is_ready(&client, &keyspace_name, &index_name).await;

    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let stop = Arc::clone(&stop);
        let client = client.clone();
        let limit = NonZeroUsize::new(1).unwrap().into();
        async move {
            let tasks = (0..concurrency)
                .map(|_| {
                    let stop = Arc::clone(&stop);
                    let client = client.clone();
                    let keyspace_name = keyspace_name.clone();
                    let index_name = index_name.clone();
                    tokio::spawn(async move {
                        let vector = vec![1.0f32; DIMENSIONS];
                        while !stop.load(Ordering::Relaxed) {
                            _ = client
                                .ann(
                                    &keyspace_name,
                                    &index_name,
                                    vector.clone().into(),
                                    None,
                                    limit,
                                )
                                .await;
                        }
                    })
                })
                .collect_vec();
            for task in tasks.into_iter() {
                task.await.unwrap();
            }
            delete_index(&db, &index_metadata);
            wait_until_index_is_removed(&client, &keyspace_name, &index_name).await;
        }
    });
    (task, stop)
}
