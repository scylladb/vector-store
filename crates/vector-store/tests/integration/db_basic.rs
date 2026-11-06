/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use anyhow::anyhow;
use anyhow::bail;
use futures::FutureExt;
use futures::Stream;
use futures::future::BoxFuture;
use futures::future::RemoteHandle;
use futures::stream;
use itertools::Itertools;
use scylla::cluster::metadata::ColumnKind;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::frame::response::result::ColumnSpec;
use scylla::frame::response::result::TableSpec;
use scylla::response::query_result::ColumnSpecs;
use scylla::routing::Token;
use scylla::value::CqlTimeuuid;
use scylla::value::CqlValue;
use scylla::value::Row;
use scylla_cdc::consumer::CDCRow;
use scylla_cdc::consumer::CDCRowSchema;
use scylla_cdc::consumer::OperationType;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::iter;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use uuid::ContextV1;
use uuid::Uuid;
use vector_store::AsyncInProgress;
use vector_store::CdcLogReaderConfig;
use vector_store::ColumnName;
use vector_store::Config;
use vector_store::DbColumn;
use vector_store::DbCustomIndex;
use vector_store::DbDriver;
use vector_store::DbIndexInfo;
use vector_store::DbIndexKind;
use vector_store::DbIndexPartitioning;
use vector_store::DbIndexedOperation;
use vector_store::DbIndexedRow;
use vector_store::DbIndexedValue;
use vector_store::DbRow;
use vector_store::DbUnsupportedIndex;
use vector_store::DbValue;
use vector_store::Dimensions;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexName;
use vector_store::IndexVersion;
use vector_store::KeyspaceName;
use vector_store::NonemptyArc;
use vector_store::NonemptyBox;
use vector_store::Percentage;
use vector_store::PrimaryKey;
use vector_store::Progress;
use vector_store::TableName;
use vector_store::Timestamp;
use vector_store::Timestamped;
use vector_store::Vector;
use vector_store::db::Db;
use vector_store::db_index::DbIndex;
use vector_store::node_state::Event;
use vector_store::node_state::NodeState;

pub(crate) type RxIndexedRow = mpsc::Receiver<(DbIndexedRow, AsyncInProgress)>;
pub(crate) type TxIndexedRow = mpsc::Sender<(DbIndexedRow, AsyncInProgress)>;
pub(crate) type ScanFn = Box<dyn FnOnce(TxIndexedRow) -> BoxFuture<'static, ()> + Send + Sync>;

pub(crate) type RangeScanFn =
    Box<dyn FnMut() -> mpsc::Receiver<anyhow::Result<DbRow>> + Send + Sync>;

pub(crate) type CdcScanFn = Box<dyn FnMut() -> mpsc::Receiver<CdcRow> + Send + Sync>;

pub(crate) type MakeInProgressGuardFn =
    Box<dyn for<'a> Fn(&'a PrimaryKey) -> Option<mpsc::Sender<()>> + Send + Sync>;

pub(crate) struct CdcRow {
    pub(crate) primary_key: Vec<CqlValue>,
    pub(crate) operation: OperationType,
    pub(crate) timestamp_us: u64,
}

pub(crate) struct FetchRow {
    pub(crate) primary_key: PrimaryKey,
    pub(crate) tx: oneshot::Sender<anyhow::Result<Option<DbRow>>>,
}

pub(crate) struct FetchVector {
    pub(crate) primary_key: PrimaryKey,
    pub(crate) tx: oneshot::Sender<anyhow::Result<Option<Vector>>>,
}

const CDC_COLUMN_NAME_STREAM_ID: &str = "cdc$stream_id";
const CDC_COLUMN_NAME_TIME: &str = "cdc$time";
const CDC_COLUMN_NAME_OPERATION: &str = "cdc$operation";

pub(crate) const TOKEN_MAX: i64 = i64::MAX;
pub(crate) const TOKEN_MIN: i64 = -TOKEN_MAX;

fn make_scan_fn(rows: impl Iterator<Item = DbIndexedRow> + Send + Sync + 'static) -> ScanFn {
    Box::new(move |tx| {
        async move {
            let (tx_in_progress, mut rx_in_progress) = mpsc::channel(1);

            for row in rows {
                let _ = tx
                    .send((row, AsyncInProgress::fullscan(tx_in_progress.clone())))
                    .await;
            }

            drop(tx_in_progress);
            while rx_in_progress.recv().await.is_some() {}
        }
        .boxed()
    })
}

pub(crate) fn make_range_scan_fn<I>(rows: I) -> RangeScanFn
where
    I: IntoIterator<Item = DbRow> + Send + Sync + 'static,
    I::IntoIter: Send + Sync + 'static,
{
    let mut rows = Some(rows);
    Box::new(move || {
        let (tx, rx) = mpsc::channel(1);
        let Some(rows) = rows.take() else {
            return rx;
        };
        tokio::spawn(async move {
            for row in rows.into_iter().map(Ok) {
                if tx.send(row).await.is_err() {
                    break;
                }
            }
        });
        rx
    })
}

pub(crate) fn make_cdc_scan_fn<I>(rows: I) -> CdcScanFn
where
    I: IntoIterator<Item = CdcRow> + Send + Sync + 'static,
    I::IntoIter: Send + Sync + 'static,
{
    let mut rows = Some(rows);
    Box::new(move || {
        let (tx, rx) = mpsc::channel(1);
        let Some(rows) = rows.take() else {
            return rx;
        };
        tokio::spawn(async move {
            for row in rows.into_iter() {
                if tx.send(row).await.is_err() {
                    break;
                }
            }
        });
        rx
    })
}

pub(crate) fn pending_scan_fn() -> ScanFn {
    Box::new(|_tx| std::future::pending::<()>().boxed())
}

pub(crate) fn pending_range_scan_fn() -> RangeScanFn {
    Box::new(move || {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            tx.closed().await;
        });
        rx
    })
}

pub(crate) fn scan_fn_vectors<I>(items: I) -> ScanFn
where
    I: IntoIterator<
        Item = (
            PrimaryKey,
            Option<Vector>,
            Box<[Option<CqlValue>]>,
            Timestamp,
        ),
    >,
    I::IntoIter: Send + Sync + 'static,
{
    make_scan_fn(
        items
            .into_iter()
            .map(|(primary_key, vector, filtering, timestamp)| DbIndexedRow {
                primary_key,
                operation: DbIndexedOperation::Upsert(
                    NonemptyBox::new(
                        iter::once(vector)
                            .map(|vector| vector.map(DbIndexedValue::Vector))
                            .chain(
                                filtering
                                    .into_iter()
                                    .map(|filtering| filtering.map(DbIndexedValue::Filtering)),
                            )
                            .map(|value| Timestamped::new(timestamp, value)),
                    )
                    .unwrap(),
                ),
            }),
    )
}

pub(crate) fn range_scan_fn_vectors<I, P, F>(items: I) -> RangeScanFn
where
    I: IntoIterator<Item = (P, Option<Vector>, F, i64)>,
    I::IntoIter: Send + Sync + 'static,
    P: IntoIterator<Item = CqlValue> + Send + Sync + 'static,
    F: IntoIterator<Item = Option<CqlValue>> + Send + Sync + 'static,
{
    make_range_scan_fn(
        items
            .into_iter()
            .map(|(primary_key, vector, filtering, timestamp)| DbRow {
                columns: primary_key
                    .into_iter()
                    .map(|v| Some(DbValue::Value(v)))
                    .chain(iter::once(vector.map(DbValue::Vector)))
                    .chain(iter::once(Some(DbValue::Value(CqlValue::BigInt(
                        timestamp,
                    )))))
                    .chain(filtering.into_iter().flat_map(|filtering| {
                        iter::once(filtering.as_ref().map(|v| DbValue::Value(v.clone()))).chain(
                            iter::once(Some(DbValue::Value(CqlValue::BigInt(timestamp)))),
                        )
                    }))
                    .collect(),
            }),
    )
}

pub(crate) fn scan_fn_documents<I>(items: I) -> ScanFn
where
    I: IntoIterator<Item = (PrimaryKey, Option<String>, Timestamp)>,
    I::IntoIter: Send + Sync + 'static,
{
    make_scan_fn(items.into_iter().map(|(primary_key, document, timestamp)| {
        DbIndexedRow {
            primary_key,
            operation: DbIndexedOperation::Upsert(
                NonemptyBox::new([Timestamped::new(
                    timestamp,
                    document.map(DbIndexedValue::Document),
                )])
                .unwrap(),
            ),
        }
    }))
}

pub(crate) fn range_scan_fn_documents<I, P>(items: I) -> RangeScanFn
where
    I: IntoIterator<Item = (P, Option<String>, i64)>,
    I::IntoIter: Send + Sync + 'static,
    P: IntoIterator<Item = CqlValue>,
{
    make_range_scan_fn(items.into_iter().map(|(primary_key, document, timestamp)| {
        DbRow {
            columns: primary_key
                .into_iter()
                .map(|v| Some(DbValue::Value(v)))
                .chain(iter::once(
                    document.map(|value| DbValue::Value(CqlValue::Text(value))),
                ))
                .chain(iter::once(Some(DbValue::Value(CqlValue::BigInt(
                    timestamp,
                )))))
                .collect(),
        }
    }))
}

pub(crate) fn cdc_scan_fn<I, P>(items: I) -> CdcScanFn
where
    I: IntoIterator<Item = (P, OperationType, u64)>,
    I::IntoIter: Send + Sync + 'static,
    P: IntoIterator<Item = CqlValue> + Send + Sync + 'static,
{
    make_cdc_scan_fn(
        items
            .into_iter()
            .map(|(primary_key, operation, timestamp_us)| CdcRow {
                primary_key: primary_key.into_iter().collect(),
                operation,
                timestamp_us,
            }),
    )
}

pub(crate) fn cdc_column_specs(
    keyspace_name: impl AsRef<str>,
    table_name: impl AsRef<str>,
    primary_key: impl IntoIterator<Item = (impl AsRef<str>, NativeType)>,
    target_name: impl AsRef<str>,
    target_type: ColumnType<'static>,
) -> Vec<ColumnSpec<'static>> {
    let table_spec = TableSpec::owned(keyspace_name.as_ref().into(), table_name.as_ref().into());

    iter::once(ColumnSpec::owned(
        CDC_COLUMN_NAME_STREAM_ID.to_string(),
        ColumnType::Native(NativeType::Blob),
        table_spec.clone(),
    ))
    .chain(iter::once(ColumnSpec::owned(
        CDC_COLUMN_NAME_TIME.to_string(),
        ColumnType::Native(NativeType::Timeuuid),
        table_spec.clone(),
    )))
    .chain(iter::once(ColumnSpec::owned(
        CDC_COLUMN_NAME_OPERATION.to_string(),
        ColumnType::Native(NativeType::TinyInt),
        table_spec.clone(),
    )))
    .chain(primary_key.into_iter().map(|(column, typ)| {
        ColumnSpec::owned(
            column.as_ref().into(),
            ColumnType::Native(typ),
            table_spec.clone(),
        )
    }))
    .chain(iter::once(ColumnSpec::owned(
        target_name.as_ref().into(),
        target_type,
        table_spec.clone(),
    )))
    .collect()
}

pub(crate) fn cdc_column_specs_vector(
    keyspace_name: impl AsRef<str>,
    table_name: impl AsRef<str>,
    primary_key: impl IntoIterator<Item = (impl AsRef<str>, NativeType)>,
    target_name: impl AsRef<str>,
    dimensions: NonZeroUsize,
) -> Vec<ColumnSpec<'static>> {
    cdc_column_specs(
        keyspace_name,
        table_name,
        primary_key,
        target_name,
        ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Float)),
            dimensions: dimensions.get() as u16,
        },
    )
}

fn timeuuid_from(timestamp_us: u64) -> CqlTimeuuid {
    let context = ContextV1::new_random();
    let seconds = timestamp_us / 1_000_000;
    let nanos = (timestamp_us % 1_000_000) * 1_000;
    let ts = uuid::timestamp::Timestamp::from_unix(&context, seconds, nanos as u32);
    Uuid::new_v1(ts, &[0; 6]).into()
}

fn row_from(cdc_row: CdcRow) -> Row {
    Row {
        columns: iter::once(CqlValue::Blob(vec![0; 8]))
            .chain(iter::once(CqlValue::Timeuuid(timeuuid_from(
                cdc_row.timestamp_us,
            ))))
            .chain(iter::once(CqlValue::TinyInt(cdc_row.operation as i8)))
            .chain(cdc_row.primary_key)
            .map(Some)
            .collect(),
    }
}

pub(crate) fn fetch_row(
    mut fetch_fn: impl FnMut(PrimaryKey) -> anyhow::Result<Option<DbRow>> + Send + 'static,
) -> mpsc::Sender<FetchRow> {
    let (tx, mut rx) = mpsc::channel::<FetchRow>(1);
    tokio::spawn(async move {
        while let Some(fetch) = rx.recv().await {
            let _ = fetch.tx.send(fetch_fn(fetch.primary_key));
        }
    });
    tx
}

pub(crate) fn fetch_vector(
    mut fetch_fn: impl FnMut(PrimaryKey) -> anyhow::Result<Option<Vector>> + Send + 'static,
) -> mpsc::Sender<FetchVector> {
    let (tx, mut rx) = mpsc::channel::<FetchVector>(1);
    tokio::spawn(async move {
        while let Some(fetch) = rx.recv().await {
            let _ = fetch.tx.send(fetch_fn(fetch.primary_key));
        }
    });
    tx
}

/// Whether the mock keeps the indexed vectors around, so that they can be read
/// back from the base table via `DbIndex::GetVectors`. Off by default: only a
/// backend that re-reads the vectors it indexed (the DiskANN Scylla one) needs
/// them, and a mock that silently answers such a read hides the tests that
/// should not depend on it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum StoreVectors {
    #[default]
    Disabled,
    Enabled,
}

impl StoreVectors {
    fn is_enabled(self) -> bool {
        self == Self::Enabled
    }
}

impl From<bool> for StoreVectors {
    fn from(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

#[derive(Clone, derive_more::Debug)]
pub(crate) struct DbBasic(#[debug(skip)] Arc<RwLock<DbMock>>);

pub(crate) fn new(node_state: mpsc::Sender<NodeState>) -> (mpsc::Sender<Db>, DbBasic) {
    new_with(node_state, StoreVectors::default())
}

/// [`new`], for a test that needs a non-default [`StoreVectors`].
pub(crate) fn new_with(
    node_state: mpsc::Sender<NodeState>,
    store_vectors: StoreVectors,
) -> (mpsc::Sender<Db>, DbBasic) {
    let (tx, mut rx) = mpsc::channel(10);
    let db = DbBasic::new(store_vectors);
    tokio::spawn({
        let db = db.clone();
        async move {
            while let Some(msg) = rx.recv().await {
                process_db(&db, msg, node_state.clone());
            }
        }
    });
    (tx, db)
}

pub(crate) struct Table {
    pub(crate) primary_keys: NonemptyArc<ColumnName>,
    pub(crate) partition_key_count: usize,
    pub(crate) columns: Arc<HashMap<ColumnName, ColumnType<'static>>>,
    pub(crate) dimensions: HashMap<ColumnName, Dimensions>,
}

pub(crate) struct Index {
    metadata: IndexMetadata,
    range_scans: BTreeMap<Token, RangeScanFn>,
    cdcs: BTreeMap<Duration, (CdcScanFn, Vec<ColumnSpec<'static>>)>,
    fetch_row: Option<mpsc::Sender<FetchRow>>,
    fetch_vector: Option<mpsc::Sender<FetchVector>>,
    fullscan_fn: Option<ScanFn>,
    cdc_fn: Option<ScanFn>,
}

impl Index {
    pub(crate) fn new(metadata: IndexMetadata) -> Self {
        Self {
            metadata,
            range_scans: BTreeMap::new(),
            cdcs: BTreeMap::new(),
            fetch_row: None,
            fetch_vector: None,
            fullscan_fn: None,
            cdc_fn: None,
        }
    }

    pub(crate) fn with_fullscan(self, scan_fn: ScanFn) -> Self {
        Self {
            fullscan_fn: Some(scan_fn),
            ..self
        }
    }

    pub(crate) fn with_cdc(self, scan_fn: ScanFn) -> Self {
        Self {
            cdc_fn: Some(scan_fn),
            ..self
        }
    }

    pub(crate) fn with_range_scan(mut self, token: Token, range_scan_fn: RangeScanFn) -> Self {
        self.range_scans.insert(token, range_scan_fn);
        self
    }

    pub(crate) fn with_cdc_scan(
        mut self,
        duration: Duration,
        scan_fn: CdcScanFn,
        column_specs: Vec<ColumnSpec<'static>>,
    ) -> Self {
        self.cdcs.insert(duration, (scan_fn, column_specs));
        self
    }

    pub(crate) fn with_fetch_row(self, tx: mpsc::Sender<FetchRow>) -> Self {
        Self {
            fetch_row: Some(tx),
            ..self
        }
    }

    pub(crate) fn with_fetch_vector(self, tx: mpsc::Sender<FetchVector>) -> Self {
        Self {
            fetch_vector: Some(tx),
            ..self
        }
    }
}

struct Keyspace {
    tables: HashMap<TableName, Table>,
    indexes: HashMap<IndexName, Index>,
}

impl Keyspace {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
        }
    }
}

struct DbMock {
    schema_version: Uuid,
    keyspaces: HashMap<KeyspaceName, Keyspace>,
    token_ring: Vec<Token>,
    nr_shards: NonZeroUsize,
    next_await_schema_agreement_failed: bool,
    next_get_db_index_failed: bool,
    next_full_scan_progress: Option<Progress>,
    simulate_endless_get_indexes_processing: bool,
    unsupported_indexes: Vec<DbUnsupportedIndex>,
    store_vectors: StoreVectors,
    vectors: HashMap<PrimaryKey, Vector>,
    make_in_progress_guard: Option<MakeInProgressGuardFn>,
}

impl DbMock {
    fn create_new_schema_version(&mut self) {
        self.schema_version = Uuid::new_v4();
    }
}

impl DbBasic {
    fn new(store_vectors: StoreVectors) -> Self {
        Self(Arc::new(RwLock::new(DbMock {
            schema_version: Uuid::new_v4(),
            keyspaces: HashMap::new(),
            token_ring: vec![],
            nr_shards: NonZeroUsize::new(1).unwrap(),
            next_await_schema_agreement_failed: false,
            next_get_db_index_failed: false,
            next_full_scan_progress: None,
            simulate_endless_get_indexes_processing: false,
            unsupported_indexes: Vec::new(),
            store_vectors,
            vectors: HashMap::new(),
            make_in_progress_guard: None,
        })))
    }

    fn record_row(&self, row: &DbIndexedRow) {
        let mut db = self.0.write().unwrap();
        match &row.operation {
            DbIndexedOperation::Upsert(values) => match values.first().value() {
                Some(DbIndexedValue::Vector(vector)) => {
                    db.vectors.insert(row.primary_key.clone(), vector.clone());
                }
                None => {
                    db.vectors.remove(&row.primary_key);
                }
                Some(_) => {}
            },
            DbIndexedOperation::Delete(_) => {
                db.vectors.remove(&row.primary_key);
            }
        }
    }

    fn get_vectors(&self, keys: &[PrimaryKey]) -> Vec<Option<Vector>> {
        let db = self.0.read().unwrap();
        keys.iter()
            .map(|key| db.vectors.get(key).cloned())
            .collect()
    }

    pub(crate) fn add_table(
        &self,
        keyspace_name: KeyspaceName,
        table_name: TableName,
        table: Table,
    ) -> anyhow::Result<()> {
        let mut db = self.0.write().unwrap();

        let keyspace = db
            .keyspaces
            .entry(keyspace_name)
            .or_insert_with(Keyspace::new);
        if keyspace.tables.contains_key(&table_name) {
            bail!("a table {table_name} already exists in a keyspace");
        }
        keyspace.tables.insert(table_name, table);

        db.create_new_schema_version();
        Ok(())
    }

    pub(crate) fn add_vector_column(
        &self,
        keyspace_name: KeyspaceName,
        table_name: TableName,
        column_name: ColumnName,
        dimensions: Dimensions,
    ) -> anyhow::Result<()> {
        let mut db = self.0.write().unwrap();

        let table = db
            .keyspaces
            .get_mut(&keyspace_name)
            .and_then(|keyspace| keyspace.tables.get_mut(&table_name))
            .ok_or_else(|| anyhow!("a table {table_name} does not exist"))?;
        table.dimensions.insert(column_name, dimensions);

        db.create_new_schema_version();
        Ok(())
    }

    pub(crate) fn add_index(&self, index: Index) -> anyhow::Result<()> {
        let mut db = self.0.write().unwrap();

        let Some(keyspace) = db.keyspaces.get_mut(&index.metadata.keyspace_name) else {
            bail!(
                "a keyspace {keyspace_name} does not exist",
                keyspace_name = index.metadata.keyspace_name
            );
        };
        if !keyspace.tables.contains_key(&index.metadata.table_name) {
            bail!("a table {} does not exist", index.metadata.table_name);
        };
        if keyspace.indexes.contains_key(&index.metadata.index_name) {
            bail!(
                "an index {index_name} already exists",
                index_name = index.metadata.index_name
            );
        }
        keyspace
            .indexes
            .insert(index.metadata.index_name.clone(), index);

        db.create_new_schema_version();
        Ok(())
    }

    /// Adds an index that discovery skips as unsupported.
    pub(crate) fn add_unsupported_index(&self, index: DbUnsupportedIndex) {
        let mut db = self.0.write().unwrap();
        db.unsupported_indexes.push(index);
        db.create_new_schema_version();
    }

    pub(crate) fn del_index(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
    ) -> anyhow::Result<()> {
        let mut db = self.0.write().unwrap();

        let Some(keyspace) = db.keyspaces.get_mut(keyspace_name) else {
            bail!("a keyspace {keyspace_name} does not exist");
        };
        if keyspace.indexes.remove(index_name).is_none() {
            bail!("an index {index_name} does not exist");
        }

        db.create_new_schema_version();
        Ok(())
    }

    pub(crate) fn set_token_ring(&self, token_ring: Vec<Token>) {
        self.0.write().unwrap().token_ring = token_ring;
    }

    pub(crate) fn set_nr_shards(&self, nr_shards: NonZeroUsize) {
        self.0.write().unwrap().nr_shards = nr_shards;
    }

    pub(crate) fn set_next_await_schema_agreement_failed(&self) {
        self.0.write().unwrap().next_await_schema_agreement_failed = true;
    }

    pub(crate) fn set_next_get_db_index_failed(&self) {
        self.0.write().unwrap().next_get_db_index_failed = true;
    }

    pub(crate) fn set_next_full_scan_progress(&self, progress: Progress) {
        self.0.write().unwrap().next_full_scan_progress = Some(progress);
    }

    pub(crate) fn simulate_endless_get_indexes_processing(&self) {
        self.0
            .write()
            .unwrap()
            .simulate_endless_get_indexes_processing = true;
    }

    #[allow(dead_code)]
    pub(crate) fn set_make_in_progress_guard(
        &self,
        make_in_progress_guard: Option<MakeInProgressGuardFn>,
    ) {
        self.0.write().unwrap().make_in_progress_guard = make_in_progress_guard;
    }

    fn is_compatible_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
pub struct Session {
    db: DbBasic,
    _config: Arc<Config>,
}

impl Session {
    fn is_compatible_with(&self, other: &Session) -> bool {
        self.is_compatible_with_db(&other.db)
    }

    fn is_compatible_with_db(&self, db: &DbBasic) -> bool {
        self.db.is_compatible_with(db)
    }
}

enum StatementInternal {
    LatestSchemaVersion,
    GetIndexes,
    GetIndexTargetType,
    GetIndexOptions,
    RangeScan { index_metadata: IndexMetadata },
    FetchVector { index_metadata: IndexMetadata },
    FetchRow { index_metadata: IndexMetadata },
}

pub struct Statement {
    session: Session,
    statement: StatementInternal,
}

impl Statement {
    fn is_compatible_with(&self, session: &Session) -> bool {
        self.session.is_compatible_with(session)
    }
}

pub struct Cluster {
    db: DbBasic,
    tables: Vec<ClusterTable>,
    token_ring: Vec<Token>,
}

impl Cluster {
    fn new(db: DbBasic) -> Self {
        let db_read = Arc::clone(&db.0);
        let db_read = db_read.read().unwrap();
        let tables = db_read
            .keyspaces
            .iter()
            .flat_map(|(keyspace_name, keyspace)| {
                keyspace.tables.iter().map(|(table_name, table)| {
                    let partition_key = table
                        .primary_keys
                        .iter()
                        .take(table.partition_key_count)
                        .cloned()
                        .collect_vec();
                    let clustering_key = table
                        .primary_keys
                        .iter()
                        .skip(table.partition_key_count)
                        .cloned()
                        .collect_vec();
                    let columns = table
                        .columns
                        .iter()
                        .map(|(column_name, typ)| {
                            let kind = if partition_key.contains(column_name) {
                                ColumnKind::PartitionKey
                            } else if clustering_key.contains(column_name) {
                                ColumnKind::Clustering
                            } else {
                                ColumnKind::Regular
                            };
                            (
                                column_name.clone(),
                                DbColumn {
                                    typ: typ.clone(),
                                    kind,
                                },
                            )
                        })
                        .chain(table.dimensions.iter().map(|(column_name, dimensions)| {
                            (
                                column_name.clone(),
                                DbColumn {
                                    typ: ColumnType::Vector {
                                        typ: Box::new(ColumnType::Native(NativeType::Float)),
                                        dimensions: dimensions.as_ref().get() as u16,
                                    },
                                    kind: ColumnKind::Regular,
                                },
                            )
                        }))
                        .collect();
                    ClusterTable {
                        keyspace_name: keyspace_name.clone(),
                        table_name: table_name.clone(),
                        partition_key,
                        clustering_key,
                        columns,
                    }
                })
            })
            .collect();
        let token_ring = db_read.token_ring.clone();
        Self {
            db,
            tables,
            token_ring,
        }
    }
    fn is_compatible_with(&self, db: &DbBasic) -> bool {
        self.db.is_compatible_with(db)
    }
}

pub struct ClusterTable {
    keyspace_name: KeyspaceName,
    table_name: TableName,
    partition_key: Vec<ColumnName>,
    clustering_key: Vec<ColumnName>,
    columns: Vec<(ColumnName, DbColumn)>,
}

fn options_from_metadata(metadata: &IndexMetadata) -> BTreeMap<String, String> {
    let mut options = BTreeMap::new();
    let class_name = match &metadata.kind {
        IndexKind::Vs(_vs) => "vector_index",
        IndexKind::Fts(_fts) => "fulltext_index",
        IndexKind::Pattern(_pattern) => "pattern_index",
    };
    options.insert("class_name".to_string(), class_name.to_string());
    let tc = metadata.target_columns.first();
    let pk = match &metadata.partitioning {
        DbIndexPartitioning::Global => "[]".to_string(),
        DbIndexPartitioning::Local(key) => format!(
            "[{}]",
            key.iter().map(|column| format!("\"{column}\"")).join(",")
        ),
    };
    let fc = metadata
        .filtering_columns
        .iter()
        .map(|column| format!("\"{column}\""))
        .join(",");
    options.insert(
        "target".to_string(),
        format!("{{\"tc\":\"{tc}\",\"pk\":{pk},\"fc\":[{fc}]}}"),
    );
    options.insert("index_version".to_string(), metadata.version.to_string());
    match &metadata.kind {
        IndexKind::Vs(vs) => {
            options.insert(
                "maximum_node_connections".to_string(),
                vs.connectivity.to_string(),
            );
            options.insert(
                "construction_beam_width".to_string(),
                vs.expansion_add.to_string(),
            );
            options.insert(
                "search_beam_width".to_string(),
                vs.expansion_search.to_string(),
            );
            options.insert("similarity_function".to_string(), vs.space_type.to_string());
            options.insert("quantization".to_string(), vs.quantization.to_string());
        }
        IndexKind::Fts(fts) => {
            options.insert("analyzer".to_string(), fts.analyzer.to_string());
            options.insert("positions".to_string(), fts.positions.to_string());
        }
        IndexKind::Pattern(pattern) => {
            options.insert(
                "case_sensitive".to_string(),
                pattern.case_sensitive.to_string(),
            );
        }
    }
    options
}

fn target_type_from_table(table: &Table, column_name: &ColumnName) -> Option<String> {
    if let Some(typ) = table.columns.get(column_name) {
        return Some(match typ {
            ColumnType::Native(NativeType::Int) => "int".to_string(),
            ColumnType::Native(NativeType::BigInt) => "bigint".to_string(),
            ColumnType::Native(NativeType::Float) => "float".to_string(),
            ColumnType::Native(NativeType::Double) => "double".to_string(),
            ColumnType::Native(NativeType::Boolean) => "boolean".to_string(),
            ColumnType::Native(NativeType::Text) => "text".to_string(),
            ColumnType::Native(NativeType::Blob) => "blob".to_string(),
            ColumnType::Native(NativeType::Timestamp) => "timestamp".to_string(),
            ColumnType::Native(NativeType::Uuid) => "uuid".to_string(),
            ColumnType::Native(NativeType::Timeuuid) => "timeuuid".to_string(),
            ColumnType::Native(NativeType::Inet) => "inet".to_string(),
            ColumnType::Native(NativeType::Date) => "date".to_string(),
            ColumnType::Native(NativeType::Time) => "time".to_string(),
            ColumnType::Native(NativeType::SmallInt) => "smallint".to_string(),
            ColumnType::Native(NativeType::TinyInt) => "tinyint".to_string(),
            ColumnType::Native(NativeType::Decimal) => "decimal".to_string(),
            ColumnType::Native(NativeType::Varint) => "varint".to_string(),
            _ => panic!("unsupported native type: {typ:?}"),
        });
    }
    table
        .dimensions
        .get(column_name)
        .map(|dim| format!("vector<float, {dim}>"))
}

pub struct CdcLogReader {
    notify: Arc<Notify>,
    _task: JoinHandle<()>,
}

impl CdcLogReader {
    pub async fn new(
        db: &DbBasic,
        mut config: CdcLogReaderConfig,
    ) -> anyhow::Result<(Self, RemoteHandle<anyhow::Result<()>>)> {
        let keyspace_name = config.keyspace.take().expect("keyspace must be set");
        let index_name = config.index_name.take().expect("index_name must be set");
        let safety_interval = config
            .safety_interval
            .take()
            .expect("safety interval must be provided");

        let (mut rx, column_specs) =
            db.0.write()
                .unwrap()
                .keyspaces
                .get_mut(&keyspace_name)
                .and_then(|keyspace| keyspace.indexes.get_mut(&index_name))
                .and_then(|index| index.cdcs.get_mut(&safety_interval))
                .map(|(cdc_scan_fn, column_specs)| (cdc_scan_fn(), column_specs.clone()))
                .unwrap_or_else(|| {
                    let (_tx, rx) = mpsc::channel(1);
                    (rx, vec![])
                });

        let consumer_factory = config
            .consumer_factory
            .take()
            .expect("consumer factory must be provided");
        let mut consumer = consumer_factory.new_consumer().await;
        let notify = Arc::new(Notify::new());
        let (remote, remote_handle) = {
            let notify = Arc::clone(&notify);
            async move {
                let row_schema = CDCRowSchema::new(ColumnSpecs::new(&column_specs));
                loop {
                    tokio::select! {
                        _ = notify.notified() => {
                            break Ok(());
                        }

                        cdc_row = rx.recv() => {
                            let Some(cdc_row) = cdc_row else {
                                break Ok(());
                            };
                            let cdc_row = CDCRow::from_row(row_from(cdc_row), &row_schema);
                            if let Err(err) = consumer.consume_cdc(cdc_row).await {
                                break Err(err);
                            }
                        }
                    }
                }
            }
        }
        .remote_handle();
        let task = tokio::spawn(remote);
        Ok((
            Self {
                notify,
                _task: task,
            },
            remote_handle,
        ))
    }

    fn stop(&self) {
        self.notify.notify_one();
    }
}

impl DbDriver for DbBasic {
    type Session = Session;
    type Statement = Statement;
    type Cluster = Cluster;
    type Table = ClusterTable;
    type CdcLogReader = CdcLogReader;
    type Metrics = ();

    async fn connect(&self, config: Arc<Config>) -> anyhow::Result<Self::Session> {
        Ok(Session {
            db: self.clone(),
            _config: Arc::clone(&config),
        })
    }

    async fn refresh_metadata(&self, session: &Self::Session) {
        assert!(session.is_compatible_with_db(self));
    }

    async fn await_schema_agreement(&self, session: &Self::Session) -> anyhow::Result<Uuid> {
        assert!(session.is_compatible_with_db(self));

        let mut db_mock = self.0.write().unwrap();
        if db_mock.next_await_schema_agreement_failed {
            db_mock.next_await_schema_agreement_failed = false;
            bail!("await_schema_agreement failed");
        }
        Ok(db_mock.schema_version)
    }

    async fn check_schema_agreement(
        &self,
        session: &Self::Session,
    ) -> anyhow::Result<Option<Uuid>> {
        assert!(session.is_compatible_with_db(self));
        Ok(Some(self.0.read().unwrap().schema_version))
    }

    async fn prepare_latest_schema_version(
        &self,
        session: &Self::Session,
    ) -> anyhow::Result<Self::Statement> {
        assert!(session.is_compatible_with_db(self));
        Ok(Statement {
            session: session.clone(),
            statement: StatementInternal::LatestSchemaVersion,
        })
    }

    async fn execute_latest_schema_version(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
    ) -> anyhow::Result<Uuid> {
        assert!(session.is_compatible_with_db(self));
        assert!(statement.is_compatible_with(session));
        assert!(matches!(
            statement.statement,
            StatementInternal::LatestSchemaVersion
        ));
        Ok(self.0.read().unwrap().schema_version)
    }

    async fn prepare_get_indexes(
        &self,
        session: &Self::Session,
    ) -> anyhow::Result<Self::Statement> {
        assert!(session.is_compatible_with_db(self));
        Ok(Statement {
            session: session.clone(),
            statement: StatementInternal::GetIndexes,
        })
    }

    async fn execute_get_indexes(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<DbIndexInfo>> + Send + 'static> {
        assert!(session.is_compatible_with_db(self));
        assert!(statement.is_compatible_with(session));
        assert!(matches!(statement.statement, StatementInternal::GetIndexes));
        if self
            .0
            .read()
            .unwrap()
            .simulate_endless_get_indexes_processing
        {
            tokio::time::sleep(std::time::Duration::MAX).await;
        }
        let indexes = self
            .0
            .read()
            .unwrap()
            .keyspaces
            .values()
            .flat_map(|keyspace| {
                keyspace.indexes.values().map(|index| {
                    Ok(DbIndexInfo {
                        keyspace_name: index.metadata.keyspace_name.clone(),
                        table_name: index.metadata.table_name.clone(),
                        index_name: index.metadata.index_name.clone(),
                        options: options_from_metadata(&index.metadata),
                    })
                })
            })
            .collect_vec();
        Ok(stream::iter(indexes))
    }

    async fn prepare_get_index_target_type(
        &self,
        session: &Self::Session,
    ) -> anyhow::Result<Self::Statement> {
        assert!(session.is_compatible_with_db(self));
        Ok(Statement {
            session: session.clone(),
            statement: StatementInternal::GetIndexTargetType,
        })
    }

    async fn execute_get_index_target_type(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        keyspace_name: &KeyspaceName,
        table_name: &TableName,
        column_name: &ColumnName,
    ) -> anyhow::Result<Option<String>> {
        assert!(session.is_compatible_with_db(self));
        assert!(statement.is_compatible_with(session));
        assert!(matches!(
            statement.statement,
            StatementInternal::GetIndexTargetType
        ));
        Ok(self
            .0
            .read()
            .unwrap()
            .keyspaces
            .get(keyspace_name)
            .and_then(|keyspace| keyspace.tables.get(table_name))
            .and_then(|table| target_type_from_table(table, column_name)))
    }

    async fn prepare_get_index_options(
        &self,
        session: &Self::Session,
    ) -> anyhow::Result<Self::Statement> {
        assert!(session.is_compatible_with_db(self));
        Ok(Statement {
            session: session.clone(),
            statement: StatementInternal::GetIndexOptions,
        })
    }

    async fn execute_get_index_options(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        keyspace_name: &KeyspaceName,
        _: &TableName,
        index_name: &IndexName,
    ) -> anyhow::Result<Option<BTreeMap<String, String>>> {
        assert!(session.is_compatible_with_db(self));
        assert!(statement.is_compatible_with(session));
        assert!(matches!(
            statement.statement,
            StatementInternal::GetIndexOptions
        ));
        Ok(self
            .0
            .read()
            .unwrap()
            .keyspaces
            .get(keyspace_name)
            .and_then(|keyspace| keyspace.indexes.get(index_name))
            .map(|index| options_from_metadata(&index.metadata)))
    }

    async fn prepare_range_scan(
        &self,
        session: &Self::Session,
        index: &IndexMetadata,
    ) -> anyhow::Result<Self::Statement> {
        assert!(session.is_compatible_with_db(self));
        Ok(Statement {
            session: session.clone(),
            statement: StatementInternal::RangeScan {
                index_metadata: index.clone(),
            },
        })
    }

    async fn execute_range_scan(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        begin: Token,
        end: Token,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<DbRow>> + Send + 'static> {
        assert!(session.is_compatible_with_db(self));
        assert!(statement.is_compatible_with(session));
        let index_metadata = match &statement.statement {
            StatementInternal::RangeScan { index_metadata } => index_metadata,
            _ => panic!("expected RangeScan statement"),
        };
        Ok(ReceiverStream::new(
            self.0
                .write()
                .unwrap()
                .keyspaces
                .get_mut(&index_metadata.keyspace_name)
                .and_then(|keyspace| keyspace.indexes.get_mut(&index_metadata.index_name))
                .and_then(|index| {
                    index
                        .range_scans
                        .keys()
                        .find(|&key| *key >= begin && *key <= end)
                        .copied()
                        .and_then(|key| index.range_scans.get_mut(&key))
                })
                .map(|range_scan_fn| range_scan_fn())
                .unwrap_or_else(|| {
                    let (_tx, rx) = mpsc::channel(1);
                    rx
                }),
        ))
    }

    async fn prepare_fetch_vector(
        &self,
        session: &Self::Session,
        index: &IndexMetadata,
    ) -> anyhow::Result<Self::Statement> {
        assert!(session.is_compatible_with_db(self));
        Ok(Statement {
            session: session.clone(),
            statement: StatementInternal::FetchVector {
                index_metadata: index.clone(),
            },
        })
    }

    async fn execute_fetch_vector(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        primary_key: &PrimaryKey,
    ) -> anyhow::Result<Option<Vector>> {
        assert!(session.is_compatible_with_db(self));
        assert!(statement.is_compatible_with(session));
        let index_metadata = match &statement.statement {
            StatementInternal::FetchVector { index_metadata } => index_metadata,
            _ => panic!("expected FetchVector statement"),
        };
        let tx_fetch_vector = self
            .0
            .read()
            .unwrap()
            .keyspaces
            .get(&index_metadata.keyspace_name)
            .and_then(|keyspace| keyspace.indexes.get(&index_metadata.index_name))
            .and_then(|index| index.fetch_vector.clone())
            .expect("fetch_vector function should be set for the index");
        let (tx, rx) = oneshot::channel();
        let primary_key = primary_key.clone();
        tx_fetch_vector
            .send(FetchVector { primary_key, tx })
            .await
            .expect("failed to send fetch_vector request");
        rx.await.expect("failed to receive fetch_vector response")
    }

    async fn prepare_fetch_row(
        &self,
        session: &Self::Session,
        index: &IndexMetadata,
    ) -> anyhow::Result<Self::Statement> {
        assert!(session.is_compatible_with_db(self));
        Ok(Statement {
            session: session.clone(),
            statement: StatementInternal::FetchRow {
                index_metadata: index.clone(),
            },
        })
    }

    async fn execute_fetch_row(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        primary_key: &PrimaryKey,
    ) -> anyhow::Result<Option<DbRow>> {
        assert!(session.is_compatible_with_db(self));
        assert!(statement.is_compatible_with(session));
        let index_metadata = match &statement.statement {
            StatementInternal::FetchRow { index_metadata } => index_metadata,
            _ => panic!("expected FetchRow statement"),
        };
        let tx_fetch_row = self
            .0
            .read()
            .unwrap()
            .keyspaces
            .get(&index_metadata.keyspace_name)
            .and_then(|keyspace| keyspace.indexes.get(&index_metadata.index_name))
            .and_then(|index| index.fetch_row.clone())
            .expect("fetch_row function should be set for the index");
        let (tx, rx) = oneshot::channel();
        let primary_key = primary_key.clone();
        tx_fetch_row
            .send(FetchRow { primary_key, tx })
            .await
            .expect("failed to send fetch_row request");
        rx.await.expect("failed to receive fetch_row response")
    }

    fn cluster(&self, session: &Self::Session) -> Self::Cluster {
        assert!(session.is_compatible_with_db(self));
        Cluster::new(self.clone())
    }

    fn is_keyspace(&self, cluster: &Self::Cluster, keyspace_name: &KeyspaceName) -> bool {
        assert!(cluster.is_compatible_with(self));
        cluster
            .tables
            .iter()
            .any(|cluster_table| &cluster_table.keyspace_name == keyspace_name)
    }

    fn is_table(
        &self,
        cluster: &Self::Cluster,
        keyspace_name: &KeyspaceName,
        table_name: &TableName,
    ) -> bool {
        assert!(cluster.is_compatible_with(self));
        cluster.tables.iter().any(|cluster_table| {
            &cluster_table.keyspace_name == keyspace_name && &cluster_table.table_name == table_name
        })
    }

    fn is_cdc(
        &self,
        cluster: &Self::Cluster,
        keyspace_name: &KeyspaceName,
        table_name: &TableName,
    ) -> bool {
        assert!(cluster.is_compatible_with(self));
        // We assume that cdc is enabled for DbBasic
        cluster.tables.iter().any(|cluster_table| {
            &cluster_table.keyspace_name == keyspace_name && &cluster_table.table_name == table_name
        })
    }

    fn nr_shards(&self, cluster: &Self::Cluster) -> NonZeroUsize {
        assert!(cluster.is_compatible_with(self));
        cluster.db.0.read().unwrap().nr_shards
    }

    fn token_ring(&self, cluster: &Self::Cluster) -> impl Iterator<Item = Token> {
        assert!(cluster.is_compatible_with(self));
        cluster.token_ring.iter().copied()
    }

    fn table<'a>(
        &self,
        cluster: &'a Self::Cluster,
        keyspace_name: &KeyspaceName,
        table_name: &TableName,
    ) -> Option<&'a Self::Table> {
        assert!(cluster.is_compatible_with(self));
        cluster
            .tables
            .iter()
            .find(|table| &table.keyspace_name == keyspace_name && &table.table_name == table_name)
    }

    fn partition_key(&self, table: &Self::Table) -> impl Iterator<Item = ColumnName> {
        table.partition_key.iter().cloned()
    }

    fn clustering_key(&self, table: &Self::Table) -> impl Iterator<Item = ColumnName> {
        table.clustering_key.iter().cloned()
    }

    fn columns<'a>(
        &self,
        table: &'a Self::Table,
    ) -> impl Iterator<Item = (ColumnName, DbColumn)> + 'a {
        table.columns.iter().cloned()
    }

    fn column(&self, table: &Self::Table, column_name: &ColumnName) -> Option<DbColumn> {
        table
            .columns
            .iter()
            .find(|(name, _)| name == column_name)
            .map(|(_, col)| col.clone())
    }

    async fn cdc_log_reader(
        &self,
        session: Self::Session,
        config: CdcLogReaderConfig,
    ) -> anyhow::Result<(Self::CdcLogReader, RemoteHandle<anyhow::Result<()>>)> {
        assert!(session.is_compatible_with_db(self));
        CdcLogReader::new(self, config).await
    }

    fn stop_cdc_log_reader(&self, cdc_log_reader: Self::CdcLogReader) {
        cdc_log_reader.stop();
    }

    fn metrics(&self, _: &Self::Session) -> Self::Metrics {
        tracing::error!("unimplemented: metrics");
        unimplemented!()
    }

    fn total_connections(&self, _: &Self::Metrics) -> u64 {
        tracing::error!("unimplemented: total_connections");
        unimplemented!()
    }

    fn connection_timeouts(&self, _: &Self::Metrics) -> u64 {
        tracing::error!("unimplemented: connection_timeouts");
        unimplemented!()
    }

    #[cfg(feature = "in-progress")]
    fn make_in_progress_guard(&self, primary_key: &PrimaryKey) -> Option<mpsc::Sender<()>> {
        self.0
            .read()
            .unwrap()
            .make_in_progress_guard
            .as_ref()
            .and_then(|fun| fun(primary_key))
    }
}

fn process_db(db: &DbBasic, msg: Db, node_state: mpsc::Sender<NodeState>) {
    match msg {
        Db::GetDbIndex { metadata, tx } => tx
            .send(new_db_index(db.clone(), metadata, node_state.clone()))
            .map_err(|_| anyhow!("Db::GetDbIndex: unable to send response"))
            .unwrap(),

        Db::LatestSchemaVersion { tx } => tx
            .send(Ok(db.0.read().unwrap().schema_version))
            .map_err(|_| anyhow!("Db::LatestSchemaVersion: unable to send response"))
            .unwrap(),

        Db::GetIndexes { tx } => {
            if db.0.read().unwrap().simulate_endless_get_indexes_processing {
                tokio::spawn(async move {
                    let _ = tx;
                    tokio::time::sleep(std::time::Duration::MAX).await;
                });
            } else {
                let response = {
                    let db = db.0.read().unwrap();
                    let indexes = db
                        .keyspaces
                        .iter()
                        .flat_map(|(keyspace_name, keyspace)| {
                            keyspace
                                .indexes
                                .iter()
                                .map(|(index_name, index)| DbCustomIndex {
                                    keyspace: keyspace_name.clone(),
                                    index: index_name.clone(),
                                    table: index.metadata.table_name.clone(),
                                    primary_key_columns: keyspace
                                        .tables
                                        .get(&index.metadata.table_name)
                                        .map(|table| table.primary_keys.clone())
                                        .unwrap(),
                                    partition_key_count: NonZeroUsize::new(
                                        keyspace
                                            .tables
                                            .get(&index.metadata.table_name)
                                            .map(|table| table.partition_key_count)
                                            .unwrap(),
                                    )
                                    .unwrap(),
                                    target_columns: index.metadata.target_columns.clone(),
                                    partitioning: index.metadata.partitioning.clone(),
                                    filtering_columns: index.metadata.filtering_columns.clone(),
                                    alternator_attribute_types: index
                                        .metadata
                                        .alternator_attribute_types
                                        .clone(),
                                    kind: match index.metadata.kind {
                                        IndexKind::Vs(_) => DbIndexKind::VectorSearch,
                                        IndexKind::Fts(_) => DbIndexKind::FullTextSearch,
                                        IndexKind::Pattern(_) => DbIndexKind::Pattern,
                                    },
                                })
                        })
                        .collect();
                    (indexes, db.unsupported_indexes.clone())
                };
                tx.send(Ok(response))
                    .map_err(|_| anyhow!("Db::GetIndexes: unable to send response"))
                    .unwrap()
            }
        }
        Db::GetIndexVersion {
            keyspace,
            table: _,
            index,
            tx,
        } => {
            let db = db.0.read().unwrap();
            let version = db
                .keyspaces
                .get(&keyspace)
                .and_then(|keyspace| keyspace.indexes.get(&index))
                .map(|index| index.metadata.version.clone())
                .or_else(|| {
                    db.unsupported_indexes
                        .iter()
                        .any(|unsupported| {
                            unsupported.index.keyspace == keyspace
                                && unsupported.index.index == index
                        })
                        .then(|| IndexVersion::from(Uuid::new_v4()))
                });
            tx.send(Ok(version))
                .map_err(|_| anyhow!("Db::GetIndexVersion: unable to send response"))
                .unwrap()
        }

        Db::GetIndexTargetDimensions {
            keyspace,
            table,
            target_column,
            tx,
            ..
        } => tx
            .send(Ok(db
                .0
                .read()
                .unwrap()
                .keyspaces
                .get(&keyspace)
                .and_then(|keyspace| keyspace.tables.get(&table))
                .and_then(|table| table.dimensions.get(&target_column))
                .cloned()))
            .map_err(|_| anyhow!("Db::GetIndexTargetType: unable to send response"))
            .unwrap(),

        Db::GetVsIndexParams {
            keyspace,
            table: _,
            index,
            tx,
        } => tx
            .send(Ok(db
                .0
                .read()
                .unwrap()
                .keyspaces
                .get(&keyspace)
                .and_then(|keyspace| keyspace.indexes.get(&index))
                .and_then(|index| {
                    index.metadata.vs().map(|vs| {
                        (
                            vs.connectivity,
                            vs.expansion_add,
                            vs.expansion_search,
                            vs.space_type,
                            vs.quantization,
                        )
                    })
                })))
            .map_err(|_| anyhow!("Db::GetVsIndexParams: unable to send response"))
            .unwrap(),

        Db::GetFtsIndexParams {
            keyspace,
            table: _,
            index,
            tx,
        } => tx
            .send(Ok(db
                .0
                .read()
                .unwrap()
                .keyspaces
                .get(&keyspace)
                .and_then(|keyspace| keyspace.indexes.get(&index))
                .and_then(|index| index.metadata.fts().copied())))
            .map_err(|_| anyhow!("Db::GetFtsIndexParams: unable to send response"))
            .unwrap(),

        Db::GetPatternIndexParams {
            keyspace,
            table: _,
            index,
            tx,
        } => tx
            .send(Ok(db
                .0
                .read()
                .unwrap()
                .keyspaces
                .get(&keyspace)
                .and_then(|keyspace| keyspace.indexes.get(&index))
                .and_then(|index| index.metadata.pattern().copied())))
            .map_err(|_| anyhow!("Db::GetPatternIndexParams: unable to send response"))
            .unwrap(),

        Db::IsValidIndex { tx, .. } => tx
            .send(true)
            .map_err(|_| anyhow!("Db::IsValidIndex: unable to send response"))
            .unwrap(),

        Db::IsValidSchema { tx, .. } => tx
            .send(true)
            .map_err(|_| anyhow!("Db::IsValidSchema: unable to send response"))
            .unwrap(),
    }
}

pub(crate) fn new_db_index(
    mut db: DbBasic,
    metadata: IndexMetadata,
    node_state: mpsc::Sender<NodeState>,
) -> anyhow::Result<(mpsc::Sender<DbIndex>, RxIndexedRow)> {
    if db.0.read().unwrap().next_get_db_index_failed {
        db.0.write().unwrap().next_get_db_index_failed = false;
        bail!("get_db_index failed");
    }

    let (tx_index, mut rx_index) = mpsc::channel(10);
    let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
    let store_vectors = db.0.read().unwrap().store_vectors.is_enabled();
    let tx_embeddings = if store_vectors && metadata.vs().is_some() {
        record_streamed_vectors(db.clone(), tx_embeddings)
    } else {
        tx_embeddings
    };
    let fullscan_finished = Arc::new(AtomicBool::new(false));
    tokio::spawn({
        let fullscan_finished = fullscan_finished.clone();
        async move {
            let fullscan_fn = fullscan(&mut db, &metadata);
            node_state
                .send(NodeState::SendEvent(Event::FullScanStarted(
                    metadata.clone(),
                )))
                .await
                .unwrap();
            let fullscan = {
                let tx_embeddings = tx_embeddings.clone();
                let fullscan_finished = fullscan_finished.clone();
                async move {
                    if let Some(fullscan_fn) = fullscan_fn {
                        fullscan_fn(tx_embeddings).await;
                    }
                    fullscan_finished.store(true, Ordering::Release);
                }
                .shared()
            };
            while !rx_index.is_closed() {
                tokio::select! {
                    _ = fullscan.clone() => break,
                    msg = rx_index.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        spawn_process_db_index(
                            db.clone(),
                            metadata.clone(),
                            Arc::clone(&fullscan_finished),
                            msg
                        ).await;
                    }
                }
            }
            node_state
                .send(NodeState::SendEvent(Event::FullScanFinished(
                    metadata.clone(),
                )))
                .await
                .unwrap();

            let cdc_fn = cdc(&mut db, &metadata);
            let cdc = {
                let tx_embeddings = tx_embeddings.clone();
                async move {
                    if let Some(cdc_fn) = cdc_fn {
                        cdc_fn(tx_embeddings).await;
                    }
                }
                .shared()
            };
            while !rx_index.is_closed() {
                tokio::select! {
                    _ = cdc.clone() => break,
                    msg = rx_index.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        spawn_process_db_index(
                            db.clone(),
                            metadata.clone(),
                            Arc::clone(&fullscan_finished),
                            msg
                        ).await;
                    }
                }
            }

            while let Some(msg) = rx_index.recv().await {
                spawn_process_db_index(
                    db.clone(),
                    metadata.clone(),
                    Arc::clone(&fullscan_finished),
                    msg,
                )
                .await;
            }
            drop(tx_embeddings);
        }
    });
    Ok((tx_index, rx_embeddings))
}

fn record_streamed_vectors(db: DbBasic, tx: TxIndexedRow) -> TxIndexedRow {
    let (tx_recorded, mut rx_recorded) = mpsc::channel(10);
    tokio::spawn(async move {
        while let Some((row, in_progress)) = rx_recorded.recv().await {
            db.record_row(&row);
            if tx.send((row, in_progress)).await.is_err() {
                break;
            }
        }
    });
    tx_recorded
}

fn fullscan(db: &mut DbBasic, metadata: &IndexMetadata) -> Option<ScanFn> {
    db.0.write()
        .unwrap()
        .keyspaces
        .get_mut(&metadata.keyspace_name)
        .and_then(|keyspace| keyspace.indexes.get_mut(&metadata.index_name))
        .and_then(|index| index.fullscan_fn.take())
}

fn cdc(db: &mut DbBasic, metadata: &IndexMetadata) -> Option<ScanFn> {
    db.0.write()
        .unwrap()
        .keyspaces
        .get_mut(&metadata.keyspace_name)
        .and_then(|keyspace| keyspace.indexes.get_mut(&metadata.index_name))
        .and_then(|index| index.cdc_fn.take())
}

async fn spawn_process_db_index(
    db: DbBasic,
    metadata: IndexMetadata,
    fullscan_finished: Arc<AtomicBool>,
    msg: DbIndex,
) {
    tokio::spawn(async move {
        match msg {
            DbIndex::GetTableColumns { tx } => tx
                .send(
                    db.0.read()
                        .unwrap()
                        .keyspaces
                        .get(&metadata.keyspace_name)
                        .and_then(|keyspace| keyspace.tables.get(&metadata.table_name))
                        .map(|table| {
                            Arc::new(
                                table
                                    .columns
                                    .iter()
                                    .map(|(name, column)| {
                                        let ColumnType::Native(column) = column else {
                                            panic!("Expected native column type")
                                        };
                                        (name.clone(), column.clone())
                                    })
                                    .collect(),
                            )
                        })
                        .unwrap_or_default(),
                )
                .map_err(|_| anyhow!("DbIndex::GetPrimaryKeyColumns: unable to send response"))
                .unwrap(),

            DbIndex::FullScanProgress { tx } => tx
                .send({
                    let db = db.0.read().unwrap();
                    db.next_full_scan_progress.unwrap_or_else(|| {
                        if fullscan_finished.load(Ordering::Acquire) {
                            Progress::Done
                        } else {
                            Progress::InProgress(Percentage::try_from(0.0).unwrap())
                        }
                    })
                })
                .map_err(|_| anyhow!("DbIndex::GetTargetColumn: unable to send response"))
                .unwrap(),

            DbIndex::GetVectors { keys, tx } => tx
                .send(Ok(db.get_vectors(&keys)))
                .map_err(|_| anyhow!("DbIndex::GetVectors: unable to send response"))
                .unwrap(),
        }
    });
}
