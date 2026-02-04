/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

pub mod db;
pub mod db_index;
mod engine;
pub mod httproutes;
mod httpserver;
mod index;
mod info;
mod internals;
mod memory;
mod metrics;
mod monitor_indexes;
mod monitor_items;
pub mod node_state;

use crate::internals::Internals;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
use db::Db;
pub use httproutes::DataType;
pub use httproutes::IndexInfo;
use index::factory;
pub use index::factory::IndexFactory;
use scylla::cluster::metadata::ColumnType;
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;
use scylla::serialize::writers::WrittenCellProof;
use scylla::value::CqlValue;
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::signal;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::task;
use utoipa::PartialSchema;
use utoipa::ToSchema;
use utoipa::openapi::KnownFormat;
use utoipa::openapi::ObjectBuilder;
use utoipa::openapi::RefOr;
use utoipa::openapi::Schema;
use utoipa::openapi::SchemaFormat;
use utoipa::openapi::schema::Type;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Config {
    pub vector_store_addr: std::net::SocketAddr,
    pub scylladb_uri: String,
    pub threads: Option<usize>,
    pub memory_limit: Option<u64>,
    pub memory_usage_check_interval: Option<Duration>,
    pub opensearch_addr: Option<String>,
    pub credentials: Option<Credentials>,
    pub usearch_simulator: Option<Vec<Duration>>,
    pub cql_keepalive_interval: Option<Duration>,
    pub cql_keepalive_timeout: Option<Duration>,
    pub cql_tcp_keepalive_interval: Option<Duration>,
    pub cql_uri_translation_map: Option<HashMap<SocketAddr, SocketAddr>>,
    pub cdc_safety_interval: Option<Duration>,
    pub cdc_sleep_interval: Option<Duration>,
    pub disable_colors: bool,
    pub tls_cert_path: Option<std::path::PathBuf>,
    pub tls_key_path: Option<std::path::PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            vector_store_addr: "127.0.0.1:6080".parse().unwrap(),
            scylladb_uri: "127.0.0.1:9042".to_string(),
            threads: None,
            memory_limit: None,
            memory_usage_check_interval: None,
            opensearch_addr: None,
            credentials: None,
            usearch_simulator: None,
            disable_colors: false,
            tls_cert_path: None,
            tls_key_path: None,
            cql_keepalive_interval: None,
            cql_keepalive_timeout: None,
            cql_tcp_keepalive_interval: None,
            cql_uri_translation_map: None,
            cdc_safety_interval: None,
            cdc_sleep_interval: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Credentials {
    pub username: Option<String>,
    pub password: Option<secrecy::SecretString>,
    pub certificate_path: Option<std::path::PathBuf>,
}

#[derive(
    Clone, Hash, Eq, PartialEq, Debug, PartialOrd, Ord, derive_more::Display, derive_more::AsRef,
)]
pub struct IndexId(String);

impl IndexId {
    pub fn new(keyspace: &KeyspaceName, index: &IndexName) -> Self {
        Self(format!("{}.{}", keyspace.0, index.0))
    }

    pub fn keyspace(&self) -> KeyspaceName {
        self.0.split_once('.').unwrap().0.to_string().into()
    }

    pub fn index(&self) -> IndexName {
        self.0.split_once('.').unwrap().1.to_string().into()
    }
}

impl SerializeValue for IndexId {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        <String as SerializeValue>::serialize(&self.0, typ, writer)
    }
}

#[derive(
    Clone,
    Debug,
    Eq,
    Hash,
    PartialEq,
    derive_more::AsRef,
    derive_more::Display,
    derive_more::From,
    serde::Deserialize,
    serde::Serialize,
    utoipa::ToSchema,
)]
#[from(String, &String, &str)]
#[as_ref(str)]
/// A keyspace name in a db.
pub struct KeyspaceName(String);

impl SerializeValue for KeyspaceName {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        <String as SerializeValue>::serialize(&self.0, typ, writer)
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    utoipa::ToSchema,
)]
#[from(String, &String, &str)]
#[as_ref(str)]
/// A name of the vector index in a db.
pub struct IndexName(String);

impl SerializeValue for IndexName {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        <String as SerializeValue>::serialize(&self.0, typ, writer)
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    utoipa::ToSchema,
)]
#[from(String, &String, &str)]
#[as_ref(str)]
/// A table name of the table with vectors in a db
pub struct TableName(String);

impl SerializeValue for TableName {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        <String as SerializeValue>::serialize(&self.0, typ, writer)
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    utoipa::ToSchema,
)]
#[from(String, &String, &str)]
#[as_ref(str)]
/// Name of the column in a db table.
pub struct ColumnName(String);

impl SerializeValue for ColumnName {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        <String as SerializeValue>::serialize(&self.0, typ, writer)
    }
}

#[derive(Clone, Debug, derive_more::From)]
pub struct PrimaryKey(Vec<CqlValue>);

impl Hash for PrimaryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        format!("{self:?}").hash(state);
    }
}

impl PartialEq for PrimaryKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for PrimaryKey {}

#[derive(
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    utoipa::ToSchema,
    PartialEq,
    PartialOrd,
    Copy,
)]
/// Distance between vectors measured using the distance function defined while creating the index.
pub struct Distance(f32);

impl SerializeValue for Distance {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        <f32 as SerializeValue>::serialize(&self.0, typ, writer)
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
)]
/// Dimensions of embeddings
pub struct Dimensions(NonZeroUsize);

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
)]
/// Limit number of neighbors per graph node
pub struct Connectivity(usize);

impl Default for Connectivity {
    fn default() -> Self {
        Self(16)
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// Control the recall of indexing
pub struct ExpansionAdd(usize);

impl Default for ExpansionAdd {
    fn default() -> Self {
        Self(128)
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// Control the quality of the search
pub struct ExpansionSearch(usize);

impl Default for ExpansionSearch {
    fn default() -> Self {
        Self(64)
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Default,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    utoipa::ToSchema,
)]
pub enum SpaceType {
    Euclidean,
    #[default]
    Cosine,
    DotProduct,
}

impl FromStr for SpaceType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "EUCLIDEAN" => Ok(Self::Euclidean),
            "COSINE" => Ok(Self::Cosine),
            "DOT_PRODUCT" => Ok(Self::DotProduct),
            _ => Err(anyhow::anyhow!("Unknown space type: {s}")),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default)]
/// Represents the quantization type for vectors.
///
/// Quantization is a process that reduces the precision of floating-point numbers,
/// which can lead to significant memory savings.
pub enum Quantization {
    /// 32-bit single-precision IEEE 754 floating-point.
    #[default]
    F32,
    /// 16-bit standard half-precision floating-point (IEEE 754).
    F16,
    /// 16-bit "Brain" floating-point.
    BF16,
    /// 8-bit signed integer.
    I8,
    /// 1-bit binary value (packed 8 per byte).
    B1,
}

impl FromStr for Quantization {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "F32" => Ok(Self::F32),
            "F16" => Ok(Self::F16),
            "BF16" => Ok(Self::BF16),
            "I8" => Ok(Self::I8),
            "B1" => Ok(Self::B1),
            _ => Err(anyhow::anyhow!("Unknown quantization type: {s}")),
        }
    }
}

#[derive(
    Clone,
    Debug,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    utoipa::ToSchema,
)]
/// The vector to use for the Approximate Nearest Neighbor search. The format of data must match the data_type of the index.
pub struct Vector(Vec<f32>);

#[derive(
    Clone,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::Display,
    derive_more::From,
)]
/// Limit the number of search result
pub struct Limit(NonZeroUsize);

impl ToSchema for Limit {
    fn name() -> Cow<'static, str> {
        Cow::Borrowed("Limit")
    }
}

impl PartialSchema for Limit {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::Integer)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32)))
            .into()
    }
}

impl Default for Limit {
    fn default() -> Self {
        Self(NonZeroUsize::new(1).unwrap())
    }
}

/// A restriction provided in a CQL query for filtering ANN search results.
#[derive(Debug)]
pub enum Restriction {
    Eq {
        lhs: ColumnName,
        rhs: CqlValue,
    },
    In {
        lhs: ColumnName,
        rhs: Vec<CqlValue>,
    },
    Lt {
        lhs: ColumnName,
        rhs: CqlValue,
    },
    Lte {
        lhs: ColumnName,
        rhs: CqlValue,
    },
    Gt {
        lhs: ColumnName,
        rhs: CqlValue,
    },
    Gte {
        lhs: ColumnName,
        rhs: CqlValue,
    },
    EqTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<CqlValue>,
    },
    InTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<Vec<CqlValue>>,
    },
    LtTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<CqlValue>,
    },
    LteTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<CqlValue>,
    },
    GtTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<CqlValue>,
    },
    GteTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<CqlValue>,
    },
}

/// A filter to apply to an ANN search. It contains restrictions from a CQL query and a flag to
/// indicate whether ALLOW FILTERING was specified in the CQL query.
#[derive(Debug)]
pub struct Filter {
    pub restrictions: Vec<Restriction>,
    pub allow_filtering: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, derive_more::From)]
pub struct IndexVersion(Uuid);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Information about an index
pub struct IndexMetadata {
    pub keyspace_name: KeyspaceName,
    pub index_name: IndexName,
    pub table_name: TableName,
    pub target_column: ColumnName,
    pub index_type: DbIndexType,
    pub filtering_columns: Arc<Vec<ColumnName>>,
    pub dimensions: Dimensions,
    pub connectivity: Connectivity,
    pub expansion_add: ExpansionAdd,
    pub expansion_search: ExpansionSearch,
    pub space_type: SpaceType,
    pub version: IndexVersion,
    pub quantization: Quantization,
}

impl IndexMetadata {
    pub fn id(&self) -> IndexId {
        IndexId::new(&self.keyspace_name, &self.index_name)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DbIndexType {
    Global,
    Local(Arc<Vec<ColumnName>>),
}

#[derive(Debug)]
pub struct DbCustomIndex {
    pub keyspace: KeyspaceName,
    pub index: IndexName,
    pub table: TableName,
    pub target_column: ColumnName,
    pub index_type: DbIndexType,
    pub filtering_columns: Arc<Vec<ColumnName>>,
}

impl DbCustomIndex {
    pub fn id(&self) -> IndexId {
        IndexId::new(&self.keyspace, &self.index)
    }
}

#[derive(Clone, Copy, Debug, derive_more::From, derive_more::AsRef)]
pub struct Timestamp(OffsetDateTime);

#[derive(Debug)]
pub struct DbEmbedding {
    pub primary_key: PrimaryKey,
    pub embedding: Option<Vector>,
    pub timestamp: Timestamp,
}

#[derive(Clone)]
/// Marker struct to indicate that an async operation is in progress.
#[allow(dead_code)]
pub struct AsyncInProgress(mpsc::Sender<()>);

pub fn block_on<Output>(threads: Option<usize>, f: impl AsyncFnOnce() -> Output) -> Output {
    if let Some(threads @ 1..) = threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .unwrap();
    }

    let mut builder = match threads {
        Some(0) | None => Builder::new_multi_thread(),
        Some(1) => Builder::new_current_thread(),
        Some(threads) => {
            let mut builder = Builder::new_multi_thread();
            builder.worker_threads(threads);
            builder
        }
    };
    builder
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { f().await })
}

pub async fn run(
    node_state: Sender<NodeState>,
    db_actor: Sender<Db>,
    internals: Sender<Internals>,
    index_factory: Box<dyn IndexFactory + Send + Sync>,
    config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<(impl Sized, SocketAddr)> {
    let metrics: Arc<Metrics> = Arc::new(metrics::Metrics::new());
    let index_engine_version = index_factory.index_engine_version();
    httpserver::new(
        node_state.clone(),
        engine::new(
            db_actor,
            index_factory,
            node_state,
            metrics.clone(),
            config_rx.clone(),
        )
        .await?,
        metrics,
        internals,
        index_engine_version,
        config_rx,
    )
    .await
}

pub async fn new_db(
    node_state: Sender<NodeState>,
    internals: Sender<Internals>,
    config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<Sender<Db>> {
    db::new(node_state, internals, config_rx).await
}

pub async fn new_node_state() -> Sender<NodeState> {
    node_state::new().await
}

pub fn new_internals() -> Sender<Internals> {
    internals::new()
}

// yield to let other tasks run before cpu-intensive processing, as it is CPU intensive and can
// block other tasks (increase tail latency)
async fn move_to_the_end_of_async_runtime_queue() {
    task::yield_now().await;
}

pub fn new_index_factory_usearch(
    config_tx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<Box<dyn IndexFactory + Send + Sync>> {
    // A semaphore that limits the concurrency of search operations, which are performed on Tokio threads.
    // This is a global concurrency limit for all indexes.
    let search_concurrency = Handle::current().metrics().num_workers();
    let tokio_semaphore = Arc::new(Semaphore::new(search_concurrency));
    // A semaphore that limits the concurrency of add/remove operations, which are performed on Rayon threads.
    // This is a global concurrency limit for all indexes.
    // The limit is set to 3 times the number of Rayon threads to ensure high throughput.
    const RAYON_CONCURRENCY_MULTIPLIER: usize = 3;
    let add_remove_concurrency =
        (rayon::current_num_threads() * RAYON_CONCURRENCY_MULTIPLIER).min(Semaphore::MAX_PERMITS);
    let rayon_semaphore = Arc::new(Semaphore::new(add_remove_concurrency));

    Ok(Box::new(index::usearch::new_usearch(
        tokio_semaphore,
        rayon_semaphore,
        config_tx,
    )?))
}

pub fn new_index_factory_opensearch(
    addr: String,
    config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<Box<dyn IndexFactory + Send + Sync>> {
    Ok(Box::new(index::opensearch::new_opensearch(
        &addr, config_rx,
    )?))
}

pub async fn wait_for_shutdown() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[derive(Clone)]
pub struct Percentage {
    value: f64,
}

impl Percentage {
    pub fn get(&self) -> f64 {
        self.value
    }
}

impl TryFrom<f64> for Percentage {
    type Error = String;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !(0.0..=100.0).contains(&value) {
            Err(format!(
                "Percentage must be between 0 and 100, got: {value}"
            ))
        } else {
            Ok(Self { value })
        }
    }
}

#[derive(Clone)]
pub enum Progress {
    Done,
    InProgress(Percentage),
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_percentage_from_f64() {
        assert_eq!(Percentage::try_from(50.0).unwrap().get(), 50.0);
        assert!(Percentage::try_from(-1.0).is_err());
        assert!(Percentage::try_from(101.0).is_err());
        assert!(Percentage::try_from(0.0).is_ok());
        assert!(Percentage::try_from(100.0).is_ok());
    }
}
