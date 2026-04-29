/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod config_manager;
pub mod db;
mod db_cdc;
pub mod db_index;
mod db_index_backend;
mod distance;
mod engine;
mod httproutes;
mod httpserver;
mod index;
mod index_key;
mod indexes;
mod info;
mod internals;
mod invariant_key;
mod memory;
mod metrics;
mod monitor_indexes;
mod monitor_items;
pub mod node_state;
mod partition_key;
mod primary_key;
mod similarity;
mod table;
mod timestamp;
mod vector;

pub use crate::config_manager::ConfigManager;
pub use crate::config_manager::load_config;
pub use crate::distance::Distance;
pub use crate::index_key::IndexKey;
pub use crate::info::Info;
use crate::internals::Internals;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
pub use crate::partition_key::PartitionKey;
pub use crate::primary_key::PrimaryKey;
pub use crate::similarity::SimilarityScore;
pub use crate::table::PartitionId;
pub use crate::table::PrimaryId;
pub use crate::timestamp::Timestamp;
use db::Db;
use index::factory;
pub use index::factory::IndexFactory;
use scylla::cluster::metadata::ColumnType;
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;
use scylla::serialize::writers::WrittenCellProof;
use scylla::value::CqlValue;
use scylla_cdc::CqlIdentifier;
use std::collections::HashMap;
use std::hash::Hash;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::signal;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::task;
use utoipa::openapi::OpenApi;
use uuid::Uuid;
pub use vector::Vector;

/// A CQL string literal that is always properly single-quoted when formatted
/// for use in CQL statements.
///
/// The inner value stores the already-quoted form of the string.
/// The [`Display`](std::fmt::Display) implementation outputs it in single quotes
/// with embedded single-quote characters escaped by doubling them
/// (`'` -> `''`), following the CQL grammar for string constants.
pub(crate) struct CqlLiteral {
    quoted: String,
}

impl CqlLiteral {
    /// Creates a new `CqlLiteral`, preserving the value exactly as given.
    ///
    /// The value will be single-quoted when formatted, with any embedded
    /// single quotes escaped by doubling.
    pub(crate) fn new(value: impl AsRef<str>) -> Self {
        let quoted = format!("'{}'", value.as_ref().replace('\'', "''"));
        Self { quoted }
    }
}

impl std::fmt::Display for CqlLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.quoted)
    }
}

pub(crate) struct KeyspaceIdentifier {
    cql_identifier: CqlIdentifier,
    is_alternator: bool,
}

impl<T: AsRef<str>> From<T> for KeyspaceIdentifier {
    fn from(value: T) -> Self {
        let value = value.as_ref();
        Self {
            cql_identifier: CqlIdentifier::new(value),
            is_alternator: value.starts_with("alternator_"),
        }
    }
}

impl KeyspaceIdentifier {
    pub(crate) fn is_alternator(&self) -> bool {
        self.is_alternator
    }
}

impl std::fmt::Display for KeyspaceIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cql_identifier.fmt(f)
    }
}

pub(crate) struct TableIdentifier {
    cql_identifier: CqlIdentifier,
}

impl<T: AsRef<str>> From<T> for TableIdentifier {
    fn from(value: T) -> Self {
        Self {
            cql_identifier: CqlIdentifier::new(value.as_ref()),
        }
    }
}

impl std::fmt::Display for TableIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cql_identifier.fmt(f)
    }
}

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
    pub cql_connection_timeout: Option<Duration>,
    pub cql_keepalive_interval: Option<Duration>,
    pub cql_keepalive_timeout: Option<Duration>,
    pub cql_tcp_keepalive_interval: Option<Duration>,
    pub cql_uri_translation_map: Option<HashMap<SocketAddr, SocketAddr>>,
    pub cdc_safety_interval: Option<Duration>,
    pub cdc_sleep_interval: Option<Duration>,
    pub cdc_fine_safety_interval: Option<Duration>,
    pub cdc_fine_sleep_interval: Option<Duration>,
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
            cql_connection_timeout: None,
            cql_keepalive_interval: None,
            cql_keepalive_timeout: None,
            cql_tcp_keepalive_interval: None,
            cql_uri_translation_map: None,
            cdc_safety_interval: None,
            cdc_sleep_interval: None,
            cdc_fine_safety_interval: None,
            cdc_fine_sleep_interval: None,
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
    Clone,
    Debug,
    Eq,
    Hash,
    PartialEq,
    derive_more::AsRef,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
#[from(String, &String, &str)]
#[as_ref(str)]
/// A keyspace name in a db.
pub struct KeyspaceName(String);

impl KeyspaceName {
    /// Returns true if this keyspace is backed by Alternator (DynamoDB-compatible API).
    /// Alternator keyspaces are prefixed with `alternator_`.
    fn is_alternator(&self) -> bool {
        self.0.starts_with("alternator_")
    }
}

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
    PartialOrd,
    Ord,
    derive_more::From,
    derive_more::AsRef,
    derive_more::Into,
    derive_more::Display,
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
    Clone, Debug, PartialEq, Eq, Hash, derive_more::From, derive_more::AsRef, derive_more::Display,
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
    Ord,
    PartialOrd,
    derive_more::From,
    derive_more::Into,
    derive_more::AsRef,
    derive_more::Display,
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

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
    PartialOrd,
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
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
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
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
)]
/// Control the quality of the search
pub struct ExpansionSearch(usize);

impl Default for ExpansionSearch {
    fn default() -> Self {
        Self(64)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default, derive_more::From)]
pub enum SpaceType {
    Euclidean,
    #[default]
    Cosine,
    DotProduct,
    Hamming,
}

impl FromStr for SpaceType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "EUCLIDEAN" => Ok(Self::Euclidean),
            "COSINE" => Ok(Self::Cosine),
            "DOT_PRODUCT" => Ok(Self::DotProduct),
            "HAMMING" => Ok(Self::Hamming),
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

#[derive(Clone, Copy, derive_more::AsRef, derive_more::Display, derive_more::From)]
/// Limit the number of search result
pub struct Limit(NonZeroUsize);

impl Default for Limit {
    fn default() -> Self {
        Self(NonZeroUsize::new(1).unwrap())
    }
}

/// A restriction provided in a CQL query for filtering ANN search results.
#[derive(Debug, PartialEq)]
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

impl IndexVersion {
    fn gregorian_ticks(&self) -> u64 {
        self.0
            .get_timestamp()
            .map(|ts| ts.to_gregorian().0)
            .unwrap_or(0)
    }
}

impl PartialOrd for IndexVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.gregorian_ticks().cmp(&other.gregorian_ticks())
    }
}

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
    pub fn key(&self) -> IndexKey {
        IndexKey::new(&self.keyspace_name, &self.index_name)
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
    pub fn key(&self) -> IndexKey {
        IndexKey::new(&self.keyspace, &self.index)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DbEmbedding {
    pub primary_key: PrimaryKey,
    pub embedding: Option<Vector>,
    pub timestamp: Timestamp,
}

#[derive(Clone, derive_more::From)]
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

pub fn openapi() -> OpenApi {
    httproutes::api()
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
