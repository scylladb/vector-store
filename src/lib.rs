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
mod metrics;
mod monitor_indexes;
mod monitor_items;
use db::Db;
use index::factory;
use index::factory::IndexFactory;
use scylla::cluster::metadata::ColumnType;
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;
use scylla::serialize::writers::WrittenCellProof;
use scylla::value::CqlValue;
use std::borrow::Cow;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Once;
use time::OffsetDateTime;
use tokio::signal;
use tokio::sync::mpsc::Sender;
use utoipa::PartialSchema;
use utoipa::ToSchema;
use utoipa::openapi::KnownFormat;
use utoipa::openapi::ObjectBuilder;
use utoipa::openapi::RefOr;
use utoipa::openapi::Schema;
use utoipa::openapi::SchemaFormat;
use utoipa::openapi::schema::Type;
use uuid::Uuid;

use crate::metrics::Metrics;

pub use httproutes::IndexInfo;
pub use httproutes::Quantization;

#[derive(Clone, derive_more::From, derive_more::Display)]
pub struct ScyllaDbUri(String);

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
    Clone, Debug, serde::Serialize, serde::Deserialize, derive_more::From, utoipa::ToSchema,
)]
/// Distance between embedding vectors measured using the distance function defined while creating the index.
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
    Default,
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

#[derive(
    Copy,
    Clone,
    Debug,
    Default,
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

#[derive(
    Copy,
    Clone,
    Debug,
    Default,
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

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    utoipa::ToSchema,
)]
pub enum SpaceType {
    Euclidean,
    Cosine,
    DotProduct,
}

impl Default for SpaceType {
    fn default() -> Self {
        Self::Cosine
    }
}

impl FromStr for SpaceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "EUCLIDEAN" => Ok(Self::Euclidean),
            "COSINE" => Ok(Self::Cosine),
            "DOT_PRODUCT" => Ok(Self::DotProduct),
            _ => Err(format!("Unknown space type: {s}")),
        }
    }
}

#[derive(
    Copy,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
)]
struct ParamM(usize);

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
/// The embedding vector to use for the Approximate Nearest Neighbor search. The format of data must match the quantization of the index.
pub struct Embedding(Vec<f32>);

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

#[derive(Clone, Debug, PartialEq, Eq, Hash, derive_more::From)]
pub struct IndexVersion(Uuid);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Information about an index
pub struct IndexMetadata {
    pub keyspace_name: KeyspaceName,
    pub index_name: IndexName,
    pub table_name: TableName,
    pub target_column: ColumnName,
    pub dimensions: Dimensions,
    pub connectivity: Connectivity,
    pub expansion_add: ExpansionAdd,
    pub expansion_search: ExpansionSearch,
    pub space_type: SpaceType,
    pub version: IndexVersion,
}

impl IndexMetadata {
    pub fn id(&self) -> IndexId {
        IndexId::new(&self.keyspace_name, &self.index_name)
    }
}

#[derive(Debug)]
pub struct DbCustomIndex {
    pub keyspace: KeyspaceName,
    pub index: IndexName,
    pub table: TableName,
    pub target_column: ColumnName,
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
    pub embedding: Option<Embedding>,
    pub timestamp: Timestamp,
}

#[derive(derive_more::From)]
pub struct HttpServerAddr(SocketAddr);

static INIT_RAYON: Once = Once::new();

pub async fn run(
    addr: HttpServerAddr,
    background_threads: Option<usize>,
    db_actor: Sender<Db>,
    index_factory: Box<dyn IndexFactory + Send + Sync>,
) -> anyhow::Result<(impl Sized, SocketAddr)> {
    if let Some(background_threads) = background_threads {
        INIT_RAYON.call_once(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(background_threads)
                .build_global()
                .expect("Failed to initialize Rayon global thread pool");
        });
    }
    let engine_actor = engine::new(db_actor, index_factory).await?;
    let metrics: Arc<Metrics> = Arc::new(metrics::Metrics::new());
    httpserver::new(addr, engine_actor, metrics).await
}

pub async fn new_db(uri: ScyllaDbUri) -> anyhow::Result<Sender<Db>> {
    db::new(uri).await
}

pub fn new_index_factory_usearch() -> anyhow::Result<Box<dyn IndexFactory + Send + Sync>> {
    Ok(Box::new(index::usearch::new_usearch()?))
}

pub fn new_index_factory_opensearch(
    addr: String,
) -> anyhow::Result<Box<dyn IndexFactory + Send + Sync>> {
    Ok(Box::new(index::opensearch::new_opensearch(&addr)?))
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
