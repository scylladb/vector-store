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
mod monitor_indexes;
mod monitor_items;

use db::Db;
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

#[derive(Clone, derive_more::From, derive_more::Display)]
pub struct ScyllaDbUri(String);

#[derive(
    Clone,
    Hash,
    Eq,
    PartialEq,
    Debug,
    serde::Deserialize,
    serde::Serialize,
    derive_more::Display,
    derive_more::AsRef,
    utoipa::ToSchema,
)]
/// DB's absolute index/table name (with keyspace) for which index should be build
#[schema(example = "vector_benchmark.vector_items")]
pub struct IndexId(String);

impl IndexId {
    pub fn new(keyspace: &KeyspaceName, index: &TableName) -> Self {
        Self(format!("{}.{}", keyspace.0, index.0))
    }

    pub fn keyspace(&self) -> KeyspaceName {
        self.0.split_once('.').unwrap().0.to_string().into()
    }

    pub fn index(&self) -> TableName {
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
    utoipa::ToSchema,
)]
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
/// Name of the column in a db table
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
/// Distance beetwen embeddings
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
/// Embeddings vector
pub struct Embeddings(Vec<f32>);

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
    pub index_name: TableName,
    pub table_name: TableName,
    pub target_column: ColumnName,
    pub dimensions: Dimensions,
    pub connectivity: Connectivity,
    pub expansion_add: ExpansionAdd,
    pub expansion_search: ExpansionSearch,
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
    pub index: TableName,
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
pub struct DbEmbeddings {
    pub primary_key: PrimaryKey,
    pub embeddings: Embeddings,
    pub timestamp: Timestamp,
}

#[derive(derive_more::From)]
pub struct HttpServerAddr(SocketAddr);

pub async fn run(
    addr: HttpServerAddr,
    background_threads: Option<usize>,
    db_actor: Sender<Db>,
) -> anyhow::Result<(impl Sized, SocketAddr)> {
    if let Some(background_threads) = background_threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(background_threads)
            .build_global()?;
    }
    let engine_actor = engine::new(db_actor).await?;
    httpserver::new(addr, engine_actor).await
}

pub async fn new_db(uri: ScyllaDbUri) -> anyhow::Result<Sender<Db>> {
    db::new(uri).await
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
