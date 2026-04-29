/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use macros::ToEnumSchema;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use utoipa::PartialSchema;
use utoipa::ToSchema;
use utoipa::openapi::KnownFormat;
use utoipa::openapi::ObjectBuilder;
use utoipa::openapi::RefOr;
use utoipa::openapi::Schema;
use utoipa::openapi::SchemaFormat;
use utoipa::openapi::schema::Type;

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::Into,
    derive_more::AsRef,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
#[from(String, &String, &str)]
#[as_ref(str)]
/// Name of the column in a db table.
pub struct ColumnName(String);

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, derive_more::From)]
/// Dimensions of embeddings
pub struct Dimensions(NonZeroUsize);

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
/// Data type and precision used for storing and processing vectors in the index.
pub enum DataType {
    /// 32-bit single-precision IEEE 754 floating-point.
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

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    PartialOrd,
    serde::Deserialize,
    derive_more::Deref,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Into,
    utoipa::ToSchema,
    serde::Serialize,
)]
/// Distance between vectors measured using the distance function defined while creating the index.
pub struct Distance(f32);

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
/// Information about a vector index, such as keyspace, name and data type.
pub struct IndexInfo {
    pub keyspace: KeyspaceName,
    pub index: IndexName,
    pub data_type: DataType,
}

impl IndexInfo {
    pub fn new(keyspace: &str, index: &str) -> Self {
        IndexInfo {
            keyspace: String::from(keyspace).into(),
            index: String::from(index).into(),
            data_type: DataType::F32,
        }
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
    derive_more::Into,
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

#[derive(ToEnumSchema, serde::Deserialize, serde::Serialize, PartialEq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
/// Operational status of the vector index.
pub enum IndexStatus {
    /// The index has been discovered and is being initialized.
    Initializing,
    /// The index is performing the initial full scan of the underlying table to populate the index.
    Bootstrapping,
    /// The index has completed the initial table scan. It is now monitoring the database for changes.
    Serving,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct IndexStatusResponse {
    pub status: IndexStatus,
    pub count: usize,
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct InfoResponse {
    /// Information about the underlying search engine.
    pub engine: String,
    /// The name of the Vector Store indexing service.
    pub service: String,
    /// The version of the Vector Store indexing service.
    pub version: String,
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
    serde::Deserialize,
    serde::Serialize,
    utoipa::ToSchema,
)]
#[from(String, &String, &str)]
#[as_ref(str)]
/// A keyspace name in a db.
pub struct KeyspaceName(String);

#[derive(
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
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

#[derive(ToEnumSchema, serde::Deserialize, serde::Serialize, PartialEq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
/// Operational status of the Vector Store indexing service.
pub enum NodeStatus {
    /// The node is starting up.
    Initializing,
    /// The node is establishing a connection to ScyllaDB.
    ConnectingToDb,
    /// The node is discovering available vector indexes in ScyllaDB.
    Bootstrapping,
    /// The node has completed the initial database scan and built the indexes defined at that time. It is now monitoring the database for changes.
    Serving,
}

/// A filter used in ANN search requests.
#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema, Clone)]
pub struct PostIndexAnnFilter {
    /// A list of filter restrictions.
    pub restrictions: Vec<PostIndexAnnRestriction>,

    /// Indicates whether 'ALLOW FILTERING' was specified in the Cql query.
    #[serde(default)]
    pub allow_filtering: bool,
}

/// A filter restriction used in ANN search requests.
#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema, Clone)]
#[serde(tag = "type")]
pub enum PostIndexAnnRestriction {
    #[serde(rename = "==")]
    Eq { lhs: ColumnName, rhs: Value },
    #[serde(rename = "IN")]
    In { lhs: ColumnName, rhs: Vec<Value> },
    #[serde(rename = "<")]
    Lt { lhs: ColumnName, rhs: Value },
    #[serde(rename = "<=")]
    Lte { lhs: ColumnName, rhs: Value },
    #[serde(rename = ">")]
    Gt { lhs: ColumnName, rhs: Value },
    #[serde(rename = ">=")]
    Gte { lhs: ColumnName, rhs: Value },
    #[serde(rename = "()==()")]
    EqTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<Value>,
    },
    #[serde(rename = "()IN()")]
    InTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<Vec<Value>>,
    },
    #[serde(rename = "()<()")]
    LtTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<Value>,
    },
    #[serde(rename = "()<=()")]
    LteTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<Value>,
    },
    #[serde(rename = "()>()")]
    GtTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<Value>,
    },
    #[serde(rename = "()>=()")]
    GteTuple {
        lhs: Vec<ColumnName>,
        rhs: Vec<Value>,
    },
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct PostIndexAnnRequest {
    pub vector: Vector,
    pub filter: Option<PostIndexAnnFilter>,
    #[serde(default)]
    pub limit: Limit,
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct PostIndexAnnResponse {
    pub primary_keys: HashMap<ColumnName, Vec<Value>>,
    pub distances: Vec<Distance>,
    pub similarity_scores: Vec<SimilarityScore>,
}

#[derive(
    Copy, Clone, Debug, serde::Serialize, serde::Deserialize, derive_more::From, utoipa::ToSchema,
)]
#[from(f32)]
/// Similarity score between vectors derived from the distance. Higher score means more similar.
pub struct SimilarityScore(f32);

#[derive(
    Clone,
    Debug,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Into,
    utoipa::ToSchema,
)]
/// The vector to use for the Approximate Nearest Neighbor search. The format of data must match the data_type of the index.
pub struct Vector(Vec<f32>);
