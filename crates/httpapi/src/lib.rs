/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use macros::ToEnumSchema;
use serde::Serialize;
use serde::Serializer;
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

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
/// Type of index, distinguishing between vector search and fulltext search indexes.
pub enum IndexType {
    /// Vector search index with its associated data type.
    Vector { data_type: DataType },
    /// Fulltext search index.
    Fulltext,
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
)]
/// Distance between vectors measured using the distance function defined while creating the index.
pub struct Distance(f32);

impl Serialize for Distance {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_saturated_f32(self.0, serializer)
    }
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
/// Information about an index, such as keyspace, name and type.
pub struct IndexInfo {
    pub keyspace: KeyspaceName,
    pub index: IndexName,
    #[serde(flatten)]
    pub index_type: IndexType,
}

impl IndexInfo {
    pub fn new(keyspace: &str, index: &str) -> Self {
        IndexInfo {
            keyspace: String::from(keyspace).into(),
            index: String::from(index).into(),
            index_type: IndexType::Vector {
                data_type: DataType::F32,
            },
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
    /// Progress of the initial full table scan that backfills the index,
    /// expressed as a percentage in the range `0.0..=100.0`. This reflects
    /// only how far the full scan has advanced; it is not a source of truth
    /// for the index state. The `status` field is authoritative: an index
    /// reporting `SERVING` is serving regardless of `build_progress`, which
    /// may not reach `100.0` (e.g. if some scan ranges failed). Defaults to
    /// `100.0` when omitted, so a new client talking to an older server that
    /// does not report the field falls back to relying solely on `status`.
    #[serde(default = "default_build_progress")]
    #[schema(minimum = 0, maximum = 100)]
    pub build_progress: f64,
}

fn default_build_progress() -> f64 {
    100.0
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
#[serde(tag = "reason", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IndexNotReadyReason {
    NodeBootstrapping,
    IndexBuilding { message: String },
}

/// The 503 response shared by the ANN and BM25 search endpoints when the index
/// is not ready to serve requests.
#[derive(utoipa::ToResponse)]
#[response(
    description = "Service Unavailable. The index is not ready to serve requests. \
The body is a JSON object with a 'reason' field: \
'NODE_BOOTSTRAPPING' when the node has not yet finished its startup sequence; \
'INDEX_BUILDING' (with a 'message' field) when the node is healthy but this index is still being constructed.",
    content_type = "application/json"
)]
pub struct IndexNotReadyResponse(#[allow(dead_code)] IndexNotReadyReason);

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

#[derive(Copy, Clone, Debug, serde::Deserialize, derive_more::From, utoipa::ToSchema)]
#[from(f32)]
/// Similarity score between vectors derived from the distance. Higher score means more similar.
pub struct SimilarityScore(f32);

impl Serialize for SimilarityScore {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_saturated_f32(self.0, serializer)
    }
}

fn serialize_saturated_f32<S>(value: f32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let value = if value == f32::INFINITY {
        f32::MAX
    } else if value == f32::NEG_INFINITY {
        f32::MIN
    } else {
        value
    };
    serializer.serialize_f32(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_finite_ann_values_serialize_as_f32_max() {
        let json = serde_json::to_value(PostIndexAnnResponse {
            primary_keys: HashMap::new(),
            distances: vec![
                Distance::from(f32::INFINITY),
                Distance::from(f32::NEG_INFINITY),
                Distance::from(1.5),
            ],
            similarity_scores: vec![
                SimilarityScore::from(f32::INFINITY),
                SimilarityScore::from(f32::NEG_INFINITY),
                SimilarityScore::from(0.5),
            ],
        })
        .unwrap();

        let distances: Vec<f32> = json["distances"]
            .as_array()
            .unwrap()
            .iter()
            .map(|distance| serde_json::from_value(distance.clone()).unwrap())
            .collect();
        let similarity_scores: Vec<f32> = json["similarity_scores"]
            .as_array()
            .unwrap()
            .iter()
            .map(|score| serde_json::from_value(score.clone()).unwrap())
            .collect();

        assert_eq!(distances, vec![f32::MAX, -f32::MAX, 1.5]);
        assert_eq!(similarity_scores, vec![f32::MAX, -f32::MAX, 0.5]);
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
    derive_more::Into,
    utoipa::ToSchema,
)]
/// The vector to use for the Approximate Nearest Neighbor search. The format of data must match the data_type of the index.
pub struct Vector(Vec<f32>);

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
/// Request body for full-text search.
pub struct PostIndexBm25Request {
    /// The text query to search for.
    pub query: String,
    #[serde(default)]
    pub limit: Limit,
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
/// Response for full-text search.
pub struct PostIndexBm25Response {
    pub primary_keys: HashMap<ColumnName, Vec<Value>>,
    pub scores: Vec<f32>,
}
