/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Distance;
use crate::IndexId;
use crate::IndexName;
use crate::KeyspaceName;
use crate::Limit;
use crate::Progress;
use crate::Vector;
use crate::db_index::DbIndexExt;
use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::index::IndexExt;
use crate::index::validator;
use crate::info::Info;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use anyhow::bail;
use axum::Router;
use axum::extract;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header;
use axum::response;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use itertools::Itertools;
use macros::ToEnumSchema;
use prometheus::Encoder;
use prometheus::ProtobufEncoder;
use prometheus::TextEncoder;
use scylla::value::CqlValue;
use serde_json::Number;
use serde_json::Value;
use std::collections::HashMap;
use std::num::NonZero;
use std::sync::Arc;
use time::Date;
use time::OffsetDateTime;
use time::Time;
use time::format_description::well_known::Iso8601;
use time::format_description::well_known::iso8601::Config;
use time::format_description::well_known::iso8601::TimePrecision;
use tokio::sync::mpsc::Sender;
use tower_http::trace::TraceLayer;
use tracing::debug;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
     info(
        title = "ScyllaDB Vector Store API",
        description = "REST API for ScyllaDB Vector Store nodes. Provides capabilities for executing vector search queries, \
        managing indexes, and checking status of Vector Store nodes.",
        license(
            name = "LicenseRef-ScyllaDB-Source-Available-1.0"
        ),
    ),
    tags(
        (
            name = "scylla-vector-store-index",
            description = "Operations for managing ScyllaDB Vector Store indexes, including listing, counting, and searching."
        ),
        (
            name = "scylla-vector-store-info",
            description = "Endpoints providing general information and status about the ScyllaDB Vector Store service."
        )

    ),
    components(
        schemas(
            KeyspaceName,
            IndexName
        )
    ),
)]
// TODO: modify HTTP API after design
struct ApiDoc;

#[derive(Clone)]
struct RoutesInnerState {
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    node_state: Sender<NodeState>,
    index_engine_version: String,
}

pub(crate) fn new(
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    node_state: Sender<NodeState>,
    index_engine_version: String,
) -> Router {
    let state = RoutesInnerState {
        engine,
        metrics: metrics.clone(),
        node_state,
        index_engine_version,
    };
    let (router, api) = new_open_api_router();
    let router = router
        .route("/metrics", get(get_metrics))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    router.merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api))
}

pub fn api() -> utoipa::openapi::OpenApi {
    new_open_api_router().1
}

fn new_open_api_router() -> (Router<RoutesInnerState>, utoipa::openapi::OpenApi) {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .merge(
            OpenApiRouter::new()
                .routes(routes!(get_indexes))
                .routes(routes!(get_index_count))
                .routes(routes!(post_index_ann))
                .routes(routes!(get_info))
                .routes(routes!(get_status)),
        )
        .split_for_parts()
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema, PartialEq, Debug)]
/// Data type and precision used for storing and processing vectors in the index.
pub enum DataType {
    F32,
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema, PartialEq, Debug)]
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

#[utoipa::path(
    get,
    path = "/api/v1/indexes",
    tag = "scylla-vector-store-index",
    description = "Returns the list of indexes managed by the Vector Store node. \
    The list includes indexes in any state (initializing, available/built, destroying). \
    Due to synchronization delays, it may temporarily differ from the list of vector indexes inside ScyllaDB.",
    responses(
        (
            status = 200,
            description = "Successful operation. Returns an array of index information representing all indexes managed by the Vector Store.",
            body = [IndexInfo]
        )
    )
)]
async fn get_indexes(State(state): State<RoutesInnerState>) -> Response {
    let indexes: Vec<_> = state
        .engine
        .get_index_ids()
        .await
        .iter()
        .map(|id| IndexInfo {
            keyspace: id.keyspace(),
            index: id.index(),
            data_type: DataType::F32, // currently the only supported data type by Vector Store
        })
        .collect();
    (StatusCode::OK, response::Json(indexes)).into_response()
}

/// A human-readable description of the error that occurred.
#[derive(utoipa::ToSchema)]
struct ErrorMessage(#[allow(dead_code)] String);

#[utoipa::path(
    get,
    path = "/api/v1/indexes/{keyspace}/{index}/count",
    tag = "scylla-vector-store-index",
    description = "Returns the number of vectors indexed by a specific vector index. \
    Reflects only available vectors and excludes any 'tombstones' \
    (elements marked for deletion but still present in the index structure).",
    params(
        ("keyspace" = KeyspaceName, Path, description = "The name of the ScyllaDB keyspace containing the vector index."),
        ("index" = IndexName, Path, description = "The name of the ScyllaDB vector index within the specified keyspace to count vectors for.")
    ),
    responses(
        (
            status = 200,
            description = "Successful count operation. Returns the total number of vectors currently stored in the index.",
            body = usize,
            content_type = "application/json"
        ),
        (
            status = 404,
            description = "Index not found. Possible causes: index does not exist, or is not discovered yet.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 500,
            description = "Error while counting vectors. Possible causes: internal error, or issues accessing the database.",
            content_type = "application/json",
            body = ErrorMessage
        )
    )
)]
async fn get_index_count(
    State(state): State<RoutesInnerState>,
    Path((keyspace, index)): Path<(KeyspaceName, IndexName)>,
) -> Response {
    let Some((index, _)) = state
        .engine
        .get_index(IndexId::new(&keyspace, &index))
        .await
    else {
        let msg = format!("missing index: {keyspace}/{index}");
        debug!("get_index_count: {msg}");
        return (StatusCode::NOT_FOUND, msg).into_response();
    };
    match index.count().await {
        Err(err) => {
            let msg = format!("index.count request error: {err}");
            debug!("get_index_count: {msg}");
            (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }

        Ok(count) => (StatusCode::OK, response::Json(count)).into_response(),
    }
}

async fn get_metrics(
    State(state): State<RoutesInnerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    for (keyspace_str, index_name_str) in state.metrics.take_dirty_indexes() {
        let keyspace = KeyspaceName::from(keyspace_str);
        let index_name = IndexName::from(index_name_str);
        let id = IndexId::new(&keyspace, &index_name);
        if let Some((index, _)) = state.engine.get_index(id).await {
            if let Ok(count) = index.count().await {
                state
                    .metrics
                    .size
                    .with_label_values(&[keyspace.as_ref().as_str(), index_name.as_ref().as_str()])
                    .set(count as f64);
            }
        }
    }
    let metric_families = state.metrics.registry.gather();

    // Decide which encoder and content-type to use
    let use_protobuf = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("application/vnd.google.protobuf"));

    let (content_type, buffer): (&'static str, Vec<u8>) = if use_protobuf {
        let mut buf = Vec::new();
        ProtobufEncoder::new()
            .encode(&metric_families, &mut buf)
            .ok();
        (
            "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited",
            buf,
        )
    } else {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&metric_families, &mut buf).ok();
        ("text/plain; version=0.0.4; charset=utf-8", buf)
    };

    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));

    (StatusCode::OK, response_headers, buffer)
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct PostIndexAnnRequest {
    #[serde(alias = "embedding")]
    pub vector: Vector,
    #[serde(default)]
    pub limit: Limit,
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct PostIndexAnnResponse {
    pub primary_keys: HashMap<ColumnName, Vec<Value>>,
    pub distances: Vec<Distance>,
}

#[utoipa::path(
    post,
    path = "/api/v1/indexes/{keyspace}/{index}/ann",
    tag = "scylla-vector-store-index",
    description = "Performs an Approximate Nearest Neighbor (ANN) search using the specified index. \
Returns the vectors most similar to the provided vector. \
The maximum number of results is controlled by the optional 'limit' parameter in the payload. \
The similarity metric is determined at index creation and cannot be changed per query.",
    params(
        ("keyspace" = KeyspaceName, Path, description = "The name of the ScyllaDB keyspace containing the vector index."),
        ("index" = IndexName, Path, description = "The name of the ScyllaDB vector index within the specified keyspace to perform the search on.")
    ),
    request_body = PostIndexAnnRequest,
    responses(
        (
            status = 200,
            description = "Successful ANN search. Returns a list of primary keys and their corresponding distances for the most similar vectors found.",
            body = PostIndexAnnResponse
        ),
        (
            status = 400,
            description = "Bad request. Possible causes: invalid vector size, malformed input, or missing required fields.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 404,
            description = "Index not found. Possible causes: index does not exist, or is not discovered yet.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 500,
            description = "Error while searching vectors. Possible causes: internal error, or search engine issues.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 503,
            description = "Service Unavailable. Indicates that a full scan of the index is in progress and the search cannot be performed at this time.",
            content_type = "application/json",
            body = ErrorMessage
        )
    )
)]
async fn post_index_ann(
    State(state): State<RoutesInnerState>,
    Path((keyspace, index)): Path<(KeyspaceName, IndexName)>,
    extract::Json(request): extract::Json<PostIndexAnnRequest>,
) -> Response {
    // Start timing
    let timer = state
        .metrics
        .latency
        .with_label_values(&[keyspace.as_ref().as_str(), index.as_ref().as_str()])
        .start_timer();

    let Some((index, db_index)) = state
        .engine
        .get_index(IndexId::new(&keyspace, &index))
        .await
    else {
        timer.observe_duration();
        let msg = format!("missing index: {keyspace}/{index}");
        debug!("post_index_ann: {msg}");
        return (StatusCode::NOT_FOUND, msg).into_response();
    };

    let scan_progress = db_index.full_scan_progress().await;

    if let Progress::InProgress(percentage) = scan_progress {
        let msg = format!(
            "Full scan is in progress, percentage: {:.2}%",
            percentage.get()
        );
        debug!("post_index_ann: {msg}");
        return (StatusCode::SERVICE_UNAVAILABLE, msg).into_response();
    }

    let search_result = index.ann(request.vector, request.limit).await;
    // Record duration in Prometheus
    timer.observe_duration();

    match search_result {
        Err(err) => match err.downcast_ref::<validator::Error>() {
            Some(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
            None => {
                let msg = format!("index.ann request error: {err}");
                debug!("post_index_ann: {msg}");
                (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
            }
        },
        Ok((primary_keys, distances)) => {
            if primary_keys.len() != distances.len() {
                let msg = format!(
                    "wrong size of an ann response: number of primary_keys = {}, number of distances = {}",
                    primary_keys.len(),
                    distances.len()
                );
                debug!("post_index_ann: {msg}");
                (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
            } else {
                let primary_key_columns = db_index.get_primary_key_columns().await;
                let primary_keys: anyhow::Result<_> = primary_key_columns
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(idx_column, column)| {
                        let primary_keys: anyhow::Result<_> = primary_keys
                            .iter()
                            .map(|primary_key| {
                                if primary_key.0.len() != primary_key_columns.len() {
                                    bail!(
                                        "wrong size of a primary key: {}, {}",
                                        primary_key_columns.len(),
                                        primary_key.0.len()
                                    );
                                }
                                Ok(primary_key)
                            })
                            .map_ok(|primary_key| primary_key.0[idx_column].clone())
                            .map_ok(to_json)
                            .collect();
                        primary_keys.map(|primary_keys| (column, primary_keys))
                    })
                    .collect();

                match primary_keys {
                    Err(err) => {
                        debug!("post_index_ann: {err}");
                        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
                    }

                    Ok(primary_keys) => (
                        StatusCode::OK,
                        response::Json(PostIndexAnnResponse {
                            primary_keys,
                            distances,
                        }),
                    )
                        .into_response(),
                }
            }
        }
    }
}

fn to_json(value: CqlValue) -> Value {
    match value {
        CqlValue::Ascii(value) => Value::String(value),
        CqlValue::Text(value) => Value::String(value),

        CqlValue::Boolean(value) => Value::Bool(value),

        CqlValue::Double(value) => {
            Value::Number(Number::from_f64(value).expect("CqlValue::Double should be finite"))
        }
        CqlValue::Float(value) => {
            Value::Number(Number::from_f64(value.into()).expect("CqlValue::Float should be finite"))
        }

        CqlValue::Int(value) => Value::Number(value.into()),
        CqlValue::BigInt(value) => Value::Number(value.into()),
        CqlValue::SmallInt(value) => Value::Number(value.into()),
        CqlValue::TinyInt(value) => Value::Number(value.into()),

        CqlValue::Uuid(value) => Value::String(value.into()),
        CqlValue::Timeuuid(value) => Value::String((*value.as_ref()).into()),

        CqlValue::Date(value) => Value::String(
            TryInto::<Date>::try_into(value)
                .expect("CqlValue::Date should be correct")
                .format(&Iso8601::DATE)
                .expect("Date should be correct"),
        ),
        CqlValue::Time(value) => Value::String(
            TryInto::<Time>::try_into(value)
                .expect("CqlValue::Time should be correct")
                .format(&Iso8601::TIME)
                .expect("Time should be correct"),
        ),
        CqlValue::Timestamp(value) => Value::String(
            TryInto::<OffsetDateTime>::try_into(value)
                .expect("CqlValue::Timestamp should be correct")
                .format({
                    const CONFIG: u128 = Config::DEFAULT
                        .set_time_precision(TimePrecision::Second {
                            decimal_digits: NonZero::new(3),
                        })
                        .encode();
                    &Iso8601::<CONFIG>
                })
                .expect("OffsetDateTime should be correct"),
        ),

        _ => unimplemented!(),
    }
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct InfoResponse {
    /// Information about the underlying search engine.
    pub engine: String,
    pub service: String,
    pub version: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/info",
    tag = "scylla-vector-store-info",
    description = "Returns information about the Vector Store service serving this API.",
    responses(
        (status = 200, description = "Vector Store service information.", body = InfoResponse)
    )
)]
async fn get_info(State(state): State<RoutesInnerState>) -> response::Json<InfoResponse> {
    response::Json(InfoResponse {
        version: Info::version().to_string(),
        service: Info::name().to_string(),
        engine: state.index_engine_version.clone(),
    })
}

#[derive(ToEnumSchema, serde::Deserialize, serde::Serialize, PartialEq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
/// Operational status of the Vector Store node.
pub enum Status {
    /// The node is starting up.
    Initializing,
    /// The node is establishing a connection to ScyllaDB.
    ConnectingToDb,
    /// The node is discovering available vector indexes in ScyllaDB.
    Bootstrapping,
    /// The node is making initial indexes discovering and vector data indexing.
    Serving,
}

impl From<crate::node_state::Status> for Status {
    fn from(status: crate::node_state::Status) -> Self {
        match status {
            crate::node_state::Status::Initializing => Status::Initializing,
            crate::node_state::Status::ConnectingToDb => Status::ConnectingToDb,
            crate::node_state::Status::IndexingEmbeddings => Status::Bootstrapping,
            crate::node_state::Status::DiscoveringIndexes => Status::Bootstrapping,
            crate::node_state::Status::Serving => Status::Serving,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/status",
    tag = "scylla-vector-store-info",
    description = "Returns the current operational status of the Vector Store node.",
    responses(
        (status = 200, description = "Successful operation. Returns the current operational status of the Vector Store node.", body = Status),
    )
)]
async fn get_status(State(state): State<RoutesInnerState>) -> Response {
    (
        StatusCode::OK,
        response::Json(Status::from(state.node_state.get_status().await)),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn to_json_conversion() {
        assert_eq!(
            to_json(CqlValue::Ascii("ascii".to_string())),
            Value::String("ascii".to_string())
        );
        assert_eq!(
            to_json(CqlValue::Text("text".to_string())),
            Value::String("text".to_string())
        );

        assert_eq!(to_json(CqlValue::Boolean(true)), Value::Bool(true));

        assert_eq!(
            to_json(CqlValue::Double(101.)),
            Value::Number(Number::from_f64(101.).unwrap())
        );
        assert_eq!(
            to_json(CqlValue::Float(201.)),
            Value::Number(Number::from_f64(201.).unwrap())
        );

        assert_eq!(to_json(CqlValue::Int(10)), Value::Number(10.into()));
        assert_eq!(to_json(CqlValue::BigInt(20)), Value::Number(20.into()));
        assert_eq!(to_json(CqlValue::SmallInt(30)), Value::Number(30.into()));
        assert_eq!(to_json(CqlValue::TinyInt(40)), Value::Number(40.into()));

        let uuid = Uuid::new_v4();
        assert_eq!(to_json(CqlValue::Uuid(uuid)), Value::String(uuid.into()));
        let uuid = Uuid::new_v4();
        assert_eq!(
            to_json(CqlValue::Timeuuid(uuid.into())),
            Value::String(uuid.into())
        );

        let now = OffsetDateTime::now_utc();
        assert_eq!(
            to_json(CqlValue::Date(now.date().into())),
            Value::String(now.date().format(&Iso8601::DATE).unwrap())
        );
        assert_eq!(
            to_json(CqlValue::Time(now.time().into())),
            Value::String(now.format(&Iso8601::TIME).unwrap())
        );
        assert_eq!(
            to_json(CqlValue::Timestamp(now.into())),
            Value::String(
                // truncate microseconds
                now.format({
                    const CONFIG: u128 = Config::DEFAULT
                        .set_time_precision(TimePrecision::Second {
                            decimal_digits: NonZero::new(3),
                        })
                        .encode();
                    &Iso8601::<CONFIG>
                })
                .unwrap()
            )
        );
    }

    #[test]
    fn status_conversion() {
        assert_eq!(
            Status::from(crate::node_state::Status::Initializing),
            Status::Initializing
        );
        assert_eq!(
            Status::from(crate::node_state::Status::ConnectingToDb),
            Status::ConnectingToDb
        );
        assert_eq!(
            Status::from(crate::node_state::Status::IndexingEmbeddings),
            Status::Bootstrapping
        );
        assert_eq!(
            Status::from(crate::node_state::Status::DiscoveringIndexes),
            Status::Bootstrapping
        );
        assert_eq!(
            Status::from(crate::node_state::Status::Serving),
            Status::Serving
        );
    }
}
