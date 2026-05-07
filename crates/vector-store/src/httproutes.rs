/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Filter;
use crate::IndexKey;
use crate::Progress;
use crate::Quantization;
use crate::Restriction;
use crate::SimilarityScore;
use crate::db_index::DbIndexExt;
use crate::distance;
use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::index::IndexExt;
use crate::index::validator;
use crate::indexes;
use crate::info::Info;
use crate::internals::Internals;
use crate::internals::InternalsExt;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use crate::vector;
use anyhow::anyhow;
use anyhow::bail;
use axum::Router;
use axum::extract;
use axum::extract::Path;
use axum::extract::State;
use axum::http::Extensions;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header;
use axum::response;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::put;
use axum_server_dual_protocol::Protocol;
use bigdecimal::BigDecimal;
use httpapi::DataType;
use httpapi::IndexInfo;
use itertools::Itertools;
use num_bigint::BigInt;
use prometheus::Encoder;
use prometheus::ProtobufEncoder;
use prometheus::TextEncoder;
use regex::Regex;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlDecimal;
use scylla::value::CqlTimeuuid;
use scylla::value::CqlValue;
use scylla::value::CqlVarint;
use serde_json::Number;
use serde_json::Value;
use std::collections::HashMap;
use std::num::NonZero;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::LazyLock;
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
        description = "REST API for ScyllaDB Vector Store indexing service. Provides capabilities for executing vector search queries, \
        managing indexes, and checking service status.",
        license(
            name = "LicenseRef-ScyllaDB-Source-Available-1.0"
        ),
        // version should be updated manually when there are changes in API
        version = "1.4.0"
    ),
    tags(
        (
            name = "scylla-vector-store-index",
            description = "Operations for managing ScyllaDB Vector Store indexes, including listing, counting, and searching."
        ),
        (
            name = "scylla-vector-store-info",
            description = "Endpoints providing general information and status about the ScyllaDB Vector Store indexing service."
        )

    ),
    components(
        schemas(
            httpapi::KeyspaceName,
            httpapi::IndexName
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
    internals: Sender<Internals>,
    index_engine_version: String,
    use_tls: bool,
}

pub(crate) fn new(
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    node_state: Sender<NodeState>,
    internals: Sender<Internals>,
    index_engine_version: String,
    use_tls: bool,
) -> Router {
    let state = RoutesInnerState {
        engine,
        metrics: metrics.clone(),
        node_state,
        internals,
        index_engine_version,
        use_tls,
    };
    let (router, api) = new_open_api_router();
    let router = router
        .route("/metrics", get(get_metrics))
        .nest("/api/internals", new_internals())
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
                .routes(routes!(get_index_status))
                .routes(routes!(post_index_ann))
                .routes(routes!(get_info))
                .routes(routes!(get_status)),
        )
        .split_for_parts()
}

impl From<crate::ColumnName> for httpapi::ColumnName {
    fn from(value: crate::ColumnName) -> Self {
        Self::from(<crate::ColumnName as Into<String>>::into(value))
    }
}

impl From<httpapi::ColumnName> for crate::ColumnName {
    fn from(value: httpapi::ColumnName) -> Self {
        Self::from(<httpapi::ColumnName as Into<String>>::into(value))
    }
}

impl From<crate::KeyspaceName> for httpapi::KeyspaceName {
    fn from(value: crate::KeyspaceName) -> Self {
        Self::from(<crate::KeyspaceName as Into<String>>::into(value))
    }
}

impl From<httpapi::KeyspaceName> for crate::KeyspaceName {
    fn from(value: httpapi::KeyspaceName) -> Self {
        Self::from(<httpapi::KeyspaceName as Into<String>>::into(value))
    }
}

impl From<crate::IndexName> for httpapi::IndexName {
    fn from(value: crate::IndexName) -> Self {
        Self::from(<crate::IndexName as Into<String>>::into(value))
    }
}

impl From<httpapi::IndexName> for crate::IndexName {
    fn from(value: httpapi::IndexName) -> Self {
        Self::from(<httpapi::IndexName as Into<String>>::into(value))
    }
}

impl From<Quantization> for DataType {
    fn from(quantization: Quantization) -> Self {
        match quantization {
            Quantization::F32 => DataType::F32,
            Quantization::F16 => DataType::F16,
            Quantization::BF16 => DataType::BF16,
            Quantization::I8 => DataType::I8,
            Quantization::B1 => DataType::B1,
        }
    }
}

impl From<httpapi::Limit> for crate::Limit {
    fn from(limit: httpapi::Limit) -> Self {
        Self::from(<httpapi::Limit as Into<NonZeroUsize>>::into(limit))
    }
}

impl From<httpapi::Vector> for vector::Vector {
    fn from(vector: httpapi::Vector) -> Self {
        Self::from(<httpapi::Vector as Into<Vec<f32>>>::into(vector))
    }
}

impl From<crate::SimilarityScore> for httpapi::SimilarityScore {
    fn from(value: crate::SimilarityScore) -> Self {
        Self::from(<crate::SimilarityScore as Into<f32>>::into(value))
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/indexes",
    tag = "scylla-vector-store-index",
    description = "Returns the list of indexes managed by the Vector Store indexing service. \
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
        .get_index_keys()
        .await
        .iter()
        .map(|(key, quantization)| IndexInfo {
            keyspace: key.keyspace().into(),
            index: key.index().into(),
            data_type: (*quantization).into(),
        })
        .collect();
    (StatusCode::OK, response::Json(indexes)).into_response()
}

/// A human-readable description of the error that occurred.
#[derive(utoipa::ToSchema)]
struct ErrorMessage(#[allow(dead_code)] String);

impl From<crate::node_state::IndexStatus> for httpapi::IndexStatus {
    fn from(status: crate::node_state::IndexStatus) -> Self {
        match status {
            crate::node_state::IndexStatus::Initializing => httpapi::IndexStatus::Initializing,
            crate::node_state::IndexStatus::FullScanning => httpapi::IndexStatus::Bootstrapping,
            crate::node_state::IndexStatus::Serving => httpapi::IndexStatus::Serving,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/indexes/{keyspace}/{index}/status",
    tag = "scylla-vector-store-index",
    description = "Retrieves the current operational status and vector count for a specific vector index. \
    The response includes the index's state and the total number of vectors currently indexed (excluding tombstoned or deleted entries). \
    This endpoint enables clients to monitor index readiness and data availability for search operations.",
    params(
        ("keyspace" = httpapi::KeyspaceName, Path, description = "The name of the ScyllaDB keyspace containing the vector index."),
        ("index" = httpapi::IndexName, Path, description = "The name of the ScyllaDB vector index within the specified keyspace to check status of.")
    ),
    responses(
        (
            status = 200,
            description = "Successful operation. Returns the current operational status of the specified vector index, including its state \
            and the total number of vectors currently indexed.",
            body = httpapi::IndexStatusResponse,
            content_type = "application/json",
            example = json!({
                "status": "SERVING",
                "count": 12345
            })
        ),
        (
            status = 404,
            description = "Index not found. Possible causes: index does not exist, or is not discovered yet.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 500,
            description = "Error while checking index state or counting vectors. Possible causes: internal error, or issues accessing the database.",
            content_type = "application/json",
            body = ErrorMessage
        )
    )
)]
async fn get_index_status(
    State(state): State<RoutesInnerState>,
    Path((keyspace_name, index_name)): Path<(httpapi::KeyspaceName, httpapi::IndexName)>,
) -> Response {
    let keyspace_name: crate::KeyspaceName = keyspace_name.into();
    let index_name: crate::IndexName = index_name.into();
    let index_key = IndexKey::new(&keyspace_name, &index_name);
    let Some((index, _)) = state.engine.get_index(index_key.clone()).await else {
        let msg = format!("missing index: {keyspace_name}.{index_name}");
        debug!("get_index_status: {msg}");
        return (StatusCode::NOT_FOUND, msg).into_response();
    };
    if let Some(index_status) = state
        .node_state
        .get_index_status(keyspace_name.as_ref(), index_name.as_ref())
        .await
    {
        match index.count(index_key).await {
            Err(err) => {
                let msg = format!("index.count request error: {err}");
                debug!("get_index_status: {msg}");
                (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
            }
            Ok(count) => (
                StatusCode::OK,
                response::Json(httpapi::IndexStatusResponse {
                    status: index_status.into(),
                    count,
                }),
            )
                .into_response(),
        }
    } else {
        let msg = format!("missing index status: {keyspace_name}.{index_name}");
        debug!("get_index_status: {msg}");
        (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
    }
}

async fn get_metrics(
    State(state): State<RoutesInnerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    for (keyspace_str, index_name_str) in state.metrics.take_dirty_indexes() {
        let keyspace = crate::KeyspaceName::from(keyspace_str);
        let index_name = crate::IndexName::from(index_name_str);
        let key = IndexKey::new(&keyspace, &index_name);
        if let Some((index, _)) = state.engine.get_index(key.clone()).await
            && let Ok(count) = index.count(key).await
        {
            state
                .metrics
                .size
                .with_label_values(&[keyspace.as_ref(), index_name.as_ref()])
                .set(count as f64);
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

fn restriction_columns(
    filter: &Option<httpapi::PostIndexAnnFilter>,
) -> (Vec<crate::ColumnName>, Vec<crate::ColumnName>) {
    let Some(filter) = filter else {
        return (Vec::new(), Vec::new());
    };
    let mut equality = Vec::new();
    let mut range = Vec::new();
    for r in &filter.restrictions {
        match r {
            httpapi::PostIndexAnnRestriction::Eq { lhs, .. }
            | httpapi::PostIndexAnnRestriction::In { lhs, .. } => {
                equality.push(lhs.as_ref().into())
            }
            httpapi::PostIndexAnnRestriction::Lt { lhs, .. }
            | httpapi::PostIndexAnnRestriction::Lte { lhs, .. }
            | httpapi::PostIndexAnnRestriction::Gt { lhs, .. }
            | httpapi::PostIndexAnnRestriction::Gte { lhs, .. } => range.push(lhs.as_ref().into()),
            httpapi::PostIndexAnnRestriction::EqTuple { lhs, .. }
            | httpapi::PostIndexAnnRestriction::InTuple { lhs, .. } => {
                equality.extend(lhs.iter().map(|name| name.as_ref().into()))
            }
            httpapi::PostIndexAnnRestriction::LtTuple { lhs, .. }
            | httpapi::PostIndexAnnRestriction::LteTuple { lhs, .. }
            | httpapi::PostIndexAnnRestriction::GtTuple { lhs, .. }
            | httpapi::PostIndexAnnRestriction::GteTuple { lhs, .. } => {
                range.extend(lhs.iter().map(|name| name.as_ref().into()))
            }
        }
    }
    (equality, range)
}

impl From<distance::DistanceValue> for httpapi::Distance {
    fn from(v: distance::DistanceValue) -> Self {
        Self::from(<distance::DistanceValue as Into<f32>>::into(v))
    }
}

impl From<distance::Distance> for httpapi::Distance {
    fn from(d: distance::Distance) -> Self {
        let val: distance::DistanceValue = d.into();
        val.into()
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/indexes/{keyspace}/{index}/ann",
    tag = "scylla-vector-store-index",
    description = "Performs an Approximate Nearest Neighbor (ANN) search using the specified index. \
Returns the vectors most similar to the provided vector. \
The maximum number of results is controlled by the optional 'limit' parameter in the payload. \
The similarity metric is determined at index creation and cannot be changed per query. \
If TLS is enabled on the server, clients must connect using a HTTPS protocol.",
    params(
        ("keyspace" = httpapi::KeyspaceName, Path, description = "The name of the ScyllaDB keyspace containing the vector index."),
        ("index" = httpapi::IndexName, Path, description = "The name of the ScyllaDB vector index within the specified keyspace to perform the search on.")
    ),
    request_body = httpapi::PostIndexAnnRequest,
    responses(
        (
            status = 200,
            description = "Successful ANN search. Returns a list of primary keys and their corresponding distances and similarity scores for the most similar vectors found.",
            body = httpapi::PostIndexAnnResponse
        ),
        (
            status = 400,
            description = "Bad request. Possible causes: invalid vector size, malformed input, or missing required fields.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 403,
            description = "Bad request. The TLS is enabled in a configuration, but client connected over the plain HTTP.",
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
#[hotpath::measure]
async fn post_index_ann(
    State(state): State<RoutesInnerState>,
    extensions: Extensions,
    Path((keyspace, index_name)): Path<(httpapi::KeyspaceName, httpapi::IndexName)>,
    extract::Json(request): extract::Json<httpapi::PostIndexAnnRequest>,
) -> Response {
    let keyspace: crate::KeyspaceName = keyspace.into();
    let index_name: crate::IndexName = index_name.into();
    if state.use_tls
        && extensions
            .get::<Protocol>()
            .is_some_and(|protocol| *protocol == Protocol::Plain)
    {
        let msg =
            "TLS is required, but the request was made over an insecure connection.".to_string();
        debug!("post_index_ann: {msg}");
        return (StatusCode::FORBIDDEN, msg).into_response();
    }

    // Start timing
    let timer = state
        .metrics
        .latency
        .with_label_values(&[keyspace.as_ref(), index_name.as_ref()])
        .start_timer();

    let index_key = IndexKey::new(&keyspace, &index_name);
    let (equality_cols, range_cols) = restriction_columns(&request.filter);
    let allow_filtering = request.filter.as_ref().is_some_and(|f| f.allow_filtering);
    let (routed_key, index, db_index) = match state
        .engine
        .get_best_index(index_key.clone(), equality_cols, range_cols)
        .await
    {
        indexes::BestIndexState::Serving {
            key: routed_key,
            index,
            db_index,
            needs_filtering,
        } => {
            if matches!(needs_filtering, indexes::NeedsFiltering::Yes(_)) && !allow_filtering {
                timer.observe_duration();

                let msg = format!(
                    "Index {keyspace}.{index_name} requires ALLOW FILTERING for this query"
                );
                debug!("post_index_ann: {msg}");
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            (routed_key, index, db_index)
        }
        indexes::BestIndexState::NotServing(db_index) => {
            timer.observe_duration();

            let scan_progress = db_index.full_scan_progress().await;
            match scan_progress {
                Progress::InProgress(percentage) => {
                    let msg = format!(
                        "Index {keyspace}.{index_name} is not available yet as it is still being constructed, progress: {:.3}%",
                        percentage.get()
                    );
                    debug!("post_index_ann: {msg}");
                    return (StatusCode::SERVICE_UNAVAILABLE, msg).into_response();
                }
                Progress::Done => {
                    let msg = format!(
                        "Index {keyspace}.{index_name} is not serving, but full scan did finish."
                    );
                    debug!("post_index_ann: {msg}");
                    return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
                }
            }
        }
        indexes::BestIndexState::NotFound => {
            timer.observe_duration();

            let msg = format!("missing index: {keyspace}.{index_name}");
            debug!("post_index_ann: {msg}");
            return (StatusCode::NOT_FOUND, msg).into_response();
        }
    };

    #[cfg(feature = "slow-test-hooks")]
    state
        .internals
        .increment_counter(format!(
            "ann-served-request--{}--{}",
            routed_key.keyspace(),
            routed_key.index(),
        ))
        .await;

    let primary_key_columns = db_index.get_primary_key_columns().await;
    let search_result = if let Some(filter) = request.filter {
        let filter = match try_from_post_index_ann_filter(
            filter,
            &primary_key_columns,
            db_index.get_table_columns().await.as_ref(),
        ) {
            Ok(filter) => filter,
            Err(err) => {
                debug!("post_index_ann: {err}");
                return (StatusCode::BAD_REQUEST, err.to_string()).into_response();
            }
        };
        index
            .filtered_ann(
                routed_key,
                request.vector.into(),
                filter,
                request.limit.into(),
            )
            .await
    } else {
        index
            .ann(routed_key, request.vector.into(), request.limit.into())
            .await
    };

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
                let similarity_scores: Vec<httpapi::SimilarityScore> = distances
                    .iter()
                    .copied()
                    .map(SimilarityScore::from)
                    .map(httpapi::SimilarityScore::from)
                    .collect();

                let primary_keys: anyhow::Result<_> = primary_key_columns
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(idx_column, column)| {
                        let primary_keys: anyhow::Result<_> = primary_keys
                            .iter()
                            .map(|primary_key| {
                                if primary_key.len() != primary_key_columns.len() {
                                    bail!(
                                        "wrong size of a primary key: {}, {}",
                                        primary_key_columns.len(),
                                        primary_key.len()
                                    );
                                }
                                Ok(primary_key)
                            })
                            .map_ok(|primary_key| {
                                primary_key
                                    .get(idx_column)
                                    .expect("primary key index out of bounds after length check")
                            })
                            .map_ok(try_to_json)
                            .map(|primary_key| primary_key.flatten())
                            .collect();
                        primary_keys.map(|primary_keys| (column.into(), primary_keys))
                    })
                    .collect();

                match primary_keys {
                    Err(err) => {
                        debug!("post_index_ann: {err}");
                        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
                    }

                    Ok(primary_keys) => (
                        StatusCode::OK,
                        response::Json(httpapi::PostIndexAnnResponse {
                            primary_keys,
                            distances: distances.into_iter().map(|d| d.into()).collect(),
                            similarity_scores,
                        }),
                    )
                        .into_response(),
                }
            }
        }
    }
}

fn try_from_post_index_ann_filter(
    json_filter: httpapi::PostIndexAnnFilter,
    primary_key_columns: &[crate::ColumnName],
    table_columns: &HashMap<crate::ColumnName, NativeType>,
) -> anyhow::Result<Filter> {
    let is_same_len = |columns: &[crate::ColumnName], values: &[Value]| -> anyhow::Result<()> {
        if columns.len() != values.len() {
            bail!(
                "Length of column tuple {columns:?} ({columns_len}) does not match length of values tuple ({values_len})",
                columns_len = columns.len(),
                values_len = values.len()
            );
        }
        Ok(())
    };
    let from_json = |column: &crate::ColumnName, value: Value| -> anyhow::Result<CqlValue> {
        if !primary_key_columns.contains(column) {
            bail!("Filtering on non primary key columns is not supported");
        };
        let Some(native_type) = table_columns.get(column) else {
            bail!(
                "Column '{column}' in filter restriction is not part of the table or is not a supported native type",
            )
        };
        try_from_json(value, native_type)
    };
    Ok(Filter {
        restrictions: json_filter
            .restrictions
            .into_iter()
            .map(|restriction| -> anyhow::Result<Restriction> {
                Ok(match restriction {
                    httpapi::PostIndexAnnRestriction::Eq { lhs, rhs } => {
                        let lhs = lhs.into();
                        Restriction::Eq {
                            rhs: from_json(&lhs, rhs)?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::In { lhs, rhs } => {
                        let lhs = lhs.into();
                        Restriction::In {
                            rhs: rhs
                                .into_iter()
                                .map(|rhs| from_json(&lhs, rhs))
                                .collect::<anyhow::Result<_>>()?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::Lt { lhs, rhs } => {
                        let lhs = lhs.into();
                        Restriction::Lt {
                            rhs: from_json(&lhs, rhs)?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::Lte { lhs, rhs } => {
                        let lhs = lhs.into();
                        Restriction::Lte {
                            rhs: from_json(&lhs, rhs)?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::Gt { lhs, rhs } => {
                        let lhs = lhs.into();
                        Restriction::Gt {
                            rhs: from_json(&lhs, rhs)?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::Gte { lhs, rhs } => {
                        let lhs = lhs.into();
                        Restriction::Gte {
                            rhs: from_json(&lhs, rhs)?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::EqTuple { lhs, rhs } => {
                        let lhs = lhs.into_iter().map(crate::ColumnName::from).collect_vec();
                        is_same_len(&lhs, &rhs)?;
                        Restriction::EqTuple {
                            rhs: rhs
                                .into_iter()
                                .enumerate()
                                .map(|(idx, rhs)| from_json(&lhs[idx], rhs))
                                .collect::<anyhow::Result<_>>()?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::InTuple { lhs, rhs } => {
                        let lhs = lhs.into_iter().map(crate::ColumnName::from).collect_vec();
                        Restriction::InTuple {
                            rhs: rhs
                                .into_iter()
                                .map(|rhs| {
                                    is_same_len(&lhs, &rhs)?;
                                    rhs.into_iter()
                                        .enumerate()
                                        .map(|(idx, rhs)| from_json(&lhs[idx], rhs))
                                        .collect::<anyhow::Result<_>>()
                                })
                                .collect::<anyhow::Result<_>>()?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::LtTuple { lhs, rhs } => {
                        let lhs = lhs.into_iter().map(crate::ColumnName::from).collect_vec();
                        is_same_len(&lhs, &rhs)?;
                        Restriction::LtTuple {
                            rhs: rhs
                                .into_iter()
                                .enumerate()
                                .map(|(idx, rhs)| from_json(&lhs[idx], rhs))
                                .collect::<anyhow::Result<_>>()?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::LteTuple { lhs, rhs } => {
                        let lhs = lhs.into_iter().map(crate::ColumnName::from).collect_vec();
                        is_same_len(&lhs, &rhs)?;
                        Restriction::LteTuple {
                            rhs: rhs
                                .into_iter()
                                .enumerate()
                                .map(|(idx, rhs)| from_json(&lhs[idx], rhs))
                                .collect::<anyhow::Result<_>>()?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::GtTuple { lhs, rhs } => {
                        let lhs = lhs.into_iter().map(crate::ColumnName::from).collect_vec();
                        is_same_len(&lhs, &rhs)?;
                        Restriction::GtTuple {
                            rhs: rhs
                                .into_iter()
                                .enumerate()
                                .map(|(idx, rhs)| from_json(&lhs[idx], rhs))
                                .collect::<anyhow::Result<_>>()?,
                            lhs,
                        }
                    }
                    httpapi::PostIndexAnnRestriction::GteTuple { lhs, rhs } => {
                        let lhs = lhs.into_iter().map(crate::ColumnName::from).collect_vec();
                        is_same_len(&lhs, &rhs)?;
                        Restriction::GteTuple {
                            rhs: rhs
                                .into_iter()
                                .enumerate()
                                .map(|(idx, rhs)| from_json(&lhs[idx], rhs))
                                .collect::<anyhow::Result<_>>()?,
                            lhs,
                        }
                    }
                })
            })
            .collect::<anyhow::Result<_>>()?,
        allow_filtering: json_filter.allow_filtering,
    })
}

fn try_to_json(value: CqlValue) -> anyhow::Result<Value> {
    match value {
        CqlValue::Ascii(value) => Ok(Value::String(value)),
        CqlValue::Text(value) => Ok(Value::String(value)),

        CqlValue::Boolean(value) => Ok(Value::Bool(value)),

        CqlValue::Double(value) => {
            Ok(Value::Number(Number::from_f64(value).ok_or_else(|| {
                anyhow!("CqlValue::Double should be finite")
            })?))
        }
        CqlValue::Float(value) => Ok(Value::Number(
            Number::from_f64(value.into())
                .ok_or_else(|| anyhow!("CqlValue::Float should be finite"))?,
        )),

        CqlValue::Int(value) => Ok(Value::Number(value.into())),
        CqlValue::BigInt(value) => Ok(Value::Number(value.into())),
        CqlValue::SmallInt(value) => Ok(Value::Number(value.into())),
        CqlValue::TinyInt(value) => Ok(Value::Number(value.into())),

        CqlValue::Uuid(value) => Ok(Value::String(value.into())),
        CqlValue::Timeuuid(value) => Ok(Value::String((*value.as_ref()).into())),

        CqlValue::Date(value) => Ok(Value::String(
            TryInto::<Date>::try_into(value)?.format(&Iso8601::DATE)?,
        )),
        CqlValue::Time(value) => Ok(Value::String(
            TryInto::<Time>::try_into(value)?
                .format(&Iso8601::TIME)?
                .strip_prefix("T")
                .ok_or_else(|| anyhow!("CqlValue::Time: wrong formatting detected"))?
                .to_string(), // remove 'T' prefix added by time crate
        )),
        CqlValue::Timestamp(value) => Ok(Value::String(
            TryInto::<OffsetDateTime>::try_into(value)?.format({
                const CONFIG: u128 = Config::DEFAULT
                    .set_time_precision(TimePrecision::Second {
                        decimal_digits: NonZero::new(3),
                    })
                    .encode();
                &Iso8601::<CONFIG>
            })?,
        )),

        CqlValue::Blob(value) => Ok(Value::String(const_hex::encode_prefixed(&value))),

        CqlValue::Varint(value) => Ok(Value::String(BigInt::from(value).to_string())),

        CqlValue::Decimal(value) => Ok(Value::String(BigDecimal::from(value).to_string())),

        _ => unimplemented!(),
    }
}

fn try_from_json(value: Value, cql_type: &NativeType) -> anyhow::Result<CqlValue> {
    match value {
        Value::String(value) => match cql_type {
            NativeType::Ascii => Ok(CqlValue::Ascii(value)),
            NativeType::Text => Ok(CqlValue::Text(value)),
            NativeType::Uuid => {
                let uuid = value
                    .parse()
                    .map_err(|err| anyhow!("Failed to parse UUID from string '{value}': {err}"))?;
                Ok(CqlValue::Uuid(uuid))
            }
            NativeType::Timeuuid => {
                let timeuuid: CqlTimeuuid = value.parse().map_err(|err| {
                    anyhow!("Failed to parse TimeUUID from string '{value}': {err}")
                })?;
                Ok(CqlValue::Timeuuid(timeuuid))
            }
            NativeType::Date => {
                let date = Date::parse(&value, &Iso8601::DATE)
                    .map_err(|err| anyhow!("Failed to parse Date from string '{value}': {err}"))?;
                Ok(CqlValue::Date(date.into()))
            }
            NativeType::Time => {
                let time = Time::parse(value.strip_prefix("T").unwrap_or(&value), &Iso8601::TIME)
                    .map_err(|err| {
                    anyhow!("Failed to parse Time from string '{value}': {err}")
                })?;
                Ok(CqlValue::Time(time.into()))
            }
            NativeType::Timestamp => {
                // CQL timestamps may use a space as the date-time separator
                // (e.g. '2024-01-01 00:00:00.000Z'), but ISO 8601 requires 'T'.
                // Only normalize when the space occurs at the expected date-time
                // boundary after a YYYY-MM-DD prefix; otherwise, leave the input
                // unchanged so that error reporting reflects the original value.
                static CQL_TIMESTAMP_RE: LazyLock<Regex> =
                    LazyLock::new(|| Regex::new(r"^(\d{4}-\d{2}-\d{2}) ").expect("valid regex"));
                let normalized = CQL_TIMESTAMP_RE.replace(&value, "${1}T");
                let datetime = OffsetDateTime::parse(&normalized, {
                    const CONFIG: u128 = Config::DEFAULT
                        .set_time_precision(TimePrecision::Second {
                            decimal_digits: NonZero::new(3),
                        })
                        .encode();
                    &Iso8601::<CONFIG>
                })
                .map_err(|err| anyhow!("Failed to parse Timestamp from string '{value}': {err}"))?;
                Ok(CqlValue::Timestamp(datetime.into()))
            }
            NativeType::Blob => {
                if !value.starts_with("0x") {
                    bail!("Blob value must be a '0x'-prefixed hex string");
                }
                let bytes = const_hex::decode(&value)
                    .map_err(|err| anyhow!("Invalid hex in blob value: {err}"))?;
                Ok(CqlValue::Blob(bytes))
            }
            NativeType::Varint => {
                let bi: BigInt = value.parse().map_err(|err| {
                    anyhow!("Failed to parse Varint from string '{value}': {err}")
                })?;
                Ok(CqlValue::Varint(CqlVarint::from(bi)))
            }
            NativeType::Decimal => {
                let bd: BigDecimal = value.parse().map_err(|err| {
                    anyhow!("Failed to parse Decimal from string '{value}': {err}")
                })?;
                Ok(CqlValue::Decimal(CqlDecimal::try_from(bd).map_err(
                    |err| anyhow!("Decimal value out of range: {err}"),
                )?))
            }
            _ => bail!("Cannot convert string to CqlValue::{cql_type:?}, unsupported type"),
        },

        Value::Bool(value) => match cql_type {
            NativeType::Boolean => Ok(CqlValue::Boolean(value)),
            _ => bail!("Cannot convert bool to CqlValue::{cql_type:?}, unsupported type"),
        },
        Value::Number(value) => match cql_type {
            NativeType::Double => {
                Ok(CqlValue::Double(value.as_f64().ok_or_else(|| {
                    anyhow!("Expected f64 for CqlValue::Double")
                })?))
            }
            NativeType::Float => {
                Ok(CqlValue::Float({
                    // there is no TryFrom<f64> for f32, so we use explicit conversion
                    let value = value
                        .as_f64()
                        .ok_or_else(|| anyhow!("Expected f32 (type f64) for CqlValue::Float"))?;
                    if !value.is_finite() || value < f32::MIN as f64 || value > f32::MAX as f64 {
                        bail!("Expected f32 for CqlValue::Float: value out of range");
                    }
                    let value = value as f32;
                    if !value.is_finite() {
                        bail!("Expected finite f32 for CqlValue::Float");
                    }
                    value
                }))
            }
            NativeType::Int => Ok(CqlValue::Int(
                value
                    .as_i64()
                    .ok_or_else(|| anyhow!("Expected i32 (type i64) for CqlValue::Int"))?
                    .try_into()
                    .map_err(|err| anyhow!("Expected i32 for CqlValue::Int: {err}"))?,
            )),
            NativeType::BigInt => {
                Ok(CqlValue::BigInt(value.as_i64().ok_or_else(|| {
                    anyhow!("Expected i64 for CqlValue::BigInt")
                })?))
            }
            NativeType::SmallInt => Ok(CqlValue::SmallInt(
                value
                    .as_i64()
                    .ok_or_else(|| anyhow!("Expected i16 (type i64) for CqlValue::SmallInt"))?
                    .try_into()
                    .map_err(|err| anyhow!("Expected i16 for CqlValue::SmallInt: {err}"))?,
            )),
            NativeType::TinyInt => Ok(CqlValue::TinyInt(
                value
                    .as_i64()
                    .ok_or_else(|| anyhow!("Expected i8 (type i64) for CqlValue::TinyInt"))?
                    .try_into()
                    .map_err(|err| anyhow!("Expected i8 for CqlValue::TinyInt: {err}"))?,
            )),
            NativeType::Varint => {
                // Varint is always an integer; reject fractional JSON numbers.
                let s = value.to_string();
                let bi: BigInt = s
                    .parse()
                    .map_err(|err| anyhow!("Failed to parse Varint from number '{s}': {err}"))?;
                Ok(CqlValue::Varint(CqlVarint::from(bi)))
            }
            NativeType::Decimal => {
                let s = value.to_string();
                let bd: BigDecimal = s
                    .parse()
                    .map_err(|err| anyhow!("Failed to parse Decimal from number '{s}': {err}"))?;
                Ok(CqlValue::Decimal(CqlDecimal::try_from(bd).map_err(
                    |err| anyhow!("Decimal value out of range: {err}"),
                )?))
            }
            _ => bail!("Cannot convert number to CqlValue::{cql_type:?}, unsupported type"),
        },

        _ => {
            bail!("Cannot convert JSON value '{value}' to CqlValue::{cql_type:?}, unsupported type")
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/info",
    tag = "scylla-vector-store-info",
    description = "Returns information about the Vector Store indexing service serving this API.",
    responses(
        (status = 200, description = "Vector Store indexing service information.", body = httpapi::InfoResponse)
    )
)]
async fn get_info(State(state): State<RoutesInnerState>) -> response::Json<httpapi::InfoResponse> {
    response::Json(httpapi::InfoResponse {
        version: Info::version().to_string(),
        service: Info::name().to_string(),
        engine: state.index_engine_version.clone(),
    })
}

impl From<crate::node_state::NodeStatus> for httpapi::NodeStatus {
    fn from(status: crate::node_state::NodeStatus) -> Self {
        match status {
            crate::node_state::NodeStatus::Initializing => httpapi::NodeStatus::Initializing,
            crate::node_state::NodeStatus::ConnectingToDb => httpapi::NodeStatus::ConnectingToDb,
            crate::node_state::NodeStatus::IndexingEmbeddings => httpapi::NodeStatus::Bootstrapping,
            crate::node_state::NodeStatus::DiscoveringIndexes => httpapi::NodeStatus::Bootstrapping,
            crate::node_state::NodeStatus::Serving => httpapi::NodeStatus::Serving,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/status",
    tag = "scylla-vector-store-info",
    description = "Returns the current operational status of the Vector Store indexing service.",
    responses(
        (status = 200, description = "Successful operation. Returns the current operational status of the Vector Store indexing service.", body = httpapi::NodeStatus),
    )
)]
async fn get_status(State(state): State<RoutesInnerState>) -> Response {
    (
        StatusCode::OK,
        response::Json(httpapi::NodeStatus::from(
            state.node_state.get_status().await,
        )),
    )
        .into_response()
}

fn new_internals() -> Router<RoutesInnerState> {
    Router::new()
        .route(
            "/counters",
            get(get_internal_counters).delete(delete_internal_counters),
        )
        .route("/counters/{id}", put(put_internal_counter))
        .route("/session-counters", get(get_internal_session_counters))
}

async fn get_internal_counters(State(state): State<RoutesInnerState>) -> Response {
    (
        StatusCode::OK,
        response::Json(state.internals.counters().await),
    )
        .into_response()
}

async fn delete_internal_counters(State(state): State<RoutesInnerState>) {
    state.internals.clear_counters().await;
}

async fn put_internal_counter(State(state): State<RoutesInnerState>, Path(id): Path<String>) {
    state.internals.start_counter(id).await;
}

async fn get_internal_session_counters(State(state): State<RoutesInnerState>) -> Response {
    (
        StatusCode::OK,
        response::Json(state.internals.session_counters().await),
    )
        .into_response()
}

#[cfg(test)]
mod tests {

    use super::*;
    use uuid::Uuid;

    #[test]
    fn try_from_post_index_ann_filter_conversion_ok() {
        let primary_key_columns = vec!["pk".into(), "ck".into()];
        let table_columns = [
            ("pk".into(), NativeType::Int),
            ("ck".into(), NativeType::Int),
            ("c1".into(), NativeType::Int),
        ]
        .into_iter()
        .collect();

        let filter = try_from_post_index_ann_filter(
            serde_json::from_str(
                r#"{
                    "restrictions": [
                        { "type": "==", "lhs": "pk", "rhs": 1 },
                        { "type": "IN", "lhs": "pk", "rhs": [2, 3] },
                        { "type": "<", "lhs": "ck", "rhs": 4 },
                        { "type": "<=", "lhs": "ck", "rhs": 5 },
                        { "type": ">", "lhs": "pk", "rhs": 6 },
                        { "type": ">=", "lhs": "pk", "rhs": 7 },
                        { "type": "()==()", "lhs": ["pk", "ck"], "rhs": [10, 20] },
                        { "type": "()IN()", "lhs": ["pk", "ck"], "rhs": [[100, 200], [300, 400]] },
                        { "type": "()<()", "lhs": ["pk", "ck"], "rhs": [30, 40] },
                        { "type": "()<=()", "lhs": ["pk", "ck"], "rhs": [50, 60] },
                        { "type": "()>()", "lhs": ["pk", "ck"], "rhs": [70, 80] },
                        { "type": "()>=()", "lhs": ["pk", "ck"], "rhs": [90, 0] }
                    ],
                    "allow_filtering": true
                }"#,
            )
            .unwrap(),
            &primary_key_columns,
            &table_columns,
        )
        .unwrap();
        assert!(filter.allow_filtering);
        assert_eq!(filter.restrictions.len(), 12);
        assert!(
            matches!(filter.restrictions.first(), Some(Restriction::Eq { lhs, rhs })
                if *lhs == "pk".into() && *rhs == CqlValue::Int(1))
        );
        assert!(
            matches!(filter.restrictions.get(1), Some(Restriction::In { lhs, rhs })
                if *lhs == "pk".into() && *rhs == vec![CqlValue::Int(2), CqlValue::Int(3)])
        );
        assert!(
            matches!(filter.restrictions.get(2), Some(Restriction::Lt { lhs, rhs })
                if *lhs == "ck".into() && *rhs == CqlValue::Int(4))
        );
        assert!(
            matches!(filter.restrictions.get(3), Some(Restriction::Lte { lhs, rhs })
                if *lhs == "ck".into() && *rhs == CqlValue::Int(5))
        );
        assert!(
            matches!(filter.restrictions.get(4), Some(Restriction::Gt { lhs, rhs })
                if *lhs == "pk".into() && *rhs == CqlValue::Int(6))
        );
        assert!(
            matches!(filter.restrictions.get(5), Some(Restriction::Gte { lhs, rhs })
                if *lhs == "pk".into() && *rhs == CqlValue::Int(7))
        );
        assert!(
            matches!(filter.restrictions.get(6), Some(Restriction::EqTuple { lhs, rhs })
                if *lhs == vec!["pk".into(), "ck".into()] && *rhs == vec![CqlValue::Int(10), CqlValue::Int(20)])
        );
        assert!(
            matches!(filter.restrictions.get(7), Some(Restriction::InTuple { lhs, rhs })
                if *lhs == vec!["pk".into(), "ck".into()] && *rhs == vec![vec![CqlValue::Int(100), CqlValue::Int(200)], vec![CqlValue::Int(300), CqlValue::Int(400)]])
        );
        assert!(
            matches!(filter.restrictions.get(8), Some(Restriction::LtTuple { lhs, rhs })
                if *lhs == vec!["pk".into(), "ck".into()] && *rhs == vec![CqlValue::Int(30), CqlValue::Int(40)])
        );
        assert!(
            matches!(filter.restrictions.get(9), Some(Restriction::LteTuple { lhs, rhs })
                if *lhs == vec!["pk".into(), "ck".into()] && *rhs == vec![CqlValue::Int(50), CqlValue::Int(60)])
        );
        assert!(
            matches!(filter.restrictions.get(10), Some(Restriction::GtTuple { lhs, rhs })
                if *lhs == vec!["pk".into(), "ck".into()] && *rhs == vec![CqlValue::Int(70), CqlValue::Int(80)])
        );
        assert!(
            matches!(filter.restrictions.get(11), Some(Restriction::GteTuple { lhs, rhs })
                if *lhs == vec!["pk".into(), "ck".into()] && *rhs == vec![CqlValue::Int(90), CqlValue::Int(0)])
        );
    }

    #[test]
    fn try_from_post_index_ann_filter_conversion_failed() {
        let primary_key_columns = vec!["pk".into(), "ck".into()];
        let table_columns = [
            ("pk".into(), NativeType::Int),
            ("ck".into(), NativeType::Int),
            ("c1".into(), NativeType::Int),
        ]
        .into_iter()
        .collect();

        // wrong primary key column
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "==", "lhs": "c1", "rhs": 1 }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );

        // unequal tuple lengths
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "()==()", "lhs": ["pk", "ck"], "rhs": [1] }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "()==()", "lhs": ["pk", "ck"], "rhs": [1, 2, 3] }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "()IN()", "lhs": ["pk", "ck"], "rhs": [[1]] }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "()IN()", "lhs": ["pk", "ck"], "rhs": [[1, 2, 3]] }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );

        // column not in the table
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "==", "lhs": "ck", "rhs": 1 }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &[("pk".into(), NativeType::Int),].into_iter().collect()
            )
            .is_err()
        );

        // type mismatch: string value for Int column
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "==", "lhs": "pk", "rhs": "hello" }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );

        // type mismatch: string value for Int column in tuple restriction
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "()<()", "lhs": ["pk", "ck"], "rhs": [1, "hello"] }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );

        // type mismatch: boolean value for Int column
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": ">", "lhs": "pk", "rhs": true }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );

        // type mismatch: string value for Int column in IN restriction
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "IN", "lhs": "pk", "rhs": ["hello"] }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );

        // type mismatch: string value for Int column in ()IN() restriction
        assert!(
            try_from_post_index_ann_filter(
                serde_json::from_str(
                    r#"{
                    "restrictions": [
                        { "type": "()IN()", "lhs": ["pk", "ck"], "rhs": [[1, "hello"]] }
                    ],
                    "allow_filtering": true
                }"#,
                )
                .unwrap(),
                &primary_key_columns,
                &table_columns
            )
            .is_err()
        );
    }

    #[test]
    fn try_from_json_conversion() {
        assert_eq!(
            try_from_json(Value::String("ascii".to_string()), &NativeType::Ascii).unwrap(),
            CqlValue::Ascii("ascii".to_string())
        );
        assert_eq!(
            try_from_json(Value::String("text".to_string()), &NativeType::Text).unwrap(),
            CqlValue::Text("text".to_string())
        );

        assert_eq!(
            try_from_json(Value::Bool(true), &NativeType::Boolean).unwrap(),
            CqlValue::Boolean(true)
        );

        assert_eq!(
            try_from_json(
                Value::Number(Number::from_f64(101.).unwrap()),
                &NativeType::Double
            )
            .unwrap(),
            CqlValue::Double(101.)
        );
        assert_eq!(
            try_from_json(
                Value::Number(Number::from_f64(201.).unwrap()),
                &NativeType::Float
            )
            .unwrap(),
            CqlValue::Float(201.)
        );
        assert!(
            try_from_json(
                Value::Number(Number::from_f64((f32::MAX as f64) * 10.).unwrap()),
                &NativeType::Float
            )
            .is_err()
        );
        assert!(
            try_from_json(
                Value::Number(Number::from_f64((f32::MIN as f64) * 10.).unwrap()),
                &NativeType::Float
            )
            .is_err()
        );

        assert_eq!(
            try_from_json(Value::Number(10.into()), &NativeType::Int).unwrap(),
            CqlValue::Int(10)
        );
        assert!(
            try_from_json(
                Value::Number((i32::MAX as i64 + 1).into()),
                &NativeType::Int
            )
            .is_err()
        );
        assert_eq!(
            try_from_json(Value::Number(20.into()), &NativeType::BigInt).unwrap(),
            CqlValue::BigInt(20)
        );
        assert_eq!(
            try_from_json(Value::Number(30.into()), &NativeType::SmallInt).unwrap(),
            CqlValue::SmallInt(30)
        );
        assert!(
            try_from_json(
                Value::Number((i16::MAX as i64 + 1).into()),
                &NativeType::SmallInt
            )
            .is_err()
        );
        assert_eq!(
            try_from_json(Value::Number(40.into()), &NativeType::TinyInt).unwrap(),
            CqlValue::TinyInt(40)
        );
        assert!(
            try_from_json(
                Value::Number((i8::MAX as i64 + 1).into()),
                &NativeType::TinyInt
            )
            .is_err()
        );

        let uuid = Uuid::new_v4();
        assert_eq!(
            try_from_json(Value::String(uuid.into()), &NativeType::Uuid).unwrap(),
            CqlValue::Uuid(uuid)
        );
        let uuid = Uuid::new_v4();
        assert_eq!(
            try_from_json(Value::String(uuid.into()), &NativeType::Timeuuid).unwrap(),
            CqlValue::Timeuuid(uuid.into())
        );

        assert_eq!(
            try_from_json(Value::String("2025-09-01".to_string()), &NativeType::Date).unwrap(),
            CqlValue::Date(
                Date::from_calendar_date(2025, time::Month::September, 1)
                    .unwrap()
                    .into()
            )
        );
        assert_eq!(
            try_from_json(
                Value::String("12:10:10.000000000".to_string()),
                &NativeType::Time
            )
            .unwrap(),
            CqlValue::Time(Time::from_hms(12, 10, 10).unwrap().into())
        );
        assert_eq!(
            try_from_json(
                Value::String(
                    // truncate microseconds
                    OffsetDateTime::from_unix_timestamp(123456789)
                        .unwrap()
                        .format({
                            const CONFIG: u128 = Config::DEFAULT
                                .set_time_precision(TimePrecision::Second {
                                    decimal_digits: NonZero::new(3),
                                })
                                .encode();
                            &Iso8601::<CONFIG>
                        })
                        .unwrap()
                ),
                &NativeType::Timestamp
            )
            .unwrap(),
            CqlValue::Timestamp(
                OffsetDateTime::from_unix_timestamp(123456789)
                    .unwrap()
                    .into()
            )
        );

        // CQL-style timestamp with space separator and Z offset
        assert_eq!(
            try_from_json(
                Value::String("2024-01-01 00:00:00.000Z".to_string()),
                &NativeType::Timestamp
            )
            .unwrap(),
            CqlValue::Timestamp(
                OffsetDateTime::from_unix_timestamp(1704067200)
                    .unwrap()
                    .into()
            )
        );

        // CQL-style timestamp with space separator, Z offset, and non-zero time
        assert_eq!(
            try_from_json(
                Value::String("1970-01-01 00:01:04.000Z".to_string()),
                &NativeType::Timestamp
            )
            .unwrap(),
            CqlValue::Timestamp(OffsetDateTime::from_unix_timestamp(64).unwrap().into())
        );

        assert_eq!(
            try_from_json(Value::String("0xdeadbeef".to_string()), &NativeType::Blob).unwrap(),
            CqlValue::Blob(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(
            try_from_json(Value::String("0x".to_string()), &NativeType::Blob).unwrap(),
            CqlValue::Blob(vec![])
        );
        assert_eq!(
            try_from_json(Value::String("0x00".to_string()), &NativeType::Blob).unwrap(),
            CqlValue::Blob(vec![0x00])
        );

        // missing 0x prefix
        assert!(try_from_json(Value::String("deadbeef".to_string()), &NativeType::Blob).is_err());
        // invalid hex characters
        assert!(try_from_json(Value::String("0xgg".to_string()), &NativeType::Blob).is_err());
        // odd-length hex digits (after stripping prefix)
        assert!(try_from_json(Value::String("0xabc".to_string()), &NativeType::Blob).is_err());

        // Varint from string
        assert_eq!(
            try_from_json(
                Value::String("-98765432109876543210987654321098765432109876543210".to_string()),
                &NativeType::Varint
            )
            .unwrap(),
            CqlValue::Varint(CqlVarint::from(
                "-98765432109876543210987654321098765432109876543210"
                    .parse::<BigInt>()
                    .unwrap()
            ))
        );
        assert!(
            try_from_json(
                Value::String("not_a_number".to_string()),
                &NativeType::Varint
            )
            .is_err()
        );
        // Varint from JSON number
        assert_eq!(
            try_from_json(Value::Number((-9876543210i64).into()), &NativeType::Varint).unwrap(),
            CqlValue::Varint(CqlVarint::from(BigInt::from(-9876543210i64)))
        );

        // Decimal from string
        assert_eq!(
            try_from_json(
                Value::String("-98765432109876543210.123456789".to_string()),
                &NativeType::Decimal
            )
            .unwrap(),
            CqlValue::Decimal(
                CqlDecimal::try_from(
                    "-98765432109876543210.123456789"
                        .parse::<BigDecimal>()
                        .unwrap()
                )
                .unwrap()
            )
        );
        assert!(
            try_from_json(
                Value::String("not_a_decimal".to_string()),
                &NativeType::Decimal
            )
            .is_err()
        );
        // Decimal from JSON number
        assert_eq!(
            try_from_json(
                Value::Number(Number::from_f64(-1.25).unwrap()),
                &NativeType::Decimal
            )
            .unwrap(),
            CqlValue::Decimal(
                CqlDecimal::try_from("-1.25".parse::<BigDecimal>().unwrap()).unwrap()
            )
        );
    }

    #[test]
    fn try_to_json_conversion() {
        assert_eq!(
            try_to_json(CqlValue::Ascii("ascii".to_string())).unwrap(),
            Value::String("ascii".to_string())
        );
        assert_eq!(
            try_to_json(CqlValue::Text("text".to_string())).unwrap(),
            Value::String("text".to_string())
        );

        assert_eq!(
            try_to_json(CqlValue::Boolean(true)).unwrap(),
            Value::Bool(true)
        );

        assert_eq!(
            try_to_json(CqlValue::Double(101.)).unwrap(),
            Value::Number(Number::from_f64(101.).unwrap())
        );
        assert_eq!(
            try_to_json(CqlValue::Float(201.)).unwrap(),
            Value::Number(Number::from_f64(201.).unwrap())
        );

        assert_eq!(
            try_to_json(CqlValue::Int(10)).unwrap(),
            Value::Number(10.into())
        );
        assert_eq!(
            try_to_json(CqlValue::BigInt(20)).unwrap(),
            Value::Number(20.into())
        );
        assert_eq!(
            try_to_json(CqlValue::SmallInt(30)).unwrap(),
            Value::Number(30.into())
        );
        assert_eq!(
            try_to_json(CqlValue::TinyInt(40)).unwrap(),
            Value::Number(40.into())
        );

        let uuid = Uuid::new_v4();
        assert_eq!(
            try_to_json(CqlValue::Uuid(uuid)).unwrap(),
            Value::String(uuid.into())
        );
        let uuid = Uuid::new_v4();
        assert_eq!(
            try_to_json(CqlValue::Timeuuid(uuid.into())).unwrap(),
            Value::String(uuid.into())
        );

        assert_eq!(
            try_to_json(CqlValue::Date(
                Date::from_calendar_date(2025, time::Month::September, 1)
                    .unwrap()
                    .into()
            ))
            .unwrap(),
            Value::String("2025-09-01".to_string())
        );
        assert_eq!(
            try_to_json(CqlValue::Time(Time::from_hms(12, 10, 10).unwrap().into())).unwrap(),
            Value::String("12:10:10.000000000".to_string())
        );
        assert_eq!(
            try_to_json(CqlValue::Timestamp(
                OffsetDateTime::from_unix_timestamp(123456789)
                    .unwrap()
                    .into()
            ))
            .unwrap(),
            Value::String(
                // truncate microseconds
                OffsetDateTime::from_unix_timestamp(123456789)
                    .unwrap()
                    .format({
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
        assert!(try_to_json(CqlValue::Float(f32::NAN)).is_err());
        assert!(try_to_json(CqlValue::Double(f64::NAN)).is_err());

        assert_eq!(
            try_to_json(CqlValue::Blob(vec![0xde, 0xad, 0xbe, 0xef])).unwrap(),
            Value::String("0xdeadbeef".to_string())
        );
        assert_eq!(
            try_to_json(CqlValue::Blob(vec![])).unwrap(),
            Value::String("0x".to_string())
        );
        assert_eq!(
            try_to_json(CqlValue::Blob(vec![0x00])).unwrap(),
            Value::String("0x00".to_string())
        );

        assert_eq!(
            try_to_json(CqlValue::Varint(CqlVarint::from(
                "-98765432109876543210987654321098765432109876543210"
                    .parse::<BigInt>()
                    .unwrap()
            )))
            .unwrap(),
            Value::String("-98765432109876543210987654321098765432109876543210".to_string())
        );

        assert_eq!(
            try_to_json(CqlValue::Decimal(
                CqlDecimal::try_from(
                    "-98765432109876543210.123456789"
                        .parse::<BigDecimal>()
                        .unwrap()
                )
                .unwrap()
            ))
            .unwrap(),
            Value::String("-98765432109876543210.123456789".to_string())
        );
    }

    #[test]
    fn node_status_conversion() {
        assert_eq!(
            httpapi::NodeStatus::from(crate::node_state::NodeStatus::Initializing),
            httpapi::NodeStatus::Initializing
        );
        assert_eq!(
            httpapi::NodeStatus::from(crate::node_state::NodeStatus::ConnectingToDb),
            httpapi::NodeStatus::ConnectingToDb
        );
        assert_eq!(
            httpapi::NodeStatus::from(crate::node_state::NodeStatus::IndexingEmbeddings),
            httpapi::NodeStatus::Bootstrapping
        );
        assert_eq!(
            httpapi::NodeStatus::from(crate::node_state::NodeStatus::DiscoveringIndexes),
            httpapi::NodeStatus::Bootstrapping
        );
        assert_eq!(
            httpapi::NodeStatus::from(crate::node_state::NodeStatus::Serving),
            httpapi::NodeStatus::Serving
        );
    }

    #[test]
    fn index_status_conversion() {
        assert_eq!(
            httpapi::IndexStatus::from(crate::node_state::IndexStatus::Initializing),
            httpapi::IndexStatus::Initializing
        );
        assert_eq!(
            httpapi::IndexStatus::from(crate::node_state::IndexStatus::FullScanning),
            httpapi::IndexStatus::Bootstrapping
        );
        assert_eq!(
            httpapi::IndexStatus::from(crate::node_state::IndexStatus::Serving),
            httpapi::IndexStatus::Serving
        );
    }
}
