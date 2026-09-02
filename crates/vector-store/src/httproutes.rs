/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Filter;
use crate::IndexKey;
use crate::IndexName;
use crate::IndexOptionsFts;
use crate::IndexOptionsVs;
use crate::KeyspaceName;
use crate::Progress;
use crate::Quantization;
use crate::Restriction;
use crate::SimilarityScore;
use crate::SpaceType;
use crate::cql_types;
use crate::distance;
use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::fts_index::FtsIndex;
use crate::fts_index::FtsIndexExt;
use crate::fts_index::QueryError;
use crate::indexes;
use crate::indexes::Indexes;
use crate::info::Info;
use crate::internals::Internals;
use crate::internals::InternalsExt;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use crate::perf;
use crate::vector;
use crate::vs_index;
use crate::vs_index::VsIndexSearch;
use crate::vs_index::VsIndexSearchExt;
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
use httpapi::DataType;
use httpapi::FulltextIndexOptions;
use httpapi::IndexInfo;
use httpapi::IndexOptions;
use httpapi::SimilarityFunction;
use httpapi::VectorIndexOptions;
use itertools::Itertools;
use prometheus::Encoder;
use prometheus::ProtobufEncoder;
use prometheus::TextEncoder;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use serde_json::Value;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
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
        version = "3.0.0"
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
            httpapi::IndexName,
            httpapi::IndexNotReadyReason
        ),
        responses(
            httpapi::IndexNotReadyResponse
        )
    ),
)]
// TODO: modify HTTP API after design
struct ApiDoc;

#[derive(Clone)]
struct RoutesInnerState {
    engine: Sender<Engine>,
    indexes: Arc<RwLock<Indexes>>,
    metrics: Arc<Metrics>,
    node_state: Sender<NodeState>,
    internals: Sender<Internals>,
    index_engine_version: String,
    use_tls: bool,
}

pub(crate) async fn new(
    indexes: Arc<RwLock<Indexes>>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    node_state: Sender<NodeState>,
    internals: Sender<Internals>,
    index_engine_version: String,
    use_tls: bool,
) -> Router {
    let state = RoutesInnerState {
        engine,
        indexes,
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
                .routes(routes!(get_index_info))
                .routes(routes!(post_index_ann))
                .routes(routes!(post_index_bm25))
                .routes(routes!(post_index_highlight))
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

impl From<SpaceType> for SimilarityFunction {
    fn from(space_type: SpaceType) -> Self {
        match space_type {
            SpaceType::Euclidean => SimilarityFunction::Euclidean,
            SpaceType::Cosine => SimilarityFunction::Cosine,
            SpaceType::DotProduct => SimilarityFunction::DotProduct,
            SpaceType::Hamming => SimilarityFunction::Hamming,
        }
    }
}

impl From<&IndexOptionsVs> for VectorIndexOptions {
    fn from(options: &IndexOptionsVs) -> Self {
        VectorIndexOptions {
            dimensions: options.dimensions.as_ref().get(),
            maximum_node_connections: *options.connectivity.as_ref(),
            construction_beam_width: *options.expansion_add.as_ref(),
            search_beam_width: *options.expansion_search.as_ref(),
            similarity_function: options.space_type.into(),
            quantization: options.quantization.into(),
        }
    }
}

impl From<&IndexOptionsFts> for FulltextIndexOptions {
    fn from(options: &IndexOptionsFts) -> Self {
        FulltextIndexOptions {
            analyzer: options.analyzer.to_string(),
            positions: *options.positions.as_ref(),
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
    The list includes both vector and fulltext indexes in any state (initializing, available/built, destroying). \
    Due to synchronization delays, it may temporarily differ from the list of indexes inside ScyllaDB.",
    responses(
        (
            status = 200,
            description = "Successful operation. Returns an array of index information representing all indexes managed by the Vector Store.",
            body = [IndexInfo],
            content_type = "application/json",
            example = json!([
                {
                    "keyspace": "my_keyspace",
                    "index": "my_vector_index",
                    "options": {
                        "type": "vector",
                        "dimensions": 384,
                        "maximum_node_connections": 16,
                        "construction_beam_width": 128,
                        "search_beam_width": 64,
                        "similarity_function": "COSINE",
                        "quantization": "F32"
                    }
                },
                {
                    "keyspace": "my_keyspace",
                    "index": "my_fulltext_index",
                    "options": {
                        "type": "fulltext",
                        "analyzer": "standard",
                        "positions": true
                    }
                }
            ])
        )
    )
)]

async fn get_indexes(State(state): State<RoutesInnerState>) -> Response {
    let indexes_guard = state.indexes.read().unwrap();

    let indexes: Vec<_> = indexes_guard
        .iter_vs()
        .map(|(key, entry)| IndexInfo {
            keyspace: key.keyspace().into(),
            index: key.index().into(),
            options: IndexOptions::Vector(entry.options().into()),
        })
        .chain(indexes_guard.iter_fts().map(|(key, entry)| IndexInfo {
            keyspace: key.keyspace().into(),
            index: key.index().into(),
            options: IndexOptions::Fulltext(entry.options().into()),
        }))
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
    description = "Retrieves the current operational status and item count for a specific index. \
    The response includes the index's state and the total number of items currently indexed (excluding tombstoned or deleted entries). \
    This endpoint enables clients to monitor index readiness and data availability for search operations.",
    params(
        ("keyspace" = httpapi::KeyspaceName, Path, description = "The name of the ScyllaDB keyspace containing the index."),
        ("index" = httpapi::IndexName, Path, description = "The name of the ScyllaDB index within the specified keyspace to check status of.")
    ),
    responses(
        (
            status = 200,
            description = "Successful operation. Returns the current operational status of the specified index, including its state \
            and the total number of items currently indexed.",
            body = httpapi::IndexStatusResponse,
            content_type = "application/json",
            example = json!({
                "status": "SERVING",
                "count": 12345,
                "build_progress": 100.0
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
            description = "Error while checking index state or counting indexed items. Possible causes: internal error, or issues accessing the database.",
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

    enum IndexSender {
        Vs(Sender<VsIndexSearch>),
        Fts(Sender<FtsIndex>),
    }

    let (index, status, progress) = {
        let indexes = state.indexes.read().unwrap();
        if let Some(entry) = indexes.get_vs(&index_key) {
            (
                IndexSender::Vs(entry.index().clone()),
                entry.status(),
                entry.progress(),
            )
        } else if let Some(entry) = indexes.get_fts(&index_key) {
            (
                IndexSender::Fts(entry.index().clone()),
                entry.status(),
                entry.progress(),
            )
        } else {
            let msg = format!("missing index: {keyspace_name}.{index_name}");
            debug!("get_index_status: {msg}");
            return (StatusCode::NOT_FOUND, msg).into_response();
        }
    };

    let count_result = match index {
        IndexSender::Vs(s) => s.count(index_key).await,
        IndexSender::Fts(s) => s.count(index_key).await,
    };
    match count_result {
        Err(err) => {
            let msg = format!("index.count request error: {err}");
            debug!("get_index_status: {msg}");
            (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }
        Ok(count) => (
            StatusCode::OK,
            response::Json(httpapi::IndexStatusResponse {
                status: status.into(),
                count,
                build_progress: progress_to_percentage(progress),
            }),
        )
            .into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/indexes/{keyspace}/{index}",
    tag = "scylla-vector-store-index",
    description = "Retrieves information about a specific index, including its search type and the options \
    it was created with. This is the same information reported per-entry by the `/api/v1/indexes` listing, \
    scoped to a single index.",
    params(
        ("keyspace" = httpapi::KeyspaceName, Path, description = "The name of the ScyllaDB keyspace containing the index."),
        ("index" = httpapi::IndexName, Path, description = "The name of the ScyllaDB index within the specified keyspace.")
    ),
    responses(
        (
            status = 200,
            description = "Successful operation. Returns the index's type and creation options.",
            body = IndexInfo,
            content_type = "application/json",
            example = json!({
                "keyspace": "my_keyspace",
                "index": "my_vector_index",
                "options": {
                    "type": "vector",
                    "dimensions": 384,
                    "maximum_node_connections": 16,
                    "construction_beam_width": 128,
                    "search_beam_width": 64,
                    "similarity_function": "COSINE",
                    "quantization": "F32"
                }
            })
        ),
        (
            status = 404,
            description = "Index not found. Possible causes: index does not exist, or is not discovered yet.",
            content_type = "application/json",
            body = ErrorMessage
        )
    )
)]
async fn get_index_info(
    State(state): State<RoutesInnerState>,
    Path((keyspace_name, index_name)): Path<(httpapi::KeyspaceName, httpapi::IndexName)>,
) -> Response {
    let keyspace_name: crate::KeyspaceName = keyspace_name.into();
    let index_name: crate::IndexName = index_name.into();
    let index_key = IndexKey::new(&keyspace_name, &index_name);

    let indexes = state.indexes.read().unwrap();
    let info = if let Some(entry) = indexes.get_vs(&index_key) {
        IndexInfo {
            keyspace: keyspace_name.into(),
            index: index_name.into(),
            options: IndexOptions::Vector(entry.options().into()),
        }
    } else if let Some(entry) = indexes.get_fts(&index_key) {
        IndexInfo {
            keyspace: keyspace_name.into(),
            index: index_name.into(),
            options: IndexOptions::Fulltext(entry.options().into()),
        }
    } else {
        let msg = format!("missing index: {keyspace_name}.{index_name}");
        debug!("get_index_info: {msg}");
        return (StatusCode::NOT_FOUND, msg).into_response();
    };

    (StatusCode::OK, response::Json(info)).into_response()
}

async fn refresh_index_metrics(
    state: &RoutesInnerState,
    keyspace: KeyspaceName,
    index_name: IndexName,
) {
    let key = IndexKey::new(&keyspace, &index_name);
    let labels = [keyspace.as_ref(), index_name.as_ref()];

    if let Some((index, _)) = state.engine.get_vs_index(key.clone()).await {
        if let Ok(count) = index.count(key).await {
            state
                .metrics
                .size
                .with_label_values(&labels)
                .set(count as f64);
        }
        return;
    }

    if let Some((index, _)) = state.engine.get_fts_index(key.clone()).await
        && let Ok(stats) = index.stats(key).await
    {
        state
            .metrics
            .size
            .with_label_values(&labels)
            .set(stats.num_docs as f64);
        state
            .metrics
            .fts_index_size_bytes
            .with_label_values(&labels)
            .set(stats.size_bytes as f64);
        state
            .metrics
            .fts_segment_count
            .with_label_values(&labels)
            .set(stats.segment_count as f64);
    }
}

/// Convert a build [`Progress`] into a percentage in the range `0.0..=100.0`.
/// A finished full scan (`Progress::Done`) maps to `100.0`.
fn progress_to_percentage(progress: Progress) -> f64 {
    match progress {
        Progress::Done => 100.0,
        Progress::InProgress(percentage) => percentage.get(),
    }
}

async fn get_metrics(
    State(state): State<RoutesInnerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    for (keyspace_str, index_name_str) in state.metrics.take_dirty_indexes() {
        let keyspace = KeyspaceName::from(keyspace_str);
        let index_name = IndexName::from(index_name_str);
        refresh_index_metrics(&state, keyspace, index_name).await;
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
            response = httpapi::IndexNotReadyResponse
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
    perf::hotpath_async(async move {
        let keyspace: crate::KeyspaceName = keyspace.into();
        let index_name: crate::IndexName = index_name.into();
        if let Some(resp) = check_insecure_tls(state.use_tls, &extensions, "post_index_ann") {
            return resp;
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
        let best_index_state = state.indexes.read().unwrap().best_index(
            &index_key,
            &equality_cols,
            &range_cols,
            request.routing,
        );
        let (routed_key, index, primary_key_columns, filtering_columns, table_columns) =
            match best_index_state {
            indexes::BestIndexState::Serving {
                key: routed_key,
                index,
                needs_filtering,
                primary_key_columns,
                filtering_columns,
                table_columns,
            } => {
                if matches!(needs_filtering, indexes::NeedsFiltering::Yes(_)) && !allow_filtering {
                    timer.observe_duration();

                    let msg = format!(
                        "Index {keyspace}.{index_name} requires ALLOW FILTERING for this query"
                    );
                    debug!("post_index_ann: {msg}");
                    return (StatusCode::BAD_REQUEST, msg).into_response();
                }
                (
                    routed_key,
                    index,
                    primary_key_columns,
                    filtering_columns,
                    table_columns,
                )
            }
            indexes::BestIndexState::NoGlobalIndex => {
                timer.observe_duration();

                let msg = format!(
                    "Global ANN query is not supported when only a local \
                    vector index is available for {keyspace}.{index_name}"
                );
                debug!("post_index_ann: {msg}");
                return (StatusCode::BAD_REQUEST, msg).into_response();
            }
            indexes::BestIndexState::NotServing(progress) => {
                timer.observe_duration();

                match progress {
                    Progress::InProgress(percentage) => {
                        let reason = index_not_ready_reason(
                            &state.node_state,
                            &keyspace,
                            &index_name,
                            percentage,
                        )
                        .await;
                        debug!(
                            "post_index_ann: index {keyspace}.{index_name} not ready: {reason:?}"
                        );
                        return (StatusCode::SERVICE_UNAVAILABLE, response::Json(reason))
                            .into_response();
                    }
                    Progress::Done => {
                        let msg = format!(
                            "Index {keyspace}.{index_name} is not serving, \
                            but full scan did finish."
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

        let search_result = if let Some(filter) = request.filter {
            let filter = match try_from_post_index_ann_filter(
                filter,
                filtering_columns.as_slice(),
                &table_columns,
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
            Err(err) => match err.downcast_ref::<vs_index::Error>() {
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
                        "wrong size of an ann response: \
                    number of primary_keys = {}, number of distances = {}",
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

                    let primary_keys =
                        try_collect_primary_keys(primary_key_columns.as_slice(), &primary_keys);

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
    })
    .await
}

async fn check_fts_serving<T>(
    serving_or_progress: Result<T, Progress>,
    node_state: &Sender<NodeState>,
    keyspace: &crate::KeyspaceName,
    index_name: &crate::IndexName,
    route_name: &str,
) -> Result<T, Response> {
    match serving_or_progress {
        Ok(serving) => Ok(serving),
        Err(Progress::InProgress(percentage)) => {
            let reason = index_not_ready_reason(node_state, keyspace, index_name, percentage).await;
            debug!("{route_name}: index {keyspace}.{index_name} not ready: {reason:?}");
            Err((StatusCode::SERVICE_UNAVAILABLE, response::Json(reason)).into_response())
        }
        Err(Progress::Done) => {
            let msg =
                format!("Index {keyspace}.{index_name} is not serving, but full scan did finish.");
            debug!("{route_name}: {msg}");
            Err((StatusCode::INTERNAL_SERVER_ERROR, msg).into_response())
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/indexes/{keyspace}/{index}/bm25",
    tag = "scylla-vector-store-index",
    description = "Performs a full-text search query against the specified index. \
Returns primary keys of the documents most relevant to the provided text query, ranked by BM25 score. \
The maximum number of results is controlled by the optional 'limit' parameter in the payload. \
If TLS is enabled on the server, clients must connect using a HTTPS protocol.",
    params(
        ("keyspace" = httpapi::KeyspaceName, Path, description = "The name of the ScyllaDB keyspace containing the index."),
        ("index" = httpapi::IndexName, Path, description = "The name of the full-text index within the specified keyspace to search.")
    ),
    request_body = httpapi::PostIndexBm25Request,
    responses(
        (
            status = 200,
            description = "Successful full-text search. Returns a list of primary keys and their corresponding relevance scores for the most relevant documents found.",
            body = httpapi::PostIndexBm25Response
        ),
        (
            status = 400,
            description = "Bad request. Possible causes: malformed input, unparsable query, or missing required fields.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 403,
            description = "Forbidden. TLS is enabled in the configuration, but the client connected over plain HTTP.",
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
            description = "Error while searching. Possible causes: internal error, or search engine issues.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 503,
            response = httpapi::IndexNotReadyResponse
        )
    )
)]
async fn post_index_bm25(
    State(state): State<RoutesInnerState>,
    extensions: Extensions,
    Path((keyspace, index_name)): Path<(httpapi::KeyspaceName, httpapi::IndexName)>,
    extract::Json(request): extract::Json<httpapi::PostIndexBm25Request>,
) -> Response {
    let keyspace: crate::KeyspaceName = keyspace.into();
    let index_name: crate::IndexName = index_name.into();
    if let Some(resp) = check_insecure_tls(state.use_tls, &extensions, "post_index_bm25") {
        return resp;
    }

    let timer = state
        .metrics
        .latency
        .with_label_values(&[keyspace.as_ref(), index_name.as_ref()])
        .start_timer();

    let index_key = IndexKey::new(&keyspace, &index_name);

    let serving_or_progress = {
        let indexes = state.indexes.read().unwrap();
        let Some(entry) = indexes.get_fts(&index_key) else {
            timer.observe_duration();

            let msg = format!("missing index: {keyspace}.{index_name}");
            debug!("post_index_bm25: {msg}");
            return (StatusCode::NOT_FOUND, msg).into_response();
        };
        if entry.status() == crate::node_state::IndexStatus::Serving {
            Ok((entry.index().clone(), entry.primary_key_columns().clone()))
        } else {
            Err(entry.progress())
        }
    };

    let (fts_sender, primary_key_columns) = match check_fts_serving(
        serving_or_progress,
        &state.node_state,
        &keyspace,
        &index_name,
        "post_index_bm25",
    )
    .await
    {
        Ok(serving) => serving,
        Err(resp) => {
            timer.observe_duration();
            return resp;
        }
    };

    let search_result = fts_sender
        .search(index_key, request.query, request.limit.into())
        .await;

    timer.observe_duration();

    match search_result {
        Err(err) => {
            let msg = format!("index.bm25 request error: {err}");
            debug!("post_index_bm25: {msg}");
            let status = if err.downcast_ref::<QueryError>().is_some() {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (status, msg).into_response()
        }
        Ok((primary_keys, scores)) => {
            if primary_keys.len() != scores.len() {
                let msg = format!(
                    "wrong size of a bm25 response: \
                    number of primary_keys = {}, number of scores = {}",
                    primary_keys.len(),
                    scores.len()
                );
                debug!("post_index_bm25: {msg}");
                return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
            }

            let primary_keys =
                try_collect_primary_keys(primary_key_columns.as_slice(), &primary_keys);

            match primary_keys {
                Err(err) => {
                    debug!("post_index_bm25: {err}");
                    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
                }
                Ok(primary_keys) => (
                    StatusCode::OK,
                    response::Json(httpapi::PostIndexBm25Response {
                        primary_keys,
                        scores,
                    }),
                )
                    .into_response(),
            }
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/indexes/{keyspace}/{index}/highlight",
    tag = "scylla-vector-store-index",
    description = "Computes a highlighted excerpt (snippet) for each of the supplied document texts. \
The index is used only to analyze the query and to weight its terms by their document frequency, prioritizing \
rarer terms when picking which fragment of a long text to show. \
Highlights are returned in the same order as the requested documents. \
A document that matches no query term yields `null`. \
If TLS is enabled on the server, clients must connect using a HTTPS protocol.",
    params(
        ("keyspace" = httpapi::KeyspaceName, Path, description = "The name of the ScyllaDB keyspace containing the index."),
        ("index" = httpapi::IndexName, Path, description = "The name of the full-text index within the specified keyspace to highlight against.")
    ),
    request_body = httpapi::PostIndexHighlightRequest,
    responses(
        (
            status = 200,
            description = "Successful highlighting. Returns one highlight per requested document, in the requested order.",
            body = httpapi::PostIndexHighlightResponse
        ),
        (
            status = 400,
            description = "Bad request. Possible causes: malformed input, unparsable query, or missing required fields.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 403,
            description = "Forbidden. TLS is enabled in the configuration, but the client connected over plain HTTP.",
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
            description = "Error while highlighting. Possible causes: internal error, or search engine issues.",
            content_type = "application/json",
            body = ErrorMessage
        ),
        (
            status = 503,
            response = httpapi::IndexNotReadyResponse
        )
    )
)]
async fn post_index_highlight(
    State(state): State<RoutesInnerState>,
    extensions: Extensions,
    Path((keyspace, index_name)): Path<(httpapi::KeyspaceName, httpapi::IndexName)>,
    extract::Json(request): extract::Json<httpapi::PostIndexHighlightRequest>,
) -> Response {
    let keyspace: crate::KeyspaceName = keyspace.into();
    let index_name: crate::IndexName = index_name.into();
    if let Some(resp) = check_insecure_tls(state.use_tls, &extensions, "post_index_highlight") {
        return resp;
    }

    let timer = state
        .metrics
        .latency
        .with_label_values(&[keyspace.as_ref(), index_name.as_ref()])
        .start_timer();

    let index_key = IndexKey::new(&keyspace, &index_name);

    let serving_or_progress = {
        let indexes = state.indexes.read().unwrap();
        let Some(entry) = indexes.get_fts(&index_key) else {
            timer.observe_duration();

            let msg = format!("missing index: {keyspace}.{index_name}");
            debug!("post_index_highlight: {msg}");
            return (StatusCode::NOT_FOUND, msg).into_response();
        };
        if entry.status() == crate::node_state::IndexStatus::Serving {
            Ok(entry.index().clone())
        } else {
            Err(entry.progress())
        }
    };

    let fts_sender = match check_fts_serving(
        serving_or_progress,
        &state.node_state,
        &keyspace,
        &index_name,
        "post_index_highlight",
    )
    .await
    {
        Ok(sender) => sender,
        Err(resp) => {
            timer.observe_duration();
            return resp;
        }
    };

    let result = fts_sender
        .highlight(index_key, request.query, request.documents)
        .await;

    timer.observe_duration();

    match result {
        Err(err) => {
            let msg = format!("index.highlight request error: {err}");
            debug!("post_index_highlight: {msg}");
            let status = if err.downcast_ref::<QueryError>().is_some() {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (status, msg).into_response()
        }
        Ok(highlights) => (
            StatusCode::OK,
            response::Json(httpapi::PostIndexHighlightResponse { highlights }),
        )
            .into_response(),
    }
}

fn try_from_post_index_ann_filter(
    json_filter: httpapi::PostIndexAnnFilter,
    filtering_columns: &[crate::ColumnName],
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
        if !filtering_columns.contains(column) {
            bail!(
                "Column '{column}' in filter restriction is not part of the primary key or filtering columns, and cannot be used for filtering"
            );
        };
        let Some(native_type) = table_columns.get(column) else {
            bail!(
                "Column '{column}' in filter restriction is not part of the table or is not a supported native type",
            )
        };
        cql_types::from_json(value, native_type)
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

fn check_insecure_tls(
    use_tls: bool,
    extensions: &Extensions,
    route_name: &str,
) -> Option<Response> {
    if use_tls
        && extensions
            .get::<Protocol>()
            .is_some_and(|protocol| *protocol == Protocol::Plain)
    {
        let msg = "TLS is required, but the request \
            was made over an insecure connection."
            .to_string();
        debug!("{route_name}: {msg}");
        return Some((StatusCode::FORBIDDEN, msg).into_response());
    }
    None
}

fn try_collect_primary_keys(
    primary_key_columns: &[crate::ColumnName],
    primary_keys: &[crate::PrimaryKey],
) -> anyhow::Result<HashMap<httpapi::ColumnName, Vec<Value>>> {
    primary_key_columns
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
                .map_ok(cql_types::to_json)
                .map(|primary_key| primary_key.flatten())
                .collect();
            primary_keys.map(|primary_keys| (column.into(), primary_keys))
        })
        .collect()
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

async fn index_not_ready_reason(
    node_state: &Sender<NodeState>,
    keyspace: &crate::KeyspaceName,
    index_name: &crate::IndexName,
    percentage: crate::Percentage,
) -> httpapi::IndexNotReadyReason {
    if node_state.get_status().await == crate::node_state::NodeStatus::Serving {
        httpapi::IndexNotReadyReason::IndexBuilding {
            message: format!(
                "Index {keyspace}.{index_name} is not available yet \
                as it is still being constructed, progress: {:.3}%",
                percentage.get()
            ),
        }
    } else {
        httpapi::IndexNotReadyReason::NodeBootstrapping
    }
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
    use crate::Analyzer;

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

    #[test]
    fn similarity_function_conversion() {
        assert_eq!(
            SimilarityFunction::from(SpaceType::Euclidean),
            SimilarityFunction::Euclidean
        );
        assert_eq!(
            SimilarityFunction::from(SpaceType::Cosine),
            SimilarityFunction::Cosine
        );
        assert_eq!(
            SimilarityFunction::from(SpaceType::DotProduct),
            SimilarityFunction::DotProduct
        );
        assert_eq!(
            SimilarityFunction::from(SpaceType::Hamming),
            SimilarityFunction::Hamming
        );
    }

    #[test]
    fn vector_index_options_conversion() {
        let options = IndexOptionsVs {
            dimensions: NonZeroUsize::new(384).unwrap().into(),
            connectivity: 32.into(),
            expansion_add: 200.into(),
            expansion_search: 100.into(),
            space_type: SpaceType::DotProduct,
            quantization: Quantization::I8,
        };
        assert_eq!(
            VectorIndexOptions::from(&options),
            VectorIndexOptions {
                dimensions: 384,
                maximum_node_connections: 32,
                construction_beam_width: 200,
                search_beam_width: 100,
                similarity_function: SimilarityFunction::DotProduct,
                quantization: DataType::I8,
            }
        );
    }

    #[test]
    fn fulltext_index_option_conversion() {
        assert_eq!(
            FulltextIndexOptions::from(&IndexOptionsFts::default()),
            FulltextIndexOptions {
                analyzer: "standard".to_string(),
                positions: true,
            }
        );
        assert_eq!(
            FulltextIndexOptions::from(&IndexOptionsFts {
                analyzer: Analyzer::German,
                positions: false.into(),
            }),
            FulltextIndexOptions {
                analyzer: "german".to_string(),
                positions: false,
            }
        );
    }

    #[test]
    fn progress_to_percentage_conversion() {
        assert_eq!(super::progress_to_percentage(Progress::Done), 100.0);
        assert_eq!(
            super::progress_to_percentage(Progress::InProgress(42.0.try_into().unwrap())),
            42.0
        );
        assert_eq!(
            super::progress_to_percentage(Progress::InProgress(0.0.try_into().unwrap())),
            0.0
        );
    }
}
