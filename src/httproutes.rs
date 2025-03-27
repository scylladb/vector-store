/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Proprietary
 */

use {
    crate::{
        engine::{Engine, EngineExt},
        index::IndexExt,
        Distance, Embeddings, IndexId, Key, Limit,
    },
    axum::{
        extract::{self, Path, State},
        http::StatusCode,
        response::{self, IntoResponse, Response},
        Router,
    },
    tokio::sync::mpsc::Sender,
    tower_http::trace::TraceLayer,
    utoipa::OpenApi,
    utoipa_axum::{router::OpenApiRouter, routes},
    utoipa_swagger_ui::SwaggerUi,
};

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "scylla-vector-store", description = "Scylla Vector Store (API will change after design)")
    )
)]
// TODO: modify HTTP API after design
struct ApiDoc;

pub(crate) fn new(engine: Sender<Engine>) -> Router {
    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .merge(
            OpenApiRouter::new()
                .routes(routes!(get_indexes))
                .routes(routes!(post_index_ann))
                .layer(TraceLayer::new_for_http())
                .with_state(engine),
        )
        .split_for_parts();

    router.merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api))
}

#[utoipa::path(
    get,
    path = "/api/v1/indexes",
    description = "Get list of current indexes",
    responses(
        (status = 200, description = "List of indexes", body = [IndexId])
    )
)]
async fn get_indexes(State(engine): State<Sender<Engine>>) -> response::Json<Vec<IndexId>> {
    response::Json(engine.get_indexes().await)
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct PostIndexAnnRequest {
    embeddings: Embeddings,
    #[serde(default = "default_limit")]
    limit: Limit,
}

fn default_limit() -> Limit {
    Limit(1)
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct PostIndexAnnResponse {
    keys: Vec<Key>,
    distances: Vec<Distance>,
}

#[utoipa::path(
    post,
    path = "/api/v1/indexes/{id}/ann",
    description = "Ann search in the index",
    params(
        ("id" = IndexId, Path, description = "Index id to search")
    ),
    request_body = PostIndexAnnRequest,
    responses(
        (status = 200, description = "Ann search result", body = PostIndexAnnResponse),
        (status = 404, description = "Index not found")
    )
)]
async fn post_index_ann(
    State(engine): State<Sender<Engine>>,
    Path(id): Path<IndexId>,
    extract::Json(request): extract::Json<PostIndexAnnRequest>,
) -> Response {
    let Some(index) = engine.get_index(id).await else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };
    match index.ann(request.embeddings, request.limit).await {
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        Ok((keys, distances)) => (
            StatusCode::OK,
            response::Json(PostIndexAnnResponse { keys, distances }),
        )
            .into_response(),
    }
}
