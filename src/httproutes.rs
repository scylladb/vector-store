/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Distance;
use crate::Embeddings;
use crate::IndexId;
use crate::Key;
use crate::KeyspaceName;
use crate::Limit;
use crate::TableName;
use crate::db_index::DbIndexExt;
use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::index::IndexExt;
use anyhow::bail;
use axum::Router;
use axum::extract;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response;
use axum::response::IntoResponse;
use axum::response::Response;
use itertools::Itertools;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tower_http::trace::TraceLayer;
use tracing::error;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use utoipa_swagger_ui::SwaggerUi;

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
    response::Json(engine.get_index_ids().await)
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct PostIndexAnnRequest {
    pub embeddings: Embeddings,
    #[serde(default)]
    pub limit: Limit,
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct PostIndexAnnResponse {
    pub primary_keys: HashMap<ColumnName, Vec<Key>>,
    pub distances: Vec<Distance>,
}

#[utoipa::path(
    post,
    path = "/api/v1/indexes/{keyspace}/{index}/ann",
    description = "Ann search in the index",
    params(
        ("keyspace" = KeyspaceName, Path, description = "Keyspace name for the table to search"),
        ("index" = TableName, Path, description = "Index to search")
    ),
    request_body = PostIndexAnnRequest,
    responses(
        (status = 200, description = "Ann search result", body = PostIndexAnnResponse),
        (status = 404, description = "Index not found")
    )
)]
async fn post_index_ann(
    State(engine): State<Sender<Engine>>,
    Path((keyspace, index)): Path<(KeyspaceName, TableName)>,
    extract::Json(request): extract::Json<PostIndexAnnRequest>,
) -> Response {
    let Some((index, db_index)) = engine.get_index(IndexId::new(&keyspace, &index)).await else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };

    match index.ann(request.embeddings, request.limit).await {
        Err(err) => {
            let msg = format!("index.ann request error: {err}");
            error!("post_index_ann: {msg}");
            (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }

        Ok((primary_keys, distances)) => {
            if primary_keys.len() != distances.len() {
                let msg = format!(
                    "wrong size of an ann response: number of primary_keys = {}, number of distances = {}",
                    primary_keys.len(),
                    distances.len()
                );
                error!("post_index_ann: {msg}");
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
                            .map_ok(|primary_key| primary_key.0[idx_column])
                            .collect();
                        primary_keys.map(|primary_keys| (column, primary_keys))
                    })
                    .collect();

                match primary_keys {
                    Err(err) => {
                        error!("post_index_ann: {err}");
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
