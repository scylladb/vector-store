/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::delete;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use serde_json::Value;
use serde_json::json;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::error;
use usearch::Index;
use usearch::IndexOptions;
use usearch::MetricKind;
use usearch::ScalarKind;

// Initial and incremental number for the index vectors reservation.
// The value was taken for initial benchmarks (size similar to benchmark size)
const RESERVE_INCREMENT: usize = 1000000;

#[derive(Clone, Default)]
struct MockServerState {
    indices: Arc<RwLock<HashMap<String, Arc<RwLock<Index>>>>>,
}

pub struct TestOpenSearchServer {
    addr: SocketAddr,
    _handle: JoinHandle<()>,
}

impl TestOpenSearchServer {
    pub async fn start() -> Self {
        let state = MockServerState::default();
        let app = Router::new()
            .route("/{index}", put(create_index))
            .route("/{index}/_doc/{id}", post(add_document))
            .route("/{index}/_doc/{id}", delete(remove_document))
            .route("/{index}/_count", get(get_count))
            .route("/{index}/_search", post(search))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:9200")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            addr,
            _handle: handle,
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

// Index creation handler
async fn create_index(
    State(state): State<MockServerState>,
    axum::extract::Path(index): axum::extract::Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let mut indices = state.indices.write().unwrap();

    if indices.contains_key(&*index) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "resource_already_exists_exception",
                "message": format!("index {} already exists", index)
            })),
        );
    };

    let dimensions = payload["mappings"]["properties"]["vector"]["dimension"]
        .as_u64()
        .unwrap_or(0) as usize;

    let settings = payload["mappings"]["properties"]["vector"]["method"]["parameters"].clone();

    let options = IndexOptions {
        dimensions,
        connectivity: settings["m"].as_u64().unwrap_or(0) as usize,
        expansion_add: settings["ef_construction"].as_u64().unwrap_or(0) as usize,
        expansion_search: settings["ef_search"].as_u64().unwrap_or(0) as usize,
        metric: MetricKind::Cos,
        quantization: ScalarKind::F32,
        multi: false,
    };

    let idx = Arc::new(RwLock::new(Index::new(&options).unwrap()));
    idx.write().unwrap().reserve(RESERVE_INCREMENT).unwrap();

    indices.insert(index.to_string(), idx);

    (
        StatusCode::OK,
        Json(json!({
            "acknowledged": true,
            "shards_acknowledged": true,
            "index": index
        })),
    )
}

async fn add_document(
    State(state): State<MockServerState>,
    axum::extract::Path((index, id)): axum::extract::Path<(String, u64)>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let indices = state.indices.read().unwrap();
    if let Some(index_lock) = indices.get(&index) {
        let vector = payload["vector"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap() as f32)
            .collect::<Vec<_>>();
        let res = index_lock.read().unwrap().add(id, &vector);
        if res.is_ok() {
            return (StatusCode::OK, Json(json!({ "result": "created" })));
        } else {
            error!(
                "Failed to add document: {:?}, length {}, dimensionaity {}",
                res,
                vector.len(),
                index_lock.read().unwrap().dimensions()
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "invalid_vector",
                })),
            );
        }
    }
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "index not found" })),
    )
}

async fn get_count(
    State(state): State<MockServerState>,
    axum::extract::Path(index): axum::extract::Path<String>,
) -> impl IntoResponse {
    let indices = state.indices.read().unwrap();
    if let Some(index_lock) = indices.get(&index) {
        return (
            StatusCode::OK,
            Json(json!({ "count": index_lock.read().unwrap().size() })),
        );
    }
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "index not found" })),
    )
}

async fn search(
    State(state): State<MockServerState>,
    axum::extract::Path(index): axum::extract::Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let indices = state.indices.read().unwrap();
    if let Some(index_lock) = indices.get(&index) {
        let vector = payload["query"]["knn"]["vector"].clone();
        let embedding = vector["vector"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap() as f32)
            .collect::<Vec<_>>();
        let limit = vector["k"].as_u64().unwrap_or(10) as usize;
        let results = index_lock.read().unwrap().search(&embedding, limit);
        if results.is_err() {
            return {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": "bad_search",
                    })),
                )
            };
        }
        let results = results.unwrap();
        let results = results
            .keys
            .iter()
            .zip(results.distances.iter())
            .map(|(id, distance)| {
                json!({
                    "_id": id.to_string(),
                    "_index": index.to_string(),
                    "_score": distance,
                    "_source": {
                        "vector": embedding,
                    }
                })
            })
            .collect::<Vec<_>>();
        return (StatusCode::OK, Json(json!({ "hits": {"hits" : results}})));
    }
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "index not found" })),
    )
}

async fn remove_document(
    State(state): State<MockServerState>,
    axum::extract::Path((index, id)): axum::extract::Path<(String, u64)>,
) -> impl IntoResponse {
    let indices = state.indices.read().unwrap();
    if let Some(index_lock) = indices.get(&index) {
        let res = index_lock.read().unwrap().remove(id);
        if res.is_err() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "remove_failed",
                })),
            );
        }
        return (StatusCode::OK, Json(json!({ "result": "deleted" })));
    }
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "index not found" })),
    )
}
