/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Distance;
use crate::Embeddings;
use crate::IndexId;
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
use scylla::value::CqlValue;
use serde_json::Number;
use serde_json::Value;
use std::collections::HashMap;
use time::Date;
use time::OffsetDateTime;
use time::Time;
use time::format_description::well_known::Iso8601;
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
    pub primary_keys: HashMap<ColumnName, Vec<Value>>,
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
                            .map_ok(|primary_key| primary_key.0[idx_column].clone())
                            .map_ok(to_json)
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
                .format(&Iso8601::DEFAULT)
                .expect("OffsetDateTime should be correct"),
        ),

        _ => unimplemented!(),
    }
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
                now.replace_millisecond(now.millisecond())
                    .unwrap()
                    .format(&Iso8601::DEFAULT)
                    .unwrap()
            )
        );
    }
}
