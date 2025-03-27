/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Proprietary
 */

mod engine;
mod httproutes;
mod httpserver;
mod index;
mod modify_indexes;
mod monitor_indexes;
mod monitor_items;

use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;
use scylla::serialize::writers::WrittenCellProof;
use scylla::serialize::SerializationError;
use std::net::SocketAddr;
use tokio::signal;

#[derive(Clone, derive_more::From, derive_more::Display)]
pub struct ScyllaDbUri(String);

#[derive(
    Clone,
    Hash,
    Eq,
    PartialEq,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// DB's absolute index/table name (with keyspace) for which index should be build
#[schema(example = "vector_benchmark.vector_items")]
struct IndexId(String);

impl SerializeValue for IndexId {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        use {
            scylla::serialize::value::{
                BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
                BuiltinTypeCheckErrorKind,
            },
            std::any,
        };

        match typ {
            ColumnType::Native(NativeType::Text) => {
                writer.set_value(self.0.as_bytes()).map_err(|_| {
                    SerializationError::new(BuiltinSerializationError {
                        rust_name: any::type_name::<Self>(),
                        got: typ.clone().into_owned(),
                        kind: BuiltinSerializationErrorKind::ValueOverflow,
                    })
                })
            }
            _ => Err(SerializationError::new(BuiltinTypeCheckError {
                rust_name: any::type_name::<Self>(),
                got: typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::Text)],
                },
            })),
        }
    }
}

#[derive(
    Clone,
    derive_more::From,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    utoipa::ToSchema,
)]
struct TableName(String);

#[derive(Clone, derive_more::From, serde::Serialize, serde::Deserialize, derive_more::Display)]
/// Name of the column in a db table
struct ColumnName(String);

#[derive(
    Copy,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// Key for index embeddings
struct Key(u64);

impl SerializeValue for Key {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        use {
            scylla::serialize::value::{
                BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
                BuiltinTypeCheckErrorKind,
            },
            std::any,
        };

        match typ {
            ColumnType::Native(NativeType::BigInt) => writer
                .set_value(self.0.to_be_bytes().as_slice())
                .map_err(|_| {
                    SerializationError::new(BuiltinSerializationError {
                        rust_name: any::type_name::<Self>(),
                        got: typ.clone().into_owned(),
                        kind: BuiltinSerializationErrorKind::ValueOverflow,
                    })
                }),
            _ => Err(SerializationError::new(BuiltinTypeCheckError {
                rust_name: any::type_name::<Self>(),
                got: typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::BigInt)],
                },
            })),
        }
    }
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, derive_more::From, utoipa::ToSchema,
)]
/// Distance beetwen embeddings
struct Distance(f32);

impl SerializeValue for Distance {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        use {
            scylla::serialize::value::{
                BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
                BuiltinTypeCheckErrorKind,
            },
            std::any,
        };

        match typ {
            ColumnType::Native(NativeType::Float) => writer
                .set_value(self.0.to_be_bytes().as_slice())
                .map_err(|_| {
                    SerializationError::new(BuiltinSerializationError {
                        rust_name: any::type_name::<Self>(),
                        got: typ.clone().into_owned(),
                        kind: BuiltinSerializationErrorKind::ValueOverflow,
                    })
                }),
            _ => Err(SerializationError::new(BuiltinTypeCheckError {
                rust_name: any::type_name::<Self>(),
                got: typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::Float)],
                },
            })),
        }
    }
}

#[derive(
    Copy, Clone, serde::Serialize, serde::Deserialize, derive_more::From, derive_more::Display,
)]
struct IndexItemsCount(u32);

impl SerializeValue for IndexItemsCount {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        use {
            scylla::serialize::value::{
                BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
                BuiltinTypeCheckErrorKind,
            },
            std::any,
        };

        match typ {
            ColumnType::Native(NativeType::Int) => writer
                .set_value(self.0.to_be_bytes().as_slice())
                .map_err(|_| {
                    SerializationError::new(BuiltinSerializationError {
                        rust_name: any::type_name::<Self>(),
                        got: typ.clone().into_owned(),
                        kind: BuiltinSerializationErrorKind::ValueOverflow,
                    })
                }),
            _ => Err(SerializationError::new(BuiltinTypeCheckError {
                rust_name: any::type_name::<Self>(),
                got: typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::Int)],
                },
            })),
        }
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    derive_more::Display,
)]
/// Dimensions of embeddings
struct Dimensions(usize);

#[derive(
    Copy,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    derive_more::Display,
)]
/// Limit number of neighbors per graph node
struct Connectivity(usize);

#[derive(
    Copy,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// Control the recall of indexing
struct ExpansionAdd(usize);

#[derive(
    Copy,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    derive_more::From,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// Control the quality of the search
struct ExpansionSearch(usize);

#[derive(
    Copy, Clone, serde::Serialize, serde::Deserialize, derive_more::From, derive_more::Display,
)]
struct ParamM(usize);

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, derive_more::From, utoipa::ToSchema,
)]
/// Embeddings vector
struct Embeddings(Vec<f32>);

#[derive(
    Clone,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    derive_more::From,
    utoipa::ToSchema,
)]
/// Limit the number of search result
struct Limit(usize);

#[derive(derive_more::From)]
pub struct HttpServerAddr(SocketAddr);

pub async fn run(
    addr: HttpServerAddr,
    background_threads: Option<usize>,
    scylladb_uri: ScyllaDbUri,
) -> anyhow::Result<impl Sized> {
    if let Some(background_threads) = background_threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(background_threads)
            .build_global()?;
    }
    let engine_actor = engine::new(scylladb_uri).await?;
    httpserver::new(addr, engine_actor).await
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
