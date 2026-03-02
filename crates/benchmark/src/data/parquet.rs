/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::data::Query;
use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Float32Type;
use arrow_array::types::Float64Type;
use arrow_array::types::Int64Type;
use futures::Stream;
use futures::StreamExt;
use futures::stream;
use futures::stream::BoxStream;
use itertools::Itertools;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio_stream::wrappers::ReadDirStream;

#[derive(Deserialize)]
pub(crate) struct Config {
    #[serde(default = "default_ext")]
    ext: String,

    #[serde(default = "default_train_file_pattern")]
    train_file_pattern: String,

    #[serde(default = "default_test_file_name")]
    test_file_name: String,

    #[serde(default = "default_neigbors_file_name")]
    neighbors_file_name: String,

    #[serde(default = "default_id_column")]
    id_column: String,

    #[serde(default = "default_embedding_column")]
    embedding_column: String,

    #[serde(default = "default_neighbors_id_column")]
    neighbors_id_column: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ext: default_ext(),
            train_file_pattern: default_train_file_pattern(),
            test_file_name: default_test_file_name(),
            neighbors_file_name: default_neigbors_file_name(),
            id_column: default_id_column(),
            embedding_column: default_embedding_column(),
            neighbors_id_column: default_neighbors_id_column(),
        }
    }
}

fn default_ext() -> String {
    "parquet".to_string()
}

fn default_train_file_pattern() -> String {
    "train".to_string()
}

fn default_test_file_name() -> String {
    "test.parquet".to_string()
}

fn default_neigbors_file_name() -> String {
    "neighbors.parquet".to_string()
}

fn default_id_column() -> String {
    "id".to_string()
}

fn default_embedding_column() -> String {
    "emb".to_string()
}

fn default_neighbors_id_column() -> String {
    "neighbors_id".to_string()
}

pub(crate) async fn dimension(path: Arc<PathBuf>, config: Arc<Config>) -> usize {
    let builder = ParquetRecordBatchStreamBuilder::new(
        File::open(path.join(&config.test_file_name)).await.unwrap(),
    )
    .await
    .unwrap();
    let mask = ProjectionMask::columns(
        builder.parquet_schema(),
        [&*config.embedding_column].into_iter(),
    );
    let mut stream = builder.with_projection(mask).build().unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    let data = batch
        .column_by_name(&config.embedding_column)
        .unwrap()
        .as_list::<i64>()
        .value(0);
    if let Some(data) = data.as_primitive_opt::<Float64Type>() {
        data.len()
    } else {
        data.as_primitive::<Float32Type>().len()
    }
}

async fn train_files(path: Arc<PathBuf>, config: Arc<Config>) -> impl Stream<Item = PathBuf> {
    let readdirs = if let Ok(readdir) = fs::read_dir(&*path).await {
        vec![ReadDirStream::new(readdir)]
    } else {
        vec![]
    };
    stream::iter(readdirs)
        .flatten()
        .filter_map(|res| async move { res.ok() })
        .filter_map(|entry| async move {
            entry
                .file_type()
                .await
                .map(|ft| ft.is_file())
                .unwrap_or(false)
                .then_some(entry.path())
        })
        .filter_map(move |path| {
            let config = Arc::clone(&config);
            async move {
                let name = path.file_name().and_then(|name| name.to_str())?;
                let ext = path.extension().and_then(|ext| ext.to_str())?;
                (name.contains(&config.train_file_pattern) && ext == config.ext).then_some(path)
            }
        })
}

fn extract_embedding(
    ids: impl Iterator<Item = i64>,
    embs: impl Iterator<Item = Option<Arc<dyn Array>>>,
) -> Vec<(i64, Vec<f32>)> {
    let embs = embs.map(|emb| emb.unwrap()).map(|emb| {
        if let Some(emb) = emb.as_primitive_opt::<Float64Type>() {
            emb.iter().map(|v| v.unwrap() as f32).collect_vec()
        } else {
            let emb = emb.as_primitive::<Float32Type>();
            emb.iter().map(|v| v.unwrap()).collect_vec()
        }
    });
    ids.zip(embs).collect_vec()
}

pub(crate) async fn ids_stream(path: Arc<PathBuf>, config: Arc<Config>) -> BoxStream<'static, i64> {
    train_files(path, Arc::clone(&config))
        .await
        .then(|path| async move {
            ParquetRecordBatchStreamBuilder::new(File::open(path).await.unwrap())
                .await
                .unwrap()
                .build()
                .unwrap()
        })
        .flatten()
        .map(move |batch| batch.unwrap())
        .map(move |batch| {
            let ids = batch
                .column_by_name(&config.id_column)
                .unwrap()
                .as_primitive::<Int64Type>();
            let ids = ids.iter().map(|id| id.unwrap()).collect_vec();

            stream::iter(ids)
        })
        .flatten()
        .boxed()
}

pub(crate) async fn vector_stream(
    path: Arc<PathBuf>,
    config: Arc<Config>,
) -> BoxStream<'static, (i64, Vec<f32>)> {
    train_files(path, Arc::clone(&config))
        .await
        .then(|path| async move {
            ParquetRecordBatchStreamBuilder::new(File::open(path).await.unwrap())
                .await
                .unwrap()
                .build()
                .unwrap()
        })
        .flatten()
        .map(move |batch| batch.unwrap())
        .map(move |batch| {
            let ids = batch
                .column_by_name(&config.id_column)
                .unwrap()
                .as_primitive::<Int64Type>();
            let ids = ids.iter().map(|id| id.unwrap());

            let ids_embs = if let Some(embs) = batch
                .column_by_name(&config.embedding_column)
                .unwrap()
                .as_list_opt::<i32>()
            {
                extract_embedding(ids, embs.iter())
            } else {
                extract_embedding(
                    ids,
                    batch
                        .column_by_name(&config.embedding_column)
                        .unwrap()
                        .as_list::<i64>()
                        .iter(),
                )
            };

            stream::iter(ids_embs)
        })
        .flatten()
        .boxed()
}

pub(crate) async fn queries(
    path: Arc<PathBuf>,
    config: Arc<Config>,
    id_ok: impl Fn(i64) -> bool,
    limit: usize,
) -> Vec<Query> {
    let stream = ParquetRecordBatchStreamBuilder::new(
        File::open(path.join(&config.test_file_name)).await.unwrap(),
    )
    .await
    .unwrap()
    .build()
    .unwrap();
    let ids_queries = stream
        .map(move |batch| batch.unwrap())
        .map({
            let config = Arc::clone(&config);
            move |batch| {
                let ids = batch
                    .column_by_name(&config.id_column)
                    .unwrap()
                    .as_primitive::<Int64Type>();
                let ids = ids.iter().map(|id| id.unwrap());

                let queries = batch
                    .column_by_name(&config.embedding_column)
                    .unwrap()
                    .as_list::<i64>();
                let queries = queries.iter().map(|query| query.unwrap()).map(|query| {
                    if let Some(query) = query.as_primitive_opt::<Float64Type>() {
                        query.iter().map(|v| v.unwrap() as f32).collect_vec()
                    } else {
                        let query = query.as_primitive::<Float32Type>();
                        query.iter().map(|v| v.unwrap()).collect_vec()
                    }
                });

                let ids_queries = ids.zip(queries).collect_vec();
                stream::iter(ids_queries)
            }
        })
        .flatten()
        .collect::<HashMap<_, _>>()
        .await;

    let stream = ParquetRecordBatchStreamBuilder::new(
        File::open(path.join(&config.neighbors_file_name))
            .await
            .unwrap(),
    )
    .await
    .unwrap()
    .build()
    .unwrap();
    let ids_neighbors = stream
        .map(move |batch| batch.unwrap())
        .map(move |batch| {
            let ids = batch
                .column_by_name(&config.id_column)
                .unwrap()
                .as_primitive::<Int64Type>();
            let ids = ids.iter().map(|id| id.unwrap());

            let neighbors = batch
                .column_by_name(&config.neighbors_id_column)
                .unwrap()
                .as_list::<i64>();
            let neighbors = neighbors
                .iter()
                .map(|neighbor| neighbor.unwrap())
                .map(|neighbor| {
                    let neighbor = neighbor.as_primitive::<Int64Type>();
                    neighbor
                        .iter()
                        .filter(|id| id_ok(id.unwrap()))
                        .take(limit)
                        .map(|v| v.unwrap())
                        .collect::<HashSet<_>>()
                });

            let ids_neighbors = ids
                .zip(neighbors)
                .filter(|(_, map)| !map.is_empty())
                .collect_vec();
            stream::iter(ids_neighbors)
        })
        .flatten()
        .collect::<HashMap<_, _>>()
        .await;

    ids_queries
        .into_iter()
        .filter(|(id, _)| ids_neighbors.contains_key(id))
        .map(|(id, query)| {
            let neighbors = ids_neighbors.get(&id).unwrap().clone();
            Query { query, neighbors }
        })
        .collect()
}
