/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod fbin;
mod parquet;

use futures::StreamExt;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tokio::sync::mpsc;
use tracing::info;

const DATASET_FILENAME: &str = "dataset.toml";
const BUCKETS_FILENAME: &str = "buckets.bin";

pub(crate) struct Query {
    pub(crate) query: Vec<f32>,
    pub(crate) neighbors: HashSet<i64>,
}

pub(crate) struct Data {
    path: Arc<PathBuf>,
    format: Format,
    buckets: Arc<BTreeMap<i64, u8>>,
}

enum Format {
    Parquet(Arc<parquet::Config>),
    Fbin(Arc<fbin::Config>),
}

impl Data {
    pub(crate) async fn dimension(&self) -> usize {
        let dim = match &self.format {
            Format::Parquet(config) => {
                parquet::dimension(Arc::clone(&self.path), Arc::clone(config)).await
            }
            Format::Fbin(config) => {
                fbin::dimension(Arc::clone(&self.path), Arc::clone(config)).await
            }
        };
        info!("Found dimension {dim} for dataset at {:?}", self.path);
        dim
    }

    pub(crate) async fn queries(&self, bucket: Option<u8>, limit: usize) -> Vec<Query> {
        let buckets = Arc::clone(&self.buckets);
        let id_ok = move |id| {
            let Some(bucket) = &bucket else {
                return true;
            };
            buckets.get(&id) == Some(bucket)
        };
        match &self.format {
            Format::Parquet(config) => {
                parquet::queries(Arc::clone(&self.path), Arc::clone(config), id_ok, limit).await
            }
            Format::Fbin(config) => {
                fbin::queries(Arc::clone(&self.path), Arc::clone(config), id_ok, limit).await
            }
        }
    }

    pub(crate) async fn vector_stream(&self) -> mpsc::Receiver<(i64, Box<[f32]>)> {
        match &self.format {
            Format::Parquet(config) => {
                parquet::vector_stream(Arc::clone(&self.path), Arc::clone(config)).await
            }
            Format::Fbin(config) => {
                fbin::vector_stream(Arc::clone(&self.path), Arc::clone(config)).await
            }
        }
    }

    pub(crate) fn buckets(&self) -> Arc<BTreeMap<i64, u8>> {
        Arc::clone(&self.buckets)
    }
}

#[derive(Deserialize)]
struct Config {
    parquet: Option<parquet::Config>,
    fbin: Option<fbin::Config>,
}

async fn format(path: &Path) -> Format {
    let toml_path = path.join(DATASET_FILENAME);
    let Ok(config) = fs::read(&toml_path).await else {
        info!("Not found {DATASET_FILENAME} in {path:?}. Using default parquet format.");
        return Format::Parquet(Arc::new(parquet::Config::default()));
    };
    let config: Config = toml::from_slice(&config)
        .unwrap_or_else(|err| panic!("Failed to parse {toml_path:?}: {err}"));
    if let Some(config) = config.parquet {
        return Format::Parquet(Arc::new(config));
    }
    if let Some(config) = config.fbin {
        return Format::Fbin(Arc::new(config));
    }
    info!("Not found format type in {DATASET_FILENAME} in {path:?}. Using default parquet format.");
    Format::Parquet(Arc::new(parquet::Config::default()))
}

async fn build_buckets(path: Arc<PathBuf>, format: &Format) -> BTreeMap<i64, u8> {
    let stream = match &format {
        Format::Parquet(config) => parquet::ids_stream(Arc::clone(&path), Arc::clone(config)).await,
        Format::Fbin(config) => fbin::ids_stream(Arc::clone(&path), Arc::clone(config)).await,
    };
    info!("Building buckets for dataset at {path:?}...");
    let mut buckets = stream
        .map(|id| (id, u8::MAX))
        .collect::<BTreeMap<_, _>>()
        .await;
    const BUCKETS: usize = 9;
    let max_buckets = [
        2,    // 50%
        5,    // 20%
        10,   // 10%
        20,   // 5%
        50,   // 2%
        100,  // 1%
        200,  // 0.5%
        500,  // 0.2%
        1000, // 0.1%
    ];
    assert!(BUCKETS == max_buckets.len());
    let mut counts = [0; BUCKETS];
    buckets.values_mut().for_each(|bucket| {
        counts.iter_mut().enumerate().for_each(|(idx, count)| {
            *count += 1;
            if *bucket == u8::MAX && *count >= max_buckets[idx] {
                *bucket = idx as u8;
                *count -= max_buckets[idx];
            }
        });
    });
    buckets
}

async fn write_buckets(path: &Path, buckets: BTreeMap<i64, u8>) {
    let path = path.join(BUCKETS_FILENAME);
    info!("Writing buckets at {path:?}...");
    let mut buckets_writer = BufWriter::new(File::create(path).await.unwrap());
    for (id, bucket) in buckets.iter().filter(|(_, bucket)| **bucket != u8::MAX) {
        buckets_writer.write_i64(*id).await.unwrap();
        buckets_writer.write_u8(*bucket).await.unwrap();
    }
}

pub(crate) async fn build_and_write_buckets(data_dir: PathBuf) {
    let data_dir = Arc::new(data_dir);
    let format = format(&data_dir).await;
    let buckets = build_buckets(Arc::clone(&data_dir), &format).await;
    write_buckets(&data_dir, buckets).await;
}

async fn read_buckets(path: &Path) -> BTreeMap<i64, u8> {
    let path = path.join(BUCKETS_FILENAME);
    info!("Readings buckets from {path:?}...");
    let Ok(file) = File::open(&path).await else {
        info!("Not found {path:?}. No buckets will be used.");
        return BTreeMap::new();
    };
    let mut buckets_reader = BufReader::new(file);
    let mut buckets = BTreeMap::new();
    while let Ok(id) = buckets_reader.read_i64().await {
        let Ok(bucket) = buckets_reader.read_u8().await else {
            break;
        };
        buckets.insert(id, bucket);
    }
    buckets
}

pub(crate) async fn new(data_dir: PathBuf) -> Data {
    Data {
        format: format(&data_dir).await,
        buckets: Arc::new(read_buckets(&data_dir).await),
        path: Arc::new(data_dir),
    }
}
