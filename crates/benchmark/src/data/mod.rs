/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod fbin;
mod parquet;

use futures::StreamExt;
use futures::stream::BoxStream;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tracing::info;

const DATASET_FILENAME: &str = "dataset.toml";
const LEVELS_FILENAME: &str = "levels.bin";

pub(crate) struct Query {
    pub(crate) query: Vec<f32>,
    pub(crate) neighbors: HashSet<i64>,
}

pub(crate) struct Data {
    path: Arc<PathBuf>,
    format: Format,
    levels: Arc<Option<HashMap<i64, u8>>>,
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

    pub(crate) async fn queries(&self, level: Option<u8>, limit: usize) -> Vec<Query> {
        let levels = Arc::clone(&self.levels);
        let id_ok = move |id| {
            let Some(level) = &level else {
                return true;
            };
            let Some(levels) = levels.as_ref() else {
                panic!("Levels not built for dataset at {:?}", self.path);
            };
            levels.get(&id) == Some(level)
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

    pub(crate) async fn vector_stream(&self) -> BoxStream<'static, (i64, Vec<f32>)> {
        match &self.format {
            Format::Parquet(config) => {
                parquet::vector_stream(Arc::clone(&self.path), Arc::clone(config)).await
            }
            Format::Fbin(config) => {
                fbin::vector_stream(Arc::clone(&self.path), Arc::clone(config)).await
            }
        }
    }

    pub(crate) async fn build_levels(&mut self) {
        let stream = match &self.format {
            Format::Parquet(config) => {
                parquet::ids_stream(Arc::clone(&self.path), Arc::clone(config)).await
            }
            Format::Fbin(config) => {
                fbin::ids_stream(Arc::clone(&self.path), Arc::clone(config)).await
            }
        };
        info!(
            "Building levels for dataset at {path:?}...",
            path = self.path
        );
        let mut levels = stream
            .map(|id| (id, u8::MAX))
            .collect::<HashMap<_, _>>()
            .await;
        const LEVELS: usize = 9;
        let max_levels = [
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
        assert!(LEVELS == max_levels.len());
        let mut counts = [0; LEVELS];
        levels.values_mut().for_each(|level| {
            counts.iter_mut().enumerate().for_each(|(idx, count)| {
                *count += 1;
                if *level == u8::MAX && *count >= max_levels[idx] {
                    *level = idx as u8;
                    *count -= max_levels[idx];
                }
            });
        });
        self.levels = Arc::new(Some(levels));
    }

    pub(crate) async fn write_levels(&self) {
        let Some(levels) = self.levels.as_ref() else {
            return;
        };
        let path = self.path.join(LEVELS_FILENAME);
        info!("Writing levels at {path:?}...");
        let mut levels_writer = BufWriter::new(File::create(path).await.unwrap());
        for (id, level) in levels.iter().filter(|(_, level)| **level != u8::MAX) {
            levels_writer.write_i64(*id).await.unwrap();
            levels_writer.write_u8(*level).await.unwrap();
        }
    }

    pub(crate) async fn read_levels(&mut self) {
        let path = self.path.join(LEVELS_FILENAME);
        info!("Readings levels from {path:?}...");
        let mut levels_reader = BufReader::new(File::open(path).await.unwrap());
        let mut levels = HashMap::new();
        loop {
            let Ok(id) = levels_reader.read_i64().await else {
                break;
            };
            let Ok(level) = levels_reader.read_u8().await else {
                break;
            };
            levels.insert(id, level);
        }
        self.levels = Arc::new(Some(levels));
    }

    pub(crate) fn level_fn(&self, local: bool) -> impl Fn(i64) -> i64 {
        let levels = Arc::clone(&self.levels);
        move |id| {
            if !local {
                return id;
            }
            let Some(levels) = levels.as_ref() else {
                panic!("Levels not built for dataset at {:?}", self.path);
            };
            *levels.get(&id).unwrap_or(&u8::MAX) as i64
        }
    }
}

#[derive(Deserialize)]
struct Config {
    parquet: Option<parquet::Config>,
    fbin: Option<fbin::Config>,
}

pub(crate) async fn new(path: PathBuf) -> Data {
    let toml_path = path.join(DATASET_FILENAME);
    let Ok(config) = fs::read(&toml_path).await else {
        info!("Not found {DATASET_FILENAME} in {path:?}. Using default parquet format.");
        return Data {
            path: Arc::new(path),
            format: Format::Parquet(Arc::new(parquet::Config::default())),
            levels: Arc::new(None),
        };
    };
    let config: Config = toml::from_slice(&config)
        .unwrap_or_else(|err| panic!("Failed to parse {toml_path:?}: {err}"));
    if let Some(config) = config.parquet {
        return Data {
            path: Arc::new(path),
            format: Format::Parquet(Arc::new(config)),
            levels: Arc::new(None),
        };
    }
    if let Some(config) = config.fbin {
        return Data {
            path: Arc::new(path),
            format: Format::Fbin(Arc::new(config)),
            levels: Arc::new(None),
        };
    }
    info!("Not found format type in {DATASET_FILENAME} in {path:?}. Using default parquet format.");
    Data {
        path: Arc::new(path),
        format: Format::Parquet(Arc::new(parquet::Config::default())),
        levels: Arc::new(None),
    }
}
