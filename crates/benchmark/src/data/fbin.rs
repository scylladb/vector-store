/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::data::Query;
use futures::StreamExt;
use futures::stream;
use futures::stream::BoxStream;
use serde::Deserialize;
use std::collections::HashSet;
use std::io::SeekFrom;
use std::mem;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::BufReader;

#[derive(Deserialize)]
pub(crate) struct Config {
    data_fbin: PathBuf,
    query_fbin: PathBuf,
    query_ibin: PathBuf,
}

#[derive(Debug, PartialEq)]
struct Header {
    count: u32,
    dimension: u32,
}

impl Header {
    const SIZE: usize = mem::size_of::<u32>() * 2;

    async fn header(path: &Path) -> Self {
        let mut file = File::open(path).await.unwrap();
        let count = file.read_u32_le().await.unwrap();
        let dimension = file.read_u32_le().await.unwrap();
        Self { count, dimension }
    }
}

pub(crate) async fn dimension(path: Arc<PathBuf>, config: Arc<Config>) -> usize {
    Header::header(&path.join(&config.data_fbin))
        .await
        .dimension as usize
}

pub(crate) async fn ids_stream(path: Arc<PathBuf>, config: Arc<Config>) -> BoxStream<'static, i64> {
    let header = Header::header(&path.join(&config.data_fbin)).await;
    stream::iter(0..header.count as i64).boxed()
}

pub(crate) async fn vector_stream(
    path: Arc<PathBuf>,
    config: Arc<Config>,
) -> BoxStream<'static, (i64, Vec<f32>)> {
    let header = Header::header(&path.join(&config.data_fbin)).await;

    let mut data_fbin = BufReader::new(File::open(path.join(&config.data_fbin)).await.unwrap());

    data_fbin
        .seek(SeekFrom::Start(Header::SIZE as u64))
        .await
        .unwrap();

    stream::unfold(
        (0, header, data_fbin),
        |(id, header, mut data_fbin)| async move {
            if id == header.count {
                return None;
            }

            let mut vector = Vec::with_capacity(header.dimension as usize);
            for _ in 0..header.dimension {
                vector.push(data_fbin.read_f32_le().await.unwrap());
            }
            Some(((id as i64, vector), (id + 1, header, data_fbin)))
        },
    )
    .boxed()
}

pub(crate) async fn queries(
    path: Arc<PathBuf>,
    config: Arc<Config>,
    _id_ok: impl Fn(i64) -> bool,
    limit: usize,
) -> Vec<Query> {
    let header_fbin = Header::header(&path.join(&config.query_fbin)).await;
    let header_ibin = Header::header(&path.join(&config.query_ibin)).await;
    assert_eq!(header_fbin.count, header_ibin.count);

    let limit = limit.min(header_ibin.dimension as usize);
    // Number of bytes to skip after reading neighbors
    let skip_ibin = ((header_ibin.dimension as usize - limit) * std::mem::size_of::<i32>()) as i64;

    let mut fbin = BufReader::new(File::open(path.join(&config.query_fbin)).await.unwrap());
    let mut ibin = BufReader::new(File::open(path.join(&config.query_ibin)).await.unwrap());

    fbin.seek(SeekFrom::Start(Header::SIZE as u64))
        .await
        .unwrap();
    ibin.seek(SeekFrom::Start(Header::SIZE as u64))
        .await
        .unwrap();

    stream::unfold(
        (0, header_fbin, header_ibin, fbin, ibin),
        |(id, header_fbin, header_ibin, mut fbin, mut ibin)| async move {
            if id == header_fbin.count {
                return None;
            }

            let mut query = Query {
                query: Vec::with_capacity(header_fbin.dimension as usize),
                neighbors: HashSet::with_capacity(limit),
            };
            for _ in 0..header_fbin.dimension {
                query.query.push(fbin.read_f32_le().await.unwrap());
            }
            for _ in 0..limit {
                query
                    .neighbors
                    .insert(ibin.read_i32_le().await.unwrap() as i64);
            }
            ibin.seek(SeekFrom::Current(skip_ibin)).await.unwrap();
            Some((query, (id + 1, header_fbin, header_ibin, fbin, ibin)))
        },
    )
    .collect()
    .await
}
