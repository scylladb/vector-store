/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Connectivity;
use crate::Dimensions;
use crate::Distance;
use crate::Embeddings;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexId;
use crate::IndexItemsCount;
use crate::Key;
use crate::Limit;
use crate::db::Db;
use crate::index::actor::Index;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::info;

pub fn new(
    id: IndexId,
    db: mpsc::Sender<Db>,
    dimensions: Dimensions,
    _connectivity: Connectivity,
    _expansion_add: ExpansionAdd,
    _expansion_search: ExpansionSearch,
) -> anyhow::Result<mpsc::Sender<Index>> {
    info!("Creating new index with id: {id}");
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 100000;
    let (tx, mut _rx) = mpsc::channel(CHANNEL_SIZE);

    Ok(tx)
}

async fn housekeeping(
    db: &mpsc::Sender<Db>,
    id: IndexId,
    items_count_db: &mut IndexItemsCount,
    items_count: &AtomicU32,
    counter_add: &AtomicUsize,
    counter_ann: &AtomicUsize,
    channel_len: usize,
) {
    {}
}

async fn add(
    idx: Arc<usearch::Index>,
    idx_lock: Arc<RwLock<()>>,
    key: Key,
    embeddings: Embeddings,
    items_count: Arc<AtomicU32>,
    counter: Arc<AtomicUsize>,
) {
}

async fn ann(
    idx: Arc<usearch::Index>,
    tx: oneshot::Sender<anyhow::Result<(Vec<Key>, Vec<Distance>)>>,
    embeddings: Embeddings,
    dimensions: Dimensions,
    limit: Limit,
    counter: Arc<AtomicUsize>,
) {
}
