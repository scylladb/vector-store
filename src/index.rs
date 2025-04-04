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
use crate::modify_indexes::ModifyIndexes;
use crate::modify_indexes::ModifyIndexesExt;
use anyhow::anyhow;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::info;
use tracing::warn;
use usearch::IndexOptions;
use usearch::ScalarKind;

// Initial and incremental number for the index vectors reservation.
// The value was taken for initial benchmarks (size similar to benchmark size)
const RESERVE_INCREMENT: usize = 1000000;

// When free space for index vectors drops below this, will reserve more space
// The ratio was taken for initial benchmarks
const RESERVE_THRESHOLD: usize = RESERVE_INCREMENT / 3;

pub(crate) enum Index {
    Add {
        key: Key,
        embeddings: Embeddings,
    },
    Ann {
        embeddings: Embeddings,
        limit: Limit,
        tx: oneshot::Sender<anyhow::Result<(Vec<Key>, Vec<Distance>)>>,
    },
}

pub(crate) trait IndexExt {
    async fn add(&self, key: Key, embeddings: Embeddings);
    async fn ann(
        &self,
        embeddings: Embeddings,
        limit: Limit,
    ) -> anyhow::Result<(Vec<Key>, Vec<Distance>)>;
}

impl IndexExt for mpsc::Sender<Index> {
    async fn add(&self, key: Key, embeddings: Embeddings) {
        self.send(Index::Add { key, embeddings })
            .await
            .unwrap_or_else(|err| warn!("IndexExt::add: unable to send request: {err}"));
    }

    async fn ann(
        &self,
        embeddings: Embeddings,
        limit: Limit,
    ) -> anyhow::Result<(Vec<Key>, Vec<Distance>)> {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Ann {
            embeddings,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }
}

pub(crate) fn new(
    id: IndexId,
    modify_actor: mpsc::Sender<ModifyIndexes>,
    _db: mpsc::Sender<Db>,
    dimensions: Dimensions,
    connectivity: Connectivity,
    expansion_add: ExpansionAdd,
    expansion_search: ExpansionSearch,
) -> anyhow::Result<mpsc::Sender<Index>> {
    let options = IndexOptions {
        dimensions: dimensions.0.get(),
        connectivity: connectivity.0,
        expansion_add: expansion_add.0,
        expansion_search: expansion_search.0,
        quantization: ScalarKind::F32,
        ..Default::default()
    };

    info!("Creating new index with id: {id}");
    let idx = Arc::new(usearch::Index::new(&options)?);
    idx.reserve(RESERVE_INCREMENT)?;

    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 100000;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        {
            let id = id.clone();
            async move {
                let mut items_count_db = IndexItemsCount(0);
                let items_count = Arc::new(AtomicU32::new(0));
                modify_actor
                    .update_items_count(id.clone(), items_count_db)
                    .await;
                let mut housekeeping_interval = time::interval(time::Duration::from_secs(1));
                let idx_lock = Arc::new(RwLock::new(()));
                let counter_add = Arc::new(AtomicUsize::new(0));
                let counter_ann = Arc::new(AtomicUsize::new(0));

                while !rx.is_closed() {
                    tokio::select! {
                        _ = housekeeping_interval.tick() => {
                            housekeeping(
                                &modify_actor,
                                id.clone(),
                                &mut items_count_db,
                                &items_count,
                                &counter_add,
                                &counter_ann,
                                rx.len()
                            ).await;
                        }

                        Some(msg) = rx.recv() => {
                            match msg {
                                Index::Add { key, embeddings } => {
                                    add(
                                        Arc::clone(&idx),
                                        Arc::clone(&idx_lock),
                                        key,
                                        embeddings,
                                        Arc::clone(&items_count),
                                        Arc::clone(&counter_add)
                                    ).await;
                                }

                                Index::Ann {
                                    embeddings,
                                    limit,
                                    tx,
                                } => {
                                    ann(
                                        Arc::clone(&idx),
                                        tx,
                                        embeddings,
                                        dimensions,
                                        limit,
                                        Arc::clone(&counter_ann)
                                    ).await;
                                }
                            }
                        }
                    }
                }
            }
        }
        .instrument(debug_span!("index", "{}", id.0)),
    );

    Ok(tx)
}

async fn housekeeping(
    modify_actor: &mpsc::Sender<ModifyIndexes>,
    id: IndexId,
    items_count_db: &mut IndexItemsCount,
    items_count: &AtomicU32,
    counter_add: &AtomicUsize,
    counter_ann: &AtomicUsize,
    channel_len: usize,
) {
    {
        let add = counter_add.load(Ordering::Relaxed);
        let ann = counter_ann.load(Ordering::Relaxed);
        if add != 0 || ann != 0 || channel_len != 0 {
            debug!("housekeeping counters: add: {add}, ann: {ann}, channel len {channel_len}",);
        }
    }
    let items = items_count.load(Ordering::Relaxed);
    if items != items_count_db.0 {
        debug!("housekeeping update items count: {items_count_db}",);
        items_count_db.0 = items;
        modify_actor.update_items_count(id, *items_count_db).await;
    }
}

async fn add(
    idx: Arc<usearch::Index>,
    idx_lock: Arc<RwLock<()>>,
    key: Key,
    embeddings: Embeddings,
    items_count: Arc<AtomicU32>,
    counter: Arc<AtomicUsize>,
) {
    rayon::spawn(move || {
        counter.fetch_add(1, Ordering::Relaxed);

        let capacity = idx.capacity();
        if capacity - idx.size() < RESERVE_THRESHOLD {
            // free space below threshold, reserve more space
            let _lock = idx_lock.write().unwrap();
            let capacity = capacity + RESERVE_INCREMENT;
            debug!("index::add: trying to reserve {capacity}");
            if let Err(err) = idx.reserve(capacity) {
                error!("index::add: unable to reserve index capacity for {capacity}: {err}");
                counter.fetch_sub(1, Ordering::Relaxed);
                return;
            }
            debug!("index::add: finished reserve {capacity}");
        }

        let _lock = idx_lock.read().unwrap();
        if let Err(err) = idx.add(key.0, &embeddings.0) {
            warn!("index::add: unable to add embeddings for key {key}: {err}");
            counter.fetch_sub(1, Ordering::Relaxed);
            return;
        };
        items_count.fetch_add(1, Ordering::Relaxed);
        counter.fetch_sub(1, Ordering::Relaxed);
    });
}

async fn ann(
    idx: Arc<usearch::Index>,
    tx: oneshot::Sender<anyhow::Result<(Vec<Key>, Vec<Distance>)>>,
    embeddings: Embeddings,
    dimensions: Dimensions,
    limit: Limit,
    counter: Arc<AtomicUsize>,
) {
    let Some(embeddings_len) = NonZeroUsize::new(embeddings.0.len()) else {
        tx.send(Err(anyhow!("index ann query: embeddings dimensions == 0")))
            .unwrap_or_else(|_| {
                warn!("index::new: Index::Ann: unable to send error response (zero dimensions)")
            });
        return;
    };
    if embeddings_len != dimensions.0 {
        tx.send(Err(anyhow!(
            "index ann query: wrong embeddings dimensions: {embeddings_len} != {dimensions}",
        )))
        .unwrap_or_else(|_| {
            warn!("index::new: Index::Ann: unable to send error response (wrong dimensions)")
        });
        return;
    }
    rayon::spawn({
        let idx = Arc::clone(&idx);
        move || {
            counter.fetch_add(1, Ordering::Relaxed);
            tx.send(
                idx.search(&embeddings.0, limit.0.get())
                    .map(|results| {
                        (
                            results.keys.into_iter().map(|key| key.into()).collect(),
                            results
                                .distances
                                .into_iter()
                                .map(|value| value.into())
                                .collect(),
                        )
                    })
                    .map_err(|err| anyhow!("index ann query: search failed: {err}")),
            )
            .unwrap_or_else(|_| warn!("index::new: Index::Ann: unable to send response"));
            counter.fetch_sub(1, Ordering::Relaxed);
        }
    });
}
