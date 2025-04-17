/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Connectivity;
use crate::Dimensions;
use crate::Embeddings;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexId;
use crate::IndexItemsCount;
use crate::Limit;
use crate::PrimaryKey;
use crate::db::Db;
use crate::db::DbExt;
use crate::index::actor::AnnR;
use crate::index::actor::Index;
use anyhow::anyhow;
use bimap::BiMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
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

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    derive_more::Display,
)]
/// Key for index embeddings
struct Key(u64);

pub(crate) fn new(
    id: IndexId,
    db: mpsc::Sender<Db>,
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
    let idx = Arc::new(RwLock::new(usearch::Index::new(&options)?));
    idx.write().unwrap().reserve(RESERVE_INCREMENT)?;

    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 100000;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        {
            let id = id.clone();
            async move {
                let mut items_count_db = IndexItemsCount(0);
                let items_count = Arc::new(AtomicU32::new(0));

                db.update_items_count(id.clone(), items_count_db)
                    .await
                    .unwrap_or_else(|err| warn!("index::new: unable update items count: {err}"));

                // bimap between PrimaryKey and Key for an usearch index
                let keys = Arc::new(RwLock::new(BiMap::new()));

                // Incremental key for a usearch index
                let usearch_key = Arc::new(AtomicU64::new(0));

                let mut housekeeping_interval = time::interval(time::Duration::from_secs(1));

                while !rx.is_closed() {
                    tokio::select! {
                        _ = housekeeping_interval.tick() => {
                            housekeeping(
                                &db,
                                id.clone(),
                                &mut items_count_db,
                                &items_count,
                            ).await;
                        }

                        Some(msg) = rx.recv() => {
                            tokio::spawn(
                                process(
                                    msg,
                                    dimensions,
                                    Arc::clone(&idx),
                                    Arc::clone(&keys),
                                    Arc::clone(&usearch_key),
                                    Arc::clone(&items_count),
                                )
                            );
                        }
                    }
                }
            }
        }
        .instrument(debug_span!("index", "{}", id.0)),
    );

    Ok(tx)
}

async fn process(
    msg: Index,
    dimensions: Dimensions,
    idx: Arc<RwLock<usearch::Index>>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    usearch_key: Arc<AtomicU64>,
    items_count: Arc<AtomicU32>,
) {
    match msg {
        Index::Add {
            primary_key,
            embeddings,
        } => {
            add(idx, keys, usearch_key, primary_key, embeddings, items_count).await;
        }

        Index::Ann {
            embeddings,
            limit,
            tx,
        } => {
            ann(idx, tx, keys, embeddings, dimensions, limit).await;
        }
    }
}

async fn housekeeping(
    db: &mpsc::Sender<Db>,
    id: IndexId,
    items_count_db: &mut IndexItemsCount,
    items_count: &AtomicU32,
) {
    let items = items_count.load(Ordering::Relaxed);
    if items != items_count_db.0 {
        debug!("housekeeping update items count: {items_count_db}",);
        items_count_db.0 = items;
        db.update_items_count(id, *items_count_db)
            .await
            .unwrap_or_else(|err| warn!("index::housekeeping: unable update items count: {err}"));
    }
}

async fn add(
    idx: Arc<RwLock<usearch::Index>>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    usearch_key: Arc<AtomicU64>,
    primary_key: PrimaryKey,
    embeddings: Embeddings,
    items_count: Arc<AtomicU32>,
) {
    let key = usearch_key.fetch_add(1, Ordering::Relaxed).into();

    if keys
        .write()
        .unwrap()
        .insert_no_overwrite(primary_key.clone(), key)
        .is_err()
    {
        warn!("index::add: primary_key already exists: {primary_key:?}");
        return;
    }

    let (tx, rx) = oneshot::channel();

    rayon::spawn(move || {
        let capacity = idx.read().unwrap().capacity();
        let free_space = capacity - idx.read().unwrap().size();
        if free_space < RESERVE_THRESHOLD {
            // free space below threshold, reserve more space
            let capacity = capacity + RESERVE_INCREMENT;
            if let Err(err) = idx.write().unwrap().reserve(capacity) {
                error!("index::add: unable to reserve index capacity for {capacity}: {err}");
                _ = tx.send(false);
                return;
            }
            debug!("index::add: reserved index capacity for {capacity}");
        }

        if let Err(err) = idx.read().unwrap().add(key.0, &embeddings.0) {
            warn!("index::add: unable to add embeddings for key {key}: {err}");
            _ = tx.send(false);
            return;
        };

        items_count.fetch_add(1, Ordering::Relaxed);
        _ = tx.send(true);
    });

    if let Ok(false) = rx.await {
        keys.write().unwrap().remove_by_right(&key);
    }
}

async fn ann(
    idx: Arc<RwLock<usearch::Index>>,
    tx_ann: oneshot::Sender<AnnR>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    embeddings: Embeddings,
    dimensions: Dimensions,
    limit: Limit,
) {
    let Some(embeddings_len) = NonZeroUsize::new(embeddings.0.len()) else {
        tx_ann
            .send(Err(anyhow!("index::ann: embeddings dimensions == 0")))
            .unwrap_or_else(|_| {
                warn!("index::ann: unable to send error response (zero dimensions)")
            });
        return;
    };
    if embeddings_len != dimensions.0 {
        tx_ann
            .send(Err(anyhow!(
                "index::ann: wrong embeddings dimensions: {embeddings_len} != {dimensions}",
            )))
            .unwrap_or_else(|_| {
                warn!("index::ann: unable to send error response (wrong dimensions)")
            });
        return;
    }

    let (tx, rx) = oneshot::channel();
    rayon::spawn(move || {
        _ = tx.send(idx.read().unwrap().search(&embeddings.0, limit.0.get()));
    });

    tx_ann
        .send(
            rx.await
                .map_err(|err| anyhow!("index::ann: unable to recv matches: {err}"))
                .and_then(|matches| {
                    matches.map_err(|err| anyhow!("index::ann: search failed: {err}"))
                })
                .and_then(|matches| {
                    let primary_keys = {
                        let keys = keys.read().unwrap();
                        matches
                            .keys
                            .into_iter()
                            .map(|key| {
                                keys.get_by_right(&key.into())
                                    .cloned()
                                    .ok_or(anyhow!("not defined primary key column {key}"))
                            })
                            .collect::<anyhow::Result<_>>()?
                    };
                    let distances = matches
                        .distances
                        .into_iter()
                        .map(|value| value.into())
                        .collect();
                    Ok((primary_keys, distances))
                }),
        )
        .unwrap_or_else(|_| warn!("index::ann: unable to send response"));
}
