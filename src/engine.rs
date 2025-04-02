/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexId;
use crate::IndexMetadata;
use crate::db::Db;
use crate::db::DbExt;
use crate::index;
use crate::index::Index;
use crate::monitor_indexes;
use crate::monitor_items;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::error;
use tracing::warn;

pub(crate) enum Engine {
    GetIndexIds {
        tx: oneshot::Sender<Vec<IndexId>>,
    },
    AddIndex {
        metadata: IndexMetadata,
    },
    DelIndex {
        id: IndexId,
    },
    GetIndex {
        id: IndexId,
        tx: oneshot::Sender<Option<mpsc::Sender<Index>>>,
    },
}

pub(crate) trait EngineExt {
    async fn get_index_ids(&self) -> Vec<IndexId>;
    async fn add_index(&self, metadata: IndexMetadata);
    async fn del_index(&self, id: IndexId);
    async fn get_index(&self, id: IndexId) -> Option<mpsc::Sender<Index>>;
}

impl EngineExt for mpsc::Sender<Engine> {
    async fn get_index_ids(&self) -> Vec<IndexId> {
        let (tx, rx) = oneshot::channel();
        if self.send(Engine::GetIndexIds { tx }).await.is_ok() {
            rx.await.unwrap_or(Vec::new())
        } else {
            Vec::new()
        }
    }

    async fn add_index(&self, metadata: IndexMetadata) {
        self.send(Engine::AddIndex { metadata })
            .await
            .unwrap_or_else(|err| warn!("EngineExt::add_index: unable to send request: {err}"));
    }

    async fn del_index(&self, id: IndexId) {
        self.send(Engine::DelIndex { id })
            .await
            .unwrap_or_else(|err| warn!("EngineExt::del_index: unable to send request: {err}"));
    }

    async fn get_index(&self, id: IndexId) -> Option<mpsc::Sender<Index>> {
        let (tx, rx) = oneshot::channel();
        if self.send(Engine::GetIndex { id, tx }).await.is_ok() {
            rx.await.ok().flatten()
        } else {
            None
        }
    }
}

pub(crate) async fn new(db: mpsc::Sender<Db>) -> anyhow::Result<mpsc::Sender<Engine>> {
    let (tx, mut rx) = mpsc::channel(10);

    let monitor_actor = monitor_indexes::new(db.clone(), tx.clone()).await?;

    tokio::spawn(async move {
        let mut indexes = HashMap::new();
        let mut monitors = HashMap::new();
        while let Some(msg) = rx.recv().await {
            match msg {
                Engine::GetIndexIds { tx } => {
                    tx.send(indexes.keys().cloned().collect())
                        .unwrap_or_else(|_| {
                            warn!("engine::Engine::GetIndexIds: unable to send response")
                        });
                }

                Engine::AddIndex { metadata } => {
                    let id = metadata.id();
                    if indexes.contains_key(&id) {
                        warn!("engine::Engine::AddIndex: trying to replace index with id {id}");
                        continue;
                    }

                    let Ok(index_actor) = index::new(
                        id.clone(),
                        db.clone(),
                        metadata.dimensions,
                        metadata.connectivity,
                        metadata.expansion_add,
                        metadata.expansion_search,
                    )
                    .inspect_err(|err| {
                        error!("unable to create index with metadata {metadata:?}: {err}")
                    }) else {
                        continue;
                    };

                    let Ok(db_index) = db.get_index_db(metadata.clone()).await.inspect_err(|err| {
                        error!("unable to create db_index from {metadata:?}: {err}")
                    }) else {
                        continue;
                    };

                    let Ok(monitor_actor) = monitor_items::new(
                        db_index.clone(),
                        index_actor.clone(),
                    )
                    .await
                    .inspect_err(|err| {
                        error!("unable to create monitor items with metadata {metadata:?}: {err}")
                    }) else {
                        continue;
                    };

                    indexes.insert(id.clone(), index_actor);
                    monitors.insert(id, monitor_actor);
                }

                Engine::DelIndex { id } => {
                    indexes.remove(&id);
                    monitors.remove(&id);
                    db.remove_index(id).await.unwrap_or_else(|err| {
                        warn!("engine::Engine::DelIndex: issue while removing index: {err}")
                    });
                }

                Engine::GetIndex { id, tx } => {
                    tx.send(indexes.get(&id).cloned()).unwrap_or_else(|_| {
                        warn!("engine::Engine::GetIndex: unable to send response")
                    });
                }
            }
        }
        drop(monitor_actor);
    });

    Ok(tx)
}
