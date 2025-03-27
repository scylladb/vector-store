/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Proprietary
 */

use crate::index;
use crate::index::Index;
use crate::modify_indexes;
use crate::modify_indexes::ModifyIndexesExt;
use crate::monitor_indexes;
use crate::monitor_items;
use crate::ColumnName;
use crate::Connectivity;
use crate::Dimensions;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexId;
use crate::ScyllaDbUri;
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
        id: IndexId,
        col_id: ColumnName,
        col_emb: ColumnName,
        dimensions: Dimensions,
        connectivity: Connectivity,
        expansion_add: ExpansionAdd,
        expansion_search: ExpansionSearch,
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
    #[allow(clippy::too_many_arguments)] // TODO: support for table params is experimental
    async fn add_index(
        &self,
        id: IndexId,
        col_id: ColumnName,
        col_emb: ColumnName,
        dimensions: Dimensions,
        connectivity: Connectivity,
        expansion_add: ExpansionAdd,
        expansion_search: ExpansionSearch,
    );
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

    async fn add_index(
        &self,
        id: IndexId,
        col_id: ColumnName,
        col_emb: ColumnName,
        dimensions: Dimensions,
        connectivity: Connectivity,
        expansion_add: ExpansionAdd,
        expansion_search: ExpansionSearch,
    ) {
        self.send(Engine::AddIndex {
            id,
            col_id,
            col_emb,
            dimensions,
            connectivity,
            expansion_add,
            expansion_search,
        })
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

pub(crate) async fn new(uri: ScyllaDbUri) -> anyhow::Result<mpsc::Sender<Engine>> {
    let (tx, mut rx) = mpsc::channel(10);

    let monitor_actor = monitor_indexes::new(uri.clone(), tx.clone()).await?;
    let modify_actor = modify_indexes::new(uri.clone()).await?;

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

                Engine::AddIndex {
                    id,
                    col_id,
                    col_emb,
                    dimensions,
                    connectivity,
                    expansion_add,
                    expansion_search,
                } => {
                    if indexes.contains_key(&id) {
                        continue;
                    }
                    if let Ok(index_actor) = index::new(
                        id.clone(),
                        modify_actor.clone(),
                        dimensions,
                        connectivity,
                        expansion_add,
                        expansion_search,
                    ) {
                        if let Ok(monitor_actor) = monitor_items::new(
                            uri.clone(),
                            id.clone().0.into(),
                            col_id.clone(),
                            col_emb.clone(),
                            index_actor.clone(),
                        )
                        .await.inspect_err(|err| error!("unable to create monitor items with uri {uri}, table {id}, col_id {col_id}, col_emb {col_emb}: {err}"))
                        {
                            indexes.insert(id.clone(), index_actor);
                            monitors.insert(id, monitor_actor);
                        }
                    } else {
                        error!("unable to create index with dimensions {dimensions}");
                    }
                }

                Engine::DelIndex { id } => {
                    indexes.remove(&id);
                    monitors.remove(&id);
                    modify_actor.del(id).await;
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
