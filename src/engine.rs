/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexId;
use crate::IndexMetadata;
use crate::db::Db;
use crate::db::DbExt;
use crate::db_index::DbIndex;
use crate::factory::IndexFactory;
use crate::index::Index;
use crate::monitor_indexes;
use crate::monitor_items;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::info;
use tracing::trace;

type GetIndexIdsR = Vec<IndexId>;
type GetIndexR = Option<(mpsc::Sender<Index>, mpsc::Sender<DbIndex>)>;

pub(crate) enum Engine {
    GetIndexIds {
        tx: oneshot::Sender<GetIndexIdsR>,
    },
    AddIndex {
        metadata: IndexMetadata,
    },
    DelIndex {
        id: IndexId,
    },
    GetIndex {
        id: IndexId,
        tx: oneshot::Sender<GetIndexR>,
    },
}

pub(crate) trait EngineExt {
    async fn get_index_ids(&self) -> GetIndexIdsR;
    async fn add_index(&self, metadata: IndexMetadata);
    async fn del_index(&self, id: IndexId);
    async fn get_index(&self, id: IndexId) -> GetIndexR;
}

impl EngineExt for mpsc::Sender<Engine> {
    async fn get_index_ids(&self) -> GetIndexIdsR {
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
            .unwrap_or_else(|err| trace!("EngineExt::add_index: unable to send request: {err}"));
    }

    async fn del_index(&self, id: IndexId) {
        self.send(Engine::DelIndex { id })
            .await
            .unwrap_or_else(|err| trace!("EngineExt::del_index: unable to send request: {err}"));
    }

    async fn get_index(&self, id: IndexId) -> GetIndexR {
        let (tx, rx) = oneshot::channel();
        if self.send(Engine::GetIndex { id, tx }).await.is_ok() {
            rx.await.ok().flatten()
        } else {
            None
        }
    }
}

pub(crate) async fn new(
    db: mpsc::Sender<Db>,
    index_factory: Box<dyn IndexFactory + Send>,
) -> anyhow::Result<mpsc::Sender<Engine>> {
    let (tx, mut rx) = mpsc::channel(10);

    let monitor_actor = monitor_indexes::new(db.clone(), tx.clone()).await?;

    tokio::spawn(
        async move {
            debug!("starting");

            let mut indexes = HashMap::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    Engine::GetIndexIds { tx } => {
                        tx.send(indexes.keys().cloned().collect())
                            .unwrap_or_else(|_| {
                                trace!("Engine::GetIndexIds: unable to send response")
                            });
                    }

                    Engine::AddIndex { metadata } => {
                        let id = metadata.id();
                        if indexes.contains_key(&id) {
                            trace!("Engine::AddIndex: trying to replace index with id {id}");
                            continue;
                        }

                        info!("creating a new index {id}");
                        let Ok(index_actor) = index_factory.create_index(
                            id.clone(),
                            metadata.dimensions,
                            metadata.connectivity,
                            metadata.expansion_add,
                            metadata.expansion_search,
                            metadata.space_type,
                        )
                        .inspect_err(|err| error!("unable to create an index {id}: {err}")) else {
                            continue;
                        };

                        let Ok((db_index, embeddings_stream)) =
                            db.get_db_index(metadata.clone()).await.inspect_err(|err| {
                                error!("unable to create a db monitoring task for an index {id}: {err}")
                            })
                        else {
                            continue;
                        };

                        let Ok(monitor_actor) =
                            monitor_items::new(id.clone(), embeddings_stream, index_actor.clone())
                                .await
                                .inspect_err(|err| {
                                    error!(
                                        "unable to create a synchronisation task between a db and an index {id}: {err}"
                                    )
                                })
                        else {
                            continue;
                        };

                        indexes.insert(id.clone(), (index_actor, monitor_actor, db_index));
                    }

                    Engine::DelIndex { id } => {
                        info!("removing an index {id}");
                        indexes.remove(&id);
                    }

                    Engine::GetIndex { id, tx } => {
                        tx.send(
                            indexes
                                .get(&id)
                                .map(|(index, _, db_index)| (index.clone(), db_index.clone())),
                        )
                        .unwrap_or_else(|_| trace!("Engine::GetIndex: unable to send response"));
                    }
                }
            }
            drop(monitor_actor);

            debug!("starting");
        }
        .instrument(debug_span!("engine")),
    );

    Ok(tx)
}
