/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Config;
use crate::DbIndexType;
use crate::IndexKey;
use crate::IndexMetadata;
use crate::Metrics;
use crate::Quantization;
use crate::db::Db;
use crate::db::DbExt;
use crate::db_index::DbIndex;
use crate::db_index::DbIndexExt;
use crate::factory::IndexFactory;
use crate::index::Index;
use crate::index::factory::IndexConfiguration;
use crate::indexes::IndexCache;
use crate::indexes::Indexes;
use crate::memory;
use crate::memory::Memory;
use crate::monitor_indexes;
use crate::monitor_items;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use crate::perf;
use crate::table::Table;
use itertools::Itertools;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;
use tracing::trace;

type GetIndexKeysR = Vec<(IndexKey, Quantization)>;
type AddIndexR = anyhow::Result<()>;
type GetIndexR = Option<(mpsc::Sender<Index>, mpsc::Sender<DbIndex>)>;
type GetIndexesR = Arc<RwLock<Indexes>>;

pub(crate) enum Engine {
    GetIndexIds {
        tx: oneshot::Sender<GetIndexKeysR>,
    },
    AddIndex {
        metadata: IndexMetadata,
        tx: oneshot::Sender<AddIndexR>,
    },
    DelIndex {
        key: IndexKey,
    },
    GetIndex {
        key: IndexKey,
        tx: oneshot::Sender<GetIndexR>,
    },
    GetIndexes {
        tx: oneshot::Sender<GetIndexesR>,
    },
}

pub(crate) trait EngineExt {
    async fn get_index_keys(&self) -> GetIndexKeysR;
    async fn add_index(&self, metadata: IndexMetadata) -> AddIndexR;
    async fn del_index(&self, key: IndexKey);
    async fn get_index(&self, key: IndexKey) -> GetIndexR;
    async fn get_indexes(&self) -> GetIndexesR;
}

impl EngineExt for mpsc::Sender<Engine> {
    async fn get_index_keys(&self) -> GetIndexKeysR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetIndexIds { tx })
            .await
            .expect("EngineExt::get_index_keys: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_index_keys: internal actor should send response")
    }

    async fn add_index(&self, metadata: IndexMetadata) -> AddIndexR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::AddIndex { metadata, tx })
            .await
            .expect("EngineExt::add_index: internal actor should receive request");
        rx.await
            .expect("EngineExt::add_index: internal actor should send response")
    }

    async fn del_index(&self, key: IndexKey) {
        self.send(Engine::DelIndex { key })
            .await
            .expect("EngineExt::del_index: internal actor should receive request");
    }

    async fn get_index(&self, key: IndexKey) -> GetIndexR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetIndex { key, tx })
            .await
            .expect("EngineExt::get_index: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_index: internal actor should send response")
    }

    async fn get_indexes(&self) -> GetIndexesR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetIndexes { tx })
            .await
            .expect("EngineExt::get_indexes: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_indexes: internal actor should send response")
    }
}

pub(crate) async fn new(
    db: mpsc::Sender<Db>,
    index_factory: Box<dyn IndexFactory + Send + Sync>,
    node_state: Sender<NodeState>,
    metrics: Arc<Metrics>,
    config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<mpsc::Sender<Engine>> {
    let (tx, mut rx) = mpsc::channel(perf::channel_size());

    let monitor_actor =
        monitor_indexes::new(db.clone(), tx.downgrade(), node_state.clone()).await?;
    let memory_actor = memory::new(config_rx);

    tokio::spawn(
        async move {
            debug!("starting");

            let indexes = Arc::new(RwLock::new(Indexes::new()));
            const CHECK_INTERVAL: Duration = Duration::from_secs(1);
            let mut interval = time::interval(CHECK_INTERVAL);
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        match msg {
                            Engine::GetIndexIds { tx } => get_index_keys(tx, &indexes).await,

                            Engine::AddIndex { metadata, tx } => {
                                add_index(
                                    metadata,
                                    tx,
                                    &db,
                                    index_factory.as_ref(),
                                    &indexes,
                                    metrics.clone(),
                                    memory_actor.clone(),
                                )
                                .await
                            }

                            Engine::DelIndex { key } => del_index(key, &indexes).await,

                            Engine::GetIndex { key, tx } => get_index(key, tx, &indexes).await,

                            Engine::GetIndexes { tx } => tx.send(Arc::clone(&indexes)).unwrap_or(()),
                        }
                    }

                    _ = interval.tick() => update_indexes(&node_state, &indexes).await,
                }

            }
            drop(monitor_actor);

            debug!("finished");
        }
        .instrument(debug_span!("engine")),
    );

    Ok(tx)
}

async fn get_index_keys(tx: oneshot::Sender<GetIndexKeysR>, indexes: &RwLock<Indexes>) {
    tx.send(
        indexes
            .read()
            .unwrap()
            .iter()
            .map(|(key, cache)| (key.clone(), cache.quantization()))
            .collect(),
    )
    .unwrap_or_else(|_| trace!("Engine::GetIndexIds: unable to send response"));
}

async fn add_index(
    metadata: IndexMetadata,
    tx: oneshot::Sender<AddIndexR>,
    db: &mpsc::Sender<Db>,
    index_factory: &(dyn IndexFactory + Send + Sync),
    indexes: &RwLock<Indexes>,
    metrics: Arc<Metrics>,
    memory: Sender<Memory>,
) {
    let key = metadata.key();
    if indexes.read().unwrap().contains_key(&key) {
        trace!("add_index: trying to replace index with key {key}");
        tx.send(Ok(()))
            .unwrap_or_else(|_| trace!("add_index: unable to send response"));
        return;
    }

    let (db_index, embeddings_stream) = match db.get_db_index(metadata.clone()).await {
        Ok((db_index, embeddings_stream)) => (db_index, embeddings_stream),
        Err(err) => {
            debug!("unable to create a db monitoring task for an index {key}: {err}");
            tx.send(Err(err))
                .unwrap_or_else(|_| trace!("add_index: unable to send response"));
            return;
        }
    };

    let primary_key_columns = db_index.get_primary_key_columns().await;
    let table_columns = db_index.get_table_columns().await;
    let partition_key_columns = match metadata.index_type {
        DbIndexType::Local(partition_key_columns) => Some(partition_key_columns),
        DbIndexType::Global => None,
    };
    let table = match Table::new(
        key.clone(),
        primary_key_columns,
        partition_key_columns,
        &metadata.filtering_columns,
        table_columns,
    ) {
        Ok(table) => Arc::new(RwLock::new(table)),
        Err(err) => {
            debug!("unable to create a table cache for an index {key}: {err}");
            tx.send(Err(err))
                .unwrap_or_else(|_| trace!("add_index: unable to send response"));
            return;
        }
    };

    let index_actor = match index_factory.create_index(
        IndexConfiguration {
            key: key.clone(),
            dimensions: metadata.dimensions,
            connectivity: metadata.connectivity,
            expansion_add: metadata.expansion_add,
            expansion_search: metadata.expansion_search,
            space_type: metadata.space_type,
            quantization: metadata.quantization,
        },
        Arc::clone(&table),
        memory,
    ) {
        Ok(actor) => actor,
        Err(err) => {
            debug!("unable to create an index {key}: {err}");
            tx.send(Err(err))
                .unwrap_or_else(|_| trace!("add_index: unable to send response"));
            return;
        }
    };

    let monitor_actor = match monitor_items::new(
        key.clone(),
        table,
        embeddings_stream,
        index_actor.clone(),
        metrics,
    )
    .await
    {
        Ok(actor) => actor,
        Err(err) => {
            debug!(
                "unable to create a synchronisation task between a db and an index {key}: {err}"
            );
            tx.send(Err(err))
                .unwrap_or_else(|_| trace!("add_index: unable to send response"));
            return;
        }
    };

    let cache = IndexCache::new(index_actor, monitor_actor, db_index, metadata.quantization).await;
    indexes.write().unwrap().insert(key.clone(), cache);
    info!("creating the index {key}");
    tx.send(Ok(()))
        .unwrap_or_else(|_| trace!("add_index: unable to send response"));
}

async fn del_index(key: IndexKey, indexes: &RwLock<Indexes>) {
    indexes.write().unwrap().remove(&key);
    info!("removed the index {key}");
}

async fn get_index(key: IndexKey, tx: oneshot::Sender<GetIndexR>, indexes: &RwLock<Indexes>) {
    tx.send(
        indexes
            .read()
            .unwrap()
            .get(&key)
            .map(|cache| (cache.index(), cache.db_index())),
    )
    .unwrap_or_else(|_| trace!("get_index: unable to send response"));
}

async fn update_indexes(node_state: &Sender<NodeState>, indexes: &RwLock<Indexes>) {
    let actual_indexes = indexes
        .read()
        .unwrap()
        .iter()
        .map(|(key, cache)| {
            (
                key.clone(),
                cache.db_index(),
                cache.progress(),
                cache.status(),
            )
        })
        .collect_vec();
    for (key, db_index, progress, status) in actual_indexes.into_iter() {
        let new_progress = db_index.full_scan_progress().await;
        if new_progress != progress
            && let Some(cache) = indexes.write().unwrap().get_mut(&key)
        {
            cache.set_progress(new_progress);
        }
        if let Some(new_status) = node_state
            .get_index_status(key.keyspace().as_ref(), key.index().as_ref())
            .await
            && new_status != status
            && let Some(cache) = indexes.write().unwrap().get_mut(&key)
        {
            cache.set_status(new_status);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use mockall::automock;

    #[automock]
    pub(crate) trait SimEngine {
        fn get_index_keys(
            &self,
            tx: oneshot::Sender<GetIndexKeysR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn add_index(
            &self,
            metadata: IndexMetadata,
            tx: oneshot::Sender<AddIndexR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn del_index(&self, key: IndexKey) -> impl Future<Output = ()> + Send + 'static;

        fn get_index(
            &self,
            key: IndexKey,
            tx: oneshot::Sender<GetIndexR>,
        ) -> impl Future<Output = ()> + Send + 'static;
    }

    pub(crate) fn new(sim: impl SimEngine + Send + 'static) -> mpsc::Sender<Engine> {
        with_size(10, sim)
    }

    pub(crate) fn with_size(
        size: usize,
        sim: impl SimEngine + Send + 'static,
    ) -> mpsc::Sender<Engine> {
        let (tx, mut rx) = mpsc::channel(size);

        tokio::spawn(
            async move {
                debug!("starting");

                while let Some(msg) = rx.recv().await {
                    match msg {
                        Engine::GetIndexIds { tx } => sim.get_index_keys(tx).await,
                        Engine::AddIndex { metadata, tx } => sim.add_index(metadata, tx).await,
                        Engine::DelIndex { key } => sim.del_index(key).await,
                        Engine::GetIndex { key, tx } => sim.get_index(key, tx).await,
                        Engine::GetIndexes { .. } => unimplemented!(),
                    }
                }

                debug!("finished");
            }
            .instrument(debug_span!("engine-test")),
        );

        tx
    }
}
