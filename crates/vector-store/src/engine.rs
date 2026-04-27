/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
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
use crate::indexes;
use crate::indexes::BestIndexState;
use crate::indexes::IndexEntry;
use crate::indexes::Indexes;
use crate::indexes::RoutingMap;
use crate::memory;
use crate::memory::Memory;
use crate::monitor_indexes;
use crate::monitor_items;
use crate::node_state::NodeState;
use crate::table::Table;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;
use tracing::trace;

type GetIndexKeysR = Vec<(IndexKey, Quantization)>;
type AddIndexR = anyhow::Result<()>;
type GetIndexR = Option<(mpsc::Sender<Index>, mpsc::Sender<DbIndex>)>;
type GetBestIndexR = BestIndexState;

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
    GetBestIndex {
        key: IndexKey,
        equality_columns: Vec<ColumnName>,
        range_columns: Vec<ColumnName>,
        tx: oneshot::Sender<GetBestIndexR>,
    },
}

pub(crate) trait EngineExt {
    async fn get_index_keys(&self) -> GetIndexKeysR;
    async fn add_index(&self, metadata: IndexMetadata) -> AddIndexR;
    async fn del_index(&self, key: IndexKey);
    async fn get_index(&self, key: IndexKey) -> GetIndexR;
    async fn get_best_index(
        &self,
        key: IndexKey,
        equality_columns: Vec<ColumnName>,
        range_columns: Vec<ColumnName>,
    ) -> GetBestIndexR;
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

    async fn get_best_index(
        &self,
        key: IndexKey,
        equality_columns: Vec<ColumnName>,
        range_columns: Vec<ColumnName>,
    ) -> GetBestIndexR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetBestIndex {
            key,
            equality_columns,
            range_columns,
            tx,
        })
        .await
        .expect("EngineExt::get_best_index: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_best_index: internal actor should send response")
    }
}

pub(crate) async fn new(
    db: mpsc::Sender<Db>,
    index_factory: Box<dyn IndexFactory + Send + Sync>,
    node_state: Sender<NodeState>,
    metrics: Arc<Metrics>,
    config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<mpsc::Sender<Engine>> {
    let (tx, mut rx) = mpsc::channel(10);

    let monitor_actor = monitor_indexes::new(
        db.clone(),
        tx.downgrade(),
        node_state.clone(),
        config_rx.clone(),
    )
    .await?;
    let memory_actor = memory::new(config_rx);

    tokio::spawn(
        async move {
            debug!("starting");

            let mut indexes = Indexes::new();
            let mut routing_map = RoutingMap::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    Engine::GetIndexIds { tx } => get_index_keys(tx, &indexes).await,

                    Engine::AddIndex { metadata, tx } => {
                        add_index(
                            metadata,
                            tx,
                            &db,
                            index_factory.as_ref(),
                            &mut indexes,
                            &mut routing_map,
                            metrics.clone(),
                            memory_actor.clone(),
                        )
                        .await
                    }

                    Engine::DelIndex { key } => {
                        del_index(key, &mut indexes, &mut routing_map).await
                    }

                    Engine::GetIndex { key, tx } => get_index(key, tx, &indexes).await,

                    Engine::GetBestIndex {
                        key,
                        equality_columns,
                        range_columns,
                        tx,
                    } => {
                        get_best_index(
                            key,
                            equality_columns,
                            range_columns,
                            tx,
                            &indexes,
                            &routing_map,
                            &node_state,
                        )
                        .await
                    }
                }
            }
            drop(monitor_actor);

            debug!("finished");
        }
        .instrument(debug_span!("engine")),
    );

    Ok(tx)
}

async fn get_index_keys(tx: oneshot::Sender<GetIndexKeysR>, indexes: &Indexes) {
    tx.send(
        indexes
            .iter()
            .map(|(key, entry)| (key.clone(), entry.quantization))
            .collect(),
    )
    .unwrap_or_else(|_| trace!("Engine::GetIndexIds: unable to send response"));
}

#[allow(clippy::too_many_arguments)]
async fn add_index(
    metadata: IndexMetadata,
    tx: oneshot::Sender<AddIndexR>,
    db: &mpsc::Sender<Db>,
    index_factory: &(dyn IndexFactory + Send + Sync),
    indexes: &mut Indexes,
    routing_map: &mut RoutingMap,
    metrics: Arc<Metrics>,
    memory: Sender<Memory>,
) {
    let key = metadata.key();
    if indexes.contains_key(&key) {
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
    let partition_key_columns = match &metadata.index_type {
        DbIndexType::Local(partition_key_columns) => Some(Arc::clone(partition_key_columns)),
        DbIndexType::Global => None,
    };
    let table = match Table::new(
        key.clone(),
        primary_key_columns.clone(),
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

    let index_entry = IndexEntry::new(
        index_actor,
        monitor_actor,
        db_index,
        primary_key_columns,
        metadata,
    );

    routing_map.add(index_entry.routing_group.clone(), key.clone());

    indexes.insert(key.clone(), index_entry);

    info!("creating the index {key}");
    tx.send(Ok(()))
        .unwrap_or_else(|_| trace!("add_index: unable to send response"));
}

async fn del_index(key: IndexKey, indexes: &mut Indexes, routing_map: &mut RoutingMap) {
    if let Some(entry) = indexes.remove(&key) {
        routing_map.remove(entry.routing_group, &key);
    }
    info!("removed the index {key}");
}

async fn get_index(key: IndexKey, tx: oneshot::Sender<GetIndexR>, indexes: &Indexes) {
    let result = indexes::get_index(&key, indexes);
    tx.send(result)
        .unwrap_or_else(|_| trace!("get_index: unable to send response"));
}

async fn get_best_index(
    key: IndexKey,
    equality_columns: Vec<ColumnName>,
    range_columns: Vec<ColumnName>,
    tx: oneshot::Sender<GetBestIndexR>,
    indexes: &Indexes,
    routing_map: &RoutingMap,
    node_state: &mpsc::Sender<NodeState>,
) {
    let result = indexes::route_index(
        &key,
        &equality_columns,
        &range_columns,
        indexes,
        routing_map,
        node_state,
    )
    .await;
    tx.send(result)
        .unwrap_or_else(|_| trace!("get_best_index: unable to send response"));
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

        fn get_best_index(
            &self,
            key: IndexKey,
            equality_columns: Vec<ColumnName>,
            range_columns: Vec<ColumnName>,
            tx: oneshot::Sender<GetBestIndexR>,
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
                        Engine::GetBestIndex {
                            key,
                            equality_columns,
                            range_columns,
                            tx,
                        } => {
                            sim.get_best_index(key, equality_columns, range_columns, tx)
                                .await
                        }
                    }
                }

                debug!("finished");
            }
            .instrument(debug_span!("engine-test")),
        );

        tx
    }
}
