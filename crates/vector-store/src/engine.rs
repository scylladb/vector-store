/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Config;
use crate::DbIndexPartitioning;
use crate::IndexKey;
use crate::IndexKind;
use crate::IndexMetadata;
use crate::Internals;
use crate::Metrics;
use crate::db::Db;
use crate::db::DbExt;
use crate::db_index::DbIndex;
use crate::db_index::DbIndexExt;
use crate::fts_index::FtsIndexFactory;
use crate::indexes::Indexes;
use crate::memory;
use crate::memory::Memory;
use crate::monitor_indexes;
use crate::monitor_items;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use crate::perf;
use crate::table::Table;
use crate::vs_index::VsIndex;
use crate::vs_index::factory::VsIndexConfiguration;
use crate::vs_index::factory::VsIndexFactory;
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

type GetVsIndexKeysR = Vec<(IndexKey, crate::IndexOptionsVs)>;
type AddIndexR = anyhow::Result<()>;
type GetVsIndexR = Option<(mpsc::Sender<VsIndex>, mpsc::Sender<DbIndex>)>;

pub(crate) enum Engine {
    GetVsIndexKeys {
        tx: oneshot::Sender<GetVsIndexKeysR>,
    },
    AddIndex {
        metadata: IndexMetadata,
        tx: oneshot::Sender<AddIndexR>,
    },
    DelIndex {
        key: IndexKey,
    },
    GetVsIndex {
        key: IndexKey,
        tx: oneshot::Sender<GetVsIndexR>,
    },
}

pub(crate) trait EngineExt {
    async fn get_vs_index_keys(&self) -> GetVsIndexKeysR;
    async fn add_index(&self, metadata: IndexMetadata) -> AddIndexR;
    async fn del_index(&self, key: IndexKey);
    async fn get_vs_index(&self, key: IndexKey) -> GetVsIndexR;
}

impl EngineExt for mpsc::Sender<Engine> {
    async fn get_vs_index_keys(&self) -> GetVsIndexKeysR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetVsIndexKeys { tx })
            .await
            .expect("EngineExt::get_vs_index_keys: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_vs_index_keys: internal actor should send response")
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

    async fn get_vs_index(&self, key: IndexKey) -> GetVsIndexR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetVsIndex { key, tx })
            .await
            .expect("EngineExt::get_vs_index: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_vs_index: internal actor should send response")
    }
}

pub(crate) struct IndexFactories {
    pub(crate) vs: Box<dyn VsIndexFactory + Send + Sync>,
    pub(crate) fts: Box<dyn FtsIndexFactory + Send + Sync>,
}

pub(crate) async fn new(
    db: mpsc::Sender<Db>,
    index_factories: IndexFactories,
    node_state: Sender<NodeState>,
    metrics: Arc<Metrics>,
    indexes: Arc<RwLock<Indexes>>,
    internals: Sender<Internals>,
    config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<mpsc::Sender<Engine>> {
    let (tx, mut rx) = mpsc::channel(perf::channel_size().into());

    let monitor_actor = monitor_indexes::new(
        db.clone(),
        tx.downgrade(),
        node_state.clone(),
        config_rx.clone(),
    )
    .await?;
    let check_interval = config_rx
        .borrow()
        .engine_status_update_interval
        .unwrap_or(Duration::from_secs(1));
    let memory_actor = memory::new(internals, config_rx);

    tokio::spawn(
        async move {
            debug!("starting");

            let mut interval = time::interval(check_interval);
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        match msg {
                            Engine::GetVsIndexKeys { tx } => get_vs_index_keys(tx, &indexes).await,

                            Engine::AddIndex { metadata, tx } => {
                                add_index(
                                    metadata,
                                    tx,
                                    &db,
                                    &index_factories,
                                    &indexes,
                                    metrics.clone(),
                                    memory_actor.clone(),
                                )
                                .await
                            }

                            Engine::DelIndex { key } => del_index(key, &indexes, &metrics).await,

                            Engine::GetVsIndex { key, tx } => get_vs_index(key, tx, &indexes).await,

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

async fn get_vs_index_keys(tx: oneshot::Sender<GetVsIndexKeysR>, indexes: &RwLock<Indexes>) {
    let keys = indexes
        .read()
        .unwrap()
        .iter_vs()
        .map(|(key, entry)| (key.clone(), entry.options().clone()))
        .collect();
    tx.send(keys)
        .unwrap_or_else(|_| trace!("Engine::GetVsIndexKeys: unable to send response"));
}

async fn add_index(
    metadata: IndexMetadata,
    tx: oneshot::Sender<AddIndexR>,
    db: &mpsc::Sender<Db>,
    index_factories: &IndexFactories,
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

    info!("creating the index {key}");

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
    let partition_key_count = db_index.get_partition_key_count().await;
    let table_columns = db_index.get_table_columns().await;
    let partition_key_columns = match &metadata.partitioning {
        DbIndexPartitioning::Local(partition_key_columns) => {
            Some(Arc::clone(partition_key_columns))
        }
        DbIndexPartitioning::Global => None,
    };
    let table = match Table::new(
        key.clone(),
        primary_key_columns.clone(),
        partition_key_count,
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

    let ctx = AddIndexContext {
        key,
        table,
        embeddings_stream,
        metrics,
        db_index,
        indexes,
        index_factories,
        memory,
        metadata,
    };

    let result = if let IndexKind::Vs(_) = ctx.metadata.kind {
        add_index_vs(ctx).await
    } else {
        add_index_fts(ctx).await
    };

    match result {
        Ok(()) => {
            tx.send(Ok(()))
                .unwrap_or_else(|_| trace!("add_index: unable to send response"));
        }
        Err(err) => {
            tx.send(Err(err))
                .unwrap_or_else(|_| trace!("add_index: unable to send response"));
        }
    }
}

struct AddIndexContext<'a> {
    key: IndexKey,
    table: Arc<RwLock<Table>>,
    embeddings_stream: mpsc::Receiver<(crate::DbIndexedRow, crate::AsyncInProgress)>,
    metrics: Arc<Metrics>,
    db_index: mpsc::Sender<DbIndex>,
    indexes: &'a RwLock<Indexes>,
    index_factories: &'a IndexFactories,
    memory: Sender<Memory>,
    metadata: IndexMetadata,
}

async fn add_index_vs(ctx: AddIndexContext<'_>) -> anyhow::Result<()> {
    let options = ctx
        .metadata
        .vs()
        .ok_or_else(|| anyhow::anyhow!("add_index_vs must be called with a vector-search index"))?;
    let vs_sender = ctx.index_factories.vs.create_index(
        VsIndexConfiguration {
            key: ctx.key.clone(),
            dimensions: options.dimensions,
            connectivity: options.connectivity,
            expansion_add: options.expansion_add,
            expansion_search: options.expansion_search,
            space_type: options.space_type,
            quantization: options.quantization,
        },
        Arc::clone(&ctx.table),
        ctx.memory,
    )?;

    let monitor_actor = monitor_items::new(
        ctx.key.clone(),
        ctx.table,
        ctx.embeddings_stream,
        vs_sender.clone(),
        ctx.metrics,
    )
    .await?;

    let entry =
        crate::indexes::VsIndexEntry::new(vs_sender, monitor_actor, ctx.db_index, ctx.metadata)
            .await?;
    ctx.indexes.write().unwrap().insert_vs(ctx.key, entry);
    Ok(())
}

async fn add_index_fts(ctx: AddIndexContext<'_>) -> anyhow::Result<()> {
    let fts_sender =
        ctx.index_factories
            .fts
            .create_index(ctx.key.clone(), Arc::clone(&ctx.table), ctx.memory);

    let monitor_actor = monitor_items::new(
        ctx.key.clone(),
        ctx.table,
        ctx.embeddings_stream,
        fts_sender.clone(),
        ctx.metrics,
    )
    .await?;

    let entry = crate::indexes::FtsIndexEntry::new(fts_sender, monitor_actor, ctx.db_index).await;
    ctx.indexes.write().unwrap().insert_fts(ctx.key, entry);
    Ok(())
}

async fn del_index(key: IndexKey, indexes: &RwLock<Indexes>, metrics: &Metrics) {
    if indexes.write().unwrap().remove(&key) {
        info!("removed the index {key}");
        metrics.remove_index_labels(key.keyspace().as_ref(), key.index().as_ref());
    }
}

async fn get_vs_index(key: IndexKey, tx: oneshot::Sender<GetVsIndexR>, indexes: &RwLock<Indexes>) {
    _ = tx.send(
        indexes
            .read()
            .unwrap()
            .get_vs(&key)
            .map(|entry| (entry.index().clone(), entry.db_index())),
    );
}

async fn update_indexes(node_state: &Sender<NodeState>, indexes: &RwLock<Indexes>) {
    let actual_indexes: Vec<_> = {
        let indexes = indexes.read().unwrap();
        indexes
            .iter_vs()
            .map(|(key, entry)| {
                (
                    key.clone(),
                    entry.db_index(),
                    entry.progress(),
                    entry.status(),
                )
            })
            .chain(indexes.iter_fts().map(|(key, entry)| {
                (
                    key.clone(),
                    entry.db_index(),
                    entry.progress(),
                    entry.status(),
                )
            }))
            .collect()
    };

    for (key, db_index, progress, status) in actual_indexes.into_iter() {
        let Some(new_status) = node_state
            .get_index_status(key.keyspace().as_ref(), key.index().as_ref())
            .await
        else {
            continue;
        };
        let new_progress = db_index.full_scan_progress().await;
        if new_progress != progress || new_status != status {
            let mut indexes = indexes.write().unwrap();
            if let Some(entry) = indexes.get_vs_mut(&key) {
                entry.set_progress(new_progress);
                entry.set_status(new_status);
            } else if let Some(entry) = indexes.get_fts_mut(&key) {
                entry.set_progress(new_progress);
                entry.set_status(new_status);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use mockall::automock;

    #[automock]
    pub(crate) trait SimEngine {
        fn get_vs_index_keys(
            &self,
            tx: oneshot::Sender<GetVsIndexKeysR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn add_index(
            &self,
            metadata: IndexMetadata,
            tx: oneshot::Sender<AddIndexR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn del_index(&self, key: IndexKey) -> impl Future<Output = ()> + Send + 'static;

        fn get_vs_index(
            &self,
            key: IndexKey,
            tx: oneshot::Sender<GetVsIndexR>,
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
                        Engine::GetVsIndexKeys { tx } => sim.get_vs_index_keys(tx).await,
                        Engine::AddIndex { metadata, tx } => sim.add_index(metadata, tx).await,
                        Engine::DelIndex { key } => sim.del_index(key).await,
                        Engine::GetVsIndex { key, tx } => sim.get_vs_index(key, tx).await,
                    }
                }

                debug!("finished");
            }
            .instrument(debug_span!("engine-test")),
        );

        tx
    }
}
