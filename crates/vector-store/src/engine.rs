/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexId;
use crate::IndexMetadata;
use crate::Metrics;
use crate::db::Db;
use crate::db::DbExt;
use crate::db_index::DbIndex;
use crate::factory::IndexFactory;
use crate::factory::Type;
use crate::index::Index;
use crate::monitor_indexes;
use crate::monitor_items;
use crate::monitor_items::MonitorItems;
use crate::node_state::NodeState;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;
use tracing::trace;

type GetIndexIdsR = Vec<IndexId>;
type AddIndexR = anyhow::Result<()>;
type GetIndexR = Option<(mpsc::Sender<Index>, mpsc::Sender<DbIndex>)>;
type GetEngineInfoR = EngineInfo;

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct EngineInfo {
    #[serde(rename = "type")]
    /// The type of the search engine used by the Vector Store.
    pub engine_type: Type,
    /// The version of the search engine.
    pub version: String,
}

pub(crate) enum Engine {
    GetIndexIds {
        tx: oneshot::Sender<GetIndexIdsR>,
    },
    AddIndex {
        metadata: IndexMetadata,
        tx: oneshot::Sender<AddIndexR>,
    },
    DelIndex {
        id: IndexId,
    },
    GetIndex {
        id: IndexId,
        tx: oneshot::Sender<GetIndexR>,
    },

    GetEngineInfo {
        tx: oneshot::Sender<GetEngineInfoR>,
    },
}

pub(crate) trait EngineExt {
    async fn get_index_ids(&self) -> GetIndexIdsR;
    async fn add_index(&self, metadata: IndexMetadata) -> AddIndexR;
    async fn del_index(&self, id: IndexId);
    async fn get_index(&self, id: IndexId) -> GetIndexR;
    async fn get_engine_info(&self) -> GetEngineInfoR;
}

impl EngineExt for mpsc::Sender<Engine> {
    async fn get_index_ids(&self) -> GetIndexIdsR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetIndexIds { tx })
            .await
            .expect("EngineExt::get_index_ids: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_index_ids: internal actor should send response")
    }

    async fn add_index(&self, metadata: IndexMetadata) -> AddIndexR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::AddIndex { metadata, tx })
            .await
            .expect("EngineExt::add_index: internal actor should receive request");
        rx.await
            .expect("EngineExt::add_index: internal actor should send response")
    }

    async fn del_index(&self, id: IndexId) {
        self.send(Engine::DelIndex { id })
            .await
            .expect("EngineExt::del_index: internal actor should receive request");
    }

    async fn get_index(&self, id: IndexId) -> GetIndexR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetIndex { id, tx })
            .await
            .expect("EngineExt::get_index: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_index: internal actor should send response")
    }

    async fn get_engine_info(&self) -> GetEngineInfoR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetEngineInfo { tx })
            .await
            .expect("EngineExt::get_engine_info: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_engine_info: internal actor should send response")
    }
}

type IndexesT = HashMap<
    IndexId,
    (
        mpsc::Sender<Index>,
        mpsc::Sender<MonitorItems>,
        mpsc::Sender<DbIndex>,
    ),
>;

pub(crate) async fn new(
    db: mpsc::Sender<Db>,
    index_factory: Box<dyn IndexFactory + Send + Sync>,
    node_state: Sender<NodeState>,
    metrics: Arc<Metrics>,
) -> anyhow::Result<mpsc::Sender<Engine>> {
    let (tx, mut rx) = mpsc::channel(10);

    let monitor_actor = monitor_indexes::new(db.clone(), tx.clone(), node_state).await?;

    tokio::spawn(
        async move {
            debug!("starting");

            let mut indexes: IndexesT = HashMap::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    Engine::GetIndexIds { tx } => get_index_ids(tx, &indexes).await,

                    Engine::AddIndex { metadata, tx } => {
                        add_index(
                            metadata,
                            tx,
                            &db,
                            index_factory.as_ref(),
                            &mut indexes,
                            metrics.clone(),
                        )
                        .await
                    }

                    Engine::DelIndex { id } => del_index(id, &mut indexes).await,

                    Engine::GetIndex { id, tx } => get_index(id, tx, &indexes).await,
                    Engine::GetEngineInfo { tx } => {
                        get_engine_info(tx, index_factory.as_ref()).await
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

async fn get_index_ids(tx: oneshot::Sender<GetIndexIdsR>, indexes: &IndexesT) {
    tx.send(indexes.keys().cloned().collect())
        .unwrap_or_else(|_| trace!("Engine::GetIndexIds: unable to send response"));
}

async fn add_index(
    metadata: IndexMetadata,
    tx: oneshot::Sender<AddIndexR>,
    db: &mpsc::Sender<Db>,
    index_factory: &(dyn IndexFactory + Send + Sync),
    indexes: &mut IndexesT,
    metrics: Arc<Metrics>,
) {
    let id = metadata.id();
    if indexes.contains_key(&id) {
        trace!("add_index: trying to replace index with id {id}");
        tx.send(Ok(()))
            .unwrap_or_else(|_| trace!("add_index: unable to send response"));
        return;
    }

    let index_actor = match index_factory.create_index(
        id.clone(),
        metadata.dimensions,
        metadata.connectivity,
        metadata.expansion_add,
        metadata.expansion_search,
        metadata.space_type,
    ) {
        Ok(actor) => actor,
        Err(err) => {
            debug!("unable to create an index {id}: {err}");
            tx.send(Err(err))
                .unwrap_or_else(|_| trace!("add_index: unable to send response"));
            return;
        }
    };

    let (db_index, embeddings_stream) = match db.get_db_index(metadata.clone()).await {
        Ok((db_index, embeddings_stream)) => (db_index, embeddings_stream),
        Err(err) => {
            debug!("unable to create a db monitoring task for an index {id}: {err}");
            tx.send(Err(err))
                .unwrap_or_else(|_| trace!("add_index: unable to send response"));
            return;
        }
    };

    let monitor_actor =
        match monitor_items::new(id.clone(), embeddings_stream, index_actor.clone(), metrics).await
        {
            Ok(actor) => actor,
            Err(err) => {
                debug!(
                    "unable to create a synchronisation task between a db and an index {id}: {err}"
                );
                tx.send(Err(err))
                    .unwrap_or_else(|_| trace!("add_index: unable to send response"));
                return;
            }
        };

    indexes.insert(id.clone(), (index_actor, monitor_actor, db_index));
    info!("create an index {id}");
    tx.send(Ok(()))
        .unwrap_or_else(|_| trace!("add_index: unable to send response"));
}

async fn del_index(id: IndexId, indexes: &mut IndexesT) {
    indexes.remove(&id);
    info!("remove an index {id}");
}

async fn get_index(id: IndexId, tx: oneshot::Sender<GetIndexR>, indexes: &IndexesT) {
    tx.send(
        indexes
            .get(&id)
            .map(|(index, _, db_index)| (index.clone(), db_index.clone())),
    )
    .unwrap_or_else(|_| trace!("get_index: unable to send response"));
}

async fn get_engine_info(
    tx: oneshot::Sender<GetEngineInfoR>,
    index_factory: &(dyn IndexFactory + Send + Sync),
) {
    tx.send(GetEngineInfoR {
        engine_type: index_factory.get_type(),
        version: index_factory.get_version(),
    })
    .unwrap_or_else(|_| trace!("get_engine_info: unable to send response"));
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use mockall::automock;

    #[automock]
    pub(crate) trait SimEngine {
        fn get_index_ids(
            &self,
            tx: oneshot::Sender<GetIndexIdsR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn add_index(
            &self,
            metadata: IndexMetadata,
            tx: oneshot::Sender<AddIndexR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn del_index(&self, id: IndexId) -> impl Future<Output = ()> + Send + 'static;

        fn get_index(
            &self,
            id: IndexId,
            tx: oneshot::Sender<GetIndexR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn get_engine_info(
            &self,
            tx: oneshot::Sender<GetEngineInfoR>,
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
                        Engine::GetIndexIds { tx } => sim.get_index_ids(tx).await,
                        Engine::AddIndex { metadata, tx } => sim.add_index(metadata, tx).await,
                        Engine::DelIndex { id } => sim.del_index(id).await,
                        Engine::GetIndex { id, tx } => sim.get_index(id, tx).await,
                        Engine::GetEngineInfo { tx } => sim.get_engine_info(tx).await,
                    }
                }

                debug!("finished");
            }
            .instrument(debug_span!("engine-test")),
        );

        tx
    }
}
