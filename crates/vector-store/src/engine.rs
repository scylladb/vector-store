/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Config;
use crate::DbIndexPartitioning;
use crate::IndexKey;
use crate::IndexKind;
use crate::IndexMetadata;
use crate::Metrics;
use crate::db::Db;
use crate::db::DbExt;
use crate::db_index::DbIndex;
use crate::db_index::DbIndexExt;
use crate::fts_index::FtsIndex;
use crate::fts_index::FtsIndexConfiguration;
use crate::fts_index::FtsIndexFactory;
use crate::indexes::Indexes;
use crate::monitor_indexes;
use crate::monitor_items;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use crate::perf;
use crate::table::Table;
use crate::vs_index::VsIndexConfiguration;
use crate::vs_index::VsIndexFactory;
use crate::vs_index::VsIndexSearch;
use scylla::cluster::metadata::NativeType;
use std::collections::HashMap;
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

type AddIndexR = anyhow::Result<()>;
type GetVsIndexR = Option<(mpsc::Sender<VsIndexSearch>, mpsc::Sender<DbIndex>)>;
type GetFtsIndexR = Option<(mpsc::Sender<FtsIndex>, mpsc::Sender<DbIndex>)>;

#[allow(clippy::enum_variant_names)]
pub(crate) enum Engine {
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
    GetFtsIndex {
        key: IndexKey,
        tx: oneshot::Sender<GetFtsIndexR>,
    },
}

pub(crate) trait EngineExt {
    async fn add_index(&self, metadata: IndexMetadata) -> AddIndexR;
    async fn del_index(&self, key: IndexKey);
    async fn get_vs_index(&self, key: IndexKey) -> GetVsIndexR;
    async fn get_fts_index(&self, key: IndexKey) -> GetFtsIndexR;
}

impl EngineExt for mpsc::Sender<Engine> {
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

    async fn get_fts_index(&self, key: IndexKey) -> GetFtsIndexR {
        let (tx, rx) = oneshot::channel();
        self.send(Engine::GetFtsIndex { key, tx })
            .await
            .expect("EngineExt::get_fts_index: internal actor should receive request");
        rx.await
            .expect("EngineExt::get_fts_index: internal actor should send response")
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
                            Engine::AddIndex { metadata, tx } => {
                                add_index(
                                    metadata,
                                    tx,
                                    &db,
                                    &index_factories,
                                    &indexes,
                                    metrics.clone(),
                                )
                                .await
                            }

                            Engine::DelIndex { key } => del_index(key, &indexes, &metrics).await,

                            Engine::GetVsIndex { key, tx } => get_vs_index(key, tx, &indexes).await,

                            Engine::GetFtsIndex { key, tx } => {
                                get_fts_index(key, tx, &indexes).await
                            }

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

/// Builds the in-memory `Table` for `metadata`, given the native-typed columns of the
/// underlying CQL table.
fn build_table(
    key: &IndexKey,
    metadata: &IndexMetadata,
    table_columns: Arc<HashMap<ColumnName, NativeType>>,
) -> anyhow::Result<Table> {
    let partition_key_columns = match &metadata.partitioning {
        DbIndexPartitioning::Local(partition_key_columns) => Some(partition_key_columns.clone()),
        DbIndexPartitioning::Global => None,
    };
    // Must match the columns db_index.rs/db_cdc actually fetch a value for,
    // or Table::new()'s column list goes out of sync with update_columns().
    let filtering_columns: Arc<[_]> = metadata.nonpk_filtering_columns().cloned().collect();
    Table::new(
        key.clone(),
        metadata.primary_key_columns.clone(),
        metadata.partition_key_count,
        partition_key_columns,
        metadata.target_columns.len(),
        filtering_columns,
        table_columns,
    )
}

async fn add_index(
    metadata: IndexMetadata,
    tx: oneshot::Sender<AddIndexR>,
    db: &mpsc::Sender<Db>,
    index_factories: &IndexFactories,
    indexes: &RwLock<Indexes>,
    metrics: Arc<Metrics>,
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

    let table_columns = db_index.get_table_columns().await;
    let table = match build_table(&key, &metadata, table_columns) {
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
    metadata: IndexMetadata,
}

async fn add_index_vs(ctx: AddIndexContext<'_>) -> anyhow::Result<()> {
    let options = ctx
        .metadata
        .vs()
        .ok_or_else(|| anyhow::anyhow!("add_index_vs must be called with a vector-search index"))?;
    let (vs_modify, vs_search) = ctx.index_factories.vs.create_index(
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
    )?;

    let monitor_actor = monitor_items::new(
        ctx.key.clone(),
        ctx.table,
        ctx.embeddings_stream,
        vs_modify,
        ctx.metrics,
    )
    .await?;

    let entry =
        crate::indexes::VsIndexEntry::new(vs_search, monitor_actor, ctx.db_index, ctx.metadata)
            .await?;
    ctx.indexes.write().unwrap().insert_vs(ctx.key, entry);
    Ok(())
}

async fn add_index_fts(ctx: AddIndexContext<'_>) -> anyhow::Result<()> {
    let options = ctx.metadata.fts().ok_or_else(|| {
        anyhow::anyhow!("add_index_fts must be called with a full-text-search index")
    })?;
    let fts_sender = ctx.index_factories.fts.create_index(
        FtsIndexConfiguration {
            key: ctx.key.clone(),
            analyzer: options.analyzer,
            positions: options.positions,
        },
        Arc::clone(&ctx.table),
    );

    let monitor_actor = monitor_items::new(
        ctx.key.clone(),
        ctx.table,
        ctx.embeddings_stream,
        fts_sender.clone(),
        ctx.metrics,
    )
    .await?;

    let entry =
        crate::indexes::FtsIndexEntry::new(ctx.metadata, fts_sender, monitor_actor, ctx.db_index)
            .await?;
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

async fn get_fts_index(
    key: IndexKey,
    tx: oneshot::Sender<GetFtsIndexR>,
    indexes: &RwLock<Indexes>,
) {
    _ = tx.send(
        indexes
            .read()
            .unwrap()
            .get_fts(&key)
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

        fn get_fts_index(
            &self,
            key: IndexKey,
            tx: oneshot::Sender<GetFtsIndexR>,
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
                        Engine::AddIndex { metadata, tx } => sim.add_index(metadata, tx).await,
                        Engine::DelIndex { key } => sim.del_index(key).await,
                        Engine::GetVsIndex { key, tx } => sim.get_vs_index(key, tx).await,
                        Engine::GetFtsIndex { key, tx } => sim.get_fts_index(key, tx).await,
                    }
                }

                debug!("finished");
            }
            .instrument(debug_span!("engine-test")),
        );

        tx
    }

    /// A local index can declare a filtering column ("ck") that is also one of the base
    /// table's primary-key columns. This drives build_table() - the code that add_index()
    /// actually calls - end to end with the real (undeduped) metadata.filtering_columns,
    /// then upserts a row the way db_index.rs/db_cdc really would (no fetched value for
    /// "ck", since it comes from the primary key) and checks the target vector and the
    /// other filtering column ("f") land on the right columns. Unlike
    /// table::tests::local_index_filtering_on_primary_key_column_stays_aligned, which
    /// hands Table::new() an already-deduped column list, this test would fail if
    /// build_table() ever went back to passing metadata.filtering_columns through as-is.
    #[test]
    fn build_table_dedupes_filtering_column_shared_with_primary_key() {
        use crate::Connectivity;
        use crate::CqlValue;
        use crate::DbIndexedValue;
        use crate::Dimensions;
        use crate::ExpansionAdd;
        use crate::ExpansionSearch;
        use crate::IndexOptionsVs;
        use crate::NonemptyArc;
        use crate::NonemptyBox;
        use crate::PrimaryKey;
        use crate::Quantization;
        use crate::Restriction;
        use crate::SpaceType;
        use crate::Timestamp;
        use crate::table::Operation;
        use crate::table::TableModify;
        use crate::table::TableSearch;
        use crate::timestamp::Timestamped;
        use std::collections::BTreeMap;
        use std::num::NonZeroUsize;
        use uuid::Uuid;

        let metadata = IndexMetadata {
            keyspace_name: "ks".into(),
            index_name: "idx".into(),
            table_name: "tbl".into(),
            // The base table's primary key: "pk" is the partition key, "ck" a
            // clustering key.
            primary_key_columns: NonemptyArc::new(["pk", "ck"]).unwrap(),
            partition_key_count: NonZeroUsize::new(1).unwrap(),
            target_columns: NonemptyArc::new(["embedding"]).unwrap(),
            // The local index is partitioned by "pk" alone, so "ck" is not part of
            // its own partition key either.
            partitioning: DbIndexPartitioning::Local(NonemptyArc::new(["pk"]).unwrap()),
            // Declares "ck" (a primary-key column) and "f" (a genuine value column)
            // as filtering columns, exactly as metadata coming from the DB would.
            filtering_columns: Arc::new(["ck".into(), "f".into()]),
            alternator_attribute_types: Arc::new(BTreeMap::new()),
            version: Uuid::new_v4().into(),
            kind: IndexKind::Vs(IndexOptionsVs {
                dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
                connectivity: Connectivity::default(),
                expansion_add: ExpansionAdd::default(),
                expansion_search: ExpansionSearch::default(),
                space_type: SpaceType::default(),
                quantization: Quantization::default(),
            }),
        };

        let table_columns = Arc::new(
            [
                ("pk".into(), NativeType::Int),
                ("ck".into(), NativeType::Int),
                ("f".into(), NativeType::Int),
            ]
            .into_iter()
            .collect(),
        );

        let mut table = build_table(&metadata.key(), &metadata, table_columns).unwrap();

        // A row fetched by the real pipeline: a value for the target vector, then a
        // value for each *non*-primary-key filtering column - "ck" has no value of
        // its own here, matching db_index.rs/db_cdc.
        let primary_key: PrimaryKey = [CqlValue::Int(1), CqlValue::Int(2)].into();
        let values = NonemptyBox::<Timestamped<DbIndexedValue>>::new([
            Timestamped::new(
                Timestamp::from_millis(100),
                Some(DbIndexedValue::Vector(vec![0.1, 0.2, 0.3].into())),
            ),
            Timestamped::new(
                Timestamp::from_millis(100),
                Some(DbIndexedValue::Filtering(CqlValue::Int(42))),
            ),
        ])
        .unwrap();

        let operations = table
            .upsert(&metadata.key(), primary_key.clone(), values)
            .unwrap();
        assert_eq!(operations.len(), 1);
        let (primary_id, partition_id) = match operations.first().unwrap() {
            Operation::AddVector {
                primary_id,
                partition_id,
                vector,
                is_update: false,
            } => {
                assert_eq!(vector, &vec![0.1, 0.2, 0.3].into());
                (*primary_id, *partition_id)
            }
            _ => panic!("Expected AddVector operation"),
        };

        assert_eq!(
            table.primary_key(partition_id, primary_id).unwrap(),
            primary_key
        );
        assert!(table.is_valid_for(
            partition_id,
            primary_id,
            &Restriction::Eq {
                lhs: "ck".into(),
                rhs: CqlValue::Int(2),
            }
        ));
        assert!(table.is_valid_for(
            partition_id,
            primary_id,
            &Restriction::Eq {
                lhs: "f".into(),
                rhs: CqlValue::Int(42),
            }
        ));
    }
}
