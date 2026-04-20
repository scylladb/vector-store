/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use anyhow::bail;
use futures::FutureExt;
use futures::future::BoxFuture;
use itertools::Itertools;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlTimeuuid;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use vector_store::AsyncInProgress;
use vector_store::ColumnName;
use vector_store::DbCustomIndex;
use vector_store::DbEmbedding;
use vector_store::Dimensions;
use vector_store::IndexMetadata;
use vector_store::IndexName;
use vector_store::IndexVersion;
use vector_store::KeyspaceName;
use vector_store::Percentage;
use vector_store::PrimaryKey;
use vector_store::Progress;
use vector_store::TableName;
use vector_store::Timestamp;
use vector_store::Vector;
use vector_store::db::Db;
use vector_store::db_index::DbIndex;
use vector_store::node_state::Event;
use vector_store::node_state::NodeState;

pub(crate) type RxEmbeddings = mpsc::Receiver<(DbEmbedding, Option<AsyncInProgress>)>;
pub(crate) type TxEmbeddings = mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>;
pub(crate) type ScanFn = Box<dyn FnOnce(TxEmbeddings) -> BoxFuture<'static, ()> + Send + Sync>;

pub(crate) fn scan_fn(
    items: impl IntoIterator<Item = (PrimaryKey, Option<Vector>, Timestamp)>,
) -> ScanFn {
    let items = Arc::new(items.into_iter().collect_vec());
    Box::new(move |tx| {
        let items = items.clone();
        async move {
            let (tx_in_progress, mut rx_in_progress) = mpsc::channel(1);

            for (primary_key, embedding, timestamp) in items.iter().cloned() {
                let _ = tx
                    .send((
                        DbEmbedding {
                            primary_key,
                            embedding,
                            timestamp,
                        },
                        Some(tx_in_progress.clone().into()),
                    ))
                    .await;
            }

            // wait until all in-progress markers are dropped
            drop(tx_in_progress);
            while rx_in_progress.recv().await.is_some() {}
        }
        .boxed()
    })
}

#[derive(Clone, derive_more::Debug)]
pub(crate) struct DbBasic(#[debug(skip)] Arc<RwLock<DbMock>>);

pub(crate) fn new(node_state: Sender<NodeState>) -> (mpsc::Sender<Db>, DbBasic) {
    let (tx, mut rx) = mpsc::channel(10);
    let db = DbBasic::new();
    tokio::spawn({
        let db = db.clone();
        async move {
            while let Some(msg) = rx.recv().await {
                process_db(&db, msg, node_state.clone());
            }
        }
    });
    (tx, db)
}

pub(crate) struct Table {
    pub(crate) primary_keys: Arc<Vec<ColumnName>>,
    pub(crate) partition_key_count: usize,
    pub(crate) columns: Arc<HashMap<ColumnName, NativeType>>,
    pub(crate) dimensions: HashMap<ColumnName, Dimensions>,
}

struct Index {
    metadata: IndexMetadata,
    version: IndexVersion,
    fullscan_fn: Option<ScanFn>,
    cdc_fn: Option<ScanFn>,
}

struct Keyspace {
    tables: HashMap<TableName, Table>,
    indexes: HashMap<IndexName, Index>,
}

impl Keyspace {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
        }
    }
}

struct DbMock {
    schema_version: CqlTimeuuid,
    keyspaces: HashMap<KeyspaceName, Keyspace>,
    next_get_db_index_failed: bool,
    next_full_scan_progress: Option<Progress>,
    simulate_endless_get_indexes_processing: bool,
}

impl DbMock {
    fn create_new_schema_version(&mut self) {
        self.schema_version = Uuid::new_v4().into();
    }
}

impl DbBasic {
    pub(crate) fn new() -> Self {
        Self(Arc::new(RwLock::new(DbMock {
            schema_version: CqlTimeuuid::from(Uuid::new_v4()),
            keyspaces: HashMap::new(),
            next_get_db_index_failed: false,
            next_full_scan_progress: None,
            simulate_endless_get_indexes_processing: false,
        })))
    }

    pub(crate) fn add_table(
        &self,
        keyspace_name: KeyspaceName,
        table_name: TableName,
        table: Table,
    ) -> anyhow::Result<()> {
        let mut db = self.0.write().unwrap();

        let keyspace = db
            .keyspaces
            .entry(keyspace_name)
            .or_insert_with(Keyspace::new);
        if keyspace.tables.contains_key(&table_name) {
            bail!("a table {table_name} already exists in a keyspace");
        }
        keyspace.tables.insert(table_name, table);

        db.create_new_schema_version();
        Ok(())
    }

    pub(crate) fn add_index(
        &self,
        metadata: IndexMetadata,
        fullscan_fn: Option<ScanFn>,
        cdc_fn: Option<ScanFn>,
    ) -> anyhow::Result<()> {
        let mut db = self.0.write().unwrap();

        let Some(keyspace) = db.keyspaces.get_mut(&metadata.keyspace_name) else {
            bail!(
                "a keyspace {keyspace_name} does not exist",
                keyspace_name = metadata.keyspace_name
            );
        };
        if !keyspace.tables.contains_key(&metadata.table_name) {
            bail!("a table {} does not exist", metadata.table_name);
        };
        if keyspace.indexes.contains_key(&metadata.index_name) {
            bail!(
                "an index {index_name} already exists",
                index_name = metadata.index_name
            );
        }
        keyspace.indexes.insert(
            metadata.index_name.clone(),
            Index {
                version: metadata.version.clone(),
                metadata,
                fullscan_fn,
                cdc_fn,
            },
        );

        db.create_new_schema_version();
        Ok(())
    }

    pub(crate) fn del_index(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
    ) -> anyhow::Result<()> {
        let mut db = self.0.write().unwrap();

        let Some(keyspace) = db.keyspaces.get_mut(keyspace_name) else {
            bail!("a keyspace {keyspace_name} does not exist");
        };
        if keyspace.indexes.remove(index_name).is_none() {
            bail!("an index {index_name} does not exist");
        }

        db.create_new_schema_version();
        Ok(())
    }

    pub(crate) fn set_next_get_db_index_failed(&self) {
        self.0.write().unwrap().next_get_db_index_failed = true;
    }

    pub(crate) fn set_next_full_scan_progress(&self, progress: Progress) {
        self.0.write().unwrap().next_full_scan_progress = Some(progress);
    }

    pub(crate) fn simulate_endless_get_indexes_processing(&self) {
        self.0
            .write()
            .unwrap()
            .simulate_endless_get_indexes_processing = true;
    }
}

fn process_db(db: &DbBasic, msg: Db, node_state: Sender<NodeState>) {
    match msg {
        Db::GetDbIndex { metadata, tx } => tx
            .send(new_db_index(db.clone(), metadata, node_state.clone()))
            .map_err(|_| anyhow!("Db::GetDbIndex: unable to send response"))
            .unwrap(),

        Db::LatestSchemaVersion { tx } => tx
            .send(Ok(Some(db.0.read().unwrap().schema_version)))
            .map_err(|_| anyhow!("Db::LatestSchemaVersion: unable to send response"))
            .unwrap(),

        Db::GetIndexes { tx } => {
            if db.0.read().unwrap().simulate_endless_get_indexes_processing {
                tokio::spawn(async move {
                    let _ = tx;
                    tokio::time::sleep(std::time::Duration::MAX).await;
                });
            } else {
                tx.send(Ok(db
                    .0
                    .read()
                    .unwrap()
                    .keyspaces
                    .iter()
                    .flat_map(|(keyspace_name, keyspace)| {
                        keyspace
                            .indexes
                            .iter()
                            .map(|(index_name, index)| DbCustomIndex {
                                keyspace: keyspace_name.clone(),
                                index: index_name.clone(),
                                table: index.metadata.table_name.clone(),
                                target_column: index.metadata.target_column.clone(),
                                index_type: index.metadata.index_type.clone(),
                                filtering_columns: index.metadata.filtering_columns.clone(),
                            })
                    })
                    .collect()))
                    .map_err(|_| anyhow!("Db::GetIndexes: unable to send response"))
                    .unwrap()
            }
        }
        Db::GetIndexVersion {
            keyspace,
            table: _,
            index,
            tx,
        } => tx
            .send(Ok(db
                .0
                .read()
                .unwrap()
                .keyspaces
                .get(&keyspace)
                .and_then(|keyspace| keyspace.indexes.get(&index))
                .map(|index| index.version.clone())))
            .map_err(|_| anyhow!("Db::GetIndexVersion: unable to send response"))
            .unwrap(),

        Db::GetIndexTargetType {
            keyspace,
            table,
            target_column,
            tx,
            ..
        } => tx
            .send(Ok(db
                .0
                .read()
                .unwrap()
                .keyspaces
                .get(&keyspace)
                .and_then(|keyspace| keyspace.tables.get(&table))
                .and_then(|table| table.dimensions.get(&target_column))
                .cloned()))
            .map_err(|_| anyhow!("Db::GetIndexTargetType: unable to send response"))
            .unwrap(),

        Db::GetIndexParams {
            keyspace,
            table: _,
            index,
            tx,
        } => tx
            .send(Ok(db
                .0
                .read()
                .unwrap()
                .keyspaces
                .get(&keyspace)
                .and_then(|keyspace| keyspace.indexes.get(&index))
                .map(|index| {
                    (
                        index.metadata.connectivity,
                        index.metadata.expansion_add,
                        index.metadata.expansion_search,
                        index.metadata.space_type,
                        index.metadata.quantization,
                    )
                })))
            .map_err(|_| anyhow!("Db::GetIndexParams: unable to send response"))
            .unwrap(),

        Db::IsValidIndex { tx, .. } => tx
            .send(true)
            .map_err(|_| anyhow!("Db::IsValidIndex: unable to send response"))
            .unwrap(),
    }
}

pub(crate) fn new_db_index(
    mut db: DbBasic,
    metadata: IndexMetadata,
    node_state: Sender<NodeState>,
) -> anyhow::Result<(mpsc::Sender<DbIndex>, RxEmbeddings)> {
    if db.0.read().unwrap().next_get_db_index_failed {
        db.0.write().unwrap().next_get_db_index_failed = false;
        bail!("get_db_index failed");
    }

    let (tx_index, mut rx_index) = mpsc::channel(10);
    let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
    let fullscan_finished = Arc::new(AtomicBool::new(false));
    tokio::spawn({
        let fullscan_finished = fullscan_finished.clone();
        async move {
            let fullscan_fn = fullscan(&mut db, &metadata);
            node_state
                .send(NodeState::SendEvent(Event::FullScanStarted(
                    metadata.clone(),
                )))
                .await
                .unwrap();
            let fullscan = {
                let tx_embeddings = tx_embeddings.clone();
                let fullscan_finished = fullscan_finished.clone();
                async move {
                    if let Some(fullscan_fn) = fullscan_fn {
                        fullscan_fn(tx_embeddings).await;
                    }
                    fullscan_finished.store(true, Ordering::Release);
                }
                .shared()
            };
            while !rx_index.is_closed() {
                tokio::select! {
                    _ = fullscan.clone() => break,
                    msg = rx_index.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        spawn_process_db_index(
                            db.clone(),
                            metadata.clone(),
                            Arc::clone(&fullscan_finished),
                            msg
                        ).await;
                    }
                }
            }
            node_state
                .send(NodeState::SendEvent(Event::FullScanFinished(
                    metadata.clone(),
                )))
                .await
                .unwrap();

            let cdc_fn = cdc(&mut db, &metadata);
            let cdc = {
                let tx_embeddings = tx_embeddings.clone();
                async move {
                    if let Some(cdc_fn) = cdc_fn {
                        cdc_fn(tx_embeddings).await;
                    }
                }
                .shared()
            };
            while !rx_index.is_closed() {
                tokio::select! {
                    _ = cdc.clone() => break,
                    msg = rx_index.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        spawn_process_db_index(
                            db.clone(),
                            metadata.clone(),
                            Arc::clone(&fullscan_finished),
                            msg
                        ).await;
                    }
                }
            }

            while let Some(msg) = rx_index.recv().await {
                spawn_process_db_index(
                    db.clone(),
                    metadata.clone(),
                    Arc::clone(&fullscan_finished),
                    msg,
                )
                .await;
            }
            drop(tx_embeddings);
        }
    });
    Ok((tx_index, rx_embeddings))
}

fn fullscan(db: &mut DbBasic, metadata: &IndexMetadata) -> Option<ScanFn> {
    db.0.write()
        .unwrap()
        .keyspaces
        .get_mut(&metadata.keyspace_name)
        .and_then(|keyspace| keyspace.indexes.get_mut(&metadata.index_name))
        .and_then(|index| index.fullscan_fn.take())
}

fn cdc(db: &mut DbBasic, metadata: &IndexMetadata) -> Option<ScanFn> {
    db.0.write()
        .unwrap()
        .keyspaces
        .get_mut(&metadata.keyspace_name)
        .and_then(|keyspace| keyspace.indexes.get_mut(&metadata.index_name))
        .and_then(|index| index.cdc_fn.take())
}

async fn spawn_process_db_index(
    db: DbBasic,
    metadata: IndexMetadata,
    fullscan_finished: Arc<AtomicBool>,
    msg: DbIndex,
) {
    tokio::spawn(async move {
        match msg {
            DbIndex::GetPrimaryKeyColumns { tx } => tx
                .send(
                    db.0.read()
                        .unwrap()
                        .keyspaces
                        .get(&metadata.keyspace_name)
                        .and_then(|keyspace| keyspace.tables.get(&metadata.table_name))
                        .map(|table| table.primary_keys.clone())
                        .unwrap_or_default(),
                )
                .map_err(|_| anyhow!("DbIndex::GetPrimaryKeyColumns: unable to send response"))
                .unwrap(),

            DbIndex::GetPartitionKeyCount { tx } => tx
                .send(
                    db.0.read()
                        .unwrap()
                        .keyspaces
                        .get(&metadata.keyspace_name)
                        .and_then(|keyspace| keyspace.tables.get(&metadata.table_name))
                        .map(|table| table.partition_key_count)
                        .unwrap_or(1),
                )
                .map_err(|_| anyhow!("DbIndex::GetPartitionKeyCount: unable to send response"))
                .unwrap(),

            DbIndex::GetTableColumns { tx } => tx
                .send(
                    db.0.read()
                        .unwrap()
                        .keyspaces
                        .get(&metadata.keyspace_name)
                        .and_then(|keyspace| keyspace.tables.get(&metadata.table_name))
                        .map(|table| table.columns.clone())
                        .unwrap_or_default(),
                )
                .map_err(|_| anyhow!("DbIndex::GetPrimaryKeyColumns: unable to send response"))
                .unwrap(),

            DbIndex::FullScanProgress { tx } => tx
                .send({
                    let db = db.0.read().unwrap();
                    db.next_full_scan_progress.unwrap_or_else(|| {
                        if fullscan_finished.load(Ordering::Acquire) {
                            Progress::Done
                        } else {
                            Progress::InProgress(Percentage::try_from(0.0).unwrap())
                        }
                    })
                })
                .map_err(|_| anyhow!("DbIndex::GetTargetColumn: unable to send response"))
                .unwrap(),
        }
    });
}
