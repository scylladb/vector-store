/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use anyhow::bail;
use futures::Stream;
use futures::StreamExt;
use futures::stream;
use itertools::Itertools;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlTimeuuid;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use vector_store::AsyncInProgress;
use vector_store::ColumnName;
use vector_store::Connectivity;
use vector_store::DbCustomIndex;
use vector_store::DbEmbedding;
use vector_store::Dimensions;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::IndexMetadata;
use vector_store::IndexName;
use vector_store::KeyspaceName;
use vector_store::PrimaryKey;
use vector_store::Progress;
use vector_store::SpaceType;
use vector_store::TableName;
use vector_store::Timestamp;
use vector_store::Vector;
use vector_store::db::Db;
use vector_store::db_index::DbIndex;
use vector_store::node_state::Event;
use vector_store::node_state::NodeState;

#[derive(Clone)]
pub(crate) struct DbBasic(Arc<RwLock<DbMock>>);

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

struct TableStore {
    table: Table,
    embeddings: HashMap<ColumnName, HashMap<PrimaryKey, (Option<Vector>, Timestamp)>>,
}

impl TableStore {
    fn new(table: Table) -> Self {
        Self {
            embeddings: table
                .dimensions
                .keys()
                .map(|key| (key.clone(), HashMap::new()))
                .collect(),
            table,
        }
    }
}

pub(crate) struct Table {
    pub(crate) primary_keys: Arc<Vec<ColumnName>>,
    pub(crate) columns: Arc<HashMap<ColumnName, NativeType>>,
    pub(crate) dimensions: HashMap<ColumnName, Dimensions>,
}

#[derive(Debug)]
struct IndexStore {
    index: Index,
    version: Uuid,
}

impl IndexStore {
    fn new(index: Index) -> Self {
        Self {
            version: Uuid::new_v4(),
            index,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Index {
    pub(crate) table_name: TableName,
    pub(crate) target_column: ColumnName,
    pub(crate) connectivity: Connectivity,
    pub(crate) expansion_add: ExpansionAdd,
    pub(crate) expansion_search: ExpansionSearch,
    pub(crate) space_type: SpaceType,
}

impl From<IndexMetadata> for Index {
    fn from(metadata: IndexMetadata) -> Self {
        Self {
            table_name: metadata.table_name,
            target_column: metadata.target_column,
            connectivity: metadata.connectivity,
            expansion_add: metadata.expansion_add,
            expansion_search: metadata.expansion_search,
            space_type: metadata.space_type,
        }
    }
}

struct Keyspace {
    tables: HashMap<TableName, TableStore>,
    indexes: HashMap<IndexName, IndexStore>,
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
    next_full_scan_progress: Progress,
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
            next_full_scan_progress: Progress::Done,
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
        keyspace.tables.insert(table_name, TableStore::new(table));

        db.create_new_schema_version();
        Ok(())
    }

    pub(crate) fn add_index(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: IndexName,
        index: Index,
    ) -> anyhow::Result<()> {
        let mut db = self.0.write().unwrap();

        let Some(keyspace) = db.keyspaces.get_mut(keyspace_name) else {
            bail!("a keyspace {keyspace_name} does not exist");
        };
        let Some(table) = keyspace.tables.get(&index.table_name) else {
            bail!("a table {} does not exist", index.table_name);
        };
        if !table.embeddings.contains_key(&index.target_column) {
            bail!(
                "a table {} does not contain a target column {}",
                index.table_name,
                index.target_column
            );
        }
        if keyspace.indexes.contains_key(&index_name) {
            bail!("an index {index_name} already exists");
        }
        keyspace.indexes.insert(index_name, IndexStore::new(index));

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

    pub(crate) fn insert_values(
        &self,
        keyspace_name: &KeyspaceName,
        table_name: &TableName,
        target_column: &ColumnName,
        values: impl IntoIterator<Item = (PrimaryKey, Option<Vector>, Timestamp)>,
    ) -> anyhow::Result<()> {
        let mut db = self.0.write().unwrap();

        let Some(keyspace) = db.keyspaces.get_mut(keyspace_name) else {
            bail!("a keyspace {keyspace_name} does not exist");
        };
        let Some(table) = keyspace.tables.get_mut(table_name) else {
            bail!("a table {table_name} does not exist");
        };
        let Some(column) = table.embeddings.get_mut(target_column) else {
            bail!("a column {target_column} does not exist in a table {table_name}");
        };

        values
            .into_iter()
            .for_each(|(primary_key, embedding, timestamp)| {
                column
                    .entry(primary_key)
                    .and_modify(|(entry_embedding, entry_timestamp)| {
                        if entry_timestamp.as_ref() < timestamp.as_ref() {
                            *entry_embedding = embedding.clone();
                            *entry_timestamp = timestamp;
                        }
                    })
                    .or_insert((embedding, timestamp));
            });

        Ok(())
    }

    pub(crate) fn set_next_get_db_index_failed(&self) {
        self.0.write().unwrap().next_get_db_index_failed = true;
    }

    pub(crate) fn set_next_full_scan_progress(&self, progress: Progress) {
        self.0.write().unwrap().next_full_scan_progress = progress;
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
                                table: index.index.table_name.clone(),
                                target_column: index.index.target_column.clone(),
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
                .map(|index| index.version.into())))
            .map_err(|_| anyhow!("Db::GetIndexVersion: unable to send response"))
            .unwrap(),

        Db::GetIndexTargetType {
            keyspace,
            table,
            target_column,
            tx,
        } => tx
            .send(Ok(db
                .0
                .read()
                .unwrap()
                .keyspaces
                .get(&keyspace)
                .and_then(|keyspace| keyspace.tables.get(&table))
                .and_then(|table| table.table.dimensions.get(&target_column))
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
                        index.index.connectivity,
                        index.index.expansion_add,
                        index.index.expansion_search,
                        index.index.space_type,
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

type RxEmbeddings = mpsc::Receiver<(DbEmbedding, Option<AsyncInProgress>)>;
pub(crate) fn new_db_index(
    db: DbBasic,
    metadata: IndexMetadata,
    node_state: Sender<NodeState>,
) -> anyhow::Result<(mpsc::Sender<DbIndex>, RxEmbeddings)> {
    if db.0.read().unwrap().next_get_db_index_failed {
        db.0.write().unwrap().next_get_db_index_failed = false;
        bail!("get_db_index failed");
    }

    let (tx_index, mut rx_index) = mpsc::channel(10);
    let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
    tokio::spawn({
        async move {
            let mut items = initial_scan(&db, &metadata);
            node_state
                .send(NodeState::SendEvent(Event::FullScanFinished(
                    metadata.clone(),
                )))
                .await
                .unwrap();
            while !rx_index.is_closed() && !tx_embeddings.is_closed() {
                tokio::select! {
                    item = items.next() => {
                        let Some(item) = item else {
                            break;
                        };
                        tokio::spawn({
                            let tx_embeddings = tx_embeddings.clone();
                            async move {
                                _ = tx_embeddings.send((item, None)).await;
                            }
                        });
                    }
                    Some(msg) = rx_index.recv() => {
                        process_db_index(&db, &metadata, msg).await;
                    }
                }
            }
            while let Some(msg) = rx_index.recv().await {
                process_db_index(&db, &metadata, msg).await;
            }
        }
    });
    Ok((tx_index, rx_embeddings))
}

fn initial_scan(db: &DbBasic, metadata: &IndexMetadata) -> impl Stream<Item = DbEmbedding> {
    stream::iter(
        db.0.read()
            .unwrap()
            .keyspaces
            .get(&metadata.keyspace_name)
            .and_then(|keyspace| keyspace.tables.get(&metadata.table_name))
            .and_then(|table| table.embeddings.get(&metadata.target_column))
            .map(|rows| {
                rows.iter()
                    .map(|(primary_key, (embedding, timestamp))| DbEmbedding {
                        primary_key: primary_key.clone(),
                        embedding: embedding.clone(),
                        timestamp: *timestamp,
                    })
                    .collect_vec()
            })
            .unwrap_or_default(),
    )
}

async fn process_db_index(db: &DbBasic, metadata: &IndexMetadata, msg: DbIndex) {
    match msg {
        DbIndex::GetPrimaryKeyColumns { tx } => tx
            .send(
                db.0.read()
                    .unwrap()
                    .keyspaces
                    .get(&metadata.keyspace_name)
                    .and_then(|keyspace| keyspace.tables.get(&metadata.table_name))
                    .map(|table| table.table.primary_keys.clone())
                    .unwrap_or_default(),
            )
            .map_err(|_| anyhow!("DbIndex::GetPrimaryKeyColumns: unable to send response"))
            .unwrap(),

        DbIndex::GetTableColumns { tx } => tx
            .send(
                db.0.read()
                    .unwrap()
                    .keyspaces
                    .get(&metadata.keyspace_name)
                    .and_then(|keyspace| keyspace.tables.get(&metadata.table_name))
                    .map(|table| table.table.columns.clone())
                    .unwrap_or_default(),
            )
            .map_err(|_| anyhow!("DbIndex::GetPrimaryKeyColumns: unable to send response"))
            .unwrap(),

        DbIndex::FullScanProgress { tx } => tx
            .send({
                let mut db = db.0.write().unwrap();
                let val = db.next_full_scan_progress.clone();
                db.next_full_scan_progress = Progress::Done;
                val
            })
            .map_err(|_| anyhow!("DbIndex::GetTargetColumn: unable to send response"))
            .unwrap(),
    }
}
