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
use scylla::value::CqlTimeuuid;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use uuid::Uuid;
use vector_store::ColumnName;
use vector_store::Connectivity;
use vector_store::DbCustomIndex;
use vector_store::DbEmbeddings;
use vector_store::Dimensions;
use vector_store::Embeddings;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::IndexMetadata;
use vector_store::IndexName;
use vector_store::KeyspaceName;
use vector_store::PrimaryKey;
use vector_store::TableName;
use vector_store::Timestamp;
use vector_store::db::Db;
use vector_store::db_index::DbIndex;

#[derive(Clone)]
pub(crate) struct DbBasic(Arc<RwLock<DbMock>>);

pub(crate) fn new() -> (mpsc::Sender<Db>, DbBasic) {
    let (tx, mut rx) = mpsc::channel(10);
    let db = DbBasic::new();
    tokio::spawn({
        let db = db.clone();
        async move {
            while let Some(msg) = rx.recv().await {
                process_db(&db, msg);
            }
        }
    });
    (tx, db)
}

struct TableStore {
    table: Table,
    embeddings: HashMap<ColumnName, HashMap<PrimaryKey, (Embeddings, Timestamp)>>,
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
    pub(crate) primary_keys: Vec<ColumnName>,
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
        values: impl IntoIterator<Item = (PrimaryKey, Embeddings, Timestamp)>,
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
            .for_each(|(primary_key, embeddings, timestamp)| {
                column
                    .entry(primary_key)
                    .and_modify(|(entry_embeddings, entry_timestamp)| {
                        if entry_timestamp.as_ref() < timestamp.as_ref() {
                            *entry_embeddings = embeddings.clone();
                            *entry_timestamp = timestamp;
                        }
                    })
                    .or_insert((embeddings, timestamp));
            });

        Ok(())
    }
}

fn process_db(db: &DbBasic, msg: Db) {
    match msg {
        Db::GetDbIndex { metadata, tx } => tx
            .send(new_db_index(db.clone(), metadata))
            .map_err(|_| anyhow!("Db::GetDbIndex: unable to send response"))
            .unwrap(),

        Db::LatestSchemaVersion { tx } => tx
            .send(Ok(Some(db.0.read().unwrap().schema_version)))
            .map_err(|_| anyhow!("Db::LatestSchemaVersion: unable to send response"))
            .unwrap(),

        Db::GetIndexes { tx } => tx
            .send(Ok(db
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
            .unwrap(),

        Db::GetIndexVersion {
            keyspace,
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
    db: DbBasic,
    metadata: IndexMetadata,
) -> anyhow::Result<(mpsc::Sender<DbIndex>, mpsc::Receiver<DbEmbeddings>)> {
    let (tx_index, mut rx_index) = mpsc::channel(10);
    let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
    tokio::spawn({
        async move {
            let mut items = initial_scan(&db, &metadata);
            while !rx_index.is_closed() {
                tokio::select! {
                    item = items.next() => {
                        let Some(item) = item else {
                            break;
                        };
                        if tx_embeddings.send(item).await.is_err() {
                            break;
                        }
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

fn initial_scan(db: &DbBasic, metadata: &IndexMetadata) -> impl Stream<Item = DbEmbeddings> {
    stream::iter(
        db.0.read()
            .unwrap()
            .keyspaces
            .get(&metadata.keyspace_name)
            .and_then(|keyspace| keyspace.tables.get(&metadata.table_name))
            .and_then(|table| table.embeddings.get(&metadata.target_column))
            .map(|rows| {
                rows.iter()
                    .map(|(primary_key, (embeddings, timestamp))| DbEmbeddings {
                        primary_key: primary_key.clone(),
                        embeddings: embeddings.clone(),
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
    }
}
