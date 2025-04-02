/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::db;
use crate::db::DbExt;
use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::ColumnName;
use crate::Dimensions;
use crate::IndexId;
use crate::IndexMetadata;
use crate::IndexVersion;
use crate::KeyspaceName;
use crate::TableName;
use anyhow::Context;
use futures::TryStreamExt;
use regex::Regex;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimeuuid;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::mem;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time;
use tracing::warn;
use uuid::Uuid;

pub(crate) enum MonitorIndexes {}

pub(crate) async fn new(
    db_session: Arc<Session>,
    db_actor: Sender<db::Db>,
    engine: Sender<Engine>,
) -> anyhow::Result<Sender<MonitorIndexes>> {
    let db = Db::new(db_session).await?;
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        const INTERVAL: Duration = Duration::from_secs(1);
        let mut interval = time::interval(INTERVAL);

        let mut schema_version = SchemaVersion::new();
        let mut indexes = HashSet::new();
        while !rx.is_closed() {
            tokio::select! {
                _ = interval.tick() => {
                    if !schema_version.has_changed(&db_actor).await {
                        continue;
                    }
                    let Ok(mut new_indexes) = get_indexes(&db, &db_actor).await.inspect_err(|err| {
                        warn!("monitor_indexes: unable to get the list of indexes: {err}");
                    }) else {
                        schema_version.reset();
                        continue;
                    };
                    del_indexes(&engine, indexes.difference(&new_indexes)).await;
                    add_indexes(&engine, new_indexes.difference(&indexes)).await;
                    mem::swap(&mut indexes, &mut new_indexes);
                }
                _ = rx.recv() => { }
            }
        }
    });
    Ok(tx)
}

struct Db {
    session: Arc<Session>,
    st_get_indexes: PreparedStatement,
    st_get_index_version: PreparedStatement,
    st_get_index_target_type: PreparedStatement,
    re_get_index_target_type: Regex,
}

impl Db {
    async fn new(session: Arc<Session>) -> anyhow::Result<Self> {
        Ok(Self {
            st_get_indexes: session
                .prepare(Self::ST_GET_INDEXES)
                .await
                .context("ST_GET_INDEXES")?,
            st_get_index_version: session
                .prepare(Self::ST_GET_INDEX_VERSION)
                .await
                .context("ST_GET_INDEX_VERSION")?,
            st_get_index_target_type: session
                .prepare(Self::ST_GET_INDEX_TARGET_TYPE)
                .await
                .context("ST_GET_INDEX_TARGET_TYPE")?,
            re_get_index_target_type: Regex::new(Self::RE_GET_INDEX_TARGET_TYPE)
                .context("RE_GET_INDEX_TARGET_TYPE")?,
            session,
        })
    }

    const ST_GET_INDEXES: &str = "
        SELECT keyspace_name, index_name, table_name, options
        FROM system_schema.indexes
        WHERE kind = 'CUSTOM'
        ALLOW FILTERING
        ";
    async fn get_indexes(&self) -> anyhow::Result<Vec<DbGetIndexes>> {
        Ok(self
            .session
            .execute_iter(self.st_get_indexes.clone(), &[])
            .await?
            .rows_stream::<(String, String, String, BTreeMap<String, String>)>()?
            .try_filter_map(|(keyspace, index, table, mut options)| async move {
                Ok(options.remove("target").map(|target| DbGetIndexes {
                    keyspace: keyspace.into(),
                    index: index.into(),
                    table: table.into(),
                    target: target.into(),
                }))
            })
            .try_collect()
            .await?)
    }

    const ST_GET_INDEX_VERSION: &str = "
        SELECT version
        FROM system_schema.scylla_tables
        WHERE keyspace_name = ? AND table_name = ?
        ";
    async fn get_index_version(
        &self,
        keyspace: KeyspaceName,
        index: TableName,
    ) -> anyhow::Result<Option<IndexVersion>> {
        Ok(self
            .session
            .execute_iter(
                self.st_get_index_version.clone(),
                (keyspace, format!("{}_index", index.0)),
            )
            .await?
            .rows_stream::<(Uuid,)>()?
            .try_next()
            .await?
            .map(|(version,)| version.into()))
    }

    const ST_GET_INDEX_TARGET_TYPE: &str = "
        SELECT type
        FROM system_schema.columns
        WHERE keyspace_name = ? AND table_name = ? AND column_name = ?
        ";
    const RE_GET_INDEX_TARGET_TYPE: &str = r"^vector<float, (?<dimensions>\d+)>$";
    async fn get_index_target_type(
        &self,
        keyspace: KeyspaceName,
        table: TableName,
        target: ColumnName,
    ) -> anyhow::Result<Option<Dimensions>> {
        Ok(self
            .session
            .execute_iter(
                self.st_get_index_target_type.clone(),
                (keyspace, table, target),
            )
            .await?
            .rows_stream::<(String,)>()?
            .try_next()
            .await?
            .and_then(|(typ,)| {
                self.re_get_index_target_type
                    .captures(&typ)
                    .and_then(|captures| captures["dimensions"].parse::<usize>().ok())
            })
            .and_then(|dimensions| {
                NonZeroUsize::new(dimensions).map(|dimensions| dimensions.into())
            }))
    }
}

#[derive(Debug)]
struct DbGetIndexes {
    keyspace: KeyspaceName,
    index: TableName,
    table: TableName,
    target: ColumnName,
}

impl DbGetIndexes {
    fn id(&self) -> IndexId {
        IndexId::new(&self.keyspace, &self.index)
    }
}

#[derive(PartialEq)]
struct SchemaVersion(Option<CqlTimeuuid>);

impl SchemaVersion {
    fn new() -> Self {
        Self(None)
    }

    async fn has_changed(&mut self, db: &Sender<db::Db>) -> bool {
        let schema_version = db.latest_schema_version().await.unwrap_or_else(|err| {
            warn!("monitor_indexes: unable to get latest schema change: {err}");
            None
        });
        if self.0 == schema_version {
            return false;
        };
        self.0 = schema_version;
        true
    }

    fn reset(&mut self) {
        self.0 = None;
    }
}

async fn get_indexes(db: &Db, db_actor: &Sender<db::Db>) -> anyhow::Result<HashSet<IndexMetadata>> {
    let mut indexes = HashSet::new();
    for idx in db.get_indexes().await?.into_iter() {
        let Some(version) = db
            .get_index_version(idx.keyspace.clone(), idx.index.clone())
            .await
            .inspect_err(|err| {
                warn!("monitor_indexes::get_indexes: unable to get index version: {err}")
            })?
        else {
            warn!("monitor_indexes::get_indexes: no version for index {idx:?}");
            continue;
        };

        let Some(dimensions) = db
            .get_index_target_type(idx.keyspace.clone(), idx.table.clone(), idx.target.clone())
            .await
            .inspect_err(|err| {
                warn!("monitor_indexes::get_indexes: unable to get index target type: {err}")
            })?
        else {
            warn!("monitor_indexes::get_indexes: missing or unsupported type for index {idx:?}");
            continue;
        };

        let (connectivity, expansion_add, expansion_search) = if let Some(params) = db_actor
            .get_index_params(idx.id())
            .await
            .inspect_err(|err| {
                warn!("monitor_indexes::get_indexes: unable to get index params: {err}")
            })? {
            params
        } else {
            warn!("monitor_indexes::get_indexes: no params for index {idx:?}");
            (0.into(), 0.into(), 0.into())
        };

        let metadata = IndexMetadata {
            keyspace_name: idx.keyspace,
            index_name: idx.index,
            table_name: idx.table,
            target_name: idx.target,
            key_name: "id".to_string().into(),
            dimensions,
            connectivity,
            expansion_add,
            expansion_search,
            version,
        };
        indexes.insert(metadata);
    }
    Ok(indexes)
}

async fn add_indexes(engine: &Sender<Engine>, idxs: impl Iterator<Item = &IndexMetadata>) {
    for idx in idxs {
        engine.add_index(idx.clone()).await;
    }
}

async fn del_indexes(engine: &Sender<Engine>, idxs: impl Iterator<Item = &IndexMetadata>) {
    for idx in idxs {
        engine.del_index(idx.id()).await;
    }
}
