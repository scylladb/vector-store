/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Connectivity;
use crate::DbCustomIndex;
use crate::Dimensions;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexId;
use crate::IndexItemsCount;
use crate::IndexMetadata;
use crate::IndexVersion;
use crate::KeyspaceName;
use crate::ScyllaDbUri;
use crate::TableName;
use crate::db_index;
use crate::db_index::DbIndex;
use anyhow::Context;
use futures::TryStreamExt;
use regex::Regex;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimeuuid;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug_span;
use tracing::warn;
use uuid::Uuid;

type GetDbIndexR = anyhow::Result<mpsc::Sender<DbIndex>>;
type LatestSchemaVersionR = anyhow::Result<Option<CqlTimeuuid>>;
type GetIndexesR = anyhow::Result<Vec<DbCustomIndex>>;
type GetIndexVersionR = anyhow::Result<Option<IndexVersion>>;
type GetIndexTargetTypeR = anyhow::Result<Option<Dimensions>>;
type GetIndexParamsR = anyhow::Result<Option<(Connectivity, ExpansionAdd, ExpansionSearch)>>;

pub(crate) enum Db {
    GetDbIndex {
        metadata: IndexMetadata,
        tx: oneshot::Sender<GetDbIndexR>,
    },

    LatestSchemaVersion {
        tx: oneshot::Sender<LatestSchemaVersionR>,
    },

    GetIndexes {
        tx: oneshot::Sender<GetIndexesR>,
    },

    GetIndexVersion {
        keyspace: KeyspaceName,
        index: TableName,
        tx: oneshot::Sender<GetIndexVersionR>,
    },

    GetIndexTargetType {
        keyspace: KeyspaceName,
        table: TableName,
        target_column: ColumnName,
        tx: oneshot::Sender<GetIndexTargetTypeR>,
    },

    GetIndexParams {
        id: IndexId,
        tx: oneshot::Sender<GetIndexParamsR>,
    },

    UpdateItemsCount {
        id: IndexId,
        items_count: IndexItemsCount,
    },

    RemoveIndex {
        id: IndexId,
    },
}

pub(crate) trait DbExt {
    async fn get_db_index(&self, metadata: IndexMetadata) -> GetDbIndexR;

    async fn latest_schema_version(&self) -> LatestSchemaVersionR;

    async fn get_indexes(&self) -> GetIndexesR;

    async fn get_index_version(&self, keyspace: KeyspaceName, index: TableName)
    -> GetIndexVersionR;

    async fn get_index_target_type(
        &self,
        keyspace: KeyspaceName,
        table: TableName,
        target_column: ColumnName,
    ) -> GetIndexTargetTypeR;

    async fn get_index_params(&self, id: IndexId) -> GetIndexParamsR;

    async fn update_items_count(
        &self,
        id: IndexId,
        items_count: IndexItemsCount,
    ) -> anyhow::Result<()>;

    async fn remove_index(&self, id: IndexId) -> anyhow::Result<()>;
}

impl DbExt for mpsc::Sender<Db> {
    async fn get_db_index(&self, metadata: IndexMetadata) -> GetDbIndexR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetDbIndex { metadata, tx }).await?;
        rx.await?
    }

    async fn latest_schema_version(&self) -> LatestSchemaVersionR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::LatestSchemaVersion { tx }).await?;
        rx.await?
    }

    async fn get_indexes(&self) -> GetIndexesR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexes { tx }).await?;
        rx.await?
    }

    async fn get_index_version(
        &self,
        keyspace: KeyspaceName,
        index: TableName,
    ) -> GetIndexVersionR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexVersion {
            keyspace,
            index,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn get_index_target_type(
        &self,
        keyspace: KeyspaceName,
        table: TableName,
        target_column: ColumnName,
    ) -> GetIndexTargetTypeR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexTargetType {
            keyspace,
            table,
            target_column,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn get_index_params(&self, id: IndexId) -> GetIndexParamsR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexParams { id, tx }).await?;
        rx.await?
    }

    async fn update_items_count(
        &self,
        id: IndexId,
        items_count: IndexItemsCount,
    ) -> anyhow::Result<()> {
        self.send(Db::UpdateItemsCount { id, items_count }).await?;
        Ok(())
    }

    async fn remove_index(&self, id: IndexId) -> anyhow::Result<()> {
        self.send(Db::RemoveIndex { id }).await?;
        Ok(())
    }
}

pub(crate) async fn new(uri: ScyllaDbUri) -> anyhow::Result<mpsc::Sender<Db>> {
    let statements = Arc::new(Statements::new(uri).await?);
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(
        async move {
            while let Some(msg) = rx.recv().await {
                tokio::spawn(process(Arc::clone(&statements), msg));
            }
        }
        .instrument(debug_span!("db")),
    );
    Ok(tx)
}

async fn process(statements: Arc<Statements>, msg: Db) {
    match msg {
        Db::GetDbIndex { metadata, tx } => tx
            .send(statements.get_db_index(metadata).await)
            .unwrap_or_else(|_| warn!("db::process: Db::GetDbIndex: unable to send response")),

        Db::LatestSchemaVersion { tx } => tx
            .send(statements.latest_schema_version().await)
            .unwrap_or_else(|_| {
                warn!("db::process: Db::LatestSchemaVersion: unable to send response")
            }),

        Db::GetIndexes { tx } => tx
            .send(statements.get_indexes().await)
            .unwrap_or_else(|_| warn!("db::process: Db::GetIndexes: unable to send response")),

        Db::GetIndexVersion {
            keyspace,
            index,
            tx,
        } => tx
            .send(statements.get_index_version(keyspace, index).await)
            .unwrap_or_else(|_| warn!("db::process: Db::GetIndexVersion: unable to send response")),

        Db::GetIndexTargetType {
            keyspace,
            table,
            target_column,
            tx,
        } => tx
            .send(
                statements
                    .get_index_target_type(keyspace, table, target_column)
                    .await,
            )
            .unwrap_or_else(|_| warn!("db::process: Db::GetIndexVersion: unable to send response")),

        Db::GetIndexParams { id, tx } => tx
            .send(statements.get_index_params(id).await)
            .unwrap_or_else(|_| warn!("db::process: Db::GetIndexParams: unable to send response")),

        Db::UpdateItemsCount { id, items_count } => {
            statements
                .update_items_count(id, items_count)
                .await
                .unwrap_or_else(|err| warn!("db::process: Db::UpdateItemsCount: {err}"));
        }

        Db::RemoveIndex { id } => {
            statements
                .remove_index(id)
                .await
                .unwrap_or_else(|err| warn!("db::process: Db::RemoveIndex: {err}"));
        }
    }
}

struct Statements {
    session: Arc<Session>,
    st_latest_schema_version: PreparedStatement,
    st_get_indexes: PreparedStatement,
    st_get_index_version: PreparedStatement,
    st_get_index_target_type: PreparedStatement,
    re_get_index_target_type: Regex,
    st_get_index_params: PreparedStatement,
    st_update_items_count: PreparedStatement,
    st_remove_index: PreparedStatement,
}

impl Statements {
    async fn new(uri: ScyllaDbUri) -> anyhow::Result<Self> {
        let session = Arc::new(
            SessionBuilder::new()
                .known_node(uri.0.as_str())
                .build()
                .await?,
        );
        Ok(Self {
            st_latest_schema_version: session
                .prepare(Self::ST_LATEST_SCHEMA_VERSION)
                .await
                .context("ST_LATEST_SCHEMA_VERSION")?,

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

            st_get_index_params: session
                .prepare(Self::ST_GET_INDEX_PARAMS)
                .await
                .context("ST_GET_INDEX_PARAMS")?,

            st_update_items_count: session
                .prepare(Self::ST_UPDATE_ITEMS_COUNT)
                .await
                .context("ST_UPDATE_ITEMS_COUNT")?,

            st_remove_index: session
                .prepare(Self::ST_REMOVE_INDEX)
                .await
                .context("ST_REMOVE_INDEX")?,

            session,
        })
    }

    async fn get_db_index(&self, metadata: IndexMetadata) -> GetDbIndexR {
        db_index::new(Arc::clone(&self.session), metadata).await
    }

    const ST_LATEST_SCHEMA_VERSION: &str = "
        SELECT state_id
        FROM system.group0_history
        WHERE key = 'history'
        ORDER BY state_id DESC
        LIMIT 1
        ";

    async fn latest_schema_version(&self) -> LatestSchemaVersionR {
        Ok(self
            .session
            .execute_iter(self.st_latest_schema_version.clone(), &[])
            .await?
            .rows_stream::<(CqlTimeuuid,)>()?
            .try_next()
            .await?
            .map(|(timeuuid,)| timeuuid))
    }

    const ST_GET_INDEXES: &str = "
        SELECT keyspace_name, index_name, table_name, options
        FROM system_schema.indexes
        WHERE kind = 'CUSTOM'
        ALLOW FILTERING
        ";

    async fn get_indexes(&self) -> GetIndexesR {
        Ok(self
            .session
            .execute_iter(self.st_get_indexes.clone(), &[])
            .await?
            .rows_stream::<(String, String, String, BTreeMap<String, String>)>()?
            .try_filter_map(|(keyspace, index, table, mut options)| async move {
                Ok(options.remove("target").map(|target| DbCustomIndex {
                    keyspace: keyspace.into(),
                    index: index.into(),
                    table: table.into(),
                    target_column: target.into(),
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
    ) -> GetIndexVersionR {
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
        target_column: ColumnName,
    ) -> GetIndexTargetTypeR {
        Ok(self
            .session
            .execute_iter(
                self.st_get_index_target_type.clone(),
                (keyspace, table, target_column),
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

    const ST_GET_INDEX_PARAMS: &str = "
        SELECT param_m, param_ef_construct, param_ef_search
        FROM vector_benchmark.vector_indexes
        WHERE id = ?
        ";

    async fn get_index_params(&self, id: IndexId) -> GetIndexParamsR {
        Ok(self
            .session
            .execute_iter(self.st_get_index_params.clone(), (id,))
            .await?
            .rows_stream::<(Option<i32>, Option<i32>, Option<i32>)>()?
            .try_filter_map(|(connectivity, expansion_add, expansion_search)| {
                Box::pin(async move {
                    Ok(connectivity
                        .zip(expansion_add)
                        .zip(expansion_search)
                        .and_then(|((connectivity, expansion_add), expansion_search)| {
                            (connectivity >= 0 && expansion_add >= 0 && expansion_search >= 0)
                                .then_some((connectivity, expansion_add, expansion_search))
                        }))
                })
            })
            .map_ok(|(connectivity, expansion_add, expansion_search)| {
                (
                    (connectivity as usize).into(),
                    (expansion_add as usize).into(),
                    (expansion_search as usize).into(),
                )
            })
            .try_next()
            .await?)
    }

    const ST_UPDATE_ITEMS_COUNT: &str = "
        UPDATE vector_benchmark.vector_indexes
            SET indexed_elements_count = ?
            WHERE id = ?
        ";

    async fn update_items_count(
        &self,
        id: IndexId,
        items_count: IndexItemsCount,
    ) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_update_items_count, (items_count, id))
            .await?;
        Ok(())
    }

    const ST_REMOVE_INDEX: &str = "
        DELETE FROM vector_benchmark.vector_indexes
        WHERE id = ?
        ";

    async fn remove_index(&self, id: IndexId) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_remove_index, (id,))
            .await?;
        Ok(())
    }
}
