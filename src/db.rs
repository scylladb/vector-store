/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Connectivity;
use crate::DbCustomIndex;
use crate::DbEmbeddings;
use crate::Dimensions;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
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
use tracing::trace;
use uuid::Uuid;

type GetDbIndexR = anyhow::Result<(mpsc::Sender<DbIndex>, mpsc::Receiver<DbEmbeddings>)>;
type LatestSchemaVersionR = anyhow::Result<Option<CqlTimeuuid>>;
type GetIndexesR = anyhow::Result<Vec<DbCustomIndex>>;
type GetIndexVersionR = anyhow::Result<Option<IndexVersion>>;
type GetIndexTargetTypeR = anyhow::Result<Option<Dimensions>>;
type GetIndexParamsR = anyhow::Result<Option<(Connectivity, ExpansionAdd, ExpansionSearch)>>;
type IsValidIndexR = bool;

pub enum Db {
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
        keyspace: KeyspaceName,
        index: TableName,
        tx: oneshot::Sender<GetIndexParamsR>,
    },

    // Schema changes are concurrent processes without an atomic view from the client/driver side.
    // A process of retrieving an index metadata from the vector-store could be faster than similar
    // process in a driver itself. The vector-store reads some schema metadata from system tables
    // directly, because they are not available from a rust driver, and it reads some other schema
    // metadata from the rust driver, so there must be an agreement between data read directly from
    // a db and a driver. This message checks if index metadata are correct and if there is an
    // agreement on a db schema in the rust driver.
    IsValidIndex {
        metadata: IndexMetadata,
        tx: oneshot::Sender<IsValidIndexR>,
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

    async fn get_index_params(&self, keyspace: KeyspaceName, index: TableName) -> GetIndexParamsR;

    async fn is_valid_index(&self, metadata: IndexMetadata) -> IsValidIndexR;
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

    async fn get_index_params(&self, keyspace: KeyspaceName, index: TableName) -> GetIndexParamsR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexParams {
            keyspace,
            index,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn is_valid_index(&self, metadata: IndexMetadata) -> IsValidIndexR {
        let (tx, rx) = oneshot::channel();
        if self.send(Db::IsValidIndex { metadata, tx }).await.is_err() {
            return false;
        };
        rx.await.is_ok()
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
            .unwrap_or_else(|_| trace!("process: Db::GetDbIndex: unable to send response")),

        Db::LatestSchemaVersion { tx } => tx
            .send(statements.latest_schema_version().await)
            .unwrap_or_else(|_| {
                trace!("process: Db::LatestSchemaVersion: unable to send response")
            }),

        Db::GetIndexes { tx } => tx
            .send(statements.get_indexes().await)
            .unwrap_or_else(|_| trace!("process: Db::GetIndexes: unable to send response")),

        Db::GetIndexVersion {
            keyspace,
            index,
            tx,
        } => tx
            .send(statements.get_index_version(keyspace, index).await)
            .unwrap_or_else(|_| trace!("process: Db::GetIndexVersion: unable to send response")),

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
            .unwrap_or_else(|_| trace!("process: Db::GetIndexVersion: unable to send response")),

        Db::GetIndexParams {
            keyspace,
            index,
            tx,
        } => tx
            .send(statements.get_index_params(keyspace, index).await)
            .unwrap_or_else(|_| trace!("process: Db::GetIndexParams: unable to send response")),

        Db::IsValidIndex { metadata, tx } => tx
            .send(statements.is_valid_index(metadata).await)
            .unwrap_or_else(|_| trace!("process: Db::IsValidIndex: unable to send response")),
    }
}

struct Statements {
    session: Arc<Session>,
    st_latest_schema_version: PreparedStatement,
    st_get_indexes: PreparedStatement,
    st_get_index_version: PreparedStatement,
    st_get_index_target_type: PreparedStatement,
    re_get_index_target_type: Regex,
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

    async fn get_index_params(
        &self,
        _keyspace: KeyspaceName,
        _index: TableName,
    ) -> GetIndexParamsR {
        Ok(Some((
            Connectivity::default(),
            ExpansionAdd::default(),
            ExpansionSearch::default(),
        )))
    }

    async fn is_valid_index(&self, metadata: IndexMetadata) -> IsValidIndexR {
        let Ok(version_begin) = self.session.await_schema_agreement().await else {
            return false;
        };
        let cluster_state = self.session.get_cluster_state();

        // check a keyspace
        let Some(keyspace) = cluster_state.get_keyspace(metadata.keyspace_name.as_ref()) else {
            return false;
        };

        // check a table
        if keyspace.tables.contains_key(metadata.table_name.as_ref()) {
            return false;
        }

        // check a cdc log table
        if keyspace
            .tables
            .contains_key(&format!("{}_scylla_cdc_log", metadata.table_name))
        {
            return false;
        }

        // check if schema version changed
        let Ok(Some(version_end)) = self.session.check_schema_agreement().await else {
            return false;
        };
        version_begin == version_end
    }
}
