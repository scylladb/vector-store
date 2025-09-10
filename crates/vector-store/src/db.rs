/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Connectivity;
use crate::Credentials;
use crate::DbCustomIndex;
use crate::DbEmbedding;
use crate::Dimensions;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexMetadata;
use crate::IndexName;
use crate::IndexVersion;
use crate::KeyspaceName;
use crate::ScyllaDbUri;
use crate::SpaceType;
use crate::TableName;
use crate::db_index;
use crate::db_index::DbIndex;
use crate::node_state::Event;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use anyhow::Context;
use futures::TryStreamExt;
use regex::Regex;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimeuuid;
use secrecy::ExposeSecret;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;
use tracing::trace;
use tracing::warn;
use uuid::Uuid;

type GetDbIndexR = anyhow::Result<(mpsc::Sender<DbIndex>, mpsc::Receiver<DbEmbedding>)>;
pub(crate) type LatestSchemaVersionR = anyhow::Result<Option<CqlTimeuuid>>;
type GetIndexesR = anyhow::Result<Vec<DbCustomIndex>>;
type GetIndexVersionR = anyhow::Result<Option<IndexVersion>>;
type GetIndexTargetTypeR = anyhow::Result<Option<Dimensions>>;
type GetIndexParamsR =
    anyhow::Result<Option<(Connectivity, ExpansionAdd, ExpansionSearch, SpaceType)>>;
type IsValidIndexR = bool;

const RECONNECT_TIMEOUT: Duration = Duration::from_secs(1);

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
        table: TableName,
        index: IndexName,
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
        table: TableName,
        index: IndexName,
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

    async fn get_index_version(
        &self,
        keyspace: KeyspaceName,
        table: TableName,
        index: IndexName,
    ) -> GetIndexVersionR;

    async fn get_index_target_type(
        &self,
        keyspace: KeyspaceName,
        table: TableName,
        target_column: ColumnName,
    ) -> GetIndexTargetTypeR;

    async fn get_index_params(
        &self,
        keyspace: KeyspaceName,
        table: TableName,
        index: IndexName,
    ) -> GetIndexParamsR;

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
        table: TableName,
        index: IndexName,
    ) -> GetIndexVersionR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexVersion {
            keyspace,
            table,
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

    async fn get_index_params(
        &self,
        keyspace: KeyspaceName,
        table: TableName,
        index: IndexName,
    ) -> GetIndexParamsR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexParams {
            keyspace,
            table,
            index,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn is_valid_index(&self, metadata: IndexMetadata) -> IsValidIndexR {
        let (tx, rx) = oneshot::channel();
        self.send(Db::IsValidIndex { metadata, tx })
            .await
            .expect("DbExt::is_valid_index: internal actor should receive request");
        rx.await
            .expect("DbExt::is_valid_index: internal actor should send response")
    }
}

pub(crate) async fn new(
    uri: ScyllaDbUri,
    node_state: Sender<NodeState>,
    credentials: Option<Credentials>,
) -> anyhow::Result<mpsc::Sender<Db>> {
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(
        async move {
            node_state.send_event(Event::ConnectingToDb).await;
            let mut statements = Statements::new(uri.clone(), credentials.clone()).await;
            while statements.is_err() {
                tracing::error!(
                    "Failed to connect to ScyllaDB (error: {}) at {}, retrying in {}s",
                    statements.err().unwrap(),
                    uri.0,
                    RECONNECT_TIMEOUT.as_secs()
                );
                sleep(RECONNECT_TIMEOUT).await;
                statements = Statements::new(uri.clone(), credentials.clone()).await;
            }
            node_state.send_event(Event::ConnectedToDb).await;
            let statements = Arc::new(statements.unwrap());
            while let Some(msg) = rx.recv().await {
                tokio::spawn(process(statements.clone(), msg, node_state.clone()));
            }
        }
        .instrument(debug_span!("db")),
    );
    Ok(tx)
}

async fn process(statements: Arc<Statements>, msg: Db, node_state: Sender<NodeState>) {
    match msg {
        Db::GetDbIndex { metadata, tx } => tx
            .send(statements.get_db_index(metadata, node_state.clone()).await)
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
            table,
            index,
            tx,
        } => tx
            .send(statements.get_index_version(keyspace, table, index).await)
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
            .unwrap_or_else(|_| trace!("process: Db::GetIndexTargetType: unable to send response")),

        Db::GetIndexParams {
            keyspace,
            table,
            index,
            tx,
        } => tx
            .send(statements.get_index_params(keyspace, table, index).await)
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
    st_get_index_target_type: PreparedStatement,
    st_get_index_options: PreparedStatement,
    re_get_index_target_type: Regex,
}

impl Statements {
    async fn new(uri: ScyllaDbUri, credentials: Option<Credentials>) -> anyhow::Result<Self> {
        let mut builder = SessionBuilder::new().known_node(uri.0.as_str());
        if let Some(Credentials { username, password }) = credentials {
            builder = builder.user(username, password.expose_secret());
        }

        let session = Arc::new(builder.build().await?);

        let cluster_state = session.get_cluster_state();

        let node = &cluster_state.get_nodes_info()[0];

        if !node.is_enabled() {
            return Err(anyhow::anyhow!("Node is not enabled"));
        }
        // From docs: If the node is enabled and does not have a sharder, this means it's not a ScyllaDB node.
        let connected_to_scylla = node.sharder().is_some();

        if connected_to_scylla {
            let version: (String,) = session
                .query_unpaged(
                    "SELECT version FROM system.versions WHERE key = 'local'",
                    &[],
                )
                .await?
                .into_rows_result()?
                .single_row()?;
            info!("Connected to ScyllaDB {} at {}", version.0, uri.0);
        } else {
            warn!("No ScyllaDB node at {}, please verify the URI", uri.0);
        }

        Ok(Self {
            st_latest_schema_version: session
                .prepare(Self::ST_LATEST_SCHEMA_VERSION)
                .await
                .context("ST_LATEST_SCHEMA_VERSION")?,

            st_get_indexes: session
                .prepare(Self::ST_GET_INDEXES)
                .await
                .context("ST_GET_INDEXES")?,

            st_get_index_target_type: session
                .prepare(Self::ST_GET_INDEX_TARGET_TYPE)
                .await
                .context("ST_GET_INDEX_TARGET_TYPE")?,

            st_get_index_options: session
                .prepare(Self::ST_GET_INDEX_OPTIONS)
                .await
                .context("ST_GET_INDEX_OPTIONS")?,

            re_get_index_target_type: Regex::new(Self::RE_GET_INDEX_TARGET_TYPE)
                .context("RE_GET_INDEX_TARGET_TYPE")?,

            session,
        })
    }

    async fn get_db_index(
        &self,
        metadata: IndexMetadata,
        node_state: Sender<NodeState>,
    ) -> GetDbIndexR {
        db_index::new(Arc::clone(&self.session), metadata, node_state).await
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

    const ST_GET_INDEX_OPTIONS: &str = "
        SELECT options
        FROM system_schema.indexes
        WHERE keyspace_name = ? AND table_name = ? AND index_name = ?
        ";

    async fn get_index_version(
        &self,
        keyspace: KeyspaceName,
        table: TableName,
        index: IndexName,
    ) -> GetIndexVersionR {
        let options = self
            .session
            .execute_iter(self.st_get_index_options.clone(), (keyspace, table, index))
            .await?
            .rows_stream::<(BTreeMap<String, String>,)>()?
            .try_next()
            .await?
            .map(|(options,)| options);
        Ok(options.map(|mut options| {
            IndexVersion(
                options
                    .remove("version")
                    .and_then(|s| s.parse::<Uuid>().ok())
                    .unwrap_or_default(),
            )
        }))
    }

    async fn get_index_params(
        &self,
        keyspace: KeyspaceName,
        table: TableName,
        index: IndexName,
    ) -> GetIndexParamsR {
        let options = self
            .session
            .execute_iter(self.st_get_index_options.clone(), (keyspace, table, index))
            .await?
            .rows_stream::<(BTreeMap<String, String>,)>()?
            .try_next()
            .await?
            .map(|(options,)| options);
        Ok(options.map(|mut options| {
            let connectivity = options
                .remove("maximum_node_connections")
                .and_then(|s| s.parse::<usize>().ok())
                .map(Connectivity)
                .unwrap_or_default();
            let expansion_add = options
                .remove("construction_beam_width")
                .and_then(|s| s.parse::<usize>().ok())
                .map(ExpansionAdd)
                .unwrap_or_default();
            let expansion_search = options
                .remove("search_beam_width")
                .and_then(|s| s.parse::<usize>().ok())
                .map(ExpansionSearch)
                .unwrap_or_default();
            let space_type = options
                .remove("similarity_function")
                .and_then(|s| s.parse().ok())
                .unwrap_or_default();
            (connectivity, expansion_add, expansion_search, space_type)
        }))
    }

    async fn is_valid_index(&self, metadata: IndexMetadata) -> IsValidIndexR {
        let Ok(version_begin) = self.session.await_schema_agreement().await else {
            debug!("is_valid_index: schema not agreed for {}", metadata.id());
            return false;
        };
        let cluster_state = self.session.get_cluster_state();

        // check a keyspace
        let Some(keyspace) = cluster_state.get_keyspace(metadata.keyspace_name.as_ref()) else {
            debug!(
                "is_valid_index: no keyspace in a cluster state for {}",
                metadata.id()
            );
            // missing the keyspace in the cluster_state, metadata should be refreshed
            self.session.refresh_metadata().await.unwrap_or(());
            return false;
        };

        // check a table
        if !keyspace.tables.contains_key(metadata.table_name.as_ref()) {
            debug!("is_valid_index: no table for {}", metadata.id());
            // missing the table in the cluster_state, metadata should be refreshed
            self.session.refresh_metadata().await.unwrap_or(());
            return false;
        }

        // check a cdc log table
        if !keyspace
            .tables
            .contains_key(&format!("{}_scylla_cdc_log", metadata.table_name))
        {
            debug!("is_valid_index: no cdc log for {}", metadata.id());
            // missing the cdc log in the cluster_state, metadata should be refreshed
            self.session.refresh_metadata().await.unwrap_or(());
            return false;
        }

        // check if schema version changed
        let Ok(Some(version_end)) = self.session.check_schema_agreement().await else {
            debug!(
                "is_valid_index: schema not agreed for {} finally",
                metadata.id()
            );
            return false;
        };
        version_begin == version_end
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use mockall::automock;
    use tracing::debug;

    #[automock]
    pub(crate) trait SimDb {
        fn get_db_index(
            &self,
            metadata: IndexMetadata,
            tx: oneshot::Sender<GetDbIndexR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn latest_schema_version(
            &self,
            tx: oneshot::Sender<LatestSchemaVersionR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn get_indexes(
            &self,
            tx: oneshot::Sender<GetIndexesR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn get_index_version(
            &self,
            keyspace: KeyspaceName,
            table: TableName,
            index: IndexName,
            tx: oneshot::Sender<GetIndexVersionR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn get_index_target_type(
            &self,
            keyspace: KeyspaceName,
            table: TableName,
            target_column: ColumnName,
            tx: oneshot::Sender<GetIndexTargetTypeR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn get_index_params(
            &self,
            keyspace: KeyspaceName,
            table: TableName,
            index: IndexName,
            tx: oneshot::Sender<GetIndexParamsR>,
        ) -> impl Future<Output = ()> + Send + 'static;

        fn is_valid_index(
            &self,
            metadata: IndexMetadata,
            tx: oneshot::Sender<IsValidIndexR>,
        ) -> impl Future<Output = ()> + Send + 'static;
    }

    pub(crate) fn new(sim: impl SimDb + Send + 'static) -> mpsc::Sender<Db> {
        with_size(10, sim)
    }

    pub(crate) fn with_size(size: usize, sim: impl SimDb + Send + 'static) -> mpsc::Sender<Db> {
        let (tx, mut rx) = mpsc::channel(size);

        tokio::spawn(
            async move {
                debug!("starting");

                while let Some(msg) = rx.recv().await {
                    match msg {
                        Db::GetDbIndex { metadata, tx } => sim.get_db_index(metadata, tx).await,

                        Db::LatestSchemaVersion { tx } => sim.latest_schema_version(tx).await,

                        Db::GetIndexes { tx } => sim.get_indexes(tx).await,

                        Db::GetIndexVersion {
                            keyspace,
                            table,
                            index,
                            tx,
                        } => sim.get_index_version(keyspace, table, index, tx).await,

                        Db::GetIndexTargetType {
                            keyspace,
                            table,
                            target_column,
                            tx,
                        } => {
                            sim.get_index_target_type(keyspace, table, target_column, tx)
                                .await
                        }

                        Db::GetIndexParams {
                            keyspace,
                            table,
                            index,
                            tx,
                        } => sim.get_index_params(keyspace, table, index, tx).await,

                        Db::IsValidIndex { metadata, tx } => sim.is_valid_index(metadata, tx).await,
                    }
                }

                debug!("finished");
            }
            .instrument(debug_span!("engine-test")),
        );

        tx
    }
}
