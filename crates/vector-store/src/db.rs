/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Config;
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
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls_pemfile::certs;
use scylla::client::session::Session;
use scylla::client::session::TlsContext;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimeuuid;
use secrecy::ExposeSecret;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
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
    mut config_rx: Option<watch::Receiver<Arc<Config>>>,
) -> anyhow::Result<mpsc::Sender<Db>> {
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(
        async move {
            // Use shared state for URI and credentials so they can be updated by config monitoring
            let current_uri = Arc::new(RwLock::new(uri.0.clone()));
            let current_credentials = Arc::new(RwLock::new(credentials.clone()));

            // Initial connection with current URI and credentials
            node_state.send_event(Event::ConnectingToDb).await;
            let uri_str = current_uri.read().await.clone();
            let creds = current_credentials.read().await.clone();
            let mut statements = Statements::new(ScyllaDbUri(uri_str.clone()), creds.clone()).await;

            // Retry loop that's responsive to config changes
            while statements.is_err() {
                let uri_str = current_uri.read().await.clone();
                tracing::error!(
                    "Failed to connect to ScyllaDB (error: {}) at {}, retrying in {}s",
                    statements.err().unwrap(),
                    uri_str,
                    RECONNECT_TIMEOUT.as_secs()
                );

                // Use select to be responsive to config changes during reconnection attempts
                if let Some(ref mut rx) = config_rx {
                    tokio::select! {
                        _ = sleep(RECONNECT_TIMEOUT) => {},
                        result = rx.changed() => {
                            if result.is_ok() {
                                let config = rx.borrow_and_update().clone();
                                let old_uri = current_uri.read().await.clone();

                                // Update URI if changed
                                if config.scylladb_uri != old_uri {
                                    info!("Config changed during initial connection: ScyllaDB URI changed from {} to {}",
                                          old_uri, config.scylladb_uri);
                                    *current_uri.write().await = config.scylladb_uri.clone();
                                }

                                // Update credentials if changed
                                let old_creds = current_credentials.read().await.clone();
                                let creds_changed = match (&config.credentials, &old_creds) {
                                    (None, None) => false,
                                    (Some(_), None) | (None, Some(_)) => true,
                                    (Some(new_creds), Some(old_creds)) => {
                                        new_creds.username != old_creds.username
                                            || new_creds.certificate_path != old_creds.certificate_path
                                            || match (&new_creds.password, &old_creds.password) {
                                                (None, None) => false,
                                                (Some(_), None) | (None, Some(_)) => true,
                                                (Some(new_pass), Some(old_pass)) => {
                                                    new_pass.expose_secret() != old_pass.expose_secret()
                                                }
                                            }
                                    }
                                };

                                if creds_changed {
                                    info!("Config changed during initial connection: ScyllaDB credentials changed");
                                    *current_credentials.write().await = config.credentials.clone();
                                }
                            }
                        }
                    }
                } else {
                    sleep(RECONNECT_TIMEOUT).await;
                }

                let uri_str = current_uri.read().await.clone();
                let creds = current_credentials.read().await.clone();
                statements = Statements::new(ScyllaDbUri(uri_str), creds).await;
            }
            node_state.send_event(Event::ConnectedToDb).await;

            // Use RwLock to share statements between the main loop and config monitoring task
            let statements = Arc::new(RwLock::new(Arc::new(statements.unwrap())));

            // Spawn config monitoring task if config_rx is provided
            if let Some(mut config_rx) = config_rx {
                let statements_clone = statements.clone();
                let node_state_clone = node_state.clone();
                let current_uri_clone = current_uri.clone();
                let current_credentials_clone = current_credentials.clone();

                tokio::spawn(async move {
                    while config_rx.changed().await.is_ok() {
                        let config = config_rx.borrow_and_update().clone();

                        let old_uri = current_uri_clone.read().await.clone();
                        let old_credentials = current_credentials_clone.read().await.clone();

                        // Check if credentials or URI changed
                        let uri_changed = config.scylladb_uri != old_uri;
                        let credentials_changed = match (&config.credentials, &old_credentials) {
                            (None, None) => false,
                            (Some(_), None) | (None, Some(_)) => true,
                            (Some(new_creds), Some(old_creds)) => {
                                new_creds.username != old_creds.username
                                    || new_creds.certificate_path != old_creds.certificate_path
                                    // SecretString doesn't implement PartialEq, so we compare exposed secrets
                                    || match (&new_creds.password, &old_creds.password) {
                                        (None, None) => false,
                                        (Some(_), None) | (None, Some(_)) => true,
                                        (Some(new_pass), Some(old_pass)) => {
                                            new_pass.expose_secret() != old_pass.expose_secret()
                                        }
                                    }
                            }
                        };

                        if uri_changed || credentials_changed {
                            if uri_changed {
                                warn!("ScyllaDB URI changed from {} to {}, reconnecting...",
                                      old_uri, config.scylladb_uri);
                                *current_uri_clone.write().await = config.scylladb_uri.clone();
                            }
                            if credentials_changed {
                                warn!("ScyllaDB credentials changed, reconnecting...");
                                *current_credentials_clone.write().await = config.credentials.clone();
                            }

                            // Attempt reconnection with new config
                            node_state_clone.send_event(Event::ConnectingToDb).await;
                            let new_uri_str = current_uri_clone.read().await.clone();
                            let new_creds = current_credentials_clone.read().await.clone();
                            let new_uri = ScyllaDbUri(new_uri_str.clone());
                            let mut new_statements = Statements::new(new_uri, new_creds.clone()).await;

                            while new_statements.is_err() {
                                tracing::error!(
                                    "Failed to reconnect to ScyllaDB (error: {}) at {}, retrying in {}s",
                                    new_statements.err().unwrap(),
                                    new_uri_str,
                                    RECONNECT_TIMEOUT.as_secs()
                                );
                                sleep(RECONNECT_TIMEOUT).await;
                                let retry_uri_str = current_uri_clone.read().await.clone();
                                let retry_creds = current_credentials_clone.read().await.clone();
                                let retry_uri = ScyllaDbUri(retry_uri_str);
                                new_statements = Statements::new(retry_uri, retry_creds).await;
                            }

                            // Update the shared statements
                            *statements_clone.write().await = Arc::new(new_statements.unwrap());
                            node_state_clone.send_event(Event::ConnectedToDb).await;
                            info!("Successfully reconnected to ScyllaDB");
                        }
                    }
                });
            }

            while let Some(msg) = rx.recv().await {
                let current_statements = statements.read().await.clone();
                tokio::spawn(process(current_statements, msg, node_state.clone()));
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

        if let Some(Credentials {
            username,
            password,
            certificate_path,
        }) = credentials
        {
            // Configure username/password authentication if provided
            if let (Some(username), Some(password)) = (username, password) {
                builder = builder.user(username, password.expose_secret());
                debug!("Username/password authentication configured");
            }

            // Configure TLS if certificate path is provided
            if let Some(cert_path) = certificate_path {
                // Load the CA certificates from the PEM file using async tokio fs
                let cert_pem = tokio::fs::read(&cert_path)
                    .await
                    .with_context(|| format!("Failed to read certificate file at {cert_path:?}"))?;

                let mut reader = Cursor::new(cert_pem);
                let ca_der: Vec<CertificateDer<'static>> = certs(&mut reader)
                    .collect::<Result<Vec<_>, _>>()
                    .context("Failed to parse certificate PEM")?;

                let mut root_store = RootCertStore::empty();
                root_store.add_parsable_certificates(ca_der);

                let client_cfg = ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();

                let tls_context = TlsContext::from(Arc::new(client_cfg));
                builder = builder.tls_context(Some(tls_context));

                debug!("TLS (rustls) enabled with certificate from {:?}", cert_path);
            }
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
                    .remove("index_version")
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
                .and_then(|s| s.to_ascii_uppercase().parse().ok())
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
