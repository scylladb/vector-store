/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::ColumnName;
use crate::Config;
use crate::Credentials;
use crate::KeyspaceName;
use crate::TableName;
use crate::db_driver::DbDriver;
use crate::db_driver::DbIndexInfo;
use anyhow::Context;
use anyhow::anyhow;
use futures::Stream;
use futures::TryStreamExt;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use scylla::client::session::Session;
use scylla::client::session::TlsContext;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use secrecy::ExposeSecret;
use std::collections::BTreeMap;
use std::sync::Arc;
use tap::Pipe;
use tap::Tap;
use tracing::debug;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

#[derive(Clone, Debug)]
struct ScyllaDriver;

impl DbDriver for ScyllaDriver {
    type Statement = PreparedStatement;

    async fn connect(&self, config: Arc<Config>) -> anyhow::Result<Arc<Session>> {
        let mut builder = SessionBuilder::new()
            .known_node(&config.scylladb_uri)
            .pipe(|builder| {
                if let Some(interval) = config.cql_keepalive_interval {
                    info!("Setting CQL keepalive interval to {interval:?}");
                    builder.keepalive_interval(interval)
                } else {
                    builder
                }
            })
            .pipe(|builder| {
                if let Some(timeout) = config.cql_keepalive_timeout {
                    info!("Setting CQL keepalive timeout to {timeout:?}");
                    builder.keepalive_timeout(timeout)
                } else {
                    builder
                }
            })
            .pipe(|builder| {
                if let Some(interval) = config.cql_tcp_keepalive_interval {
                    info!("Setting CQL TCP keepalive interval to {interval:?}");
                    builder.tcp_keepalive_interval(interval)
                } else {
                    builder
                }
            })
            .pipe(|builder| {
                if let Some(translation_map) = config.cql_uri_translation_map.as_ref() {
                    info!("Setting CQL translation map to {translation_map:?}");
                    builder.address_translator(Arc::new(translation_map.clone()))
                } else {
                    builder
                }
            })
            .pipe(
                |builder| match (&config.cql_preferred_datacenter, &config.cql_preferred_rack) {
                    (Some(dc), Some(rack)) => {
                        info!("Setting preferred CQL datacenter/rack to {dc}/{rack}");
                        builder.prefer_datacenter_and_rack(dc.clone(), rack.clone())
                    }
                    (Some(dc), None) => {
                        info!("Setting preferred CQL datacenter to {dc}");
                        builder.prefer_datacenter(dc.clone())
                    }
                    (None, _) => builder,
                },
            );

        if let Some(Credentials {
            username,
            password,
            certificate_path,
        }) = &config.credentials
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

                let ca_der = CertificateDer::pem_slice_iter(&cert_pem)
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

        let session = if let Some(timeout) = config.cql_connection_timeout {
            info!("Setting CQL connection timeout to {timeout:?}");
            let session = tokio::time::timeout(timeout, builder.build())
                .await
                .map_err(|_| {
                    anyhow!(
                        "Connection to ScyllaDB at {} timed out after {timeout:?}",
                        config.scylladb_uri
                    )
                })??;
            Arc::new(session)
        } else {
            Arc::new(builder.build().await?)
        };

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
            info!(
                "Connected to ScyllaDB {} at {}",
                version.0, config.scylladb_uri
            );
        } else {
            warn!(
                "No ScyllaDB node at {}, please verify the URI",
                config.scylladb_uri
            );
        }
        Ok(session)
    }

    async fn prepare_latest_schema_version(
        &self,
        session: &Session,
    ) -> anyhow::Result<Self::Statement> {
        const QUERY: &str = "
            SELECT schema_version
            FROM system.local
            WHERE key='local'
        ";
        Ok(session
            .prepare(QUERY)
            .await
            .context(format!("query: {QUERY}"))?
            .tap_mut(|stmt| {
                // Use ONE consistency for schema version queries - this is a local query
                // that reads from system.local, so ONE is appropriate. During reading
                // indexes list we will check the schema agreement.
                stmt.set_consistency(Consistency::One);
                stmt.set_is_idempotent(true);
            }))
    }

    async fn execute_latest_schema_version(
        &self,
        session: &Session,
        statement: &Self::Statement,
    ) -> anyhow::Result<Uuid> {
        Ok(session
            .execute_unpaged(statement, &[])
            .await?
            .into_rows_result()?
            .single_row::<(Uuid,)>()?
            .0)
    }

    async fn prepare_get_indexes(&self, session: &Session) -> anyhow::Result<Self::Statement> {
        const QUERY: &str = "
            SELECT keyspace_name, index_name, table_name, options
            FROM system_schema.indexes
            WHERE kind = 'CUSTOM'
            ALLOW FILTERING
        ";
        session
            .prepare(QUERY)
            .await
            .context(format!("query: {QUERY}"))
    }

    async fn execute_get_indexes(
        &self,
        session: &Session,
        statement: &Self::Statement,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<DbIndexInfo>> + Send + 'static> {
        Ok(session
            .execute_iter(statement.clone(), &[])
            .await?
            .rows_stream::<(String, String, String, BTreeMap<String, String>)>()?
            .map_ok(
                |(keyspace_name, index_name, table_name, options)| DbIndexInfo {
                    keyspace_name: keyspace_name.into(),
                    index_name: index_name.into(),
                    table_name: table_name.into(),
                    options,
                },
            )
            .map_err(|err| anyhow::anyhow!("Failed to fetch indexes: {err}")))
    }

    async fn prepare_get_index_target_type(
        &self,
        session: &Session,
    ) -> anyhow::Result<Self::Statement> {
        const QUERY: &str = "
            SELECT type
            FROM system_schema.columns
            WHERE keyspace_name = ? AND table_name = ? AND column_name = ?
        ";
        session
            .prepare(QUERY)
            .await
            .context(format!("query: {QUERY}"))
    }

    async fn execute_get_index_target_type(
        &self,
        session: &Session,
        statement: &Self::Statement,
        keyspace: &KeyspaceName,
        table: &TableName,
        target: &ColumnName,
    ) -> anyhow::Result<Option<String>> {
        Ok(session
            .execute_iter(statement.clone(), (keyspace, table, target))
            .await?
            .rows_stream::<(String,)>()?
            .try_next()
            .await?
            .map(|(type_name,)| type_name))
    }
}

pub(super) fn new() -> impl DbDriver {
    ScyllaDriver
}
