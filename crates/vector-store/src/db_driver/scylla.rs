/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::ColumnName;
use crate::Config;
use crate::CqlLiteral;
use crate::Credentials;
use crate::IndexMetadata;
use crate::IndexName;
use crate::KeyspaceIdentifier;
use crate::KeyspaceName;
use crate::PrimaryKey;
use crate::TableIdentifier;
use crate::TableName;
use crate::Vector;
use crate::db_driver::DbDriver;
use crate::db_driver::DbIndexInfo;
use crate::db_index::NonRetryable;
use crate::db_value::DbRow;
use anyhow::Context;
use anyhow::anyhow;
use futures::Stream;
use futures::TryStreamExt;
use itertools::Itertools;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use scylla::client::session::Session;
use scylla::client::session::TlsContext;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::ClusterState;
use scylla::cluster::metadata::Column;
use scylla::cluster::metadata::Table;
use scylla::routing::Token;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use scylla_cdc::CqlIdentifier;
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
    type Cluster = Arc<ClusterState>;
    type Table = Table;

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

    async fn prepare_get_index_options(
        &self,
        session: &Session,
    ) -> anyhow::Result<Self::Statement> {
        const QUERY: &str = "
            SELECT options
            FROM system_schema.indexes
            WHERE keyspace_name = ? AND table_name = ? AND index_name = ?
        ";
        session
            .prepare(QUERY)
            .await
            .context(format!("query: {QUERY}"))
    }

    async fn execute_get_index_options(
        &self,
        session: &Session,
        statement: &Self::Statement,
        keyspace: &KeyspaceName,
        table: &TableName,
        index: &IndexName,
    ) -> anyhow::Result<Option<BTreeMap<String, String>>> {
        Ok(session
            .execute_iter(statement.clone(), (keyspace, table, index))
            .await?
            .rows_stream::<(BTreeMap<String, String>,)>()?
            .try_next()
            .await?
            .map(|(options,)| options))
    }

    async fn prepare_range_scan(
        &self,
        session: &Session,
        index: &IndexMetadata,
    ) -> anyhow::Result<Self::Statement> {
        let st_partition_key_list = index
            .primary_key_columns
            .iter()
            .take(index.partition_key_count.get())
            .map(|c| CqlIdentifier::new(c.as_ref()))
            .join(", ");
        let st_primary_key_list = index
            .primary_key_columns
            .iter()
            .map(|c| CqlIdentifier::new(c.as_ref()))
            .join(", ");
        let keyspace_identifier = KeyspaceIdentifier::from(&index.keyspace_name);
        let table_identifier = TableIdentifier::from(&index.table_name);
        let query = range_scan_query(
            &keyspace_identifier,
            &table_identifier,
            index
                .target_columns
                .iter()
                .chain(index.nonpk_partition_key_columns().into_iter().flatten())
                .chain(index.nonpk_filtering_columns()),
            &st_primary_key_list,
            &st_partition_key_list,
        );
        Ok(session
            .prepare(query.as_str())
            .await
            .context(format!("query: {query}"))?
            .pipe(|mut stmt| {
                stmt.set_is_idempotent(true);
                stmt
            }))
    }

    async fn execute_range_scan(
        &self,
        session: &Session,
        statement: &Self::Statement,
        begin: Token,
        end: Token,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<DbRow>> + Send + 'static> {
        Ok(session
            .execute_iter(statement.clone(), (begin.value(), end.value()))
            .await?
            .rows_stream::<DbRow>()
            .context(NonRetryable)?
            .map_err(anyhow::Error::from))
    }

    async fn prepare_fetch_vector(
        &self,
        session: &Session,
        index: &IndexMetadata,
    ) -> anyhow::Result<Self::Statement> {
        let keyspace_identifier = KeyspaceIdentifier::from(&index.keyspace_name);
        let table_identifier = TableIdentifier::from(&index.table_name);
        let query = fetch_vector_query(
            &keyspace_identifier,
            &table_identifier,
            index.target_columns.iter(),
            index.primary_key_columns.iter(),
        );
        Ok(session
            .prepare(query.as_str())
            .await
            .context(format!("query: {query}"))?
            .pipe(|mut stmt| {
                stmt.set_is_idempotent(true);
                stmt
            }))
    }

    async fn execute_fetch_vector(
        &self,
        session: &Session,
        statement: &Self::Statement,
        primary_key: &PrimaryKey,
    ) -> anyhow::Result<Option<Vector>> {
        Ok(session
            .execute_unpaged(statement, primary_key)
            .await?
            .into_rows_result()?
            .maybe_first_row::<(Option<Vector>,)>()?
            .and_then(|(vector,)| vector))
    }

    async fn prepare_fetch_row(
        &self,
        session: &Session,
        index: &IndexMetadata,
    ) -> anyhow::Result<Self::Statement> {
        let keyspace_identifier = KeyspaceIdentifier::from(&index.keyspace_name);
        let table_identifier = TableIdentifier::from(&index.table_name);
        let query = request_query(
            &keyspace_identifier,
            &table_identifier,
            index
                .target_columns
                .iter()
                .chain(index.nonpk_partition_key_columns().into_iter().flatten())
                .chain(index.nonpk_filtering_columns()),
            index.primary_key_columns.iter(),
        );
        Ok(session
            .prepare(query.as_str())
            .await
            .context(format!("query: {query}"))?
            .pipe(|mut stmt| {
                stmt.set_is_idempotent(true);
                stmt
            }))
    }

    async fn execute_fetch_row(
        &self,
        session: &Session,
        statement: &Self::Statement,
        primary_key: &PrimaryKey,
    ) -> anyhow::Result<Option<DbRow>> {
        Ok(session
            .execute_unpaged(statement, primary_key)
            .await?
            .into_rows_result()?
            .maybe_first_row::<DbRow>()?)
    }

    fn cluster(&self, session: &Session) -> Self::Cluster {
        session.get_cluster_state()
    }

    fn is_keyspace(&self, cluster: &Self::Cluster, keyspace: &KeyspaceName) -> bool {
        cluster.get_keyspace(keyspace.as_ref()).is_some()
    }

    fn is_table(
        &self,
        cluster: &Self::Cluster,
        keyspace: &KeyspaceName,
        table: &TableName,
    ) -> bool {
        cluster
            .get_keyspace(keyspace.as_ref())
            .is_some_and(|ks| ks.tables.contains_key(table.as_ref()))
    }

    fn is_cdc(&self, cluster: &Self::Cluster, keyspace: &KeyspaceName, table: &TableName) -> bool {
        cluster
            .get_keyspace(keyspace.as_ref())
            .is_some_and(|ks| ks.tables.contains_key(&format!("{table}_scylla_cdc_log")))
    }

    fn table<'a>(
        &self,
        cluster: &'a Self::Cluster,
        keyspace: &KeyspaceName,
        table: &TableName,
    ) -> Option<&'a Self::Table> {
        cluster
            .get_keyspace(keyspace.as_ref())
            .and_then(|ks| ks.tables.get(table.as_ref()))
    }

    fn partition_key(&self, table: &Self::Table) -> impl Iterator<Item = ColumnName> {
        table.partition_key.iter().map(ColumnName::from)
    }

    fn clustering_key(&self, table: &Self::Table) -> impl Iterator<Item = ColumnName> {
        table.clustering_key.iter().map(ColumnName::from)
    }

    fn column<'a>(&self, table: &'a Self::Table, column: &ColumnName) -> Option<&'a Column> {
        table.columns.get(column.as_ref())
    }
}

pub(super) fn new() -> impl DbDriver {
    ScyllaDriver
}

/// Alternator tables store all user attributes in a single map column `:attrs`.
/// Because Alternator (DynamoDB-compatible) is schemaless, different items can
/// have different types for the same attribute name, so a map is used instead
/// of dedicated typed columns.
///
/// Current Alternator schema stores this as map<utf8, bytes>: attribute names
/// are text keys and attribute values are serialized blobs.
const ALTERNATOR_ATTRS_COLUMN: &str = ":attrs";

fn column_accessors<'a>(
    keyspace: &KeyspaceIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
) -> impl Iterator<Item = String> {
    let attributes = keyspace
        .is_alternator()
        .then(|| CqlIdentifier::new(ALTERNATOR_ATTRS_COLUMN));
    columns.into_iter().map(move |column| match &attributes {
        Some(attributes) => format!("{attributes}[{}]", CqlLiteral::new(column)),
        None => CqlIdentifier::new(column.as_ref()).to_string(),
    })
}

fn build_columns_list<'a>(
    keyspace: &KeyspaceIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
) -> String {
    itertools::join(
        column_accessors(keyspace, columns).flat_map(|column| {
            let writetime = format!("writetime({column})");
            [column, writetime]
        }),
        ", ",
    )
}

/// Builds the CQL range scan query appropriate for the given keyspace.
///
/// For CQL-native tables, selects the vector column directly.
/// For Alternator tables, selects from the `:attrs` map column.
pub(crate) fn range_scan_query<'a>(
    keyspace: &KeyspaceIdentifier,
    table: &TableIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
    primary_key_list: &str,
    partition_key_list: &str,
) -> String {
    let columns = build_columns_list(keyspace, columns);
    format!(
        "
        SELECT {primary_key_list}, {columns}
        FROM {keyspace}.{table}
        WHERE
            token({partition_key_list}) >= ?
            AND token({partition_key_list}) <= ?
        BYPASS CACHE
        "
    )
}

/// Builds the CQL request query appropriate for the given keyspace.
///
/// For CQL-native tables, selects the vector column directly.
/// For Alternator tables, selects from the `:attrs` map column.
pub(crate) fn request_query<'a, 'b>(
    keyspace: &KeyspaceIdentifier,
    table: &TableIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
    primary_key_columns: impl IntoIterator<Item = &'b ColumnName>,
) -> String {
    let columns = build_columns_list(keyspace, columns);
    let restrictions = itertools::join(
        primary_key_columns
            .into_iter()
            .map(AsRef::as_ref)
            .map(CqlIdentifier::new)
            .map(|column| format!("{column} = ?")),
        " AND ",
    );
    format!(
        "
            SELECT {columns}
            FROM {keyspace}.{table}
            WHERE {restrictions}
            "
    )
}

/// Builds the CQL query fetching the values of the given columns for a single
/// row, appropriate for the given keyspace.
pub(crate) fn fetch_vector_query<'a, 'b>(
    keyspace: &KeyspaceIdentifier,
    table: &TableIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
    primary_key_columns: impl IntoIterator<Item = &'b ColumnName>,
) -> String {
    let columns = itertools::join(column_accessors(keyspace, columns), ", ");
    let restrictions = itertools::join(
        primary_key_columns
            .into_iter()
            .map(AsRef::as_ref)
            .map(CqlIdentifier::new)
            .map(|column| format!("{column} = ?")),
        " AND ",
    );
    format!(
        "
            SELECT {columns}
            FROM {keyspace}.{table}
            WHERE {restrictions}
            "
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_scan_query_quotes_lowercase_identifiers() {
        let query = range_scan_query(
            &KeyspaceIdentifier::from("ks"),
            &TableIdentifier::from("tbl"),
            &[ColumnName::from("embedding")],
            &CqlIdentifier::new("id").to_string(),
            &CqlIdentifier::new("id").to_string(),
        );
        assert!(query.contains(r#""embedding""#));
        assert!(query.contains(r#"FROM "ks"."tbl""#));
        assert!(query.contains(r#"token("id")"#));
    }

    #[test]
    fn range_scan_query_quotes_mixed_case_identifiers() {
        let pk_list = [
            CqlIdentifier::new("UserId"),
            CqlIdentifier::new("CreatedAt"),
        ]
        .iter()
        .join(", ");
        let query = range_scan_query(
            &KeyspaceIdentifier::from("MyKeyspace"),
            &TableIdentifier::from("MyTable"),
            &[ColumnName::from("EmbeddingCol")],
            &pk_list,
            &CqlIdentifier::new("UserId").to_string(),
        );
        assert!(
            query.contains(r#""EmbeddingCol""#),
            "mixed-case embedding column must be quoted"
        );
        assert!(
            query.contains(r#"FROM "MyKeyspace"."MyTable""#),
            "mixed-case keyspace/table must be quoted"
        );
        assert!(
            query.contains(r#""UserId", "CreatedAt""#),
            "mixed-case primary key columns must be quoted"
        );
    }

    #[test]
    fn range_scan_query_quotes_uppercase_identifiers() {
        let query = range_scan_query(
            &KeyspaceIdentifier::from("UPPER_KS"),
            &TableIdentifier::from("UPPER_TBL"),
            &[ColumnName::from("VEC")],
            &CqlIdentifier::new("ID").to_string(),
            &CqlIdentifier::new("ID").to_string(),
        );
        assert!(
            query.contains(r#""VEC""#),
            "uppercase embedding column must be quoted"
        );
        assert!(
            query.contains(r#"FROM "UPPER_KS"."UPPER_TBL""#),
            "uppercase keyspace/table must be quoted"
        );
    }

    #[test]
    fn range_scan_query_quotes_special_character_identifiers() {
        let pk_list = [CqlIdentifier::new(":pk"), CqlIdentifier::new(":sk")]
            .iter()
            .join(", ");
        let query = range_scan_query(
            &KeyspaceIdentifier::from("my-app"),
            &TableIdentifier::from("my-table:v1"),
            &[ColumnName::from("my-vector")],
            &pk_list,
            &CqlIdentifier::new(":pk").to_string(),
        );
        assert!(
            query.contains(r#""my-vector""#),
            "hyphenated embedding column must be quoted"
        );
        assert!(
            query.contains(r#"FROM "my-app"."my-table:v1""#),
            "special-character keyspace/table must be quoted"
        );
        assert!(
            query.contains(r#"token(":pk")"#),
            "special-character partition key must be quoted"
        );
    }

    #[test]
    fn alternator_range_scan_query_basic() {
        let pk_list = [CqlIdentifier::new(":pk"), CqlIdentifier::new(":sk")]
            .iter()
            .join(", ");
        let query = range_scan_query(
            &KeyspaceIdentifier::from("alternator_my-app"),
            &TableIdentifier::from("my-table"),
            &[ColumnName::from("v")],
            &pk_list,
            &CqlIdentifier::new(":pk").to_string(),
        );
        assert!(
            query.contains(r#"":attrs"['v']"#),
            "attribute name must be single-quoted inside :attrs map access: {query}"
        );
        assert!(
            query.contains(r#"writetime(":attrs"['v'])"#),
            "writetime must wrap the same :attrs map access: {query}"
        );
        assert!(
            query.contains(r#"FROM "alternator_my-app"."my-table""#),
            "keyspace and table must be double-quoted: {query}"
        );
        assert!(
            query.contains(r#"token(":pk")"#),
            "partition key must be double-quoted: {query}"
        );
    }

    #[test]
    fn alternator_range_scan_query_special_attribute_name() {
        let pk_list = CqlIdentifier::new(":pk").to_string();
        let query = range_scan_query(
            &KeyspaceIdentifier::from("alternator_ks"),
            &TableIdentifier::from("tbl"),
            &[ColumnName::from("my-vector:v1")],
            &pk_list,
            &pk_list,
        );
        assert!(
            query.contains(r#"":attrs"['my-vector:v1']"#),
            "special characters in attribute name must appear verbatim inside single quotes: {query}"
        );
        assert!(
            query.contains(r#"writetime(":attrs"['my-vector:v1'])"#),
            "writetime must use the same single-quoted attribute access: {query}"
        );
    }

    #[test]
    fn alternator_range_scan_query_mixed_case_attribute() {
        let pk_list = CqlIdentifier::new("pk").to_string();
        let query = range_scan_query(
            &KeyspaceIdentifier::from("alternator_Ks"),
            &TableIdentifier::from("Tbl"),
            &[ColumnName::from("EmbeddingCol")],
            &pk_list,
            &pk_list,
        );
        assert!(
            query.contains(r#"":attrs"['EmbeddingCol']"#),
            "mixed-case attribute name must be preserved as-is inside single quotes: {query}"
        );
        assert!(
            query.contains(r#"FROM "alternator_Ks"."Tbl""#),
            "mixed-case keyspace/table must be double-quoted: {query}"
        );
    }

    #[test]
    fn alternator_range_scan_query_attribute_with_quotes() {
        let pk_list = CqlIdentifier::new(":pk").to_string();
        let query = range_scan_query(
            &KeyspaceIdentifier::from("alternator_ks"),
            &TableIdentifier::from("tbl"),
            &[ColumnName::from("it's a \"test\"")],
            &pk_list,
            &pk_list,
        );
        assert!(
            query.contains(r#"":attrs"['it''s a "test"']"#),
            "single quotes in attribute name must be escaped by doubling: {query}"
        );
        assert!(
            query.contains(r#"writetime(":attrs"['it''s a "test"'])"#),
            "writetime must use the same escaped attribute access: {query}"
        );
    }
}
