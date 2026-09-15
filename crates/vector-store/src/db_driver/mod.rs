/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod scylla;

use crate::ColumnName;
use crate::Config;
use crate::IndexMetadata;
use crate::IndexName;
use crate::KeyspaceName;
use crate::PrimaryKey;
use crate::TableName;
use crate::Vector;
use crate::db_value::DbRow;
use ::scylla::client::session::Session;
use ::scylla::cluster::metadata::Column;
use ::scylla::routing::Token;
use futures::Stream;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use uuid::Uuid;

pub struct DbIndexInfo {
    pub keyspace_name: KeyspaceName,
    pub index_name: IndexName,
    pub table_name: TableName,
    pub options: BTreeMap<String, String>,
}

pub trait DbDriver: Clone + Send + Sync + 'static {
    type Statement: Send + Sync;
    type Cluster: Send + Sync;
    type Table;

    fn connect(
        &self,
        config: Arc<Config>,
    ) -> impl Future<Output = anyhow::Result<Arc<Session>>> + Send;

    fn prepare_latest_schema_version(
        &self,
        session: &Session,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_latest_schema_version(
        &self,
        session: &Session,
        statement: &Self::Statement,
    ) -> impl Future<Output = anyhow::Result<Uuid>> + Send;

    fn prepare_get_indexes(
        &self,
        session: &Session,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_get_indexes(
        &self,
        session: &Session,
        statement: &Self::Statement,
    ) -> impl Future<
        Output = anyhow::Result<impl Stream<Item = anyhow::Result<DbIndexInfo>> + Send + 'static>,
    > + Send;

    fn prepare_get_index_target_type(
        &self,
        session: &Session,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_get_index_target_type(
        &self,
        session: &Session,
        statement: &Self::Statement,
        keyspace: &KeyspaceName,
        table: &TableName,
        target: &ColumnName,
    ) -> impl Future<Output = anyhow::Result<Option<String>>> + Send;

    fn prepare_get_index_options(
        &self,
        session: &Session,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_get_index_options(
        &self,
        session: &Session,
        statement: &Self::Statement,
        keyspace: &KeyspaceName,
        table: &TableName,
        index: &IndexName,
    ) -> impl Future<Output = anyhow::Result<Option<BTreeMap<String, String>>>> + Send;

    fn prepare_range_scan(
        &self,
        session: &Session,
        index: &IndexMetadata,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_range_scan(
        &self,
        session: &Session,
        statement: &Self::Statement,
        begin: Token,
        end: Token,
    ) -> impl Future<
        Output = anyhow::Result<impl Stream<Item = anyhow::Result<DbRow>> + Send + 'static>,
    > + Send;

    fn prepare_fetch_vector(
        &self,
        session: &Session,
        index: &IndexMetadata,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_fetch_vector(
        &self,
        session: &Session,
        statement: &Self::Statement,
        primary_key: &PrimaryKey,
    ) -> impl Future<Output = anyhow::Result<Option<Vector>>> + Send;

    fn prepare_fetch_row(
        &self,
        session: &Session,
        index: &IndexMetadata,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_fetch_row(
        &self,
        session: &Session,
        statement: &Self::Statement,
        primary_key: &PrimaryKey,
    ) -> impl Future<Output = anyhow::Result<Option<DbRow>>> + Send;

    fn cluster(&self, session: &Session) -> Self::Cluster;

    fn is_keyspace(&self, cluster: &Self::Cluster, keyspace: &KeyspaceName) -> bool;

    fn is_table(&self, cluster: &Self::Cluster, keyspace: &KeyspaceName, table: &TableName)
    -> bool;

    fn is_cdc(&self, cluster: &Self::Cluster, keyspace: &KeyspaceName, table: &TableName) -> bool;

    fn nr_shards(&self, cluster: &Self::Cluster) -> NonZeroUsize;

    fn token_ring(&self, cluster: &Self::Cluster) -> impl Iterator<Item = Token>;

    fn table<'a>(
        &self,
        cluster: &'a Self::Cluster,
        keyspace: &KeyspaceName,
        table: &TableName,
    ) -> Option<&'a Self::Table>;

    fn partition_key(&self, table: &Self::Table) -> impl Iterator<Item = ColumnName>;

    fn clustering_key(&self, table: &Self::Table) -> impl Iterator<Item = ColumnName>;

    fn columns<'a>(&self, table: &'a Self::Table)
    -> impl Iterator<Item = (ColumnName, &'a Column)>;

    fn column<'a>(&self, table: &'a Self::Table, column: &ColumnName) -> Option<&'a Column>;
}

pub fn new_scylla() -> impl DbDriver {
    scylla::new()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use futures::stream;
    use std::iter;

    #[derive(Clone, Debug)]
    pub(crate) struct UnimplementedDbDriver;

    impl DbDriver for UnimplementedDbDriver {
        type Statement = ();
        type Cluster = ();
        type Table = ();
        async fn connect(&self, _: Arc<Config>) -> anyhow::Result<Arc<Session>> {
            unimplemented!()
        }

        async fn prepare_latest_schema_version(
            &self,
            _: &Session,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_latest_schema_version(
            &self,
            _: &Session,
            _: &Self::Statement,
        ) -> anyhow::Result<Uuid> {
            unimplemented!()
        }

        async fn prepare_get_indexes(&self, _: &Session) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_get_indexes(
            &self,
            _: &Session,
            _: &Self::Statement,
        ) -> anyhow::Result<impl Stream<Item = anyhow::Result<DbIndexInfo>> + Send + 'static>
        {
            Ok(stream::poll_fn(|_| unimplemented!()))
        }

        async fn prepare_get_index_target_type(
            &self,
            _: &Session,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_get_index_target_type(
            &self,
            _: &Session,
            _: &Self::Statement,
            _: &KeyspaceName,
            _: &TableName,
            _: &ColumnName,
        ) -> anyhow::Result<Option<String>> {
            unimplemented!()
        }

        async fn prepare_get_index_options(&self, _: &Session) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_get_index_options(
            &self,
            _: &Session,
            _: &Self::Statement,
            _: &KeyspaceName,
            _: &TableName,
            _: &IndexName,
        ) -> anyhow::Result<Option<BTreeMap<String, String>>> {
            unimplemented!()
        }

        async fn prepare_range_scan(
            &self,
            _: &Session,
            _: &IndexMetadata,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_range_scan(
            &self,
            _: &Session,
            _: &Self::Statement,
            _: Token,
            _: Token,
        ) -> anyhow::Result<impl Stream<Item = anyhow::Result<DbRow>> + Send + 'static> {
            Ok(stream::poll_fn(|_| unimplemented!()))
        }

        async fn prepare_fetch_vector(
            &self,
            _: &Session,
            _: &IndexMetadata,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_fetch_vector(
            &self,
            _: &Session,
            _: &Self::Statement,
            _: &PrimaryKey,
        ) -> anyhow::Result<Option<Vector>> {
            unimplemented!()
        }

        async fn prepare_fetch_row(
            &self,
            _: &Session,
            _: &IndexMetadata,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_fetch_row(
            &self,
            _: &Session,
            _: &Self::Statement,
            _: &PrimaryKey,
        ) -> anyhow::Result<Option<DbRow>> {
            unimplemented!()
        }

        fn cluster(&self, _: &Session) -> Self::Cluster {
            unimplemented!()
        }

        fn is_keyspace(&self, _: &Self::Cluster, _: &KeyspaceName) -> bool {
            unimplemented!()
        }

        fn is_table(&self, _: &Self::Cluster, _: &KeyspaceName, _: &TableName) -> bool {
            unimplemented!()
        }

        fn is_cdc(&self, _: &Self::Cluster, _: &KeyspaceName, _: &TableName) -> bool {
            unimplemented!()
        }

        fn nr_shards(&self, _: &Self::Cluster) -> NonZeroUsize {
            unimplemented!()
        }

        fn token_ring(&self, _: &Self::Cluster) -> impl Iterator<Item = Token> {
            iter::from_fn(|| unimplemented!())
        }

        fn table<'a>(
            &self,
            _: &'a Self::Cluster,
            _: &KeyspaceName,
            _: &TableName,
        ) -> Option<&'a Self::Table> {
            unimplemented!()
        }

        fn partition_key(&self, _: &Self::Table) -> impl Iterator<Item = ColumnName> {
            iter::from_fn(|| unimplemented!())
        }

        fn clustering_key(&self, _: &Self::Table) -> impl Iterator<Item = ColumnName> {
            iter::from_fn(|| unimplemented!())
        }

        fn columns<'a>(
            &self,
            _: &'a Self::Table,
        ) -> impl Iterator<Item = (ColumnName, &'a Column)> {
            iter::from_fn(|| unimplemented!())
        }

        fn column<'a>(&self, _: &'a Self::Table, _: &ColumnName) -> Option<&'a Column> {
            unimplemented!()
        }
    }
}
