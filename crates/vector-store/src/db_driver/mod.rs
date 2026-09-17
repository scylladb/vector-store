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
use ::scylla::cluster::metadata::Column;
use ::scylla::routing::Token;
use futures::Stream;
use futures::future::RemoteHandle;
use scylla_cdc::checkpoints::CDCCheckpointSaver;
use scylla_cdc::consumer::ConsumerFactory;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub struct DbIndexInfo {
    pub keyspace_name: KeyspaceName,
    pub index_name: IndexName,
    pub table_name: TableName,
    pub options: BTreeMap<String, String>,
}

#[derive(Default)]
pub struct CdcLogReaderConfig {
    keyspace: Option<KeyspaceName>,
    table_name: Option<TableName>,
    consumer_factory: Option<Arc<dyn ConsumerFactory>>,
    start_timestamp: Option<Duration>,
    safety_interval: Option<Duration>,
    sleep_interval: Option<Duration>,
    should_save_progress: Option<bool>,
    checkpoint_saver: Option<Arc<dyn CDCCheckpointSaver>>,
}

pub trait DbDriver: Clone + Send + Sync + 'static {
    type Session: Clone + Send + Sync + 'static;
    type Statement: Send + Sync;
    type Cluster: Send + Sync;
    type Table;
    type CdcLogReader: Send;
    type Metrics: Send + Sync;

    fn connect(
        &self,
        config: Arc<Config>,
    ) -> impl Future<Output = anyhow::Result<Self::Session>> + Send;

    fn refresh_metadata(&self, session: &Self::Session) -> impl Future<Output = ()> + Send;

    fn await_schema_agreement(
        &self,
        session: &Self::Session,
    ) -> impl Future<Output = anyhow::Result<Uuid>> + Send;

    fn check_schema_agreement(
        &self,
        session: &Self::Session,
    ) -> impl Future<Output = anyhow::Result<Option<Uuid>>> + Send;

    fn prepare_latest_schema_version(
        &self,
        session: &Self::Session,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_latest_schema_version(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
    ) -> impl Future<Output = anyhow::Result<Uuid>> + Send;

    fn prepare_get_indexes(
        &self,
        session: &Self::Session,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_get_indexes(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
    ) -> impl Future<
        Output = anyhow::Result<impl Stream<Item = anyhow::Result<DbIndexInfo>> + Send + 'static>,
    > + Send;

    fn prepare_get_index_target_type(
        &self,
        session: &Self::Session,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_get_index_target_type(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        keyspace: &KeyspaceName,
        table: &TableName,
        target: &ColumnName,
    ) -> impl Future<Output = anyhow::Result<Option<String>>> + Send;

    fn prepare_get_index_options(
        &self,
        session: &Self::Session,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_get_index_options(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        keyspace: &KeyspaceName,
        table: &TableName,
        index: &IndexName,
    ) -> impl Future<Output = anyhow::Result<Option<BTreeMap<String, String>>>> + Send;

    fn prepare_range_scan(
        &self,
        session: &Self::Session,
        index: &IndexMetadata,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_range_scan(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        begin: Token,
        end: Token,
    ) -> impl Future<
        Output = anyhow::Result<impl Stream<Item = anyhow::Result<DbRow>> + Send + 'static>,
    > + Send;

    fn prepare_fetch_vector(
        &self,
        session: &Self::Session,
        index: &IndexMetadata,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_fetch_vector(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        primary_key: &PrimaryKey,
    ) -> impl Future<Output = anyhow::Result<Option<Vector>>> + Send;

    fn prepare_fetch_row(
        &self,
        session: &Self::Session,
        index: &IndexMetadata,
    ) -> impl Future<Output = anyhow::Result<Self::Statement>> + Send;

    fn execute_fetch_row(
        &self,
        session: &Self::Session,
        statement: &Self::Statement,
        primary_key: &PrimaryKey,
    ) -> impl Future<Output = anyhow::Result<Option<DbRow>>> + Send;

    fn cluster(&self, session: &Self::Session) -> Self::Cluster;

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

    fn cdc_log_reader(
        &self,
        session: Self::Session,
        config: CdcLogReaderConfig,
    ) -> impl Future<Output = anyhow::Result<(Self::CdcLogReader, RemoteHandle<anyhow::Result<()>>)>>
    + Send;

    fn stop_cdc_log_reader(&self, cdc_log_reader: Self::CdcLogReader);

    fn metrics(&self, session: &Self::Session) -> Self::Metrics;

    fn total_connections(&self, metrics: &Self::Metrics) -> u64;

    fn connection_timeouts(&self, metrics: &Self::Metrics) -> u64;
}

impl CdcLogReaderConfig {
    pub fn keyspace(mut self, keyspace: KeyspaceName) -> Self {
        self.keyspace = Some(keyspace);
        self
    }

    pub fn table_name(mut self, table_name: TableName) -> Self {
        self.table_name = Some(table_name);
        self
    }

    pub fn consumer_factory(mut self, consumer_factory: Arc<dyn ConsumerFactory>) -> Self {
        self.consumer_factory = Some(consumer_factory);
        self
    }

    pub fn start_timestamp(mut self, start_timestamp: Duration) -> Self {
        self.start_timestamp = Some(start_timestamp);
        self
    }

    pub fn safety_interval(mut self, safety_interval: Duration) -> Self {
        self.safety_interval = Some(safety_interval);
        self
    }

    pub fn sleep_interval(mut self, sleep_interval: Duration) -> Self {
        self.sleep_interval = Some(sleep_interval);
        self
    }

    pub fn should_save_progress(mut self, should_save_progress: bool) -> Self {
        self.should_save_progress = Some(should_save_progress);
        self
    }

    pub fn checkpoint_saver(mut self, checkpoint_saver: Arc<dyn CDCCheckpointSaver>) -> Self {
        self.checkpoint_saver = Some(checkpoint_saver);
        self
    }
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
        type Session = ();
        type Statement = ();
        type Cluster = ();
        type Table = ();
        type CdcLogReader = ();
        type Metrics = ();

        async fn connect(&self, _: Arc<Config>) -> anyhow::Result<Self::Session> {
            unimplemented!()
        }

        async fn refresh_metadata(&self, _: &Self::Session) {
            unimplemented!()
        }

        async fn await_schema_agreement(&self, _: &Self::Session) -> anyhow::Result<Uuid> {
            unimplemented!()
        }

        async fn check_schema_agreement(&self, _: &Self::Session) -> anyhow::Result<Option<Uuid>> {
            unimplemented!()
        }

        async fn prepare_latest_schema_version(
            &self,
            _: &Self::Session,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_latest_schema_version(
            &self,
            _: &Self::Session,
            _: &Self::Statement,
        ) -> anyhow::Result<Uuid> {
            unimplemented!()
        }

        async fn prepare_get_indexes(&self, _: &Self::Session) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_get_indexes(
            &self,
            _: &Self::Session,
            _: &Self::Statement,
        ) -> anyhow::Result<impl Stream<Item = anyhow::Result<DbIndexInfo>> + Send + 'static>
        {
            Ok(stream::poll_fn(|_| unimplemented!()))
        }

        async fn prepare_get_index_target_type(
            &self,
            _: &Self::Session,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_get_index_target_type(
            &self,
            _: &Self::Session,
            _: &Self::Statement,
            _: &KeyspaceName,
            _: &TableName,
            _: &ColumnName,
        ) -> anyhow::Result<Option<String>> {
            unimplemented!()
        }

        async fn prepare_get_index_options(
            &self,
            _: &Self::Session,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_get_index_options(
            &self,
            _: &Self::Session,
            _: &Self::Statement,
            _: &KeyspaceName,
            _: &TableName,
            _: &IndexName,
        ) -> anyhow::Result<Option<BTreeMap<String, String>>> {
            unimplemented!()
        }

        async fn prepare_range_scan(
            &self,
            _: &Self::Session,
            _: &IndexMetadata,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_range_scan(
            &self,
            _: &Self::Session,
            _: &Self::Statement,
            _: Token,
            _: Token,
        ) -> anyhow::Result<impl Stream<Item = anyhow::Result<DbRow>> + Send + 'static> {
            Ok(stream::poll_fn(|_| unimplemented!()))
        }

        async fn prepare_fetch_vector(
            &self,
            _: &Self::Session,
            _: &IndexMetadata,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_fetch_vector(
            &self,
            _: &Self::Session,
            _: &Self::Statement,
            _: &PrimaryKey,
        ) -> anyhow::Result<Option<Vector>> {
            unimplemented!()
        }

        async fn prepare_fetch_row(
            &self,
            _: &Self::Session,
            _: &IndexMetadata,
        ) -> anyhow::Result<Self::Statement> {
            unimplemented!()
        }

        async fn execute_fetch_row(
            &self,
            _: &Self::Session,
            _: &Self::Statement,
            _: &PrimaryKey,
        ) -> anyhow::Result<Option<DbRow>> {
            unimplemented!()
        }

        fn cluster(&self, _: &Self::Session) -> Self::Cluster {
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

        async fn cdc_log_reader(
            &self,
            _: Self::Session,
            _: CdcLogReaderConfig,
        ) -> anyhow::Result<(Self::CdcLogReader, RemoteHandle<anyhow::Result<()>>)> {
            unimplemented!()
        }

        fn stop_cdc_log_reader(&self, _: Self::CdcLogReader) {
            unimplemented!()
        }

        fn metrics(&self, _: &Self::Session) -> Self::Metrics {
            unimplemented!()
        }

        fn total_connections(&self, _: &Self::Metrics) -> u64 {
            unimplemented!()
        }

        fn connection_timeouts(&self, _: &Self::Metrics) -> u64 {
            unimplemented!()
        }
    }
}
