/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod scylla;

use crate::ColumnName;
use crate::Config;
use crate::IndexName;
use crate::KeyspaceName;
use crate::TableName;
use ::scylla::client::session::Session;
use futures::Stream;
use std::collections::BTreeMap;
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
}

pub fn new_scylla() -> impl DbDriver {
    scylla::new()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use futures::stream;

    #[derive(Clone, Debug)]
    pub(crate) struct UnimplementedDbDriver;

    impl DbDriver for UnimplementedDbDriver {
        type Statement = ();
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
    }
}
