/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod scylla;

use crate::Config;
use ::scylla::client::session::Session;
use std::sync::Arc;
use uuid::Uuid;

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
}

pub fn new_scylla() -> impl DbDriver {
    scylla::new()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

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
    }
}
