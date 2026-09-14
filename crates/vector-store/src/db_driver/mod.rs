/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod scylla;

use crate::Config;
use ::scylla::client::session::Session;
use std::sync::Arc;

pub trait DbDriver: Clone + Send + Sync + 'static {
    fn connect(
        &self,
        config: Arc<Config>,
    ) -> impl Future<Output = anyhow::Result<Arc<Session>>> + Send;
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
        async fn connect(&self, _: Arc<Config>) -> anyhow::Result<Arc<Session>> {
            unimplemented!()
        }
    }
}
