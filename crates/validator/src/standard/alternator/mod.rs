/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use super::StandardCluster;
use crate::TestActors;
use crate::common::Cluster;
use std::sync::Arc;

mod batch_write_item;
mod create_table;
mod delete_item;
mod lwt;
mod put_item;
mod query;
mod ttl;
mod types;
mod update_item;
mod update_table;

e2etest::group!(name = alternator, fixtures = (), parent = super::standard);

/// Like `TestContext`, but without a keyspace: each Alternator table is its own.
pub(crate) struct AlternatorContext {
    cluster: Arc<StandardCluster>,
}

impl e2etest::Fixture for AlternatorContext {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let cluster = setup.setup::<StandardCluster>().await?;
        Some(Self { cluster })
    }

    async fn teardown(self) {}
}

impl AlternatorContext {
    pub fn actors(&self) -> &TestActors {
        self.cluster.actors()
    }
}
