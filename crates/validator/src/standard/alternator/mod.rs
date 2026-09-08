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
mod put_item;
mod query;
mod ttl;
mod types;
mod update_item;
mod update_table;

// The groups needing a differently-configured cluster (`always_use_lwt`,
// enforce-authorization) cannot coexist with the standard one and live outside
// this namespace.
e2etest::group!(name = alternator, fixtures = (), parent = super::standard);

/// Alternator tests need no keyspace of their own — each table they create is
/// one — so this hands out the cluster handles and nothing else.
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
