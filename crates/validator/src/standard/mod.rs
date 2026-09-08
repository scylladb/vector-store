/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Groups sharing the default cluster, so a run starts it once instead of once
//! per group.

mod alternator;
mod ann;
mod cdc;
mod coexisting_indexes;
mod crud;
mod filtering;
mod fts;
mod index_create;
mod index_modify;
mod index_status;
mod quantization_and_rescoring;
mod serde;
mod similarity_functions;

use crate::TestActors;
use crate::common::*;
use std::sync::Arc;

e2etest::group!(
    name = standard,
    fixtures = (StandardCluster),
    parent = crate::validator
);

/// The default cluster, started once for the whole `standard` umbrella group.
///
/// Its tests must confine themselves to their own schema: it is shared, so
/// restarting a node, installing rules, editing DNS or granting roles would
/// break whatever sibling group runs next to them.
struct StandardCluster {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for StandardCluster {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        init(&actors).await;
        // The Alternator port opens after the CQL one, which is all
        // `wait_for_ready` checks.
        crate::common::alternator::wait_for_alternator(actors.services_subnet.ip(DB_OCTET_1)).await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}
