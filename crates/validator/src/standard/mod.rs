/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

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
use crate::common::alternator::wait_for_alternator;
use crate::common::*;
use httpclient::HttpClient;
use scylla::client::session::Session;
use std::sync::Arc;

e2etest::group!(
    name = standard,
    fixtures = (StandardCluster),
    parent = crate::validator
);

/// The default cluster, shared by every group under `standard`.
///
/// Tests may change only their own schema. Anything that affects the whole
/// cluster, like restarting a node or granting a role, would break the other
/// groups running on it.
struct StandardCluster {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for StandardCluster {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        init(&actors).await;
        wait_for_alternator(actors.services_subnet.ip(DB_OCTET_1)).await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

impl Cluster for StandardCluster {
    fn actors(&self) -> &TestActors {
        &self.actors
    }

    async fn connect(actors: &TestActors) -> (Arc<Session>, Vec<HttpClient>) {
        prepare_connection(actors).await
    }
}

type TestContext = TestEnv<StandardCluster>;
