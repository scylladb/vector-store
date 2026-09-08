/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Groups that cannot share a cluster and start one of their own.

mod alternator_auth;
mod alternator_lwt;
mod auth;
mod connection_timeout;
mod db_timeout;
mod high_availability;
mod reconnect;
mod tls_reload;

use crate::TestActors;
use crate::common::*;
use e2etest_firewall::FirewallExt;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use httpclient::HttpClient;
use scylla::client::session::Session;
use std::sync::Arc;

e2etest::group!(name = owned, fixtures = (), parent = crate::validator);

/// A proxy cluster of one group's own, for tests that go further than rules —
/// stopping nodes, cutting their traffic off, restarting Vector Store with a
/// different configuration. No sibling group could survive that.
struct OwnedProxyCluster {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for OwnedProxyCluster {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        init_with_proxy_single_vs(&actors).await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

impl Cluster for OwnedProxyCluster {
    fn actors(&self) -> &TestActors {
        &self.actors
    }

    async fn connect(actors: &TestActors) -> (Arc<Session>, Vec<HttpClient>) {
        prepare_connection_single_vs_no_tls(actors).await
    }

    async fn reset(actors: &TestActors) {
        actors.db_proxy.turn_off_rules().await;
        actors.firewall.turn_off_rules().await;
    }
}

type OwnedProxyContext = TestEnv<OwnedProxyCluster>;

/// Starts nothing: the tests whose subject is the cluster configuration build
/// the node configs themselves. Name it in test arguments only, so whatever a
/// test started is stopped before the next one runs.
struct CustomCluster {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for CustomCluster {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

impl CustomCluster {
    fn actors(&self) -> &TestActors {
        &self.actors
    }
}
