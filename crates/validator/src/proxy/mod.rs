/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Groups sharing one scylla-proxy cluster, so a run starts it once instead of
//! once per group.

mod cdc;
mod full_scan;
mod routing;

use crate::TestActors;
use crate::common::*;
use e2etest_firewall::FirewallExt;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use httpclient::HttpClient;
use scylla::client::session::Session;
use std::sync::Arc;

e2etest::group!(
    name = proxy,
    fixtures = (ProxyCluster),
    parent = crate::validator
);

/// scylla-proxy in front of the database with a single Vector Store node,
/// started once for the whole `proxy` umbrella group. The proxy works at CQL
/// frame level and cannot carry TLS, so this cluster runs without it.
struct ProxyCluster {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for ProxyCluster {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        init_with_proxy_single_vs(&actors).await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

impl Cluster for ProxyCluster {
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

/// Name it in test arguments only, never in a group's `fixtures = (...)`: the
/// rules are cluster-global, so every test must start from its own and leave
/// none behind, even after failing half way through.
type ProxyTestContext = TestEnv<ProxyCluster>;
