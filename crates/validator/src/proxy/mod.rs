/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod cdc;
mod db_timeout;
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

/// A cluster behind scylla-proxy, shared by every group under `proxy`.
///
/// It runs a single Vector Store node and no TLS, since the proxy works on
/// CQL frames and cannot carry TLS.
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

/// WARNING: name it in test arguments only, never in a group's
/// `fixtures = (...)`, so the proxy rules are reset after every test, even a
/// failed one.
type ProxyTestContext = TestEnv<ProxyCluster>;
