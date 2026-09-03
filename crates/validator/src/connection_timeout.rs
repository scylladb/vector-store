/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common::*;
use e2etest_vector_store_cluster::VectorStoreClusterExt;
use std::sync::Arc;
use std::time::Duration;
use tap::Pipe;
use tracing::info;

const CONNECTION_TIMEOUT: &str = "5s";

e2etest::group!(
    name = connection_timeout,
    fixtures = (Fixture),
    parent = crate::validator
);

struct Fixture {
    actors: TestActors,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<crate::TestEnv>().await?.new_cluster().await;
        init_with_proxy_single_vs(&actors).await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }

    // The group owns its cluster, and the firewall rules it installs are
    // scoped to that cluster's addresses, so nothing it touches is shared with
    // its siblings. Its tests still run one at a time: they restart nodes and
    // cut off traffic for the whole cluster.
    fn group_can_run_concurrently() -> bool {
        true
    }

    // Its nodes claim loopback addresses, ports and routes, which every other
    // cluster claims too, so it runs in a network namespace of its own, where
    // it is alone in claiming them.
    fn group_needs_own_namespace() -> bool {
        true
    }
}

/// Test that the CQL connection timeout causes session creation to fail
/// when the database is unreachable, and that vector-store recovers once
/// connectivity is restored.
///
/// Steps:
/// - Start vector-store with proxy (normal connectivity).
/// - Stop vector-store.
/// - Block all traffic through the proxy (simulating unreachable DB).
/// - Restart vector-store with VECTOR_STORE_CQL_CONNECTION_TIMEOUT set.
/// - Verify that session-create-failure counter increments (timeout fires).
/// - Restore connectivity by allowing traffic through the proxy.
/// - Verify that vector-store eventually connects and becomes ready.
#[e2etest::test(group = connection_timeout)]
async fn connection_timeout_triggers_session_failure(fixture: Arc<Fixture>) {
    let actors = &fixture.actors;
    info!("started");

    info!("Stop vector-store");
    actors.vs.stop().await;

    info!("Block all traffic through the proxy");
    actors.drop_traffic(get_default_db_proxy_ips(actors)).await;

    info!("Restart vector-store with CQL connection timeout");
    actors
        .vs
        .start(get_proxy_vs_node_configs(actors).pipe(|mut nodes| {
            let translation_map = get_proxy_translation_map(actors);
            for node in nodes.iter_mut() {
                node.envs.insert(
                    "VECTOR_STORE_CQL_URI_TRANSLATION_MAP".to_string(),
                    serde_json::to_string(&translation_map).unwrap(),
                );
                node.envs.insert(
                    "VECTOR_STORE_CQL_CONNECTION_TIMEOUT".to_string(),
                    CONNECTION_TIMEOUT.to_string(),
                );
            }
            nodes.truncate(1);
            nodes
        }))
        .await;

    info!("Wait for VS HTTP to become reachable");
    let vs_ips = get_default_vs_ips(actors);
    let vs_addr = std::net::SocketAddr::from((vs_ips[0], VS_PORT));
    let client = httpclient::HttpClient::new(vs_addr);
    wait_for(
        || async { client.status().await.is_ok() },
        "VS HTTP endpoint must be reachable",
        Duration::from_secs(30),
    )
    .await;

    info!("Start tracking session-create-failure counter");
    client.internals_clear_counters().await.unwrap();
    client
        .internals_start_counter("session-create-failure".to_string())
        .await
        .unwrap();

    info!("Wait for session-create-failure counter to increment (connection timeout must fire)");
    wait_for(
        || async {
            if let Ok(counters) = client.internals_counters().await
                && let Some(counter) = counters.get("session-create-failure")
            {
                info!("session-create-failure counter: {counter}");
                return *counter > 0;
            }
            false
        },
        "session-create-failure counter must increment due to connection timeout",
        Duration::from_secs(30),
    )
    .await;

    info!("Start tracking session-create-success counter before restoring connectivity");
    client.internals_clear_counters().await.unwrap();
    client
        .internals_start_counter("session-create-success".to_string())
        .await
        .unwrap();

    info!("Restore connectivity");
    actors.turn_off_firewall_rules().await;

    info!("Wait for vector-store to reconnect successfully");
    wait_for(
        || async {
            if let Ok(counters) = client.internals_counters().await
                && let Some(counter) = counters.get("session-create-success")
            {
                info!("session-create-success counter: {counter}");
                return *counter > 0;
            }
            false
        },
        "session-create-success counter must increment after connectivity restored",
        Duration::from_secs(30),
    )
    .await;

    info!("finished");
}
