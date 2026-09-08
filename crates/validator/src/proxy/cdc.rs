/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::common::*;
use bytes::BytesMut;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::ResponseOpcode;
use scylla_proxy::ResponseReaction;
use scylla_proxy::ResponseRule;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = cdc_proxy,
    fixtures = (FixtureProxy),
    parent = crate::validator
);

struct FixtureProxy {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for FixtureProxy {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        init_with_proxy_single_vs(&actors).await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

/// Check that the CDC reader retries after encountering an error in the response from the
/// database.
///
/// Steps:
/// 1. Create a table and an index using a scylla proxy.
/// 2. Create an index and wait for it to be built.
/// 3. Setup the proxy to inject an artificial error into the CDC reader response.
/// 4. Insert a row with a marker in pk into the table.
/// 5. Wait for the CDC reader to stop and start again, indicating that it retried after the error.
/// 6. Remove the error injection and wait for the index to be built successfully.
/// 7. Drop the keyspace.
#[e2etest::test(group = cdc_proxy)]
async fn reader_retries_after_error(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().unwrap();

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk ASCII PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    let index_name = unique_index_name();
    let index_ident = format!("{keyspace}.{index_name}");

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, table.as_ref(), "v").index_name(index_name),
    )
    .await;
    wait_for_index(client, &index).await;

    const MARKER: &str = "test-cdc-retry";

    info!("Inject artificial errors into the CDC reader to test retry logic");
    actors
        .db_proxy
        .change_response_rules(Some(vec![ResponseRule(
            Condition::And(
                Box::new(Condition::ResponseOpcode(ResponseOpcode::Result)),
                Box::new(Condition::BodyContainsCaseSensitive(
                    MARKER.as_bytes().iter().copied().collect(),
                )),
            ),
            ResponseReaction::transform_frame(Arc::new(|mut input| {
                let marker = MARKER.as_bytes();
                let Some(offset) =
                    (0..input.body.len()).find(|i| input.body[*i..].starts_with(marker))
                else {
                    return input;
                };
                // Corrupt the marker in the frame body to trigger an error in the CDC reader
                let mut body = BytesMut::from(input.body);
                body[offset..offset + MARKER.len()].fill(0xff);
                input.body = body.freeze();
                input
            })),
        )]))
        .await;

    let wide_started = format!("{index_ident}-wide-cdc-handler-started");
    let wide_stopped = format!("{index_ident}-wide-cdc-handler-stopped");
    let fine_started = format!("{index_ident}-fine-cdc-handler-started");
    let fine_stopped = format!("{index_ident}-fine-cdc-handler-stopped");

    client.internals_clear_counters().await.unwrap();
    for name in [&wide_started, &wide_stopped, &fine_started, &fine_stopped] {
        client.internals_start_counter(name.clone()).await.unwrap();
    }

    let log_counters = async || {
        let counters = client.internals_counters().await.unwrap();
        info!(
            "counters: wide started={} stopped={}, fine started={} stopped={}",
            counters.get(&wide_started).copied().unwrap_or(0),
            counters.get(&wide_stopped).copied().unwrap_or(0),
            counters.get(&fine_started).copied().unwrap_or(0),
            counters.get(&fine_stopped).copied().unwrap_or(0),
        );
    };

    info!("Insert rows with marker");
    let pk = format!("{MARKER}-pk");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES ('{pk}', [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert data");

    for count in 1..=3 {
        log_counters().await;
        wait_for(
            || async {
                let counters = match client.internals_counters().await {
                    Ok(c) => c,
                    Err(_) => return false,
                };
                counters.get(&wide_stopped).copied().unwrap_or(0) >= count
                    && counters.get(&fine_stopped).copied().unwrap_or(0) >= count
            },
            format!("waiting for index readers to stop (iteration {count})"),
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await;

        log_counters().await;
        wait_for(
            || async {
                let counters = match client.internals_counters().await {
                    Ok(c) => c,
                    Err(_) => return false,
                };
                counters.get(&wide_started).copied().unwrap_or(0) >= count
                    && counters.get(&fine_started).copied().unwrap_or(0) >= count
            },
            format!("waiting for index readers to start (iteration {count})"),
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await;
    }

    log_counters().await;

    info!("Remove the error injection");
    actors.db_proxy.turn_off_rules().await;

    wait_for(
        || async {
            let status = client.index_status(&index.keyspace, &index.index).await;
            matches!(status, Ok(s) if s.count == 1)
        },
        "Waiting for all rows to be indexed after error injection is removed",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}
