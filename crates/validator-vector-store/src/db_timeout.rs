/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::ResponseReaction;
use scylla_proxy::ResponseRule;
use std::time::Duration;
use tokio::time;
use tracing::info;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init_with_proxy)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "client_timeout_doesnt_stop_cdc",
            timeout,
            client_timeout_doesnt_stop_cdc,
        )
}

/// Test that CDC is working after rust driver session's client timeout.
///
/// Steps:
/// - Create a keyspace and a table with a vector column.
/// - Create a vector index on the vector column (table without data).
/// - Wait until vector-stores create indexes.
/// - Insert 10 vectors into the table that will be picked up by CDC.
/// - Simulate a client timeout using proxy.
/// - Insert 10 vectors into the table that will be picked up by CDC.
/// - Wait until vector-stores finish CDC readers.
/// - Bring back similator to normal operation.
/// - Wait until vector-stores update indexes using CDC.
/// - Drop the keyspace.
#[framed]
async fn client_timeout_doesnt_stop_cdc(actors: TestActors) {
    info!("started");
    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, v VECTOR<FLOAT, 1>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    info!("Initially, the index should have 0 vectors");
    let index = create_index(&session, &clients, &table, "v").await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 0, "Expected 0 vectors to be indexed");
    }

    info!("Insert initial data that will be picked up by CDC");
    const DATA_SIZE: usize = 10;
    let insert_vectors = async |start| {
        for i in start..start + DATA_SIZE {
            session
                .query_unpaged(
                    format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                    (i as i32, vec![i as f32]),
                )
                .await
                .expect("failed to insert data");
        }
    };
    insert_vectors(0).await;

    info!("Waiting till all vector-stores update indexes using CDC");
    wait_for(
        || async {
            for client in &clients {
                loop {
                    let index_status = wait_for_index(client, &index).await;
                    if index_status.count == DATA_SIZE {
                        break;
                    }
                    time::sleep(Duration::from_secs(1)).await;
                }
            }
            true
        },
        "Waiting for all vector-stores update indexes using CDC",
        Duration::from_secs(30),
    )
    .await;

    info!("Restart internals counter for cdc handler errors");
    let counter = format!("{}.{}-cdc-handler-errors", index.keyspace, index.index);
    for client in &clients {
        client.internals_clear_counters().await.unwrap();
        client
            .internals_start_counter(counter.clone())
            .await
            .unwrap();
    }

    info!("Simulate client timeout using proxy");
    actors
        .db_proxy
        .change_response_rules(Some(vec![ResponseRule(
            Condition::True,
            ResponseReaction::drop_frame(),
        )]))
        .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        info!("Current index status: {index_status:?}");
    }

    info!("Insert new data that will be picked up by CDC");
    insert_vectors(10).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        info!("Current index status: {index_status:?}");
    }

    info!("Waiting till all vector-stores' CDC handler error");
    wait_for(
        || async {
            for client in &clients {
                loop {
                    let counters = client.internals_counters().await.unwrap();
                    info!("Counters: {counters:?}");
                    if *counters.get(&counter).unwrap() > 0 {
                        break;
                    }
                    time::sleep(Duration::from_secs(1)).await;
                }
            }
            true
        },
        "Waiting for all vector-stores' CDC handler error",
        Duration::from_secs(30),
    )
    .await;

    info!("Stop timeout simulation");
    actors.db_proxy.turn_off_rules().await;
    // turning off rules sometimes doesn't help, cdc rust driver sessions are still broken
    // so we restart the proxy completely
    actors.db_proxy.stop().await;
    actors
        .db_proxy
        .start(common::get_default_scylla_proxy_node_configs(&actors).await)
        .await;

    info!("Waiting till all vector-stores update indexes with new data using CDC");
    wait_for(
        || async {
            for client in &clients {
                loop {
                    let index_status = wait_for_index(client, &index).await;
                    info!("Current index status: {index_status:?}");
                    if index_status.count == 2 * DATA_SIZE {
                        break;
                    }
                    time::sleep(Duration::from_secs(1)).await;
                }
            }
            true
        },
        "Waiting for all vector-stores update indexes with new data using CDC",
        Duration::from_secs(30),
    )
    .await;

    info!("Drop keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");
    info!("finished");
}
