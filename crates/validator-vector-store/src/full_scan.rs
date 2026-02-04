/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use std::time::Duration;
use tracing::info;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init_with_proxy_single_vs)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "full_scan_is_completed_when_responding_to_messages_concurrently",
            timeout,
            full_scan_is_completed_when_responding_to_messages_concurrently,
        )
}

#[framed]
async fn full_scan_is_completed_when_responding_to_messages_concurrently(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        Some("CDC = {'enabled': true}"),
    )
    .await;

    info!("Inserting data to the table");
    const DATASET_SIZE: i32 = 100;
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..DATASET_SIZE {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (id, embedding) VALUES (?, ?)"),
                (i, embedding.clone()),
            )
            .await
            .expect("failed to insert data");
    }

    info!("Slow communication between vector-store and scylla using proxy");
    const FRAME_DELAY: Duration = Duration::from_millis(100);
    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    info!("Creating index");
    let index = create_index(&session, &clients, &table, "embedding").await;

    info!("Checking that full scan isn't completed");
    let result = session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await;
    match &result {
        Err(e) if format!("{e:?}").contains("503 Service Unavailable") => {}
        _ => panic!("Expected SERVICE_UNAVAILABLE error, got: {result:?}"),
    }

    info!("Recovering normal communication");
    actors.db_proxy.turn_off_rules().await;

    info!("Waiting for index to be built");
    let index_status = wait_for_index(client, &index).await;
    assert_eq!(
        index_status.count, DATASET_SIZE as usize,
        "Expected {DATASET_SIZE} vectors to be indexed"
    );

    info!("Checking that ANN search works after index is built");
    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to query ANN search");

    info!("Dropping index");
    session
        .query_unpaged(format!("DROP INDEX {}", index.index), ())
        .await
        .expect("failed to drop an index");

    wait_for(
        || async { client.indexes().await.is_empty() },
        "index must be removed",
        Duration::from_secs(60),
    )
    .await;

    info!("Dropping keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
