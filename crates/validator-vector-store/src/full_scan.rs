/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::time::Duration;
use tracing::info;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "full_scan_is_completed_when_responding_to_messages_concurrently",
            timeout,
            full_scan_is_completed_when_responding_to_messages_concurrently,
        )
}

async fn full_scan_is_completed_when_responding_to_messages_concurrently(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        Some("CDC = {'enabled': true}"),
    )
    .await;

    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..1000 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (id, embedding) VALUES (?, ?)"),
                (i, embedding.clone()),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(&session, &clients, &table, "embedding").await;

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

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 1000,
            "Expected 1000 vectors to be indexed"
        );
    }

    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to query ANN search");

    session
        .query_unpaged(format!("DROP INDEX {}", index.index), ())
        .await
        .expect("failed to drop an index");

    for client in &clients {
        wait_for(
            || async { client.indexes().await.is_empty() },
            "Waiting for index deletion",
            Duration::from_secs(20),
        )
        .await;
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
