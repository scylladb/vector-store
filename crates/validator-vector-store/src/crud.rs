/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use tracing::info;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "simple_create_drop_index",
            timeout,
            simple_create_drop_index,
        )
        .with_test(
            "simple_create_drop_multiple_indexes",
            timeout,
            simple_create_drop_multiple_indexes,
        )
        .with_test(
            "drop_table_removes_index",
            timeout,
            drop_table_removes_index,
        )
}

#[framed]
async fn simple_create_drop_index(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id BIGINT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        Some("CDC = {'enabled': true}"),
    )
    .await;

    let index = create_index(&session, &clients, &table, "embedding").await;

    assert_eq!(index.keyspace, keyspace);

    session
        .query_unpaged(format!("DROP INDEX {}", index.index), ())
        .await
        .expect("failed to drop an index");

    for client in &clients {
        wait_for(
            || async { client.indexes().await.is_empty() },
            "Waiting for index deletion",
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await;
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn simple_create_drop_multiple_indexes(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, v1 VECTOR<FLOAT, 3>, v2 VECTOR<FLOAT, 3>",
        None,
    )
    .await;

    // Create index on column v1
    let index1 = create_index(&session, &clients, &table, "v1").await;

    // Wait for the full scan to complete and check if ANN query succeeds on v1
    for client in &clients {
        wait_for_index(client, &index1).await;
    }
    assert!(
        session
            .query_unpaged(
                format!("SELECT * FROM {table} ORDER BY v1 ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
                (),
            )
            .await
            .is_ok()
    );

    // ANN query on v2 should not succeed without the index
    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY v2 ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect_err("ANN query should fail when index does not exist");

    // Create index on column v2
    let index2 = create_index(&session, &clients, &table, "v2").await;

    // Check if ANN query on v1 still succeeds
    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY v1 ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to run ANN query");

    // Wait for the full scan to complete and check if ANN query succeeds on v2
    for client in &clients {
        wait_for_index(client, &index2).await;
    }
    assert!(
        session
            .query_unpaged(
                format!("SELECT * FROM {table} ORDER BY v2 ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
                (),
            )
            .await
            .is_ok()
    );

    // Drop index on column v1
    session
        .query_unpaged(format!("DROP INDEX {}", index1.index), ())
        .await
        .expect("failed to drop an index");

    info!("waiting for the first index to be dropped");

    // Wait for the first index to be dropped
    for client in &clients {
        wait_for(
            || async { client.indexes().await.len() == 1 },
            "Waiting for the first index to be dropped",
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await;
    }

    // ANN query on v1 should not succeed after dropping the index
    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY v1 ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect_err("ANN query should fail when index does not exist");

    // Check if ANN query on v2 still succeeds
    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY v2 ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to run ANN query");

    // Drop index on column v2
    session
        .query_unpaged(format!("DROP INDEX {}", index2.index), ())
        .await
        .expect("failed to drop an index");

    // Wait for the second index to be dropped
    for client in &clients {
        wait_for(
            || async { client.indexes().await.is_empty() },
            "Waiting for all indexes to be dropped",
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await;
    }

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn drop_table_removes_index(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        Some("CDC = {'enabled': true}"),
    )
    .await;

    let stmt: scylla::statement::prepared::PreparedStatement = session
        .prepare(format!(
            "INSERT INTO {table} (id, embedding) VALUES (?, [1.0, 2.0, 3.0])"
        ))
        .await
        .expect("failed to prepare a statement");

    for id in 0..1000 {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

    let _ = create_index(&session, &clients, &table, "embedding").await;

    let stmt = session
        .prepare(format!("DROP TABLE {keyspace}.{table}"))
        .await
        .expect("failed to prepare a statement");
    session
        .execute_unpaged(&stmt, ())
        .await
        .expect("failed to drop table");

    for client in &clients {
        wait_for(
            || async { client.indexes().await.is_empty() },
            "Waiting for index deletion",
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await;
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
