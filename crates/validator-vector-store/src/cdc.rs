/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use std::time::Duration;
use tracing::info;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

const FINE_GRAINED_CDC_MAX_LATENCY: Duration = Duration::from_secs(2);

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "cdc_insert_visible_immediately",
            timeout,
            cdc_insert_visible_immediately,
        )
        .with_test(
            "cdc_update_visible_immediately",
            timeout,
            cdc_update_visible_immediately,
        )
        .with_test(
            "cdc_delete_visible_immediately",
            timeout,
            cdc_delete_visible_immediately,
        )
}

#[framed]
async fn cdc_insert_visible_immediately(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(status.count, 0, "Index should start empty");

    // Insert after index creation - this should be picked up by the fine-grained CDC reader
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (1, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert data");

    // Should timeout only when using wide-framed CDC reader
    wait_for(
        || async {
            let status = client.index_status(&index.keyspace, &index.index).await;
            matches!(status, Ok(s) if s.count == 1)
        },
        "Waiting for CDC insert",
        FINE_GRAINED_CDC_MAX_LATENCY,
    )
    .await;

    // Verify ANN query returns the inserted row
    let result = get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [1.0, 2.0, 3.0] LIMIT 1"),
        &session,
    )
    .await;
    assert_eq!(result.rows_num(), 1, "Expected 1 row from ANN query");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[framed]
async fn cdc_update_visible_immediately(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (1, [0.0, 0.0, 0.0])"),
            (),
        )
        .await
        .expect("failed to insert data");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (2, [5.0, 5.0, 5.0])"),
            (),
        )
        .await
        .expect("failed to insert data");

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .options([("similarity_function", "euclidean")]),
    )
    .await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(
        status.count, 2,
        "Index should have 2 vectors after full scan"
    );

    // Verify ANN query for [10,10,10] returns pk=2 (closer than pk=1)
    let result = get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [10.0, 10.0, 10.0] LIMIT 1"),
        &session,
    )
    .await;
    let rows: Vec<(i32,)> = result
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|r| r.expect("failed to get row"))
        .collect();
    assert_eq!(rows.len(), 1, "Expected 1 row");
    assert_eq!(
        rows[0].0, 2,
        "Before update: pk=2 should be closest to [10,10,10]"
    );

    // Update pk=1 vector to [10,10,10] (closer than pk=2) - fine-grained CDC reader should pick this up
    session
        .query_unpaged(
            format!("UPDATE {table} SET v = [10.0, 10.0, 10.0] WHERE pk = 1"),
            (),
        )
        .await
        .expect("failed to update data");

    // Should timeout only when using wide-framed CDC reader
    wait_for(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [10.0, 10.0, 10.0] LIMIT 1"),
                &session,
            )
            .await;
            result.is_some_and(|result| {
                let rows: Vec<(i32,)> = result
                    .rows::<(i32,)>()
                    .expect("failed to get rows")
                    .map(|r| r.expect("failed to get row"))
                    .collect();
                rows.len() == 1 && rows[0].0 == 1
            })
        },
        "Waiting for CDC update",
        FINE_GRAINED_CDC_MAX_LATENCY,
    )
    .await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[framed]
async fn cdc_delete_visible_immediately(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs(&actors).await;
    let client = clients.first().unwrap();
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    for pk in 1..=3 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES ({pk}, [1.0, 1.0, 1.0])"),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let status = wait_for_index(client, &index).await;
    assert_eq!(status.count, 3, "Index should have 3 vectors");

    // Delete one row - fine-grained CDC reader should pick this up
    session
        .query_unpaged(format!("DELETE FROM {table} WHERE pk = 2"), ())
        .await
        .expect("failed to delete data");

    // Check that count decreases to 2 within fine-grained CDC latency
    wait_for(
        || async {
            let status = client.index_status(&index.keyspace, &index.index).await;
            matches!(status, Ok(s) if s.count == 2)
        },
        "Waiting for CDC delete",
        FINE_GRAINED_CDC_MAX_LATENCY,
    )
    .await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}
