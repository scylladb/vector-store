/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use crate::tests::*;
use std::time::Duration;
use tracing::debug;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "ann_query_returns_expected_results",
            timeout,
            ann_query_returns_expected_results,
        )
        .with_test(
            "ann_query_respects_limit",
            timeout,
            ann_query_respects_limit,
        )
        .with_test(
            "ann_query_respects_limit_over_1000_vectors",
            timeout,
            ann_query_respects_limit_over_1000_vectors,
        )
}

async fn ann_query_returns_expected_results(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert 1000 vectors
    for i in 0..1000 {
        let embedding: Vec<f32> = vec![
            if i < 100 { 0.0 } else { (i % 3) as f32 },
            if i < 100 { 0.0 } else { (i % 5) as f32 },
            if i < 100 { 0.0 } else { (i % 7) as f32 },
        ];
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i, embedding),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(&session, &client, &table, "v", None).await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(1000) },
        "Waiting for 1000 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if the query returns the expected results (recall at least 85%)
    let rows = get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 100"),
        &session,
    )
    .await;
    assert_eq!(rows.len(), 100);
    let correct = rows
        .iter()
        .filter(|row| {
            let pk: i32 = row.columns[0].as_ref().unwrap().as_int().unwrap();
            pk < 100
        })
        .count();
    debug!("Number of matching results: {}", correct);
    assert!(
        correct >= 85,
        "Expected more than 85 matching results, got {correct}"
    );

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn ann_query_respects_limit(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert 10 vectors
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..10 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i, &embedding),
            )
            .await
            .expect("failed to insert data");
    }

    // Create index
    let index = create_index(&session, &client, &table, "v", None).await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(10) },
        "Waiting for 10 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if queries return the expected number of results
    let rows = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    assert_eq!(rows.len(), 10);

    let rows = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1000"),
        &session,
    )
    .await;
    assert_eq!(rows.len(), 10); // Should return only 10, as there are only 10 vectors

    // Check if LIMIT over 1000 fails
    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1001"),
            (),
        )
        .await
        .expect_err("LIMIT over 1000 should fail");

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn ann_query_respects_limit_over_1000_vectors(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert 1111 vectors
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..1111 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i, &embedding),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(&session, &client, &table, "v", None).await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(1111) },
        "Waiting for 1111 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if queries return the expected number of results
    let rows = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    assert_eq!(rows.len(), 10);

    // Due to VECTOR-221 the test fails. Uncomment after fixing.
    // let rows = get_query_results(
    //     format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1000"),
    //     &session,
    // )
    // .await;
    // assert_eq!(rows.len(), 1000);

    // Check if LIMIT over 1000 fails
    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1001"),
            (),
        )
        .await
        .expect_err("LIMIT over 1000 should fail");

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
