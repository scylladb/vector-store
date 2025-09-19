/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use crate::tests::*;
use std::time::Duration;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "vector_similarity_function_returns_expected_results",
            timeout,
            vector_similarity_function_returns_expected_results,
        )
        .with_test(
            "vector_similarity_function_with_clustering_key",
            timeout,
            vector_similarity_function_with_clustering_key,
        )
        .with_test(
            "vector_similarity_function_with_multi_column_partition_key",
            timeout,
            vector_similarity_function_with_multi_column_partition_key,
        )
}

async fn vector_similarity_function_returns_expected_results(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert test data
    let embeddings: Vec<Vec<f32>> = vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
        vec![7.0, 8.0, 9.0],
    ];
    for (i, embedding) in embeddings.into_iter().enumerate() {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i as i32, &embedding),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(
        &session,
        &client,
        &table,
        "v",
        Some("{'similarity_function' : 'EUCLIDEAN'}"),
    )
    .await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(3) },
        "Waiting for 3 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if the query returns the expected distances
    let rows = get_query_results(
        format!(
            "SELECT pk, vector_similarity() FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 5"
        ),
        &session,
    )
    .await;
    assert_eq!(rows.len(), 3);

    // Expected results are calculated using Euclidean distance formula
    let expected_distances = [(0, 14.0), (1, 77.0), (2, 194.0)];
    for (i, row) in rows.iter().enumerate() {
        let pk: i32 = row.columns[0].as_ref().unwrap().as_int().unwrap();
        let similarity: f32 = row.columns[1].as_ref().unwrap().as_float().unwrap();
        assert_eq!(
            (pk, similarity),
            expected_distances[i],
            "Row {i} does not match expected result"
        );
    }

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn vector_similarity_function_with_clustering_key(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    // Insert test data
    let embeddings: Vec<Vec<f32>> = vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
        vec![7.0, 8.0, 9.0],
    ];
    for (i, embedding) in embeddings.into_iter().enumerate() {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (123, i as i32, &embedding),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(
        &session,
        &client,
        &table,
        "v",
        Some("{'similarity_function' : 'EUCLIDEAN'}"),
    )
    .await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(3) },
        "Waiting for 3 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if the query returns the expected distances
    let rows = get_query_results(
        format!(
            "SELECT ck, vector_similarity() FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 5"
        ),
        &session,
    )
    .await;
    assert_eq!(rows.len(), 3);

    // Expected results are calculated using Euclidean distance formula
    let expected_distances = [(0, 14.0), (1, 77.0), (2, 194.0)];
    for (i, row) in rows.iter().enumerate() {
        let ck: i32 = row.columns[0].as_ref().unwrap().as_int().unwrap();
        let similarity: f32 = row.columns[1].as_ref().unwrap().as_float().unwrap();
        assert_eq!(
            (ck, similarity),
            expected_distances[i],
            "Row {i} does not match expected result"
        );
    }

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn vector_similarity_function_with_multi_column_partition_key(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk1 INT, pk2 INT, v VECTOR<FLOAT, 3>, PRIMARY KEY ((pk1, pk2))",
        None,
    )
    .await;

    // Insert test data
    let embeddings: Vec<Vec<f32>> = vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
        vec![7.0, 8.0, 9.0],
    ];
    for (i, embedding) in embeddings.into_iter().enumerate() {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk1, pk2, v) VALUES (?, ?, ?)"),
                (123, i as i32, &embedding),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(
        &session,
        &client,
        &table,
        "v",
        Some("{'similarity_function' : 'EUCLIDEAN'}"),
    )
    .await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(3) },
        "Waiting for 3 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if the query returns the expected distances
    let rows = get_query_results(
        format!(
            "SELECT pk2, vector_similarity() FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 5"
        ),
        &session,
    )
    .await;
    assert_eq!(rows.len(), 3);

    // Expected results are calculated using Euclidean distance formula
    let expected_distances = [(0, 14.0), (1, 77.0), (2, 194.0)];
    for (i, row) in rows.iter().enumerate() {
        let pk: i32 = row.columns[0].as_ref().unwrap().as_int().unwrap();
        let similarity: f32 = row.columns[1].as_ref().unwrap().as_float().unwrap();
        assert_eq!(
            (pk, similarity),
            expected_distances[i],
            "Row {i} does not match expected result"
        );
    }

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
