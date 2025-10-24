/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use crate::tests::*;
use scylla::client::session::Session;
use std::time::Duration;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "similarity_cosine_function_with_single_column_partition_key",
            timeout,
            similarity_cosine_function_with_single_column_partition_key,
        )
        .with_test(
            "similarity_euclidean_function_with_single_column_partition_key",
            timeout,
            similarity_euclidean_function_with_single_column_partition_key,
        )
        .with_test(
            "similarity_dot_product_function_with_single_column_partition_key",
            timeout,
            similarity_dot_product_function_with_single_column_partition_key,
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

/// Normilized (L2 norm = 1) embeddings for testing
pub(crate) static EMBEDDINGS: [[f32; 3]; 3] = [
    [0.267261, 0.534522, 0.801784],
    [0.455842, 0.569803, 0.683763],
    [0.502571, 0.574366, 0.646162],
];

/// Expected results for similarity functions when querying with [1.0, 0.0, -1.0]
pub(crate) const SIMILARITY_RESULTS: [(&str, [(i32, f32); 3]); 3] = [
    ("cosine", [(2, 1.1015341), (1, 1.1611645), (0, 1.3779647)]),
    ("euclidean", [(2, 3.2871814), (1, 3.4558413), (0, 4.069046)]),
    (
        "dot_product",
        [(2, 1.1435909), (1, 1.227921), (0, 1.534523)],
    ),
];

async fn assert_similarity_function_results(
    session: &Session,
    table: &str,
    key_column: &str,
    similarity_function: &str,
) {
    let results = get_query_results(
        format!(
            "SELECT {key_column}, similarity_{similarity_function}(v, [1.0, 0.0, -1.0]) FROM {table} ORDER BY v ANN OF [1.0, 0.0, -1.0] LIMIT 5"
        ),
        session,
    )
    .await;
    let rows = results.rows::<(i32, f32)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 3);

    let (_, expected_distances) = SIMILARITY_RESULTS
        .iter()
        .find(|(name, _)| *name == similarity_function)
        .expect("similarity function not found");
    for (i, row) in rows.enumerate() {
        let row = row.expect("failed to get row");
        let (key, distance) = row;
        assert_eq!(
            (key, distance),
            expected_distances[i],
            "Row {i} does not match expected result"
        );
    }
}

async fn similarity_cosine_function_with_single_column_partition_key(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert test data
    for (i, embedding) in EMBEDDINGS.into_iter().enumerate() {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i as i32, embedding.as_slice()),
            )
            .await
            .expect("failed to insert data");
    }

    let similarity_function = "cosine";
    let index = create_index(
        &session,
        &client,
        &table,
        "v",
        Some(format!(
            "{{'similarity_function' : '{similarity_function}'}}"
        )),
    )
    .await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(3) },
        "Waiting for 3 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if the query returns the expected distances
    assert_similarity_function_results(&session, &table, "pk", similarity_function).await;

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn similarity_euclidean_function_with_single_column_partition_key(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert test data
    for (i, embedding) in EMBEDDINGS.into_iter().enumerate() {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i as i32, embedding.as_slice()),
            )
            .await
            .expect("failed to insert data");
    }

    let similarity_function = "euclidean";
    let index = create_index(
        &session,
        &client,
        &table,
        "v",
        Some(format!(
            "{{'similarity_function' : '{similarity_function}'}}"
        )),
    )
    .await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(3) },
        "Waiting for 3 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if the query returns the expected distances
    assert_similarity_function_results(&session, &table, "pk", similarity_function).await;

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn similarity_dot_product_function_with_single_column_partition_key(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert test data
    for (i, embedding) in EMBEDDINGS.into_iter().enumerate() {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i as i32, embedding.as_slice()),
            )
            .await
            .expect("failed to insert data");
    }

    let similarity_function = "dot_product";
    let index = create_index(
        &session,
        &client,
        &table,
        "v",
        Some(format!(
            "{{'similarity_function' : '{similarity_function}'}}"
        )),
    )
    .await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(3) },
        "Waiting for 3 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if the query returns the expected distances
    assert_similarity_function_results(&session, &table, "pk", similarity_function).await;

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
    for (i, embedding) in EMBEDDINGS.into_iter().enumerate() {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (123, i as i32, &embedding.as_slice()),
            )
            .await
            .expect("failed to insert data");
    }

    let similarity_function = "euclidean";
    let index = create_index(
        &session,
        &client,
        &table,
        "v",
        Some(format!(
            "{{'similarity_function' : '{similarity_function}'}}"
        )),
    )
    .await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(3) },
        "Waiting for 3 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if the query returns the expected distances
    assert_similarity_function_results(&session, &table, "ck", similarity_function).await;

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
    for (i, embedding) in EMBEDDINGS.into_iter().enumerate() {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk1, pk2, v) VALUES (?, ?, ?)"),
                (123, i as i32, &embedding.as_slice()),
            )
            .await
            .expect("failed to insert data");
    }

    let similarity_function = "euclidean";
    let index = create_index(
        &session,
        &client,
        &table,
        "v",
        Some(format!(
            "{{'similarity_function' : '{similarity_function}'}}"
        )),
    )
    .await;

    wait_for(
        || async { client.count(&index.keyspace, &index.index).await == Some(3) },
        "Waiting for 3 vectors to be indexed",
        Duration::from_secs(5),
    )
    .await;

    // Check if the query returns the expected distances
    assert_similarity_function_results(&session, &table, "pk2", similarity_function).await;

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
