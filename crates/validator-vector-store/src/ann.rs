/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time;
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
            "ann_query_returns_expected_results",
            timeout,
            ann_query_returns_expected_results,
        )
        .with_test(
            "ann_query_returns_expected_results_multicolumn_pk",
            timeout,
            ann_query_returns_expected_results_multicolumn_pk,
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
        .with_test(
            "ann_query_returns_rows_identified_by_composite_primary_key",
            timeout,
            ann_query_returns_rows_identified_by_composite_primary_key,
        )
        .with_test(
            "ann_query_returns_rows_using_cdc",
            timeout,
            ann_query_returns_rows_using_cdc,
        )
}

#[framed]
async fn ann_query_returns_expected_results(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Create a map of pk -> embedding
    let mut embeddings: HashMap<i32, Vec<f32>> = HashMap::new();
    for i in 0..1000 {
        let embedding = vec![
            if i < 100 { 0.0 } else { (i % 3) as f32 },
            if i < 100 { 0.0 } else { (i % 5) as f32 },
            if i < 100 { 0.0 } else { (i % 7) as f32 },
        ];
        embeddings.insert(i, embedding);
    }

    // Insert 1000 vectors from the map
    for (pk, embedding) in &embeddings {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (pk, embedding),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 1000,
            "Expected 1000 vectors to be indexed"
        );
    }

    // Check if the query returns the expected results (recall at least 85%)
    let results = get_query_results(
        format!("SELECT pk, v FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 100"),
        &session,
    )
    .await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert!(rows.rows_remaining() <= 100);
    for row in rows {
        let row = row.expect("failed to get row");
        let (pk, v) = row;
        assert!(
            embeddings.contains_key(&pk),
            "pk {pk} not found in embeddings"
        );
        let expected = embeddings.get(&pk).unwrap();
        assert_eq!(&v, expected, "Returned vector does not match for pk={pk}");
    }

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn ann_query_returns_expected_results_multicolumn_pk(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk TEXT, ck TEXT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    let data: [(&'static str, &'static str, Vec<f32>); 2] = [
        ("pk-1", "ck-1", vec![0.0, 0.0, 0.0]),
        ("pk-2", "ck-2", vec![0.0, 0.0, 1.0]),
    ];
    for (pk, ck, v) in &data {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (pk, ck, v),
            )
            .await
            .expect("failed to insert data");
    }
    create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 2"),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 2)
        },
        "Waiting for ANN query to return 2 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    let rows: HashSet<String> = result
        .rows::<(String,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect();

    // Assert that the values returned are from pk column.
    assert_eq!(
        rows,
        [("pk-1".to_string()), ("pk-2".to_string()),]
            .into_iter()
            .collect::<HashSet<String>>()
    );

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn ann_query_respects_limit(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

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
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 10, "Expected 10 vectors to be indexed");
    }

    // Check if queries return the expected number of results
    let results = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert!(rows.rows_remaining() <= 10);

    let results = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1000"),
        &session,
    )
    .await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert!(rows.rows_remaining() <= 10); // Should return only 10, as there are only 10 vectors

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

#[framed]
async fn ann_query_respects_limit_over_1000_vectors(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

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

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 1111,
            "Expected 1111 vectors to be indexed"
        );
    }

    // Check if queries return the expected number of results
    let results = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert!(rows.rows_remaining() <= 10);

    let results = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1000"),
        &session,
    )
    .await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert!(rows.rows_remaining() <= 1000);

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

#[framed]
async fn ann_query_returns_rows_identified_by_composite_primary_key(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk TEXT, ck TEXT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;
    let data: [(&'static str, &'static str, Vec<f32>); 4] = [
        ("pk-1", "ck-1", vec![0.0, 0.0, 0.0]),
        ("pk-1", "ck-2", vec![1.0, 1.0, 1.0]),
        ("pk-2", "ck-1", vec![0.0, 0.0, 0.0]),
        ("pk-2", "ck-2", vec![1.0, 1.0, 1.0]),
    ];
    for (pk, ck, v) in &data {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (pk, ck, v),
            )
            .await
            .expect("failed to insert data");
    }
    create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk, ck FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 2"),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 2)
        },
        "Waiting for ANN query to return 2 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    let rows: HashSet<(String, String)> = result
        .rows::<(String, String)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();

    // Assert that we have the expected rows, ('pk-1', 'ck-1') and ('pk-2', 'ck-1'), as they have the closest vectors.
    assert_eq!(
        rows,
        [
            ("pk-1".to_string(), "ck-1".to_string()),
            ("pk-2".to_string(), "ck-1".to_string()),
        ]
        .into_iter()
        .collect::<HashSet<(String, String)>>()
    );

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test that ANN queries return correct results when data is inserted using CDC.
///
/// Steps:
/// 1. Prepare a cluster with scylladb and vector-store nodes.
/// 2. Create a keyspace and a table with a vector column.
/// 3. Create a vector index on the vector column (table without data).
/// 4. Wait until vector-stores create indexes.
/// 5. Insert data into the table that will be picked up by CDC.
/// 6. Wait until vector-stores update indexes using CDC.
/// 7. Perform an ANN query and verify the results.
/// 8. Drop the keyspace.
#[framed]
async fn ann_query_returns_rows_using_cdc(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk TEXT, ck TEXT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    info!("Initially, the index should have 0 vectors");
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 0, "Expected 0 vectors to be indexed");
    }

    info!("Insert data that will be picked up by CDC");
    let data: [(&'static str, &'static str, Vec<f32>); 4] = [
        ("pk-1", "ck-1", vec![0.0, 0.0, 0.0]),
        ("pk-1", "ck-2", vec![1.0, 1.0, 1.0]),
        ("pk-2", "ck-1", vec![0.0, 0.0, 0.0]),
        ("pk-2", "ck-2", vec![1.0, 1.0, 1.0]),
    ];
    for (pk, ck, v) in &data {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (pk, ck, v),
            )
            .await
            .expect("failed to insert data");
    }

    info!("Waiting till all vector-stores update indexes using CDC");
    wait_for(
        || async {
            for client in &clients {
                loop {
                    let index_status = wait_for_index(client, &index).await;
                    if index_status.count == 4 {
                        break;
                    }
                    time::sleep(Duration::from_secs(1)).await;
                }
            }
            true
        },
        "Waiting for all vector-stores update indexes using CDC",
        Duration::from_secs(60),
    )
    .await;

    info!("Now perform the ANN query");
    let result = get_opt_query_results(
        format!("SELECT pk, ck FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 2"),
        &session,
    )
    .await
    .unwrap();
    assert_eq!(result.rows_num(), 2);
    let rows: HashSet<(String, String)> = result
        .rows::<(String, String)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();

    info!(
        "Assert that we have the expected rows, ('pk-1', 'ck-1') and ('pk-2', 'ck-1'), as they have the closest vectors."
    );
    assert_eq!(
        rows,
        [
            ("pk-1".to_string(), "ck-1".to_string()),
            ("pk-2".to_string(), "ck-1".to_string()),
        ]
        .into_iter()
        .collect::<HashSet<(String, String)>>()
    );

    info!("Drop keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
