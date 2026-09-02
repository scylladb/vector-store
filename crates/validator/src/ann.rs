/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = ann,
    fixtures = (TestContext),
    parent = crate::standard
);

#[e2etest::test(group = ann)]
async fn ann_query_returns_expected_results(ctx: Arc<TestContext>) {
    info!("started");

    let table = ctx
        .create_table("pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None)
        .await;

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
    let stmt = ctx
        .session
        .prepare(format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"))
        .await
        .expect("failed to prepare insert statement");
    for (pk, embedding) in &embeddings {
        ctx.session
            .execute_unpaged(&stmt, (pk, embedding))
            .await
            .expect("failed to insert data");
    }

    let index = ctx.create_index(&table, "v").await;

    for client in &ctx.clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 1000,
            "Expected 1000 vectors to be indexed"
        );
    }

    // Check if the query returns the expected results (recall at least 85%)
    let results = get_query_results(
        format!("SELECT pk, v FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 100"),
        &ctx.session,
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

    info!("finished");
}

#[e2etest::test(group = ann)]
async fn ann_query_returns_expected_results_multicolumn_pk(ctx: Arc<TestContext>) {
    info!("started");

    let table = ctx
        .create_table(
            "pk TEXT, ck TEXT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
            None,
        )
        .await;

    let data: [(&'static str, &'static str, Vec<f32>); 2] = [
        ("pk-1", "ck-1", vec![0.0, 0.0, 0.0]),
        ("pk-2", "ck-2", vec![0.0, 0.0, 1.0]),
    ];
    for (pk, ck, v) in &data {
        ctx.session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (pk, ck, v),
            )
            .await
            .expect("failed to insert data");
    }
    ctx.create_index(&table, "v").await;

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 2"),
                &ctx.session,
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

    info!("finished");
}

#[e2etest::test(group = ann)]
async fn ann_query_respects_limit(ctx: Arc<TestContext>) {
    info!("started");

    let table = ctx
        .create_table("pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None)
        .await;

    // Insert 10 vectors
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..10 {
        ctx.session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i, &embedding),
            )
            .await
            .expect("failed to insert data");
    }

    // Create index
    let index = ctx.create_index(&table, "v").await;

    for client in &ctx.clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 10, "Expected 10 vectors to be indexed");
    }

    // Check if queries return the expected number of results
    let results = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &ctx.session,
    )
    .await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert!(rows.rows_remaining() <= 10);

    let results = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1000"),
        &ctx.session,
    )
    .await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert!(rows.rows_remaining() <= 10); // Should return only 10, as there are only 10 vectors

    // Check if LIMIT over 1000 fails
    ctx.session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1001"),
            (),
        )
        .await
        .expect_err("LIMIT over 1000 should fail");

    info!("finished");
}

#[e2etest::test(group = ann)]
async fn ann_query_respects_limit_over_1000_vectors(ctx: Arc<TestContext>) {
    info!("started");

    let table = ctx
        .create_table("pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None)
        .await;

    // Insert 1111 vectors
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    let stmt = ctx
        .session
        .prepare(format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"))
        .await
        .expect("failed to prepare insert statement");
    for i in 0..1111 {
        ctx.session
            .execute_unpaged(&stmt, (i, &embedding))
            .await
            .expect("failed to insert data");
    }

    let index = ctx.create_index(&table, "v").await;

    for client in &ctx.clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 1111,
            "Expected 1111 vectors to be indexed"
        );
    }

    // Check if queries return the expected number of results
    let results = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &ctx.session,
    )
    .await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert!(rows.rows_remaining() <= 10);

    let results = get_query_results(
        format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1000"),
        &ctx.session,
    )
    .await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert!(rows.rows_remaining() <= 1000);

    // Check if LIMIT over 1000 fails
    ctx.session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 1001"),
            (),
        )
        .await
        .expect_err("LIMIT over 1000 should fail");

    info!("finished");
}

#[e2etest::test(group = ann)]
async fn ann_query_returns_rows_identified_by_composite_primary_key(ctx: Arc<TestContext>) {
    info!("started");

    let table = ctx
        .create_table(
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
        ctx.session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (pk, ck, v),
            )
            .await
            .expect("failed to insert data");
    }
    ctx.create_index(&table, "v").await;

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk, ck FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 2"),
                &ctx.session,
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

    info!("finished");
}

/// Test that ANN queries return correct results when data is inserted using CDC.
///
/// Steps:
/// 1. Create a table with a vector column in the shared keyspace.
/// 2. Create a vector index on the vector column (table without data).
/// 3. Wait until vector-stores create indexes.
/// 4. Insert data into the table that will be picked up by CDC.
/// 5. Wait until vector-stores update indexes using CDC.
/// 6. Perform an ANN query and verify the results.
#[e2etest::test(group = ann)]
async fn ann_query_returns_rows_using_cdc(ctx: Arc<TestContext>) {
    info!("started");

    let table = ctx
        .create_table(
            "pk TEXT, ck TEXT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
            None,
        )
        .await;

    info!("Initially, the index should have 0 vectors");
    let index = ctx.create_index(&table, "v").await;

    for client in &ctx.clients {
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
        ctx.session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (pk, ck, v),
            )
            .await
            .expect("failed to insert data");
    }

    info!("Waiting till all vector-stores update indexes using CDC");
    wait_for_index_count(&ctx.clients, &index, 4).await;

    info!("Now perform the ANN query");
    let result = get_opt_query_results(
        format!("SELECT pk, ck FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 2"),
        &ctx.session,
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

    info!("finished");
}
