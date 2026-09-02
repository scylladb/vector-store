/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = similarity_function,
    fixtures = (TestContext),
    parent = crate::standard
);

async fn run_similarity_function_test(
    ctx: &TestContext,
    similarity_function: Option<&str>,
    vectors: Vec<(i32, Vec<f32>)>,
    expected_best_pks: Vec<i32>,
) {
    let table = ctx
        .create_table("pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None)
        .await;

    for (pk, v) in &vectors {
        ctx.session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (pk, v),
            )
            .await
            .expect("failed to insert data");
    }

    let index = match similarity_function {
        Some(func) => {
            create_index(
                ctx.index_query(&table, "v")
                    .options([("similarity_function", func)]),
            )
            .await
        }
        None => ctx.create_index(&table, "v").await,
    };

    for client in &ctx.clients {
        wait_for_index(client, &index).await;
    }

    let limit = expected_best_pks.len();
    let results = get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [1.0, 0.0, 0.0] LIMIT {limit}"),
        &ctx.session,
    )
    .await;
    let rows: Vec<(i32,)> = results
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|r| r.expect("failed to get row"))
        .collect();

    assert_eq!(
        rows.len(),
        expected_best_pks.len(),
        "Expected {} result(s)",
        expected_best_pks.len()
    );

    let result_pks: Vec<i32> = rows.iter().map(|r| r.0).collect();
    for expected_pk in &expected_best_pks {
        assert!(
            result_pks.contains(expected_pk),
            "Expected pk={} to be in the nearest neighbors, got {:?}",
            expected_pk,
            result_pks
        );
    }
}

#[e2etest::test(group = similarity_function)]
async fn test_similarity_function_euclidean(ctx: Arc<TestContext>) {
    info!("started");

    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![1.0f32, 1.0, 1.0]),
    ];

    run_similarity_function_test(&ctx, Some("EUCLIDEAN"), vectors, vec![1]).await;

    info!("finished");
}

#[e2etest::test(group = similarity_function)]
async fn test_similarity_function_cosine(ctx: Arc<TestContext>) {
    info!("started");

    // With cosine similarity, both pk=1 and pk=4 should have the same similarity (same direction)
    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![2.0f32, 0.0, 0.0]), // Same direction as pk=1 but different magnitude
    ];

    run_similarity_function_test(&ctx, Some("COSINE"), vectors, vec![1, 4]).await;

    info!("finished");
}

#[e2etest::test(group = similarity_function)]
async fn test_similarity_function_dot_product(ctx: Arc<TestContext>) {
    info!("started");

    // With dot product, pk=4 should have highest similarity (2.0 * 1.0 = 2.0)
    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![2.0f32, 0.0, 0.0]), // Higher dot product with query vector
    ];

    run_similarity_function_test(&ctx, Some("DOT_PRODUCT"), vectors, vec![4]).await;

    info!("finished");
}

#[e2etest::test(group = similarity_function)]
async fn test_similarity_function_default_is_cosine(ctx: Arc<TestContext>) {
    info!("started");

    // Default is COSINE, so both pk=1 and pk=4 should have the same similarity (same direction)
    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![2.0f32, 0.0, 0.0]), // Same direction as pk=1
    ];

    run_similarity_function_test(&ctx, None, vectors, vec![1, 4]).await;

    info!("finished");
}

#[e2etest::test(group = similarity_function)]
async fn test_similarity_function_lowercase(ctx: Arc<TestContext>) {
    info!("started");

    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![1.0f32, 1.0, 1.0]),
    ];

    run_similarity_function_test(&ctx, Some("euclidean"), vectors, vec![1]).await;

    info!("finished");
}
