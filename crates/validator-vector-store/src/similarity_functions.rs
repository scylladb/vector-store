/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use tracing::info;
use vector_search_validator_tests::common;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, common::init)
        .with_cleanup(timeout, common::cleanup)
        .with_test(
            "test_similarity_function_euclidean",
            timeout,
            test_similarity_function_euclidean,
        )
        .with_test(
            "test_similarity_function_cosine",
            timeout,
            test_similarity_function_cosine,
        )
        .with_test(
            "test_similarity_function_dot_product",
            timeout,
            test_similarity_function_dot_product,
        )
        .with_test(
            "test_similarity_function_default_is_cosine",
            timeout,
            test_similarity_function_default_is_cosine,
        )
        .with_test(
            "test_similarity_function_lowercase",
            timeout,
            test_similarity_function_lowercase,
        )
}

async fn run_similarity_function_test(
    actors: &TestActors,
    similarity_function: Option<&str>,
    vectors: Vec<(i32, Vec<f32>)>,
    expected_best_pks: Vec<i32>,
) {
    let (session, clients) = prepare_connection(actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    for (pk, v) in &vectors {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (pk, v),
            )
            .await
            .expect("failed to insert data");
    }

    let index = match similarity_function {
        Some(func) => {
            let index_name = unique_index_name();
            session
                .query_unpaged(
                    format!(
                        "CREATE INDEX {index_name} ON {table}(v) USING 'vector_index' WITH OPTIONS = {{'similarity_function': '{func}'}}"
                    ),
                    (),
                )
                .await
                .expect("failed to create an index");
            vector_store::IndexInfo::new(keyspace.as_ref(), index_name.as_ref())
        }
        None => create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await,
    };

    for client in &clients {
        wait_for_index(client, &index).await;
    }

    let limit = expected_best_pks.len();
    let results = get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [1.0, 0.0, 0.0] LIMIT {limit}"),
        &session,
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

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");
}

#[framed]
async fn test_similarity_function_euclidean(actors: TestActors) {
    info!("started");

    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![1.0f32, 1.0, 1.0]),
    ];

    run_similarity_function_test(&actors, Some("EUCLIDEAN"), vectors, vec![1]).await;

    info!("finished");
}

#[framed]
async fn test_similarity_function_cosine(actors: TestActors) {
    info!("started");

    // With cosine similarity, both pk=1 and pk=4 should have the same similarity (same direction)
    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![2.0f32, 0.0, 0.0]), // Same direction as pk=1 but different magnitude
    ];

    run_similarity_function_test(&actors, Some("COSINE"), vectors, vec![1, 4]).await;

    info!("finished");
}

#[framed]
async fn test_similarity_function_dot_product(actors: TestActors) {
    info!("started");

    // With dot product, pk=4 should have highest similarity (2.0 * 1.0 = 2.0)
    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![2.0f32, 0.0, 0.0]), // Higher dot product with query vector
    ];

    run_similarity_function_test(&actors, Some("DOT_PRODUCT"), vectors, vec![4]).await;

    info!("finished");
}

#[framed]
async fn test_similarity_function_default_is_cosine(actors: TestActors) {
    info!("started");

    // Default is COSINE, so both pk=1 and pk=4 should have the same similarity (same direction)
    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![2.0f32, 0.0, 0.0]), // Same direction as pk=1
    ];

    run_similarity_function_test(&actors, None, vectors, vec![1, 4]).await;

    info!("finished");
}

#[framed]
async fn test_similarity_function_lowercase(actors: TestActors) {
    info!("started");

    let vectors = vec![
        (1, vec![1.0f32, 0.0, 0.0]),
        (2, vec![0.0f32, 1.0, 0.0]),
        (3, vec![0.0f32, 0.0, 1.0]),
        (4, vec![1.0f32, 1.0, 1.0]),
    ];

    run_similarity_function_test(&actors, Some("euclidean"), vectors, vec![1]).await;

    info!("finished");
}
