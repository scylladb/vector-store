/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use httpclient::HttpClient;
use scylla::client::session::Session;
use std::collections::HashMap;
use tracing::info;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;
use vector_store::IndexInfo;
use vector_store::TableName;

/// Generate test vectors for quantization precision testing
/// Creates a query vector and embeddings with small directional differences
/// that will be lost during quantization when using the cosine metric.
fn generate_test_vectors(num_vectors: usize) -> (Vec<f32>, HashMap<i32, Vec<f32>>) {
    let query_vector = vec![0.5, 0.3, 0.7];

    let embeddings: HashMap<i32, Vec<f32>> = (0..num_vectors)
        .map(|i| {
            let offset = (i as f32) * 0.0001; // Small offset to test quantization precision loss
            let embedding = vec![
                query_vector[0] + offset * 2.0, // Different weights to rotate in space
                query_vector[1] + offset * 4.0,
                query_vector[2] + offset * 8.0,
            ];
            (i as i32, embedding)
        })
        .collect();

    (query_vector, embeddings)
}

#[framed]
async fn create_index(
    session: &Session,
    clients: &[HttpClient],
    table: &TableName,
    options: impl IntoIterator<Item = (impl AsRef<str>, impl AsRef<str>)>,
) -> IndexInfo {
    let index =
        common::create_index(CreateIndexQuery::new(session, clients, table, "v").options(options))
            .await;
    for client in clients {
        wait_for_index(client, &index).await;
    }
    index
}

#[framed]
async fn insert_vectors(session: &Session, table: &TableName, embeddings: &HashMap<i32, Vec<f32>>) {
    for (pk, embedding) in embeddings {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (pk, embedding),
            )
            .await
            .expect("failed to insert data");
    }
}

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "non_quantized_index_returns_correctly_ranked_vectors",
            timeout,
            non_quantized_index_returns_correctly_ranked_vectors,
        )
        .with_test(
            "quantized_index_returns_incorrectly_ranked_vectors_due_to_precision_loss",
            timeout,
            quantized_index_returns_incorrectly_ranked_vectors_due_to_precision_loss,
        )
        .with_test(
            "rescoring_ranks_results_correctly_for_quantized_index",
            timeout,
            rescoring_ranks_results_correctly_for_quantized_index,
        )
        .with_test(
            "searching_and_rescoring_works_for_binary_quantization",
            timeout,
            searching_and_rescoring_works_for_binary_quantization,
        )
}

#[framed]
// This test confirms that with full f32 precision, the vector search can distinguish
// between very similar vectors and rank them correctly. The test vectors are generated
// with small, incremental differences. A correct implementation should order them by their
// primary key (pk), which corresponds to their increasing distance from the query vector.
// The success of this test depends on the underlying floating-point precision (f32) being
// sufficient to handle the small differences in the test data.
async fn non_quantized_index_returns_correctly_ranked_vectors(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let (query_vector, embeddings) = generate_test_vectors(500);

    insert_vectors(&session, &table, &embeddings).await;
    info!("inserted vectors into table");

    // Set oversampling to search the entire dataset for more predictable results (5.0 * LIMIT 100 = 500 embeddings)
    create_index(
        &session,
        &clients,
        &table,
        [
            ("quantization", "f32"),
            ("oversampling", "5.0"),
            ("rescoring", "false"),
        ],
    )
    .await;
    info!("created index with f32 quantization");

    let results = get_query_results(
        format!(
            "SELECT pk, v FROM {table} ORDER BY v ANN OF [{}, {}, {}] LIMIT 100",
            query_vector[0], query_vector[1], query_vector[2]
        ),
        &session,
    )
    .await;

    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");

    info!("retrieved {} rows from query", rows.rows_remaining());

    assert!(rows.rows_remaining() > 0, "Should return some results");
    assert!(
        rows.into_iter().is_sorted_by_key(|row| {
            let (pk, _) = row.expect("failed to get row");
            pk
        }),
        "Results should be sorted by pk. With f32 quantization (full precision), \
        vectors are distinct enough to be ranked correctly"
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
// This test verifies that f16 quantization leads to a loss of precision.
// Test vectors are generated such that their distance from the query vector increases
// with their primary key (pk). The differences are small and are lost when vectors
// are converted to f16.
//
// As a result, the vector search algorithm treats many distinct vectors as identical.
// When multiple vectors have the same similarity score, their relative order in the
// result set is not guaranteed. This is a general characteristic of ANN algorithms like HNSW.
//
// We assert that the results are NOT sorted by pk, which would be the correct order
// if full precision were maintained. This demonstrates the effect of precision loss.
async fn quantized_index_returns_incorrectly_ranked_vectors_due_to_precision_loss(
    actors: TestActors,
) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let (query_vector, embeddings) = generate_test_vectors(500);

    insert_vectors(&session, &table, &embeddings).await;
    info!("inserted vectors into table");

    // Set oversampling to search the entire dataset for more predictable results (5.0 * LIMIT 100 = 500 embeddings)
    create_index(
        &session,
        &clients,
        &table,
        [
            ("quantization", "f16"),
            ("oversampling", "5.0"),
            ("rescoring", "false"),
        ],
    )
    .await;
    info!("created index with f16 quantization");

    let results = get_query_results(
        format!(
            "SELECT pk, v FROM {table} ORDER BY v ANN OF [{}, {}, {}] LIMIT 100",
            query_vector[0], query_vector[1], query_vector[2]
        ),
        &session,
    )
    .await;

    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");

    info!("retrieved {} rows from query", rows.rows_remaining());

    assert!(rows.rows_remaining() > 0, "Should return some results");
    assert!(
        !rows.into_iter().is_sorted_by_key(|row| {
            let (pk, _) = row.expect("failed to get row");
            pk
        }),
        "Results should not be sorted by pk. \
        Due to quantization and precision loss, many vectors become identical after quantization, leading to incorrect ranking"
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
// This test demonstrates that rescoring can correct the ranking inaccuracies introduced by quantization.
//
// The initial search is performed on the quantized (f16) index. As shown in the previous test,
// this leads to incorrectly ordered results for vectors with tied similarity scores due to precision loss.
//
// By enabling rescoring, the system recalculates the exact distances using the original, full-precision
// vectors for a set of top candidates. This corrects the ranking. The test asserts that the final
// results are sorted by primary key (pk), confirming that rescoring successfully restored the correct order.
async fn rescoring_ranks_results_correctly_for_quantized_index(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let (query_vector, embeddings) = generate_test_vectors(500);

    insert_vectors(&session, &table, &embeddings).await;
    info!("inserted vectors into table");

    // Set oversampling to search the entire dataset for more predictable results (5.0 * LIMIT 100 = 500 embeddings)
    create_index(
        &session,
        &clients,
        &table,
        [
            ("quantization", "f16"),
            ("oversampling", "5.0"),
            ("rescoring", "true"),
        ],
    )
    .await;
    info!("created index with f16 quantization and rescoring enabled");

    let results = get_query_results(
        format!(
            "SELECT pk, v FROM {table} ORDER BY v ANN OF [{}, {}, {}] LIMIT 100",
            query_vector[0], query_vector[1], query_vector[2],
        ),
        &session,
    )
    .await;

    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");

    info!("retrieved {} rows from query", rows.rows_remaining());

    assert!(rows.rows_remaining() > 0, "Should return some results");
    assert!(
        rows.into_iter().is_sorted_by_key(|row| {
            let (pk, _) = row.expect("failed to get row");
            pk
        }),
        "Results should be sorted by pk. The index is quantized, so the initial search returns incorrectly ranked results. \
        However, rescoring corrects the ranking despite the precision loss"
    );

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
// Binary quantization has a special implementation in vector-store.
// Hence we need to verify that search and rescoring also work correctly with it.
async fn searching_and_rescoring_works_for_binary_quantization(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let (query_vector, embeddings) = generate_test_vectors(500);

    insert_vectors(&session, &table, &embeddings).await;
    info!("inserted vectors into table");

    // Set oversampling to search the entire dataset for more predictable results (5.0 * LIMIT 100 = 500 embeddings)
    create_index(
        &session,
        &clients,
        &table,
        [
            ("quantization", "b1"),
            ("oversampling", "5.0"),
            ("rescoring", "true"),
        ],
    )
    .await;
    info!("created index with b1 (binary) quantization and rescoring enabled");

    let results = get_query_results(
        format!(
            "SELECT pk, v FROM {table} ORDER BY v ANN OF [{}, {}, {}] LIMIT 100",
            query_vector[0], query_vector[1], query_vector[2],
        ),
        &session,
    )
    .await;

    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");

    info!("retrieved {} rows from query", rows.rows_remaining());

    assert!(rows.rows_remaining() > 0, "Should return some results");

    assert!(
        rows.into_iter().is_sorted_by_key(|row| {
            let (pk, _) = row.expect("failed to get row");
            pk
        }),
        "Results should be sorted by pk. The index is quantized, so the initial search returns incorrectly ranked results. \
        However, rescoring corrects the ranking despite the precision loss"
    );

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
