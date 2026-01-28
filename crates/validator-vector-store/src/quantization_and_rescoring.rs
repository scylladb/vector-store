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

/// Generate test vectors for quantization precision testing
/// Creates a query vector and embeddings with small directional differences
/// for cosine metric (vectors are not normalized, hence they have magnitude differences) that will be lost during quantization.
fn generate_test_vectors(num_vectors: usize) -> (Vec<f32>, HashMap<i32, Vec<f32>>) {
    let query_vector = vec![0.5, 0.3, 0.7];

    let mut embeddings: HashMap<i32, Vec<f32>> = HashMap::new();
    for i in 0..num_vectors {
        let offset = (i as f32) * 0.0001; // Larger offset for clearer ranking differences
        let embedding = vec![
            query_vector[0] + offset * 2.0, // Different weights to rotate in space
            query_vector[1] + offset * 4.0,
            query_vector[2] + offset * 8.0,
        ];
        embeddings.insert(i as i32, embedding);
    }

    (query_vector, embeddings)
}

async fn create_index(
    session: &Session,
    clients: &[HttpClient],
    table: &str,
    options: &str,
) -> IndexInfo {
    let index = create_index_with_options(session, clients, table, "v", Some(options)).await;
    for client in clients {
        wait_for_index(client, &index).await;
    }
    index
}

async fn insert_vectors(session: &Session, table: &str, embeddings: &HashMap<i32, Vec<f32>>) {
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
            "rescoring_makes_the_results_to_be_ranked_correctly_for_quantized_index",
            timeout,
            rescoring_makes_the_results_to_be_ranked_correctly_for_quantized_index,
        )
        .with_test(
            "searching_and_rescoring_works_for_binary_quantzation",
            timeout,
            searching_and_rescoring_works_for_binary_quantzation,
        )
}

#[framed]
async fn non_quantized_index_returns_correctly_ranked_vectors(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let (query_vector, embeddings) = generate_test_vectors(500);

    insert_vectors(&session, &table, &embeddings).await;
    // Set oversampling to search entire dataset for more predicable results (5.0 * LIMIT 100 = 500  embeddinsgs)
    create_index(
        &session,
        &clients,
        &table,
        "{'quantization': 'f32', 'oversampling': '5.0', 'rescoring': 'false'}",
    )
    .await;

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

    assert!(rows.rows_remaining() > 0, "Should return some results");
    assert!(
        rows.into_iter().is_sorted_by_key(|row| {
            let (pk, _) = row.expect("failed to get row");
            pk
        }),
        "Results should be sorted by pk. In f32 quantization (full precision) \
        results should be sorted by pk. Vectors are distinct enough to be ranked correctly.",
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn quantized_index_returns_incorrectly_ranked_vectors_due_to_precision_loss(
    actors: TestActors,
) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let (query_vector, embeddings) = generate_test_vectors(500);

    insert_vectors(&session, &table, &embeddings).await;
    // Set oversampling to search entire dataset for more predicable results (5.0 * LIMIT 100 = 500  embeddinsgs)
    create_index(
        &session,
        &clients,
        &table,
        "{'quantization': 'f16', 'oversampling': '5.0', 'rescoring': 'false'}",
    )
    .await;

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

    assert!(rows.rows_remaining() > 0, "Should return some results");
    assert!(
        rows.into_iter().is_sorted_by_key(|row| {
            let (pk, _) = row.expect("failed to get row");
            pk
        }) == false,
        "Results should not be sorted by pk. \
        Due to quantization and precision loss many vectors become identical after quantization, leading to incorrect ranking.",
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn rescoring_makes_the_results_to_be_ranked_correctly_for_quantized_index(
    actors: TestActors,
) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let (query_vector, embeddings) = generate_test_vectors(500);

    insert_vectors(&session, &table, &embeddings).await;
    // Set oversampling to search entire dataset for more predicable results (5.0 * LIMIT 100 = 500  embeddinsgs)
    create_index(
        &session,
        &clients,
        &table,
        "{'quantization': 'f16', 'oversampling': '5.0', 'rescoring': 'true'}",
    )
    .await;

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

    assert!(rows.rows_remaining() > 0, "Should return some results");
    assert!(
        rows.into_iter().is_sorted_by_key(|row| {
            let (pk, v) = row.expect("failed to get row");
            println!("Returned pk: {}, vector: {:?}", pk, v);
            pk
        }),
        "Results should be sorted by pk. The index is quantized, so the initial search returns incorrectly ranked results. \
        However, rescoring corrects the ranking despite the precision loss.",
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
async fn searching_and_rescoring_works_for_binary_quantzation(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;
    let (query_vector, embeddings) = generate_test_vectors(500);

    insert_vectors(&session, &table, &embeddings).await;
    // Set oversampling to search entire dataset for more predicable results (5.0 * LIMIT 100 = 500  embeddinsgs)
    create_index(
        &session,
        &clients,
        &table,
        "{'quantization': 'b1', 'oversampling': '5.0', 'rescoring': 'true'}",
    )
    .await;

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

    assert!(rows.rows_remaining() > 0, "Should return some results");

    assert!(
        rows.into_iter().is_sorted_by_key(|row| {
            let (pk, _) = row.expect("failed to get row");
            pk
        }),
        "Results should be sorted by pk. The index is quantized, so the initial search returns incorrectly ranked results. \
        However, rescoring corrects the ranking despite the precision loss.",
    );

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
