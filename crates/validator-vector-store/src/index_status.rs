/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use tracing::info;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;
use vector_store::IndexName;
use vector_store::KeyspaceName;
use vector_store::httproutes::IndexStatus;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "status_returned_correctly",
            timeout,
            status_returned_correctly,
        )
        .with_test(
            "status_returns_404_for_non_existent_index",
            timeout,
            status_returns_404_for_non_existent_index,
        )
}

#[framed]
async fn status_returned_correctly(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert some vectors
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..10000 {
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
            index_status.status,
            IndexStatus::Serving,
            "Expected index status to be Serving after indexing is complete"
        );
        assert_eq!(
            index_status.count, 10000,
            "Expected 10000 vectors to be indexed"
        );
    }

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn status_returns_404_for_non_existent_index(actors: TestActors) {
    info!("started");

    let (_session, clients) = prepare_connection(&actors).await;

    // Assert that querying the status of the dropped index returns an HTTP 404 error
    let keyspace_name = KeyspaceName::from("non_existent_keyspace".to_string());
    let index_name = IndexName::from("non_existent_index".to_string());
    for client in &clients {
        let index_status = client.index_status(&keyspace_name, &index_name).await;
        assert!(index_status.is_err());
        assert!(index_status.err().unwrap().to_string().contains("404"));
    }

    info!("finished");
}
