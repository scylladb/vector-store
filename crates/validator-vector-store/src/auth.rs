/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init_with_auth)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "vs_doesnt_work_without_permission",
            timeout,
            vs_doesnt_work_without_permission,
        )
        .with_test(
            "vs_works_when_permission_granted",
            timeout,
            vs_works_when_permission_granted,
        )
}

#[framed]
async fn vs_doesnt_work_without_permission(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_with_auth(&actors, "cassandra", "cassandra").await;
    session
        .query_unpaged(
            "CREATE ROLE alice WITH PASSWORD = 'alice_password' AND LOGIN = true",
            (),
        )
        .await
        .expect("failed to create role alice");

    let _ = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v1 VECTOR<FLOAT, 3>", None).await;

    // Create index
    session
        .query_unpaged(
            format!("CREATE INDEX idx_v1 ON {table}(v1) USING 'vector_index'"),
            (),
        )
        .await
        .expect("failed to create an index");

    sleep(Duration::from_secs(1)).await;

    let mut failed_to_create = true;

    for client in clients.iter() {
        if client
            .indexes()
            .await
            .iter()
            .any(|idx| idx.index.to_string() == "idx_v1")
        {
            failed_to_create = false;
            break;
        }
    }
    assert!(
        failed_to_create,
        "Index creation should fail without proper permissions"
    );

    session
        .query_unpaged("DROP ROLE alice", ())
        .await
        .expect("failed to drop role alice");

    info!("finished");
}

#[framed]
async fn vs_works_when_permission_granted(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_with_auth(&actors, "cassandra", "cassandra").await;

    session
        .query_unpaged(
            "CREATE ROLE alice WITH PASSWORD = 'alice_password' AND LOGIN = true",
            (),
        )
        .await
        .expect("failed to create role alice");
    session
        .query_unpaged("GRANT VECTOR_SEARCH_INDEXING ON ALL KEYSPACES TO alice", ())
        .await
        .expect("failed to grant permissions to alice");

    let _ = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v1 VECTOR<FLOAT, 3>", None).await;

    // Create index on column v1
    let _ = create_index(&session, &clients, &table, "v1").await;

    // Wait for the full scan to complete and check if ANN query succeeds on v1
    wait_for(
        || async {
            session
                .query_unpaged(
                    format!("SELECT * FROM {table} ORDER BY v1 ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
                    (),
                )
                .await
                .is_ok()
        },
        "Waiting for full scan to complete. ANN query should succeed",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("finished");
}
