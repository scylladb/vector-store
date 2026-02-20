/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;
use vector_store::httproutes::NodeStatus;

const WAITING_FOR_DB_DISCOVERY: Duration = Duration::from_secs(5);

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
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
        .with_test("cdc_works_with_auth", timeout, cdc_works_with_auth)
        .with_cleanup(timeout, common::cleanup)
}

#[framed]
async fn vs_doesnt_work_without_permission(actors: TestActors) {
    info!("started");

    let mut scylla_configs = get_default_scylla_node_configs(&actors).await;
    let mut vs_configs = get_default_vs_node_configs(&actors);

    for config in scylla_configs.iter_mut() {
        config.config = Some(
            "authenticator: PasswordAuthenticator\nauthorizer: CassandraAuthorizer"
                .as_bytes()
                .to_vec(),
        );
    }

    for config in vs_configs.iter_mut() {
        config.user = Some("alice".to_string());
        config.password = Some("alice_password".to_string());
    }

    info!("Initializing cluster");
    init_dns(&actors).await;
    actors.db.start(scylla_configs).await;
    assert!(actors.db.wait_for_ready().await);
    actors.vs.start(vs_configs).await;

    info!("Waiting for DB discovery");
    sleep(WAITING_FOR_DB_DISCOVERY).await;

    info!("Connecting to scylladb as cassandra");
    let (session, clients) = prepare_connection_with_auth(&actors, "cassandra", "cassandra").await;

    info!("Vector-store's should be in ConnectingToDb state");
    for client in clients.iter() {
        assert_eq!(client.status().await.unwrap(), NodeStatus::ConnectingToDb);
    }

    info!("Creating a role alice without permissions");
    session
        .query_unpaged(
            "CREATE ROLE alice WITH PASSWORD = 'alice_password' AND LOGIN = true",
            (),
        )
        .await
        .expect("failed to create role alice");

    info!("Waiting for DB discovery");
    sleep(WAITING_FOR_DB_DISCOVERY).await;

    info!("Vector-store's should be in ConnectingToDb state");
    for client in clients.iter() {
        assert_eq!(client.status().await.unwrap(), NodeStatus::ConnectingToDb);
    }

    info!("Cleaning up");
    cleanup(actors).await;

    info!("finished");
}

#[framed]
async fn vs_works_when_permission_granted(actors: TestActors) {
    info!("started");

    let mut scylla_configs = get_default_scylla_node_configs(&actors).await;
    let mut vs_configs = get_default_vs_node_configs(&actors);

    for config in scylla_configs.iter_mut() {
        config.config = Some(
            "authenticator: PasswordAuthenticator\nauthorizer: CassandraAuthorizer"
                .as_bytes()
                .to_vec(),
        );
    }

    for config in vs_configs.iter_mut() {
        config.user = Some("alice".to_string());
        config.password = Some("alice_password".to_string());
    }

    info!("Initializing cluster");
    init_dns(&actors).await;
    actors.db.start(scylla_configs).await;
    assert!(actors.db.wait_for_ready().await);
    actors.vs.start(vs_configs).await;

    info!("Waiting for DB discovery");
    sleep(WAITING_FOR_DB_DISCOVERY).await;

    info!("Connecting to scylladb as cassandra");
    let (session, clients) = prepare_connection_with_auth(&actors, "cassandra", "cassandra").await;

    info!("Vector-store's should be in ConnectingToDb state");
    for client in clients.iter() {
        assert_eq!(client.status().await.unwrap(), NodeStatus::ConnectingToDb);
    }

    info!("Creating a role alice with permissions");
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

    info!("Waiting for vector-store ready state");
    assert!(actors.vs.wait_for_ready().await);

    info!("Creating keyspace, table and index");
    let _ = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v1 VECTOR<FLOAT, 3>", None).await;

    // Create index on column v1
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v1")).await;

    info!("Waiting for index to be ready");
    for client in clients.iter() {
        wait_for_index(client, &index).await;
    }

    info!("Cleaning up");
    cleanup(actors).await;

    info!("finished");
}

#[framed]
async fn cdc_works_with_auth(actors: TestActors) {
    info!("started");

    let mut scylla_configs = get_default_scylla_node_configs(&actors).await;
    let mut vs_configs = get_default_vs_node_configs(&actors);

    for config in scylla_configs.iter_mut() {
        config.config = Some(
            "authenticator: PasswordAuthenticator\nauthorizer: CassandraAuthorizer"
                .as_bytes()
                .to_vec(),
        );
    }

    for config in vs_configs.iter_mut() {
        config.user = Some("alice".to_string());
        config.password = Some("alice_password".to_string());
    }

    info!("Initializing cluster");
    init_dns(&actors).await;
    actors.db.start(scylla_configs).await;
    assert!(actors.db.wait_for_ready().await);
    actors.vs.start(vs_configs).await;

    info!("Waiting for DB discovery");
    sleep(WAITING_FOR_DB_DISCOVERY).await;

    info!("Connecting to scylladb as cassandra");
    let (session, clients) = prepare_connection_with_auth(&actors, "cassandra", "cassandra").await;

    info!("Creating a role alice with VECTOR_SEARCH_INDEXING and CDC permissions");
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
        .expect("failed to grant VECTOR_SEARCH_INDEXING to alice");

    info!("Waiting for vector-store ready state");
    assert!(actors.vs.wait_for_ready().await);

    info!("Creating keyspace and table");
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v1 VECTOR<FLOAT, 3>", None).await;

    info!("Creating index on column v1");
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v1")).await;

    info!("Waiting for index to be ready");
    for client in clients.iter() {
        wait_for_index(client, &index).await;
    }

    info!("Inserting data into CDC-enabled table");
    let stmt = session
        .prepare(format!("INSERT INTO {table} (pk, v1) VALUES (?, ?)"))
        .await
        .expect("failed to prepare insert statement");

    for i in 0..10 {
        let vector = vec![i as f32, (i + 1) as f32, (i + 2) as f32];
        session
            .execute_unpaged(&stmt, (i, vector))
            .await
            .expect("failed to insert row");
    }

    info!("Waiting for CDC to propagate data to index");
    wait_for(
        || async {
            let result = session
                .query_unpaged(
                    format!("SELECT * FROM {table} ORDER BY v1 ANN OF [0.0, 1.0, 2.0] LIMIT 5"),
                    (),
                )
                .await;
            match result {
                Ok(res) => res
                    .into_rows_result()
                    .map(|r| r.rows_num() == 5)
                    .unwrap_or(false),
                Err(_) => false,
            }
        },
        "Waiting for ANN query to return results",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("Dropping keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("Cleaning up");
    cleanup(actors).await;

    info!("finished");
}
