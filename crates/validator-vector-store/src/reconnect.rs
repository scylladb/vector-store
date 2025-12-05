/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use vector_search_validator_tests::ScyllaClusterExt;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "reconnect_doesnt_break_fullscan",
            timeout,
            reconnect_doesnt_break_fullscan,
        )
        .with_test(
            "restarting_one_node_doesnt_break_fullscan",
            timeout,
            restarting_one_node_doesnt_break_fullscan,
        )
        .with_test(
            "restarting_all_nodes_doesnt_break_fullscan",
            timeout,
            restarting_all_nodes_doesnt_break_fullscan,
        )
}

async fn reconnect_doesnt_break_fullscan(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        Some("CDC = {'enabled': true}"),
    )
    .await;

    let stmt = session
        .prepare(format!(
            "INSERT INTO {table} (id, embedding) VALUES (?, [1.0, 2.0, 3.0])"
        ))
        .await
        .expect("failed to prepare a statement");

    for id in 0..1000 {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

    let index = create_index(&session, &clients, &table, "embedding").await;

    let result = session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 1"),
            (),
        )
        .await;

    match &result {
        Err(e) if format!("{e:?}").contains("503 Service Unavailable") => {}
        _ => panic!("Expected SERVICE_UNAVAILABLE error, got: {result:?}"),
    }

    actors.db.down().await;

    sleep(Duration::from_secs(1)).await;
    for client in &clients {
        let status = client
            .index_status(&index.keyspace, &index.index)
            .await
            .expect("failed to get index status")
            .status;
        assert!(
            status == IndexStatus::Bootstrapping,
            "Full scan should be interrupted by disconnect"
        );
    }
    actors
        .db
        .up(get_default_scylla_node_configs(&actors).await, None)
        .await;

    assert!(actors.db.wait_for_ready().await);

    wait_for(
        || async {
            session
                .query_unpaged(
                    format!(
                        "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 1"
                    ),
                    (),
                )
                .await
                .is_ok()
        },
        "Waiting for index build",
        Duration::from_secs(20),
    )
    .await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn restarting_one_node_doesnt_break_fullscan(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        None,
    )
    .await;

    let stmt = session
        .prepare(format!(
            "INSERT INTO {table} (id, embedding) VALUES (?, [1.0, 2.0, 3.0])"
        ))
        .await
        .expect("failed to prepare a statement");

    for id in 0..1000 {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

    let results = get_query_results(format!("SELECT * FROM {table}"), &session).await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 1000);

    // Flush to disk to ensure data is persisted before restarting nodes
    actors.db.flush().await;

    let index = create_index(&session, &clients, &table, "embedding").await;

    for client in &clients {
        let index_status = client
            .index_status(&index.keyspace, &index.index)
            .await
            .expect("failed to get index status");
        assert_eq!(index_status.status, IndexStatus::Bootstrapping);
    }

    let node_configs = get_default_scylla_node_configs(&actors).await;
    let node_config = node_configs.first().unwrap();
    info!("Restarting node {}", node_config.db_ip);
    actors.db.restart(node_config).await;
    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 1000,
            "Expected 1000 vectors to be indexed"
        );
    }

    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to query ANN search");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn restarting_all_nodes_doesnt_break_fullscan(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        None,
    )
    .await;

    let stmt = session
        .prepare(format!(
            "INSERT INTO {table} (id, embedding) VALUES (?, [1.0, 2.0, 3.0])"
        ))
        .await
        .expect("failed to prepare a statement");

    for id in 0..1000 {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

    let results = get_query_results(format!("SELECT * FROM {table}"), &session).await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 1000);

    // Flush to disk to ensure data is persisted before restarting nodes
    actors.db.flush().await;

    let index = create_index(&session, &clients, &table, "embedding").await;

    for client in &clients {
        let index_status = client
            .index_status(&index.keyspace, &index.index)
            .await
            .expect("failed to get index status");
        assert_eq!(index_status.status, IndexStatus::Bootstrapping);
    }

    let node_configs = get_default_scylla_node_configs(&actors).await;

    // Restart each node one by one
    for node_config in &node_configs {
        info!("Restarting node {}", node_config.db_ip);
        actors.db.restart(node_config).await;
    }

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 1000,
            "Expected 1000 vectors to be indexed"
        );
    }

    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to query ANN search");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
