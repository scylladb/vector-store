/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common::*;
use async_backtrace::framed;
use e2etest::TestCase;
use e2etest_firewall::FirewallExt;
use e2etest_scylla_cluster::ScyllaClusterExt;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use e2etest_vector_store_cluster::VectorStoreClusterExt;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use std::time::Duration;
use tap::Pipe;
use tracing::info;
use vector_store::httproutes::IndexStatus;

const FRAME_DELAY: Duration = Duration::from_millis(100);
const DATASET_SIZE: i32 = 100;
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(12); // slightly more than default 10s

#[framed]
pub(crate) async fn new() -> TestCase<TestActors> {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init_with_proxy_single_vs)
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
        .with_test(
            "restarting_vs_cluster_does_not_break_setup",
            timeout,
            test_restarting_vs_cluster_does_not_break_setup,
        )
}

#[framed]
async fn reconnect_doesnt_break_fullscan(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().unwrap();

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

    info!("Inserting data into the table");
    for id in 0..DATASET_SIZE {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

    info!("Restart internals counter for cdc handler errors");
    client.internals_clear_counters().await.unwrap();
    client
        .internals_start_counter("session-create-success".to_string())
        .await
        .unwrap();
    client
        .internals_start_counter("session-create-failure".to_string())
        .await
        .unwrap();

    info!("Slow communication between vector-store and scylla using proxy");
    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    info!("Creating index");
    let index = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;

    info!("Checking that full scan isn't completed");
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

    info!("Disconnect scylla-proxy");
    actors
        .firewall
        .drop_traffic(get_default_db_proxy_ips(&actors))
        .await;

    info!(
        "counters: {:?}, {:?}",
        client.internals_counters().await.unwrap(),
        client.internals_session_counters().await.unwrap()
    );
    wait_for(
        || async {
            if let Ok(counters) = client.internals_counters().await {
                if let Some(counter) = counters.get("session-create-success")
                    && *counter > 0
                {
                    return true;
                }
                if let Some(counter) = counters.get("session-create-failure")
                    && *counter > 0
                {
                    return true;
                }
            }
            if let Ok(counters) = client.internals_session_counters().await
                && let Some(counter) = counters.get("total-connections")
                && *counter == 0
            {
                return true;
            }
            false
        },
        "Connection must be closed",
        KEEPALIVE_TIMEOUT,
    )
    .await;
    info!(
        "counters: {:?}, {:?}",
        client.internals_counters().await.unwrap(),
        client.internals_session_counters().await.unwrap()
    );

    info!("Check index status is still BOOTSTRAPPING");
    let status = client
        .index_status(&index.keyspace, &index.index)
        .await
        .expect("failed to get index status")
        .status;
    assert!(
        status == IndexStatus::Bootstrapping,
        "Full scan should be interrupted by disconnect"
    );

    info!("Reconnect scylla-proxy");
    actors.firewall.turn_off_rules().await;

    info!("Disable rules on scylla-proxy");
    actors.db_proxy.turn_off_rules().await;

    wait_for(
        || async {
            info!("counters: {:?}", client.internals_session_counters().await);
            info!(
                "status: {:?}",
                client.index_status(&index.keyspace, &index.index).await
            );
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
        "index must be built",
        Duration::from_secs(60),
    )
    .await;

    info!("Dropping keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn restarting_one_node_doesnt_break_fullscan(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().unwrap();

    info!("Creating a table and inserting data");
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

    for id in 0..DATASET_SIZE {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

    let results = get_query_results(format!("SELECT * FROM {table}"), &session).await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), DATASET_SIZE as usize);

    // Flush to disk to ensure data is persisted before restarting nodes
    actors.db.flush().await;

    info!("Slow communication between vector-store and scylla using proxy");
    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    info!("Creating index");
    let index = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;

    info!("Checking that full scan isn't completed");
    wait_for(
        || async {
            client
                .index_status(&index.keyspace, &index.index)
                .await
                .expect("failed to get index status")
                .status
                == IndexStatus::Bootstrapping
        },
        format!("index {index:?} must be bootstrapping"),
        Duration::from_secs(10),
    )
    .await;

    let proxy_addr = get_default_scylla_proxy_node_configs(&actors)
        .await
        .first()
        .unwrap()
        .proxy_addr;
    let total_connections = *client
        .internals_session_counters()
        .await
        .unwrap()
        .get("total-connections")
        .unwrap();

    info!("Disconnect scylla-proxy {proxy_addr}");
    actors.firewall.drop_traffic(vec![proxy_addr]).await;

    wait_for(
        || async {
            let counters = client.internals_session_counters().await;
            info!("session counters: {:?}", counters);
            *counters.unwrap().get("total-connections").unwrap() < total_connections
        },
        format!("connections to {proxy_addr} must be closed"),
        KEEPALIVE_TIMEOUT,
    )
    .await;

    info!("Reconnect scylla-proxy");
    actors.firewall.turn_off_rules().await;

    info!("Disable rules on scylla-proxy");
    actors.db_proxy.turn_off_rules().await;

    info!("Waiting for all vectors to be indexed");
    let index_status = wait_for_index(client, &index).await;
    assert_eq!(
        index_status.count, DATASET_SIZE as usize,
        "Expected {DATASET_SIZE} vectors to be indexed"
    );

    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to query ANN search");

    info!("Dropping keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn restarting_all_nodes_doesnt_break_fullscan(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().unwrap();

    info!("Creating a table and inserting data");
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

    for id in 0..DATASET_SIZE {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

    let results = get_query_results(format!("SELECT * FROM {table}"), &session).await;
    let rows = results
        .rows::<(i32, Vec<f32>)>()
        .expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), DATASET_SIZE as usize);

    // Flush to disk to ensure data is persisted before restarting nodes
    actors.db.flush().await;

    info!("Slow communication between vector-store and scylla using proxy");
    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    info!("Creating index");
    let index = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;

    wait_for(
        || async {
            client
                .index_status(&index.keyspace, &index.index)
                .await
                .expect("failed to get index status")
                .status
                == IndexStatus::Bootstrapping
        },
        format!("index {index:?} must be bootstrapping"),
        Duration::from_secs(10),
    )
    .await;

    info!("Restart each node one by one");
    for proxy_addr in get_default_scylla_proxy_node_configs(&actors)
        .await
        .into_iter()
        .map(|config| config.proxy_addr)
    {
        let total_connections = *client
            .internals_session_counters()
            .await
            .unwrap()
            .get("total-connections")
            .unwrap();

        info!("Disconnect scylla-proxy {proxy_addr}");
        actors.firewall.drop_traffic(vec![proxy_addr]).await;

        wait_for(
            || async {
                let counters = client.internals_session_counters().await;
                info!("session counters: {:?}", counters);
                *counters.unwrap().get("total-connections").unwrap() < total_connections
            },
            format!("connections to {proxy_addr} must be closed"),
            KEEPALIVE_TIMEOUT,
        )
        .await;

        let total_connections = *client
            .internals_session_counters()
            .await
            .unwrap()
            .get("total-connections")
            .unwrap();

        info!("Reconnect scylla-proxy");
        actors.firewall.turn_off_rules().await;

        wait_for(
            || async {
                let counters = client.internals_session_counters().await;
                info!("session counters: {:?}", counters);
                *counters.unwrap().get("total-connections").unwrap() > total_connections
            },
            format!("connections to {proxy_addr} must be opened"),
            KEEPALIVE_TIMEOUT,
        )
        .await;
    }

    info!("Disable rules on scylla-proxy");
    actors.db_proxy.turn_off_rules().await;

    info!("Waiting for all vectors to be indexed");
    let index_status = wait_for_index(client, &index).await;
    assert_eq!(
        index_status.count, DATASET_SIZE as usize,
        "Expected {DATASET_SIZE} vectors to be indexed"
    );

    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to query ANN search");

    info!("Dropping keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[framed]
async fn test_restarting_vs_cluster_does_not_break_setup(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().unwrap();

    info!("Creating a table and inserting data");
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

    for id in 0..DATASET_SIZE {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

    info!("Slow communication between vector-store and scylla using proxy");
    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    info!("Creating index");
    let index = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;

    info!("Stopping VS cluster");
    actors.vs.stop().await;

    info!("Disable rules on scylla-proxy");
    actors.db_proxy.turn_off_rules().await;

    actors
        .vs
        .start(get_proxy_vs_node_configs(&actors).pipe(|mut nodes| {
            let translation_map = get_proxy_translation_map(&actors);
            for node in nodes.iter_mut() {
                node.envs.insert(
                    "VECTOR_STORE_CQL_URI_TRANSLATION_MAP".to_string(),
                    serde_json::to_string(&translation_map).unwrap(),
                );
            }
            nodes.truncate(1);
            nodes
        }))
        .await;

    info!("Waiting for all vectors to be indexed");
    let index_status = wait_for_index(client, &index).await;
    assert_eq!(
        index_status.count, DATASET_SIZE as usize,
        "Expected {DATASET_SIZE} vectors to be indexed"
    );

    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to query ANN search");

    info!("Dropping keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
