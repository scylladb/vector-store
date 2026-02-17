/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use scylla::statement::Statement;
use std::time::Duration;
use tracing::info;
use vector_search_validator_tests::FirewallExt;
use vector_search_validator_tests::ScyllaNodeConfig;
use vector_search_validator_tests::common::*;
use vector_search_validator_tests::*;

const VS_OCTET_4: u8 = 24;
const VS_OCTET_5: u8 = 25;
const VS_OCTET_6: u8 = 26;

const VS_NAMES_EXTENDED: [&str; 6] = ["vs1", "vs2", "vs3", "vs4", "vs5", "vs6"];

/// Number of ANN queries to run after each VS node is stopped.
const ANN_QUERY_BATCH_SIZE: usize = 5;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_cleanup(timeout, cleanup)
        .with_test(
            "secondary_uri_works_correctly",
            timeout,
            test_secondary_uri_works_correctly,
        )
        .with_test(
            "ann_queries_succeed_when_stopping_vs_nodes_across_azs",
            timeout,
            test_ann_queries_succeed_when_stopping_vs_nodes_across_azs,
        )
}

#[framed]
async fn test_secondary_uri_works_correctly(actors: TestActors) {
    info!("started");

    let vs_urls = get_default_vs_urls(&actors).await;
    let db_ips = get_default_db_ips(&actors);
    let vs_url = &vs_urls[0];

    let scylla_configs: Vec<ScyllaNodeConfig> = vec![
        ScyllaNodeConfig {
            db_ip: db_ips[0],
            primary_vs_uris: vec![vs_url.clone()],
            secondary_vs_uris: vec![],
            args: default_scylla_args(),
            config: None,
        },
        ScyllaNodeConfig {
            db_ip: db_ips[1],
            primary_vs_uris: vec![],
            secondary_vs_uris: vec![vs_url.clone()],
            args: default_scylla_args(),
            config: None,
        },
        ScyllaNodeConfig {
            db_ip: db_ips[2],
            primary_vs_uris: vec![],
            secondary_vs_uris: vec![vs_url.clone()],
            args: default_scylla_args(),
            config: None,
        },
    ];
    let vs_configs = vec![VectorStoreNodeConfig {
        vs_ip: actors.services_subnet.ip(VS_OCTET_1),
        db_ip: actors.services_subnet.ip(DB_OCTET_1),
        envs: Default::default(),
        user: None,
        password: None,
    }];
    init_with_config(actors.clone(), scylla_configs, vs_configs).await;

    let vs_ips = vec![actors.services_subnet.ip(VS_OCTET_1)];
    let (session, clients) = prepare_connection_with_custom_vs_ips(&actors, vs_ips).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert vectors
    for i in 0..100 {
        let embedding = vec![i as f32, (i * 2) as f32, (i * 3) as f32];
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
            index_status.count, 100,
            "Expected 100 vectors to be indexed"
        );
    }

    // Down the first node with primary URI
    let first_node_ip = actors.services_subnet.ip(DB_OCTET_1);
    info!("Bringing down node {first_node_ip}");
    actors.db.down_node(first_node_ip).await;

    // Should work via secondary URIs
    let results = get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;

    let rows = results
        .rows::<(i32,)>()
        .expect("failed to get rows after node down");
    assert!(
        rows.rows_remaining() <= 10,
        "Expected at most 10 results from ANN query after node down"
    );

    // Drop keyspace
    session
        .query_unpaged(
            {
                let mut stmt = Statement::new(format!("DROP KEYSPACE IF EXISTS {keyspace}"));
                stmt.set_is_idempotent(true);
                stmt
            },
            (),
        )
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Reproduces a bug where ANN search queries fail with "Vector Store request
/// was aborted" when VS nodes are progressively stopped across availability
/// zones.
///
/// Setup:
///   - 3 DB nodes, one per AZ
///   - 6 VS nodes, 2 per AZ
///   - Each DB node has primary VS URIs pointing to VS nodes in its AZ and
///     secondary URIs pointing to VS nodes in other AZs.
///
/// Scenario:
///   1. Block traffic to first VS node in AZ1 via firewall
///   2. Block traffic to second VS node in AZ1 (AZ1 has no VS nodes)
///   3. Block traffic to first VS node in AZ2
///   After each step, verify that ANN search queries still succeed.
#[framed]
async fn test_ann_queries_succeed_when_stopping_vs_nodes_across_azs(actors: TestActors) {
    info!("started");

    let vs_urls = get_default_vs_urls(&actors).await;
    let vs_ips = get_default_vs_ips(&actors);
    let db_ips = get_default_db_ips(&actors);
    // let vs_url = &vs_urls[0];
    // let db_ip = actors.services_subnet.ip(DB_OCTET_1);

    let scylla_configs: Vec<ScyllaNodeConfig> = db_ips
        .iter()
        .map(|&db_ip| ScyllaNodeConfig {
            db_ip,
            primary_vs_uris: vec![vs_urls[0].clone(), vs_urls[1].clone()],
            secondary_vs_uris: vec![vs_urls[2].clone()],
            args: default_scylla_args(),
            config: None,
        })
        .collect();

    let vs_configs: Vec<VectorStoreNodeConfig> = vs_ips
        .iter()
        .zip(db_ips.iter())
        .map(|(&vs_ip, &db_ip)| VectorStoreNodeConfig {
            vs_ip,
            db_ip,
            envs: Default::default(),
            user: None,
            password: None,
        })
        .collect();
    init_with_config(actors.clone(), scylla_configs, vs_configs).await;

    // Create session and HTTP clients for all VS nodes
    let (session, clients) = prepare_connection_with_custom_vs_ips(&actors, vs_ips.to_vec()).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert one vector
    let embedding = vec![1.0f32, 2.0f32, 3.0f32];
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
            (1, &embedding),
        )
        .await
        .expect("failed to insert data");

    let index = create_index(&session, &clients, &table, "v").await;

    // Wait for index to be SERVING on all VS nodes
    for client in &clients {
        wait_for_index(client, &index).await;
    }

    let ann_query = format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10");

    // Verify baseline: ANN search works before any node failures
    let result = session.query_unpaged(ann_query.clone(), {}).await;

    assert!(
        result.is_ok(),
        "Baseline ANN query failed before blocking any VS nodes: {:?}",
        result.err(),
    );

    info!(
        "Step 1: Blocking traffic to first VS primary node: {}",
        vs_ips[0]
    );
    actors.firewall.drop_traffic(vec![vs_ips[0]]).await;

    for i in 0..ANN_QUERY_BATCH_SIZE {
        let result = session.query_unpaged(ann_query.clone(), {}).await;
        assert!(result.is_ok(), "ANN query {i} failed: {:?}", result.err());
    }

    info!(
        "Step 2: Blocking traffic to second primary VS node: {}",
        vs_ips[1]
    );
    actors
        .firewall
        .drop_traffic(vec![vs_ips[0], vs_ips[1]])
        .await;

    for i in 0..ANN_QUERY_BATCH_SIZE {
        let result = session.query_unpaged(ann_query.clone(), {}).await;
        assert!(result.is_ok(), "ANN query {i} failed: {:?}", result.err());
    }

    // Cleanup: Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE IF EXISTS {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
