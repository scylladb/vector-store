/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use httpclient::HttpClient;
use scylla::client::session_builder::SessionBuilder;
use std::sync::Arc;
use sysinfo::System;
use tracing::info;
use vector_search_validator_tests::common::IndexStatus;
use vector_search_validator_tests::*;

pub(crate) async fn new() -> TestCase {
    let timeout = common::DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_cleanup(timeout, common::cleanup)
        .with_test(
            "memory_limit_during_index_build",
            timeout,
            memory_limit_during_index_build,
        )
}

/// The test case scenario:
/// - start scylla cluster
/// - create a keyspace and a table with 100_0000 vectors
/// - check used memory - setup limit memory as `used memory + 20MB`
/// - start the vector-store with the memory limit
/// - start building an index
/// - vector-store should finish building an index, but not all vectors should be stored -
///   vector-store should reach the memory limit and discard some vectors
/// - drop the keyspace
async fn memory_limit_during_index_build(actors: TestActors) {
    info!("started");

    // Start DB
    let vs_ip = actors.services_subnet.ip(common::VS_OCTET_1);
    actors
        .dns
        .upsert(common::VS_NAMES[0].to_string(), vs_ip)
        .await;
    let node_configs = common::get_default_scylla_node_configs(&actors).await;
    let db_ip = node_configs.first().unwrap().db_ip;
    actors.db.start(node_configs.clone(), None).await;
    assert!(actors.db.wait_for_ready().await);

    // Create table
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(db_ip.to_string())
            .build()
            .await
            .expect("failed to create session"),
    );

    let keyspace = common::create_keyspace(&session).await;
    let table =
        common::create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert some vectors
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..100000 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i, &embedding),
            )
            .await
            .expect("failed to insert data");
    }

    // Start VS with memory limit
    let system_info = System::new_all();
    let used_memory = if let Some(cgroup) = system_info.cgroup_limits() {
        cgroup.rss
    } else {
        system_info.used_memory()
    };

    const LIMIT_MEMORY: u64 = 20 * 1024 * 1024; // 20 MB - it shouldn't be enough to index all vectors
    let limit_memory = used_memory + 100 * 1024 * 1024;
    info!(
        "Setting VS memory limit to {LIMIT_MEMORY} bytes, current used memory is {used_memory} bytes, "
    );

    actors
        .vs
        .start(
            (vs_ip, common::VS_PORT).into(),
            (db_ip, common::DB_PORT).into(),
            [
                (
                    "VECTOR_STORE_MEMORY_LIMIT".to_string(),
                    limit_memory.to_string(),
                ),
                (
                    "VECTOR_STORE_MEMORY_USAGE_CHECK_INTERVAL".to_string(),
                    "10ms".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        )
        .await;

    assert!(actors.vs.wait_for_ready().await);

    // Create index
    let client = HttpClient::new(
        (
            actors.services_subnet.ip(common::VS_OCTET_1),
            common::VS_PORT,
        )
            .into(),
    );

    let index = common::create_index(&session, &client, &table, "v").await;

    let index_status = common::wait_for_index(&client, &index).await;
    assert_eq!(
        index_status.status,
        IndexStatus::Serving,
        "Expected index status to be Serving after indexing is complete"
    );

    info!("Current used memory is {used_memory} bytes, limit memory is {limit_memory} bytes",);

    assert!(
        index_status.count < 100000,
        "Expected less than 10000 vectors to be indexed"
    );

    // Drop keyspace
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
