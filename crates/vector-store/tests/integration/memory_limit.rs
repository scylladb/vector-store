/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_basic;
use crate::db_basic::Index;
use crate::db_basic::Table;
use ::time::OffsetDateTime;
use httpclient::HttpClient;
use scylla::value::CqlValue;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;
use tokio::sync::watch;
use tracing::info;
use uuid::Uuid;
use vector_store::Config;
use vector_store::Connectivity;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::IndexMetadata;
use vector_store::SpaceType;
use vector_store::httproutes::NodeStatus;

#[tokio::test]
/// The test case scenario:
/// - start scylla cluster
/// - create a keyspace and a table with 1_000_000 vectors
/// - check used memory - setup limit memory as `used memory + 20MB`
/// - start the vector-store with the memory limit
/// - start building an index
/// - vector-store should finish building an index, but not all vectors should be stored -
///   vector-store should reach the memory limit and discard some vectors
async fn memory_limit_during_index_build() {
    crate::enable_tracing();

    let node_state = vector_store::new_node_state().await;

    let (db_actor, db) = db_basic::new(node_state.clone());

    let index = IndexMetadata {
        keyspace_name: "ksp".to_string().into(),
        table_name: "tbl".to_string().into(),
        index_name: "idx".to_string().into(),
        target_column: "v".to_string().into(),
        dimensions: NonZeroUsize::new(3).unwrap().into(),
        connectivity: Connectivity::default(),
        expansion_add: ExpansionAdd::default(),
        expansion_search: ExpansionSearch::default(),
        space_type: SpaceType::default(),
        version: Uuid::new_v4().into(),
    };

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: vec!["pk".to_string().into()],
            dimensions: [(index.target_column.clone(), index.dimensions)]
                .into_iter()
                .collect(),
        },
    )
    .unwrap();

    db.add_index(
        &index.keyspace_name,
        index.index_name.clone(),
        Index {
            table_name: index.table_name.clone(),
            target_column: index.target_column.clone(),
            connectivity: index.connectivity,
            expansion_add: index.expansion_add,
            expansion_search: index.expansion_search,
            space_type: index.space_type,
        },
    )
    .unwrap();

    const VECTOR_COUNT: i32 = 1_000_000;
    db.insert_values(
        &index.keyspace_name,
        &index.table_name,
        &index.target_column,
        (0..VECTOR_COUNT).map(|i| {
            (
                vec![CqlValue::Int(i)].into(),
                Some(vec![0.0, 0.0, 0.0].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            )
        }),
    )
    .unwrap();

    // Set memory limit for Vector Store
    let mut system_info = System::new_all();
    system_info.refresh_memory();
    let used_memory = if let Some(cgroup) = system_info.cgroup_limits() {
        cgroup.rss
    } else {
        system_info.used_memory()
    };

    const LIMIT_MEMORY: u64 = 20 * 1024 * 1024; // 20 MB - it shouldn't be enough to index all vectors
    let limit_memory = used_memory + LIMIT_MEMORY;
    info!(
        "Setting VS memory limit to {LIMIT_MEMORY} bytes, current used memory is {used_memory} bytes, "
    );

    let mut config = Config {
        memory_limit: Some(limit_memory),
        memory_usage_check_interval: Some(Duration::from_millis(10)),
        ..Default::default()
    };
    config.vector_store_addr = SocketAddr::from(([127, 0, 0, 1], 0));

    let (_config_tx, config_rx) = watch::channel(Arc::new(config));
    let index_factory = vector_store::new_index_factory_usearch(config_rx.clone()).unwrap();

    let node_state = node_state.clone();
    let (_server, addr) = vector_store::run(node_state, db_actor, index_factory, config_rx)
        .await
        .unwrap();

    let client = HttpClient::new(addr);

    crate::wait_for(
        || async {
            client
                .status()
                .await
                .ok()
                .map(|status| status == NodeStatus::Serving)
                .unwrap_or(false)
        },
        "Waiting for index to be build",
    )
    .await;

    assert!(
        client
            .index_status(&index.keyspace_name, &index.index_name)
            .await
            .unwrap()
            .count
            < VECTOR_COUNT as usize,
        "Expected less than {VECTOR_COUNT} vectors to be indexed"
    );
}
