/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::create_config_channels;
use crate::db_basic;
use crate::db_basic::Table;
use crate::usearch::test_config;
use httpapi::NodeStatus;
use httpclient::HttpClient;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;
use tracing::info;
use uuid::Uuid;
use vector_store::Config;
use vector_store::Connectivity;
use vector_store::DbIndexType;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::HttpServerExt;
use vector_store::IndexMetadata;
use vector_store::Quantization;
use vector_store::SpaceType;
use vector_store::Timestamp;

#[tokio::test]
/// The test case scenario:
/// - start scylla cluster
/// - create a keyspace and a table with 1_000 vectors
/// - check used memory - setup limit memory as `used memory + 20MB`
/// - start the vector-store with the memory limit
/// - start building an index
/// - vector-store should finish building an index, but not all vectors should be stored -
///   vector-store should reach the memory limit and discard some vectors
async fn memory_limit_during_index_build() {
    crate::enable_tracing();

    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();

    let (db_actor, db) = db_basic::new(node_state.clone());

    let index = IndexMetadata {
        keyspace_name: "ksp".into(),
        table_name: "tbl".into(),
        index_name: "idx".into(),
        target_column: "v".into(),
        index_type: DbIndexType::Global,
        filtering_columns: Arc::new(Vec::new()),
        dimensions: NonZeroUsize::new(3).unwrap().into(),
        connectivity: Connectivity::default(),
        expansion_add: ExpansionAdd::default(),
        expansion_search: ExpansionSearch::default(),
        space_type: SpaceType::default(),
        version: Uuid::new_v4().into(),
        quantization: Quantization::default(),
    };

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: Arc::new(vec!["pk".into()]),
            partition_key_count: 1,
            columns: Arc::new([("pk".into(), NativeType::Int)].into_iter().collect()),
            dimensions: [(index.target_column.clone(), index.dimensions)]
                .into_iter()
                .collect(),
        },
    )
    .unwrap();

    const VECTOR_COUNT: i32 = 1_000;
    db.add_index(
        index.clone(),
        Some(db_basic::scan_fn((0..VECTOR_COUNT).map(|i| {
            (
                [CqlValue::Int(i)].into(),
                Some(vec![0.0, 0.0, 0.0].into()),
                Timestamp::from_unix_timestamp(10),
            )
        }))),
        None,
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

    let config = Config {
        memory_limit: Some(limit_memory),
        memory_usage_check_interval: Some(Duration::from_millis(10)),
        ..test_config()
    };

    let (receivers, _senders) = create_config_channels(config).await;
    let index_factory = vector_store::new_index_factory_usearch(receivers.config.clone()).unwrap();

    let node_state = node_state.clone();
    let (server, _mtls) =
        vector_store::run(node_state, db_actor, internals, index_factory, receivers)
            .await
            .unwrap();
    let addr = (*server.address().await.borrow()).unwrap();

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
            .index_status(&index.keyspace_name.into(), &index.index_name.into())
            .await
            .unwrap()
            .count
            < VECTOR_COUNT as usize,
        "Expected less than {VECTOR_COUNT} vectors to be indexed"
    );
}
