/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::create_config_channels;
use crate::db_basic;
use crate::db_basic::Table;
use crate::usearch::test_config;
use futures::FutureExt;
use httpapi::NodeStatus;
use httpclient::HttpClient;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;
use vector_store::AsyncInProgress;
use vector_store::Config;
use vector_store::Connectivity;
use vector_store::DbIndexPartitioning;
use vector_store::DbIndexedOperation;
use vector_store::DbIndexedRow;
use vector_store::DbIndexedValue;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::HttpServerExt;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsVs;
use vector_store::NonemptyArc;
use vector_store::NonemptyBox;
use vector_store::Quantization;
use vector_store::SpaceType;
use vector_store::Timestamp;
use vector_store::Timestamped;

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
        target_columns: NonemptyArc::new(["v"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new([]),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Vs(IndexOptionsVs {
            dimensions: NonZeroUsize::new(3).unwrap().into(),
            connectivity: Connectivity::default(),
            expansion_add: ExpansionAdd::default(),
            expansion_search: ExpansionSearch::default(),
            space_type: SpaceType::default(),
            quantization: Quantization::default(),
        }),
    };

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: NonemptyArc::new(["pk"]).unwrap(),
            partition_key_count: 1,
            columns: Arc::new([("pk".into(), NativeType::Int)].into_iter().collect()),
            dimensions: [(
                index.target_columns.first().clone(),
                index.vs().unwrap().dimensions,
            )]
            .into_iter()
            .collect(),
        },
    )
    .unwrap();

    let (tx_outer, mut rx_outer) = mpsc::channel(1);
    db.add_index(
        index.clone(),
        Some(Box::new(move |tx_inner| {
            async move {
                let mut pk = 0;
                while let Some(item) = rx_outer.recv().await {
                    pk += 1;
                    let (tx_in_progress, mut rx_in_progress) = mpsc::channel(1);
                    tx_inner
                        .send((
                            DbIndexedRow {
                                primary_key: [CqlValue::Int(pk)].into(),
                                operation: DbIndexedOperation::Upsert(
                                    NonemptyBox::new([Timestamped::new(
                                        Timestamp::from_millis(10),
                                        Some(DbIndexedValue::Vector(item)),
                                    )])
                                    .unwrap(),
                                ),
                            },
                            AsyncInProgress::Fullscan(tx_in_progress.clone()),
                        ))
                        .await
                        .unwrap();
                    // wait until in-progress marker is dropped
                    drop(tx_in_progress);
                    while rx_in_progress.recv().await.is_some() {}
                }
            }
            .boxed()
        })),
        None,
    )
    .unwrap();

    const LIMIT_MEMORY: u64 = 20 * 1024 * 1024; // 20 MB - it shouldn't be enough to index any vector
    let mut config = Config {
        memory_limit: Some(LIMIT_MEMORY),
        memory_usage_check_interval: Some(Duration::from_millis(10)),
        ..test_config()
    };

    let (receivers, senders) = create_config_channels(config.clone()).await;
    let index_factory = vector_store::new_index_factory_usearch(receivers.config.clone()).unwrap();

    let node_state = node_state.clone();
    let (server, _mtls) =
        vector_store::run(node_state, db_actor, internals, index_factory, receivers)
            .await
            .unwrap();
    let addr = (*server.address().await.borrow()).unwrap();

    let client = HttpClient::new(addr);

    info!("Waiting for index to be bootstrapping");
    crate::wait_for(
        || async {
            client
                .status()
                .await
                .is_ok_and(|status| status == NodeStatus::Bootstrapping)
        },
        "Waiting for index to be bootstrapping",
    )
    .await;

    const VECTOR_COUNT: usize = 10;

    info!("Send vectors to be indexed - they shouldn't be indexed, because of the memory limit");
    for i in 0..VECTOR_COUNT {
        tx_outer
            .send(vec![i as f32, i as f32, i as f32].into())
            .await
            .unwrap();
    }

    let keyspace_name = index.keyspace_name.into();
    let index_name = index.index_name.into();
    assert_eq!(
        client
            .index_status(&keyspace_name, &index_name)
            .await
            .unwrap()
            .count,
        0,
        "Expected all vectors not to be indexed"
    );

    info!("Remove memory limit - it should allow to index all vectors");
    client
        .internals_start_counter("memory-usage-below-limit".to_string())
        .await
        .unwrap();
    config.memory_limit = None;
    senders.config.send(Arc::new(config)).unwrap();
    crate::wait_for(
        || async {
            client.internals_counters().await.is_ok_and(|map| {
                map.get("memory-usage-below-limit")
                    .is_some_and(|counter| *counter == 1)
            })
        },
        "Waiting for memory usage to be below limit after removing the memory limit",
    )
    .await;

    info!("Send vectors to be indexed - they should be indexed");
    for i in 0..VECTOR_COUNT {
        tx_outer
            .send(vec![i as f32, i as f32, i as f32].into())
            .await
            .unwrap();
    }

    info!("Waiting for index to be serving");
    drop(tx_outer);
    crate::wait_for(
        || async {
            client
                .status()
                .await
                .is_ok_and(|status| status == NodeStatus::Serving)
        },
        "Waiting for index to be serving",
    )
    .await;

    assert_eq!(
        client
            .index_status(&keyspace_name, &index_name)
            .await
            .unwrap()
            .count,
        VECTOR_COUNT,
        "Expected only last part of vectors to be indexed"
    );
}
