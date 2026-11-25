/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::create_config_channels;
use crate::db_basic;
use crate::db_basic::Index;
use crate::db_basic::TOKEN_MIN;
use crate::db_basic::Table;
use crate::vs_index::usearch_test_config;
use httpapi::NodeStatus;
use httpclient::HttpClient;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::routing::Token;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;
use vector_store::Config;
use vector_store::Connectivity;
use vector_store::DbIndexPartitioning;
use vector_store::DbRow;
use vector_store::DbValue;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::HttpServerExt;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsVs;
use vector_store::NonemptyArc;
use vector_store::Quantization;
use vector_store::SpaceType;

#[tokio::test]
#[cfg_attr(not(feature = "in-progress"), ignore = "requires in-progress")]
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

    let db = db_basic::new();

    let index = IndexMetadata {
        keyspace_name: "ksp".into(),
        primary_key_columns: NonemptyArc::new(["pk"]).unwrap(),
        partition_key_count: NonZeroUsize::new(1).unwrap(),
        table_name: "tbl".into(),
        index_name: "idx".into(),
        target_columns: NonemptyArc::new(["v"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new([]),
        alternator_attribute_types: Default::default(),
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
            columns: Arc::new(
                [("pk".into(), ColumnType::Native(NativeType::Int))]
                    .into_iter()
                    .collect(),
            ),
            dimensions: [(
                index.target_columns.first().clone(),
                index.vs().unwrap().dimensions,
            )]
            .into_iter()
            .collect(),
        },
    )
    .unwrap();

    let (tx_outer, rx_outer) = mpsc::channel(1);
    db.add_index(Index::new(index.clone()).with_range_scan(
        Token::new(TOKEN_MIN),
        Box::new({
            let mut rx_outer = Some(rx_outer);
            let db = db.clone();
            move || {
                let (tx, rx) = mpsc::channel(1);
                let Some(mut rx_outer) = rx_outer.take() else {
                    return rx;
                };
                let db = db.clone();

                tokio::spawn(async move {
                    let mut pk = 0;
                    while let Some(item) = rx_outer.recv().await {
                        pk += 1;
                        let (tx_in_progress, mut rx_in_progress) = mpsc::channel(1);
                        db.set_make_in_progress_guard(Some(Box::new({
                            let tx_in_progress = tx_in_progress.clone().downgrade();
                            move |_| tx_in_progress.upgrade()
                        })));
                        tx.send(Ok(DbRow {
                            columns: vec![
                                Some(DbValue::Value(CqlValue::Int(pk))),
                                Some(DbValue::Vector(item)),
                                Some(DbValue::Value(CqlValue::BigInt(10))),
                            ],
                        }))
                        .await
                        .unwrap();
                        // wait until in-progress marker is dropped
                        drop(tx_in_progress);
                        while rx_in_progress.recv().await.is_some() {}
                    }
                });
                rx
            }
        }),
    ))
    .unwrap();

    const LIMIT_MEMORY: u64 = 20 * 1024 * 1024; // 20 MB - it shouldn't be enough to index any vector
    let mut config = Config {
        memory_limit: Some(LIMIT_MEMORY),
        memory_usage_check_interval: Some(Duration::from_millis(10)),
        ..usearch_test_config()
    };

    let (receivers, senders) = create_config_channels(config.clone()).await;

    let (server, _mtls) = vector_store::run(db, receivers).await.unwrap();
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
