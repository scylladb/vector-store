/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::usearch;
use crate::usearch::test_config;
use crate::wait_for;
use scylla::cluster::metadata::NativeType;
use vector_store::DbIndexType;
use vector_store::httproutes::NodeStatus;

#[tokio::test]
async fn status_is_serving_after_creation() {
    crate::enable_tracing();
    let (_index, client, _db, _server, _node_state) = usearch::setup_store_and_wait_for_index(
        DbIndexType::Global,
        ["pk".into(), "ck".into()],
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        [],
    )
    .await;

    let result = client.status().await;
    assert_eq!(result.unwrap(), NodeStatus::Serving);
}

#[tokio::test]
async fn status_is_bootstrapping_while_discovering_indexes() {
    crate::enable_tracing();
    let (run, _index, db, _node_state) = usearch::setup_store(
        test_config(),
        DbIndexType::Global,
        ["pk".into(), "ck".into()],
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        [],
    )
    .await;
    db.simulate_endless_get_indexes_processing();
    let (client, _server, _config_tx) = run.await;

    // assert that status is Bootstrapping while indexes are being discovered
    wait_for(
        || async { client.status().await.unwrap() == NodeStatus::Bootstrapping },
        "Waiting for status to be Bootstrapping",
    )
    .await;
}
