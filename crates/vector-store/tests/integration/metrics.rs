/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::usearch;
use crate::wait_for;
use httpclient::HttpClient;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use tokio::sync::mpsc::Sender;
use vector_store::DbIndexPartitioning;
use vector_store::IndexMetadata;
use vector_store::Timestamp;
use vector_store::node_state::NodeState;

async fn setup_single_vector_index() -> (
    IndexMetadata,
    HttpClient,
    DbBasic,
    impl Sized,
    Sender<NodeState>,
) {
    usearch::setup_store_and_wait_for_index(
        DbIndexPartitioning::Global,
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        Some(db_basic::scan_fn_vectors([(
            [CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
            Some(vec![1., 1., 1.].into()),
            [].into(),
            Timestamp::from_millis(10),
        )])),
        None,
        Some(1),
    )
    .await
}

#[tokio::test]
async fn index_labels_present_in_metrics_endpoint() {
    crate::enable_tracing();

    let (index, client, _db, _server, _node_state) = setup_single_vector_index().await;

    let expected_labels = format!(
        r#"index_name="{}",keyspace="{}""#,
        index.index_name, index.keyspace_name,
    );
    wait_for(
        || async { client.get_metrics_text().await.contains(&expected_labels) },
        "Waiting for index labels to appear in /metrics",
    )
    .await;
}

#[tokio::test]
async fn deleted_index_labels_absent_from_metrics_endpoint() {
    crate::enable_tracing();

    let (index, client, db, _server, _node_state) = setup_single_vector_index().await;

    let expected_labels = format!(
        r#"index_name="{}",keyspace="{}""#,
        index.index_name, index.keyspace_name,
    );
    wait_for(
        || async { client.get_metrics_text().await.contains(&expected_labels) },
        "Waiting for index labels to appear in /metrics",
    )
    .await;

    db.del_index(&index.keyspace_name, &index.index_name)
        .unwrap();

    wait_for(
        || async { !client.get_metrics_text().await.contains(&expected_labels) },
        "Waiting for deleted index labels to disappear from /metrics",
    )
    .await;
}
