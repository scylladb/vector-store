/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::fts;
use crate::vs_index;
use crate::wait_for;
use httpclient::HttpClient;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use vector_store::Config;
use vector_store::DbIndexPartitioning;
use vector_store::IndexMetadata;
use vector_store::Timestamp;
use vector_store::node_state::NodeState;

async fn setup_single_vector_index(
    config: Config,
) -> (
    IndexMetadata,
    HttpClient,
    DbBasic,
    impl Sized,
    Sender<NodeState>,
) {
    vs_index::setup_store_and_wait_for_index(
        config,
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

    let (index, client, _db, _server, _node_state) =
        setup_single_vector_index(vs_index::usearch_test_config()).await;

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

    let (index, client, db, _server, _node_state) =
        setup_single_vector_index(vs_index::usearch_test_config()).await;

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

#[tokio::test]
async fn fts_index_metrics_present_in_metrics_endpoint() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = fts::setup_fts_and_wait(
        [
            (vec![CqlValue::Int(1)], "hello world", 10),
            (vec![CqlValue::Int(2)], "foo bar", 20),
        ],
        2,
    )
    .await;

    let expected_labels = format!(r#"index_name="{index_name}",keyspace="{keyspace_name}""#);

    wait_for(
        || async {
            let metrics = client.get_metrics_text().await;
            metrics.contains(&format!("fts_index_size_bytes{{{expected_labels}}}"))
                && metrics.contains(&format!("fts_segment_count{{{expected_labels}}}"))
        },
        "Waiting for fts index metrics to appear in /metrics",
    )
    .await;
}

#[tokio::test]
async fn highlight_latency_recorded_under_highlight_operation() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        fts::setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    client
        .highlight(
            &keyspace_name,
            &index_name,
            "hello".into(),
            vec!["hello there".into()],
        )
        .await;
    let metrics = client.get_metrics_text().await;

    let index_labels = format!(r#"index_name="{index_name}",keyspace="{keyspace_name}""#);
    assert!(
        metrics.contains(&format!(
            r#"request_latency_seconds_count{{{index_labels},operation="highlight"}} 1"#
        )),
        "expected a single highlight latency sample in /metrics:\n{metrics}"
    );
    assert!(
        !metrics.contains(&format!(
            r#"request_latency_seconds_count{{{index_labels},operation="bm25"}}"#
        )),
        "highlight must not be recorded under the bm25 operation:\n{metrics}"
    );
}

// Checks that index_size counts a vector that the index adds after a refresh of the gauges. The
// usearch simulator adds the full scan's one vector 2s after the index receives it, and the gauges
// are refreshed every second, so a refresh falls within those 2s. When the index was marked dirty
// as the vector was handed to it, that refresh counted the index without the vector and took the
// mark, and nothing refreshed index_size afterwards.
#[tokio::test]
async fn index_size_counts_a_vector_added_after_a_refresh() {
    crate::enable_tracing();

    // search, add or remove, reserve
    let simulator = vec![Duration::ZERO, Duration::from_secs(2), Duration::ZERO];
    let config = Config {
        usearch_simulator: Some(simulator),
        ..vs_index::usearch_test_config()
    };
    let (index, client, _db, _server, _node_state) = setup_single_vector_index(config).await;

    let size_line = format!(
        r#"index_size{{index_name="{}",keyspace="{}"}} 1"#,
        index.index_name, index.keyspace_name,
    );
    wait_for(
        || async {
            let metrics = client.get_metrics_text().await;
            metrics.lines().any(|metric| metric == size_line)
        },
        "Waiting for index_size to count the vector added after a refresh",
    )
    .await;
}
