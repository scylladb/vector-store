/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::db_basic::ScanFn;
use crate::fts;
use crate::vs_index;
use crate::wait_for;
use futures::FutureExt;
use httpclient::HttpClient;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
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
    vs_index::setup_store_and_wait_for_index(
        vs_index::usearch_test_config(),
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

fn gauge_value(metrics: &str, name: &str, labels: &str) -> Option<f64> {
    let prefix = format!("{name}{{{labels}}} ");
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.parse().ok())
}

/// Writes the rows of `first`, then holds the rows of `second` back until the returned sender
/// fires.
fn scan_fn_with_gated_second(first: ScanFn, second: ScanFn) -> (oneshot::Sender<()>, ScanFn) {
    let (release_tx, release_rx) = oneshot::channel();
    let scan_fn: ScanFn = Box::new(move |tx| {
        async move {
            first(tx.clone()).await;
            let _ = release_rx.await;
            second(tx).await;
        }
        .boxed()
    });
    (release_tx, scan_fn)
}

fn document(pk: i32, text: &str, timestamp_millis: u64) -> ScanFn {
    db_basic::scan_fn_documents([(
        [CqlValue::Int(pk)].into(),
        Some(text.to_string()),
        Timestamp::from_millis(timestamp_millis),
    )])
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

// Checks that the gauges of a full-text index in /metrics follow the reader after each commit.
// The test adds one document and waits until it is searchable. Then it adds a second document
// and reads /metrics until the gauges show two documents in two segments. Without the fix, a
// scrape before the second commit records the old values, and nothing updates them later. This
// failure needs a scrape between the second document and its commit (every 1s), but
// the test reads /metrics in a tight loop, so without the fix it fails reliably.
#[tokio::test]
async fn fts_index_metrics_follow_commit_after_scrape() {
    crate::enable_tracing();

    let (release_second_document, cdc_fn) =
        scan_fn_with_gated_second(document(1, "hello world", 10), document(2, "foo bar", 20));
    // Returns once the first document is searchable.
    let (client, keyspace_name, index_name, _db, _hold) =
        fts::setup_fts_with_cdc_and_wait(Vec::<(Vec<CqlValue>, &str, u64)>::new(), Some(cdc_fn), 1)
            .await;

    // Push second document and start scraping in wait_for.
    release_second_document.send(()).unwrap();

    let labels = format!(r#"index_name="{index_name}",keyspace="{keyspace_name}""#);
    wait_for(
        || async {
            let metrics = client.get_metrics_text().await;
            gauge_value(&metrics, "index_size", &labels) == Some(2.0)
                && gauge_value(&metrics, "fts_segment_count", &labels) == Some(2.0)
        },
        "Waiting for fts index metrics to reflect the second commit",
    )
    .await;
}
