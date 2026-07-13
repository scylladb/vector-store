/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::create_config_channels;
use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::db_basic::ScanFn;
use crate::db_basic::Table;
use crate::wait_for;
use httpapi::IndexStatus;
use httpclient::HttpClient;
use reqwest::StatusCode;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use vector_store::ColumnName;
use vector_store::Config;
use vector_store::DbIndexPartitioning;
use vector_store::HttpServerExt;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsFts;
use vector_store::NonemptyArc;
use vector_store::NonemptyIteratorExt;
use vector_store::Timestamp;
use vector_store::node_state::NodeState;

async fn setup_fts_store(
    primary_keys: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    fullscan_fn: Option<ScanFn>,
    cdc_fn: Option<ScanFn>,
) -> (
    impl std::future::Future<Output = (HttpClient, impl Sized, impl Sized)>,
    IndexMetadata,
    DbBasic,
    Sender<NodeState>,
) {
    let config = Config {
        vector_store_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        fulltext_indexes: true,
        ..Default::default()
    };

    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();
    let (db_actor, db) = db_basic::new(node_state.clone());

    let columns: Arc<HashMap<_, _>> = Arc::new(columns.into_iter().collect());
    let primary_key_columns = primary_keys.into_iter().collect_nonempty_arc().unwrap();
    let index = IndexMetadata {
        keyspace_name: "fts_ks".into(),
        primary_key_columns: primary_key_columns.clone(),
        table_name: "documents".into(),
        index_name: "fts_idx".into(),
        target_columns: NonemptyArc::new(["content"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: columns.keys().cloned().collect(),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Fts(IndexOptionsFts {}),
    };

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: primary_key_columns,
            partition_key_count,
            columns,
            dimensions: HashMap::new(),
        },
    )
    .unwrap();

    db.add_index(index.clone(), fullscan_fn, cdc_fn).unwrap();

    let (receivers, senders) = create_config_channels(config).await;
    let index_factory = vector_store::new_index_factory_usearch(receivers.config.clone()).unwrap();

    let run = {
        let node_state = node_state.clone();
        async move {
            let (server, _mtls) = vector_store::run(
                node_state,
                db_actor,
                internals,
                index_factory,
                receivers,
                vector_store::new_metrics(),
            )
            .await
            .unwrap();
            let addr = (*server.address().await.borrow()).unwrap();

            (HttpClient::new(addr), server, senders)
        }
    };

    (run, index, db, node_state)
}

#[tokio::test]
async fn fts_index_returns_proper_count() {
    crate::enable_tracing();

    let (run, index, _db, _node_state) = setup_fts_store(
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        Some(db_basic::scan_fn_documents([
            (
                [CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
                Some("hello world".to_string()),
                Timestamp::from_millis(10),
            ),
            (
                [CqlValue::Int(2), CqlValue::Text("two".to_string())].into(),
                Some("foo bar".to_string()),
                Timestamp::from_millis(20),
            ),
        ])),
        None,
    )
    .await;

    let (client, _server, _config_tx) = run.await;

    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| status.status == IndexStatus::Serving && status.count == 2)
        },
        "Waiting for FTS index to be serving with count == 2",
    )
    .await;
}

pub(crate) async fn setup_fts_and_wait(
    documents: impl IntoIterator<Item = (Vec<CqlValue>, &str, u64)>,
    expected_count: usize,
) -> (
    HttpClient,
    httpapi::KeyspaceName,
    httpapi::IndexName,
    impl Sized,
) {
    let docs: Vec<_> = documents
        .into_iter()
        .map(|(pk, doc, ts)| {
            (
                pk.into_iter().collect(),
                Some(doc.to_string()),
                Timestamp::from_millis(ts),
            )
        })
        .collect();

    let (run, index, db, node_state) = setup_fts_store(
        ["pk".into()],
        1,
        [("pk".to_string().into(), NativeType::Int)],
        Some(db_basic::scan_fn_documents(docs)),
        None,
    )
    .await;

    let (client, server, config_tx) = run.await;
    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| {
                    status.status == IndexStatus::Serving && status.count == expected_count
                })
        },
        "Waiting for FTS index to be serving",
    )
    .await;

    (
        client,
        keyspace_name,
        index_name,
        (server, config_tx, db, node_state),
    )
}

#[tokio::test]
async fn fts_bm25_search_returns_matching_docs() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _hold) = setup_fts_and_wait(
        [
            (vec![CqlValue::Int(1)], "the quick brown fox", 10),
            (vec![CqlValue::Int(2)], "lazy dog sleeps all day", 20),
        ],
        2,
    )
    .await;

    let (primary_keys, scores) = client
        .bm25(
            &keyspace_name,
            &index_name,
            "fox".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(primary_keys.get(&"pk".into()).unwrap().len(), 1);
    assert_eq!(scores.len(), 1);
    assert!(scores[0] > 0.0);
    assert_eq!(
        primary_keys.get(&"pk".into()).unwrap()[0].as_i64().unwrap(),
        1
    );
}

#[tokio::test]
async fn fts_bm25_search_returns_empty_for_no_match() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    let (primary_keys, scores) = client
        .bm25(
            &keyspace_name,
            &index_name,
            "nonexistentterm".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert!(primary_keys.get(&"pk".into()).unwrap().is_empty());
    assert!(scores.is_empty());
}

#[tokio::test]
async fn fts_bm25_search_respects_limit() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _hold) = setup_fts_and_wait(
        [
            (vec![CqlValue::Int(1)], "rust programming language", 10),
            (vec![CqlValue::Int(2)], "rust systems programming", 20),
            (vec![CqlValue::Int(3)], "rust is fast and safe", 30),
            (vec![CqlValue::Int(4)], "rust memory safety", 40),
            (vec![CqlValue::Int(5)], "rust concurrency model", 50),
        ],
        5,
    )
    .await;

    let (primary_keys, scores) = client
        .bm25(
            &keyspace_name,
            &index_name,
            "rust".into(),
            NonZeroUsize::new(2).unwrap().into(),
        )
        .await;

    assert_eq!(primary_keys.get(&"pk".into()).unwrap().len(), 2);
    assert_eq!(scores.len(), 2);
}

#[tokio::test]
async fn fts_bm25_search_not_found_returns_404() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    let _ = (keyspace_name, index_name);
    let response = client
        .post_bm25(
            &"nonexistent_ks".into(),
            &"nonexistent_idx".into(),
            "hello".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fts_bm25_search_not_serving_returns_503() {
    crate::enable_tracing();

    let fullscan_fn = |_| {
        use futures::FutureExt;
        async move {
            // wait indefinitely to simulate a long-running indexing operation (Bootstrapping)
            std::future::pending::<()>().await;
        }
        .boxed()
    };
    let (run, index, _db, _node_state) = setup_fts_store(
        ["pk".into()],
        1,
        [("pk".to_string().into(), NativeType::Int)],
        Some(Box::new(fullscan_fn)),
        None,
    )
    .await;
    let (client, _server, _config_tx) = run.await;
    let keyspace_name: httpapi::KeyspaceName = index.keyspace_name.clone().into();
    let index_name: httpapi::IndexName = index.index_name.clone().into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| status.status == IndexStatus::Bootstrapping)
        },
        "Waiting for FTS index to be bootstrapping",
    )
    .await;

    let response = client
        .post_bm25(
            &keyspace_name,
            &index_name,
            "hello".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn fts_empty_index_has_zero_count() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _hold) = setup_fts_and_wait([], 0).await;

    let status = client
        .index_status(&keyspace_name, &index_name)
        .await
        .unwrap();

    assert_eq!(status.status, IndexStatus::Serving);
    assert_eq!(status.count, 0);
}

#[tokio::test]
async fn fts_empty_index_returns_empty_bm25_results() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _hold) = setup_fts_and_wait([], 0).await;

    let (primary_keys, scores) = client
        .bm25(
            &keyspace_name,
            &index_name,
            "anyterm".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert!(primary_keys.get(&"pk".into()).unwrap().is_empty());
    assert!(scores.is_empty());
}

#[tokio::test]
async fn fts_index_appears_in_indexes_list() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    let indexes = client.indexes().await;
    let fts_index = indexes
        .iter()
        .find(|i| i.keyspace == keyspace_name && i.index == index_name);

    assert!(fts_index.is_some());
    assert_eq!(fts_index.unwrap().index_type, httpapi::IndexType::Fulltext);
}
