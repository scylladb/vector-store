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
use vector_store::IndexOptionsPattern;
use vector_store::NonemptyArc;
use vector_store::NonemptyIteratorExt;
use vector_store::Timestamp;
use vector_store::node_state::NodeState;

fn pattern_index_metadata(
    primary_key_columns: impl IntoIterator<Item = ColumnName>,
) -> IndexMetadata {
    let primary_key_columns = primary_key_columns
        .into_iter()
        .collect_nonempty_arc()
        .unwrap();
    IndexMetadata {
        keyspace_name: "pattern_ks".into(),
        table_name: "documents".into(),
        index_name: "pattern_idx".into(),
        partition_key_count: primary_key_columns.len(),
        primary_key_columns,
        target_columns: NonemptyArc::new(["content"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new([]),
        alternator_attribute_types: Default::default(),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Pattern(IndexOptionsPattern::default()),
    }
}

async fn setup_pattern_store(
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
        ..Default::default()
    };

    let node_state = vector_store::new_node_state().await;
    let (db_actor, db) = db_basic::new(node_state.clone());

    let columns: Arc<HashMap<_, _>> = Arc::new(columns.into_iter().collect());
    let index = pattern_index_metadata(primary_keys);

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: index.primary_key_columns.clone(),
            partition_key_count,
            columns,
            dimensions: HashMap::new(),
        },
    )
    .unwrap();

    db.add_index(index.clone(), fullscan_fn, cdc_fn).unwrap();

    let (receivers, senders) = create_config_channels(config).await;

    let run = {
        let node_state = node_state.clone();
        async move {
            let (server, _mtls) = vector_store::run(Some(node_state), Some(db_actor), receivers)
                .await
                .unwrap();
            let addr = (*server.address().await.borrow()).unwrap();

            (HttpClient::new(addr), server, senders)
        }
    };

    (run, index, db, node_state)
}

#[tokio::test]
async fn pattern_index_returns_proper_count() {
    crate::enable_tracing();

    let (run, index, _db, _node_state) = setup_pattern_store(
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
                .is_ok_and(|status| status.status == IndexStatus::Serving && status.count == 0)
        },
        "Waiting for pattern index to be serving with count == 0",
    )
    .await;
}

pub(crate) async fn setup_pattern_and_wait(
    documents: impl IntoIterator<Item = (Vec<CqlValue>, &str, u64)>,
    expected_count: usize,
) -> (
    HttpClient,
    httpapi::KeyspaceName,
    httpapi::IndexName,
    DbBasic,
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

    let (run, index, db, node_state) = setup_pattern_store(
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
        "Waiting for pattern index to be serving",
    )
    .await;

    (
        client,
        keyspace_name,
        index_name,
        db,
        (server, config_tx, node_state),
    )
}

#[tokio::test]
async fn pattern_like_search_returns_error() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_pattern_and_wait(
        [
            (vec![CqlValue::Int(1)], "the quick brown fox", 10),
            (vec![CqlValue::Int(2)], "lazy dog sleeps all day", 20),
        ],
        0,
    )
    .await;

    let response = client
        .post_like(
            &keyspace_name,
            &index_name,
            "%fox%".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
