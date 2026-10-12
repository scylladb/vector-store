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
use httpapi::IndexNotReadyReason;
use httpapi::IndexOptions;
use httpapi::IndexStatus;
use httpapi::PatternIndexOptions;
use httpclient::HttpClient;
use reqwest::StatusCode;
use rstest::rstest;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use vector_store::CaseSensitive;
use vector_store::ColumnName;
use vector_store::Config;
use vector_store::DbIndexPartitioning;
use vector_store::HttpServerExt;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsPattern;
use vector_store::NonemptyArc;
use vector_store::NonemptyIteratorExt;
use vector_store::Percentage;
use vector_store::Progress;
use vector_store::Timestamp;
use vector_store::node_state::NodeState;

fn pattern_index_metadata(
    primary_key_columns: impl IntoIterator<Item = ColumnName>,
    options: IndexOptionsPattern,
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
        kind: IndexKind::Pattern(options),
    }
}

async fn setup_pattern_store(
    primary_keys: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    fullscan_fn: Option<ScanFn>,
    cdc_fn: Option<ScanFn>,
    options: IndexOptionsPattern,
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
    let index = pattern_index_metadata(primary_keys, options);

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
    setup_pattern_with_and_wait(
        documents,
        expected_count,
        IndexOptionsPattern::default(),
        None,
    )
    .await
}

async fn setup_pattern_with_and_wait(
    documents: impl IntoIterator<Item = (Vec<CqlValue>, &str, u64)>,
    expected_count: usize,
    options: IndexOptionsPattern,
    cdc_fn: Option<ScanFn>,
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
        cdc_fn,
        options,
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

/// The `pk` of the rows matching `pattern`, sorted, so tests do not depend on the result order.
async fn like_ids(
    client: &HttpClient,
    keyspace_name: &httpapi::KeyspaceName,
    index_name: &httpapi::IndexName,
    pattern: &str,
    limit: usize,
) -> Vec<i64> {
    let primary_keys = client
        .like(
            keyspace_name,
            index_name,
            pattern.into(),
            NonZeroUsize::new(limit).unwrap().into(),
        )
        .await;
    let mut ids: Vec<i64> = primary_keys
        .get(&"pk".into())
        .unwrap()
        .iter()
        .map(|pk| pk.as_i64().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

async fn wait_for_like_ids(
    client: &HttpClient,
    keyspace_name: &httpapi::KeyspaceName,
    index_name: &httpapi::IndexName,
    pattern: &str,
    expected: &[i64],
) {
    wait_for(
        || async { like_ids(client, keyspace_name, index_name, pattern, 100).await == expected },
        "Waiting for the pattern search to see the change",
    )
    .await;
}

#[tokio::test]
async fn pattern_index_visible_in_indexes() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_pattern_and_wait(
        [
            (vec![CqlValue::Int(1)], "the quick brown fox", 10),
            (vec![CqlValue::Int(2)], "lazy dog sleeps all day", 20),
        ],
        2,
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].keyspace, keyspace_name);
    assert_eq!(indexes[0].index, index_name);
    assert_eq!(
        indexes[0].options,
        IndexOptions::Pattern(PatternIndexOptions {
            case_sensitive: true
        })
    );
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
        IndexOptionsPattern::default(),
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
        "Waiting for pattern index to be serving with count == 2",
    )
    .await;
}

#[rstest]
#[case::prefix_matches_only_values_starting_with_it("AB-%", vec![1, 2])]
#[case::substring_matches_anywhere_in_the_value("%024%", vec![1, 4, 5])]
#[case::underscore_matches_exactly_one_character("%-_", vec![2, 3])]
#[tokio::test]
async fn pattern_like_matches_the_documented_examples(
    #[case] pattern: &str,
    #[case] expected: Vec<i64>,
) {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_pattern_and_wait(
        [
            (vec![CqlValue::Int(1)], "AB-1024-XL", 10),
            (vec![CqlValue::Int(2)], "AB-2048-S", 20),
            (vec![CqlValue::Int(3)], "ab-4096-M", 30),
            (vec![CqlValue::Int(4)], "XAB-1024", 40),
            (vec![CqlValue::Int(5)], "CD-1024-AB", 50),
        ],
        5,
    )
    .await;

    let ids = like_ids(&client, &keyspace_name, &index_name, pattern, 100).await;

    // Only the values matching the pattern are found.
    assert_eq!(ids, expected);
}

#[rstest]
#[case::uppercase_prefix_matches_any_letter_case("AB-%", vec![1, 2, 3])]
#[case::lowercase_prefix_matches_any_letter_case("ab-%", vec![1, 2, 3])]
#[tokio::test]
async fn pattern_like_on_case_insensitive_index_matches_the_documented_examples(
    #[case] pattern: &str,
    #[case] expected: Vec<i64>,
) {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_pattern_with_and_wait(
        [
            (vec![CqlValue::Int(1)], "AB-1024-XL", 10),
            (vec![CqlValue::Int(2)], "AB-2048-S", 20),
            (vec![CqlValue::Int(3)], "ab-4096-M", 30),
            (vec![CqlValue::Int(4)], "XAB-1024", 40),
            (vec![CqlValue::Int(5)], "CD-1024-AB", 50),
        ],
        5,
        IndexOptionsPattern {
            case_sensitive: CaseSensitive::from(false),
        },
        None,
    )
    .await;

    let ids = like_ids(&client, &keyspace_name, &index_name, pattern, 100).await;

    // Only the values matching the pattern in any letter case are found.
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn pattern_like_respects_limit() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_pattern_and_wait(
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

    let ids = like_ids(&client, &keyspace_name, &index_name, "%rust%", 2).await;

    assert_eq!(ids.len(), 2);
}

#[tokio::test]
async fn pattern_like_returns_empty_for_no_match() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_pattern_and_wait([(vec![CqlValue::Int(1)], "AB-1024-XL", 10)], 1).await;

    let primary_keys = client
        .like(
            &keyspace_name,
            &index_name,
            "%nothing like this%".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(primary_keys, HashMap::from([("pk".into(), vec![])]));
}

#[rstest]
#[case::only_a_percent("%")]
#[case::only_an_underscore("_")]
#[case::an_underscore_between_percents("%_%")]
#[tokio::test]
async fn pattern_like_without_literal_characters_returns_400(#[case] pattern: &str) {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_pattern_and_wait([(vec![CqlValue::Int(1)], "AB-1024-XL", 10)], 1).await;

    let response = client
        .post_like(
            &keyspace_name,
            &index_name,
            pattern.into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    // The message tells the user why the pattern is rejected.
    let body = response.text().await.unwrap();
    assert!(body.contains("at least one literal character"), "{body}");
}

#[tokio::test]
async fn pattern_like_not_found_returns_404() {
    crate::enable_tracing();

    let (client, _keyspace_name, _index_name, _db, _hold) =
        setup_pattern_and_wait([(vec![CqlValue::Int(1)], "AB-1024-XL", 10)], 1).await;

    let response = client
        .post_like(
            &"nonexistent_ks".into(),
            &"nonexistent_idx".into(),
            "AB-%".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn pattern_like_follows_an_update() {
    crate::enable_tracing();

    let (tx_cdc, rx_cdc) = mpsc::channel(1);
    let (client, keyspace_name, index_name, _db, _hold) = setup_pattern_with_and_wait(
        [
            (vec![CqlValue::Int(1)], "AB-1024-XL", 10),
            (vec![CqlValue::Int(2)], "AB-2048-S", 20),
        ],
        2,
        IndexOptionsPattern::default(),
        Some(db_basic::channel_scan_fn_documents(rx_cdc)),
    )
    .await;

    tx_cdc
        .send((
            [CqlValue::Int(1)].into(),
            Some("CD-1024-XL".to_string()),
            Timestamp::from_millis(30),
        ))
        .await
        .unwrap();

    // Wait until the new value of row 1 is found.
    wait_for_like_ids(&client, &keyspace_name, &index_name, "CD-%", &[1]).await;
    // The old value of row 1 is no longer found.
    assert_eq!(
        like_ids(&client, &keyspace_name, &index_name, "AB-%", 100).await,
        vec![2]
    );
    // The new value of row 1 is found by a substring.
    assert_eq!(
        like_ids(&client, &keyspace_name, &index_name, "%-1024-%", 100).await,
        vec![1]
    );
}

#[tokio::test]
async fn pattern_like_follows_a_delete() {
    crate::enable_tracing();

    let (tx_cdc, rx_cdc) = mpsc::channel(1);
    let (client, keyspace_name, index_name, _db, _hold) = setup_pattern_with_and_wait(
        [
            (vec![CqlValue::Int(1)], "AB-1024-XL", 10),
            (vec![CqlValue::Int(2)], "AB-2048-S", 20),
        ],
        2,
        IndexOptionsPattern::default(),
        Some(db_basic::channel_scan_fn_documents(rx_cdc)),
    )
    .await;

    tx_cdc
        .send(([CqlValue::Int(2)].into(), None, Timestamp::from_millis(30)))
        .await
        .unwrap();

    // Wait until the deleted row 2 is no longer found while row 1 still is.
    wait_for_like_ids(&client, &keyspace_name, &index_name, "AB-%", &[1]).await;
}

#[tokio::test]
async fn pattern_like_returns_503_and_node_bootstrapping_reason_while_the_index_is_building() {
    crate::enable_tracing();

    let (run, index, _db, _node_state) = setup_pattern_store(
        ["pk".into()],
        1,
        [("pk".to_string().into(), NativeType::Int)],
        Some(db_basic::pending_scan_fn()),
        None,
        IndexOptionsPattern::default(),
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
                .is_ok_and(|status| status.status == IndexStatus::Bootstrapping)
        },
        "Waiting for pattern index to be bootstrapping",
    )
    .await;

    let response = client
        .post_like(
            &keyspace_name,
            &index_name,
            "%AB%".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let reason: IndexNotReadyReason = response.json().await.unwrap();
    assert_eq!(reason, IndexNotReadyReason::NodeBootstrapping);
}

#[tokio::test]
async fn pattern_like_returns_503_and_index_building_reason_while_the_node_is_serving() {
    crate::enable_tracing();

    let (client, _keyspace_name, _index_name, db, _hold) =
        setup_pattern_and_wait([(vec![CqlValue::Int(1)], "AB-1024-XL", 10)], 1).await;

    // The node is already serving its first index. A second index added now is
    // non-initial, so it stays bootstrapping while the node keeps serving.
    let index = IndexMetadata {
        index_name: "pattern_idx_building".into(),
        ..pattern_index_metadata(["pk".into()], IndexOptionsPattern::default())
    };
    db.add_index(index.clone(), Some(db_basic::pending_scan_fn()), None)
        .unwrap();
    db.set_next_full_scan_progress(Progress::InProgress(Percentage::try_from(75.0).unwrap()));
    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| status.status == IndexStatus::Bootstrapping)
        },
        "Waiting for pattern index to be bootstrapping",
    )
    .await;

    let response = client
        .post_like(
            &keyspace_name,
            &index_name,
            "%AB%".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let reason: IndexNotReadyReason = response.json().await.unwrap();
    let IndexNotReadyReason::IndexBuilding { message } = reason else {
        panic!("expected IndexBuilding, got {reason:?}");
    };
    assert!(
        message.contains("pattern_ks.pattern_idx_building"),
        "{message}"
    );
    assert!(message.contains("progress: 75.000%"), "{message}");
}

#[rstest]
#[case::one_cjk_character("%帅%", vec![3])]
#[case::two_cjk_characters("%将军%", vec![1, 2])]
#[case::three_cjk_characters("%将军来%", vec![2])]
#[case::four_cjk_characters("%将军来了%", vec![2])]
#[case::uppercase_latin_characters("%NG%", vec![4, 5])]
#[case::lowercase_latin_characters("%ng%", vec![5])]
#[case::latin_word("%gamer%", vec![4])]
#[tokio::test]
async fn pattern_like_finds_substrings_of_cjk_and_latin_values(
    #[case] pattern: &str,
    #[case] expected: Vec<i64>,
) {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_pattern_and_wait(
        [
            (vec![CqlValue::Int(1)], "宇将军", 10),
            (vec![CqlValue::Int(2)], "将军来了", 20),
            (vec![CqlValue::Int(3)], "元帅", 30),
            (vec![CqlValue::Int(4)], "NGgamer", 40),
            (vec![CqlValue::Int(5)], "kingNG", 50),
        ],
        5,
    )
    .await;

    let ids = like_ids(&client, &keyspace_name, &index_name, pattern, 100).await;

    // Only the values holding the keyword are found.
    assert_eq!(ids, expected);
}

#[rstest]
#[case::prefix("AB-%", vec![1])]
#[case::suffix("%-XL", vec![1])]
#[case::whole_value_without_wildcards("AB-1024-XL", vec![1])]
#[case::start_of_a_value_without_wildcards("AB-1024", vec![])]
#[case::underscore_matches_exactly_one_character("100_", vec![2, 3])]
#[case::escaped_percent(r"%\%", vec![2])]
#[case::escaped_underscore(r"a\_b", vec![4])]
#[case::unescaped_underscore("a_b", vec![4, 5])]
#[case::empty_pattern("", vec![6])]
#[tokio::test]
async fn pattern_like_follows_the_like_rules(#[case] pattern: &str, #[case] expected: Vec<i64>) {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_pattern_and_wait(
        [
            (vec![CqlValue::Int(1)], "AB-1024-XL", 10),
            (vec![CqlValue::Int(2)], "100%", 20),
            (vec![CqlValue::Int(3)], "1000", 30),
            (vec![CqlValue::Int(4)], "a_b", 40),
            (vec![CqlValue::Int(5)], "axb", 50),
            (vec![CqlValue::Int(6)], "", 60),
        ],
        6,
    )
    .await;

    let ids = like_ids(&client, &keyspace_name, &index_name, pattern, 100).await;

    // Only the values matching the pattern by the LIKE rules are found.
    assert_eq!(ids, expected);
}
