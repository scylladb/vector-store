/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::create_config_channels;
use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::db_basic::Index;
use crate::db_basic::RangeScanFn;
use crate::db_basic::TOKEN_MAX;
use crate::db_basic::TOKEN_MIN;
use crate::db_basic::Table;
use crate::wait_for;
use httpapi::IndexNotReadyReason;
use httpapi::IndexStatus;
use httpclient::HttpClient;
use reqwest::StatusCode;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::routing::Token;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
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

fn fts_index_metadata(primary_key_columns: impl IntoIterator<Item = ColumnName>) -> IndexMetadata {
    let primary_key_columns = primary_key_columns
        .into_iter()
        .collect_nonempty_arc()
        .unwrap();
    IndexMetadata {
        keyspace_name: "fts_ks".into(),
        table_name: "documents".into(),
        index_name: "fts_idx".into(),
        partition_key_count: primary_key_columns.len(),
        primary_key_columns,
        target_columns: NonemptyArc::new(["content"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new([]),
        alternator_attribute_types: Default::default(),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Fts(IndexOptionsFts::default()),
    }
}

async fn setup_fts_store(
    primary_keys: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    range_scan_fn: RangeScanFn,
) -> (
    impl std::future::Future<Output = (HttpClient, impl Sized, impl Sized)>,
    IndexMetadata,
    DbBasic,
) {
    let config = Config {
        vector_store_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };

    let db = db_basic::new();

    let columns: Arc<HashMap<_, _>> = Arc::new(
        columns
            .into_iter()
            .map(|(name, column)| (name, ColumnType::Native(column)))
            .collect(),
    );
    let index = fts_index_metadata(primary_keys);

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

    db.add_index(Index::new(index.clone()).with_range_scan(Token::new(TOKEN_MIN), range_scan_fn))
        .unwrap();

    let (receivers, senders) = create_config_channels(config).await;

    let run = {
        let db = db.clone();
        async move {
            let (server, _mtls) = vector_store::run(db, receivers).await.unwrap();
            let addr = (*server.address().await.borrow()).unwrap();

            (HttpClient::new(addr), server, senders)
        }
    };

    (run, index, db)
}

#[tokio::test]
async fn fts_index_returns_proper_count() {
    crate::enable_tracing();

    let (run, index, _db) = setup_fts_store(
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
            ("content".to_string().into(), NativeType::Text),
        ],
        db_basic::range_scan_fn_documents([
            (
                [CqlValue::Int(1), CqlValue::Text("one".to_string())],
                Some("hello world".to_string()),
                10,
            ),
            (
                [CqlValue::Int(2), CqlValue::Text("two".to_string())],
                Some("foo bar".to_string()),
                20,
            ),
        ]),
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
    documents: impl IntoIterator<Item = (Vec<CqlValue>, &str, i64)>,
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
        .map(|(pk, doc, ts)| (pk, Some(doc.to_string()), ts))
        .collect();

    let (run, index, db) = setup_fts_store(
        ["pk".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("content".to_string().into(), NativeType::Text),
        ],
        db_basic::range_scan_fn_documents(docs),
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

    (client, keyspace_name, index_name, db, (server, config_tx))
}

#[tokio::test]
async fn fts_bm25_search_returns_matching_docs() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_fts_and_wait(
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

    let (client, keyspace_name, index_name, _db, _hold) =
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

    let (client, keyspace_name, index_name, _db, _hold) = setup_fts_and_wait(
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

    let (client, keyspace_name, index_name, _db, _hold) =
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
async fn fts_bm25_search_unparsable_query_returns_400() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "a quiet fox", 10)], 1).await;

    let response = client
        .post_bm25(
            &keyspace_name,
            &index_name,
            "fox AND".into(),
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn fts_bm25_search_returns_503_and_node_bootstrapping_reason_when_index_is_building_during_node_bootstrapping()
 {
    crate::enable_tracing();

    let (run, index, _db) = setup_fts_store(
        ["pk".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("content".to_string().into(), NativeType::Text),
        ],
        db_basic::pending_range_scan_fn(),
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
    let reason: IndexNotReadyReason = response.json().await.unwrap();
    assert_eq!(reason, IndexNotReadyReason::NodeBootstrapping);
}

#[tokio::test]
async fn fts_bm25_search_returns_503_and_index_building_reason_when_index_is_building_while_node_is_serving()
 {
    crate::enable_tracing();

    let (client, _keyspace_name, _index_name, db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    // The node is already serving its first index. A second index added now is
    // non-initial, so it stays bootstrapping while the node keeps serving.
    let index = IndexMetadata {
        index_name: "fts_idx_building".into(),
        ..fts_index_metadata([ColumnName::from("pk")])
    };
    db.set_token_ring(vec![
        Token::new(TOKEN_MIN / 2),
        Token::new(0),
        Token::new(TOKEN_MAX / 2),
    ]);
    db.add_index(
        Index::new(index.clone())
            .with_range_scan(Token::new(TOKEN_MIN), db_basic::pending_range_scan_fn()),
    )
    .unwrap();

    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| {
                    status.status == IndexStatus::Bootstrapping && status.build_progress == 75.0
                })
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
    let reason: IndexNotReadyReason = response.json().await.unwrap();
    let IndexNotReadyReason::IndexBuilding { message } = reason else {
        panic!("expected IndexBuilding, got {reason:?}");
    };
    assert!(
        message.contains(&format!("{keyspace_name}.{index_name}")),
        "unexpected message: {message}"
    );
    assert!(
        message.contains("progress: 75.000%"),
        "unexpected message: {message}"
    );
}

#[tokio::test]
async fn fts_empty_index_has_zero_count() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_fts_and_wait([], 0).await;

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

    let (client, keyspace_name, index_name, _db, _hold) = setup_fts_and_wait([], 0).await;

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

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    let indexes = client.indexes().await;
    let fts_index = indexes
        .iter()
        .find(|i| i.keyspace == keyspace_name && i.index == index_name);

    assert!(fts_index.is_some());
    assert!(matches!(
        fts_index.unwrap().options,
        httpapi::IndexOptions::Fulltext(_)
    ));
}

#[tokio::test]
async fn fts_highlight_marks_query_terms_in_supplied_text() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_fts_and_wait(
        [(vec![CqlValue::Int(1)], "the quick brown fox jumps", 10)],
        1,
    )
    .await;

    let highlights = client
        .highlight(
            &keyspace_name,
            &index_name,
            "fox".into(),
            vec!["a completely different fox story".into()],
        )
        .await;

    assert_eq!(highlights.len(), 1);
    assert_eq!(
        highlights[0].as_deref(),
        Some("a completely different <b>fox</b> story")
    );
}

#[tokio::test]
async fn fts_highlight_returns_null_for_unmatched_document() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "the quick brown fox", 10)], 1).await;

    let highlights = client
        .highlight(
            &keyspace_name,
            &index_name,
            "fox".into(),
            vec!["turtles all the way down".into()],
        )
        .await;

    assert_eq!(highlights[0], None);
}

#[tokio::test]
async fn fts_highlight_preserves_document_order() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) = setup_fts_and_wait(
        [
            (vec![CqlValue::Int(1)], "turtles", 10),
            (vec![CqlValue::Int(2)], "fox", 20),
            (vec![CqlValue::Int(3)], "dog", 30),
        ],
        3,
    )
    .await;

    let highlights = client
        .highlight(
            &keyspace_name,
            &index_name,
            "fox OR dog OR turtles".into(),
            vec![
                "quick fox jumps".into(),
                "turtles swim slowly".into(),
                "lazy dog sleeps".into(),
            ],
        )
        .await;

    assert_eq!(highlights.len(), 3);
    assert_eq!(highlights[0].as_deref(), Some("quick <b>fox</b> jumps"));
    assert_eq!(highlights[1].as_deref(), Some("<b>turtles</b> swim slowly"));
    assert_eq!(highlights[2].as_deref(), Some("lazy <b>dog</b> sleeps"));
}

#[tokio::test]
async fn fts_highlight_not_found_returns_404() {
    crate::enable_tracing();

    let (client, _keyspace_name, _index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    let response = client
        .post_highlight(
            &"nonexistent_ks".into(),
            &"nonexistent_idx".into(),
            "hello".into(),
            vec!["hello world".into()],
        )
        .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fts_highlight_unparsable_query_returns_400() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    let response = client
        .post_highlight(
            &keyspace_name,
            &index_name,
            "hello AND".into(),
            vec!["hello world".into()],
        )
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn fts_highlight_not_serving_returns_503() {
    crate::enable_tracing();

    let (run, index, _db) = setup_fts_store(
        ["pk".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("content".to_string().into(), NativeType::Text),
        ],
        db_basic::pending_range_scan_fn(),
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
        .post_highlight(
            &keyspace_name,
            &index_name,
            "hello".into(),
            vec!["hello world".into()],
        )
        .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// Sends the head of a highlight request whose body is declared over the 640 MiB limit and
/// only its first bytes, then returns the answer, which must come before the rest of the body.
async fn answer_to_a_body_declared_over_the_limit(
    client: &HttpClient,
    keyspace_name: &str,
    index_name: &str,
    content_type: &str,
) -> String {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let addr = client
        .url()
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap()
        .to_string();
    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let head = format!(
        "POST /api/v1/indexes/{keyspace_name}/{index_name}/highlight HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\r\n",
        640 * 1024 * 1024 + 1
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream
        .write_all(br#"{"query":"hello","documents":["hello world""#)
        .await
        .unwrap();

    // The server goes on discarding a body that never comes: read until the answer is
    // complete.
    let mut answer = Vec::new();
    let read_answer = async {
        let mut chunk = [0; 1024];
        loop {
            let complete = String::from_utf8_lossy(&answer)
                .split_once("\r\n\r\n")
                .is_some_and(|(head, body)| {
                    head.lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .and_then(|len| len.parse::<usize>().ok())
                        .is_some_and(|len| body.len() >= len)
                });
            if complete {
                break;
            }
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(read) => answer.extend_from_slice(&chunk[..read]),
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), read_answer)
        .await
        .expect("the answer must come before the rest of the body");
    String::from_utf8_lossy(&answer).into_owned()
}

#[tokio::test]
async fn fts_highlight_answers_413_before_reading_a_body_declared_over_the_limit() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    let answer = answer_to_a_body_declared_over_the_limit(
        &client,
        keyspace_name.as_ref(),
        index_name.as_ref(),
        "application/json",
    )
    .await;

    assert!(
        answer.starts_with("HTTP/1.1 413 "),
        "unexpected answer: {answer:?}"
    );
    assert!(
        answer.ends_with("the highlight request body is too large"),
        "unexpected answer: {answer:?}"
    );
}

#[tokio::test]
async fn fts_highlight_answers_415_before_413_for_a_body_declared_over_the_limit() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    let answer = answer_to_a_body_declared_over_the_limit(
        &client,
        keyspace_name.as_ref(),
        index_name.as_ref(),
        "text/plain",
    )
    .await;

    assert!(
        answer.starts_with("HTTP/1.1 415 "),
        "unexpected answer: {answer:?}"
    );
}

/// Sends a highlight request the way ScyllaDB does: the whole body first, and only
/// then reads the answer. Returns the status and the body of the answer.
///
/// A server that closes the connection with the body unread would make the write fail
/// with a reset, or the answer be lost.
async fn post_highlight_whole_body_first(
    client: &HttpClient,
    keyspace_name: &str,
    index_name: &str,
    body: &[u8],
) -> (StatusCode, String) {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let addr = client
        .url()
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap();
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST /api/v1/indexes/{keyspace_name}/{index_name}/highlight HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream
        .write_all(body)
        .await
        .expect("the server must not close the connection before the body is sent");
    let mut answer = Vec::new();
    stream
        .read_to_end(&mut answer)
        .await
        .expect("the server must not reset the connection before the answer is read");
    let answer = String::from_utf8_lossy(&answer);
    let status = answer
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or_else(|| panic!("no HTTP answer: {answer:?}"));
    let body = answer.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    (StatusCode::from_u16(status).unwrap(), body.to_string())
}

#[tokio::test]
async fn fts_highlight_of_a_document_over_two_mib() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "the quick brown fox", 10)], 1).await;
    // The whole body used to be limited to 2 MiB.
    let document = format!("{} fox", "lorem ipsum ".repeat(3 * 1024 * 1024 / 12));

    let highlights = client
        .highlight(&keyspace_name, &index_name, "fox".into(), vec![document])
        .await;

    assert_eq!(highlights.len(), 1);
    assert!(
        highlights[0].as_deref().unwrap().ends_with("<b>fox</b>"),
        "unexpected highlight: {:?}",
        highlights[0]
    );
}

#[tokio::test]
async fn fts_highlight_of_a_missing_index_answers_a_client_sending_the_whole_body_first() {
    crate::enable_tracing();

    let (client, _keyspace_name, _index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;
    // Large enough that the server would close with most of it unread.
    let body = format!(
        r#"{{"query":"hello","documents":["{}"]}}"#,
        "x".repeat(8 * 1024 * 1024)
    );

    let (status, _) = post_highlight_whole_body_first(
        &client,
        "nonexistent_ks",
        "nonexistent_idx",
        body.as_bytes(),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fts_highlight_of_a_document_over_the_limit_answers_413() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;
    // One document past the limit of 100 MiB, then another document.
    let body = format!(
        r#"{{"query":"hello","documents":["{}","hello"]}}"#,
        "x".repeat(100 * 1024 * 1024 + 1)
    );

    let (status, message) = post_highlight_whole_body_first(
        &client,
        keyspace_name.as_ref(),
        index_name.as_ref(),
        body.as_bytes(),
    )
    .await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        message.contains("may be at most 104857600 bytes"),
        "unexpected message: {message}"
    );
}

#[tokio::test]
async fn fts_highlight_rejects_bodies_as_the_json_extractor_did() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;
    let url = format!(
        "{}/indexes/{keyspace_name}/{index_name}/highlight",
        client.url()
    );
    let post = |body: &'static str, content_type: Option<&'static str>| {
        let mut request = reqwest::Client::new().post(&url).body(body);
        if let Some(content_type) = content_type {
            request = request.header("Content-Type", content_type);
        }
        async move { request.send().await.unwrap().status() }
    };
    let valid = r#"{"query":"hello","documents":["hello world"]}"#;

    assert_eq!(post(valid, Some("application/json")).await, StatusCode::OK);
    assert_eq!(
        post(valid, Some("application/json; charset=utf-8")).await,
        StatusCode::OK
    );
    assert_eq!(post(valid, None).await, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        post(valid, Some("text/plain")).await,
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        post(
            r#"{"query":"hello","documents":["a"#,
            Some("application/json")
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(r#"{"query":"hello"}"#, Some("application/json")).await,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn fts_highlight_rejects_documents_before_the_query() {
    crate::enable_tracing();

    let (client, keyspace_name, index_name, _db, _hold) =
        setup_fts_and_wait([(vec![CqlValue::Int(1)], "hello world", 10)], 1).await;

    let (status, message) = post_highlight_whole_body_first(
        &client,
        keyspace_name.as_ref(),
        index_name.as_ref(),
        br#"{"documents":["hello world"],"query":"hello"}"#,
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        message.contains("field `query` must come before field `documents`"),
        "unexpected message: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fts_back_to_back_updates_keep_one_latest_document_per_row() {
    crate::enable_tracing();

    const ROWS: i32 = 300;
    const VERSIONS: i64 = 3;
    let rows = (0..ROWS).flat_map(|row| {
        (0..VERSIONS).map(move |version| {
            (
                [CqlValue::Int(row)],
                Some(format!("r{row}v{version}")),
                10 + version,
            )
        })
    });
    let scan = db_basic::range_scan_fn_documents(rows);

    let (run, index, _db) = setup_fts_store(
        ["pk".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("content".to_string().into(), NativeType::Text),
        ],
        scan,
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
                .is_ok_and(|status| status.status == IndexStatus::Serving)
        },
        "Waiting for FTS index to be serving",
    )
    .await;

    let search = async |query: String| {
        let (primary_keys, _) = client
            .bm25(
                &keyspace_name,
                &index_name,
                query,
                NonZeroUsize::new(10).unwrap().into(),
            )
            .await;
        primary_keys
            .get(&"pk".into())
            .unwrap()
            .iter()
            .map(|pk| pk.as_i64().unwrap())
            .collect::<Vec<_>>()
    };

    let status = client
        .index_status(&keyspace_name, &index_name)
        .await
        .unwrap();
    assert_eq!(status.count, ROWS as usize);
    for row in 0..ROWS as i64 {
        assert_eq!(search(format!("r{row}v{}", VERSIONS - 1)).await, [row]);
        for version in 0..VERSIONS - 1 {
            assert!(search(format!("r{row}v{version}")).await.is_empty());
        }
    }
}
