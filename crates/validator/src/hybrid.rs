/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Tests for hybrid search: a vector search and a full-text search fused by `RRF()`.

use crate::TestActors;
use crate::common::*;
use httpapi::IndexInfo;
use httpapi::KeyspaceName;
use httpapi::Limit;
use httpclient::HttpClient;
use scylla::client::session::Session;
use serde_json::Value;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = hybrid,
    fixtures = (Cluster, Fixture),
    parent = crate::validator
);

const QUERY_VECTOR: [f32; 2] = [1.0, 0.0];
const QUERY_VECTOR_CQL: &str = "[1.0, 0.0]";
const ROW_COUNT: usize = 6;
const SCORE_TOLERANCE: f32 = 1e-5;

const DOCUMENTS: [&str; ROW_COUNT] = [
    "a fox among many other animals in the wide green meadow",
    "fox fox fox",
    "lazy cat sleeps",
    "quick brown dog",
    "fox fox and a hound",
    "vector store search",
];

struct Cluster {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for Cluster {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        init(&actors).await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

struct Fixture {
    _cluster: Arc<Cluster>,
    session: Arc<Session>,
    clients: Vec<HttpClient>,
    keyspace: KeyspaceName,
    table: TableName,
    vector_index: IndexInfo,
    fulltext_index: IndexInfo,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let cluster = setup.setup::<Cluster>().await?;
        let actors = setup.setup::<TestActors>().await?;
        Some(setup_hybrid_table(cluster, &actors).await)
    }

    async fn teardown(self) {
        self.session
            .query_unpaged(format!("DROP KEYSPACE {}", self.keyspace), ())
            .await
            .expect("failed to drop keyspace");
    }
}

/// Creates a table with 6 rows, `pk` = 0..6:
/// - `v`   = a unit vector at `pk * 15` degrees, so the ANN order for `[1.0, 0.0]` is pk 0, 1, .., 5
/// - `doc` = a document from `DOCUMENTS`, so the BM25 order for 'fox' is pk 1, 4, 0
///
/// A vector index is created on `v` and a fulltext index on `doc`.
async fn setup_hybrid_table(cluster: Arc<Cluster>, actors: &TestActors) -> Fixture {
    let (session, clients) = prepare_connection(actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, v VECTOR<FLOAT, 2>, doc TEXT",
        None,
    )
    .await;

    insert_rows(&session, &table).await;

    let vector_index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .options([("similarity_function", "euclidean")]),
    )
    .await;
    let fulltext_index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "doc").index_type("fulltext_index"),
    )
    .await;

    wait_for_index_count(&clients, &vector_index, ROW_COUNT).await;
    wait_for_index_count(&clients, &fulltext_index, ROW_COUNT).await;

    Fixture {
        _cluster: cluster,
        session,
        clients,
        keyspace,
        table,
        vector_index,
        fulltext_index,
    }
}

async fn insert_rows(session: &Session, table: &TableName) {
    for (pk, doc) in (0..).zip(DOCUMENTS) {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v, doc) VALUES (?, ?, ?)"),
                (pk, unit_vector(pk), doc),
            )
            .await
            .expect("failed to insert data");
    }
}

fn unit_vector(pk: i32) -> Vec<f32> {
    let angle = (pk as f32 * 15.0).to_radians();
    vec![angle.cos(), angle.sin()]
}

fn rrf_query(table: &TableName, select: &str, term: &str, limit: usize) -> String {
    format!(
        "SELECT {select} FROM {table} \
         ORDER BY RRF(ANN(v, {QUERY_VECTOR_CQL}), BM25(doc, '{term}')) LIMIT {limit}"
    )
}

async fn fused_rows<T>(fixture: &Fixture, select: &str, term: &str, limit: usize) -> Vec<T>
where
    T: for<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata>,
{
    get_query_results(
        rrf_query(&fixture.table, select, term, limit),
        &fixture.session,
    )
    .await
    .rows::<T>()
    .expect("failed to get rows")
    .map(|row| row.expect("failed to get row"))
    .collect()
}

async fn fused_pks(fixture: &Fixture, term: &str, limit: usize) -> Vec<i32> {
    fused_rows::<(i32,)>(fixture, "pk", term, limit)
        .await
        .into_iter()
        .map(|(pk,)| pk)
        .collect()
}

fn to_limit(limit: usize) -> Limit {
    NonZeroUsize::new(limit)
        .expect("limit must be positive")
        .into()
}

async fn ann_scores_by_pk(fixture: &Fixture, limit: usize) -> HashMap<i32, f32> {
    let response = fixture.clients[0]
        .post_ann(
            &fixture.vector_index.keyspace,
            &fixture.vector_index.index,
            QUERY_VECTOR.to_vec().into(),
            None,
            to_limit(limit),
        )
        .await;
    scores_by_pk(response, "similarity_scores").await
}

async fn bm25_scores_by_pk(fixture: &Fixture, term: &str, limit: usize) -> HashMap<i32, f32> {
    let response = fixture.clients[0]
        .post_bm25(
            &fixture.fulltext_index.keyspace,
            &fixture.fulltext_index.index,
            term.to_string(),
            to_limit(limit),
        )
        .await;
    scores_by_pk(response, "scores").await
}

async fn scores_by_pk(response: reqwest::Response, score_field: &str) -> HashMap<i32, f32> {
    let body: Value = response.json().await.expect("failed to parse response");
    let pks = body["primary_keys"]["pk"]
        .as_array()
        .expect("primary_keys.pk is not an array");
    let scores = body[score_field]
        .as_array()
        .expect("scores are not an array");
    pks.iter()
        .zip(scores)
        .map(|(pk, score)| {
            (
                pk.as_i64().expect("pk is not an integer") as i32,
                score.as_f64().expect("score is not a number") as f32,
            )
        })
        .collect()
}

fn assert_score_close(pk: i32, actual: Option<f32>, expected: Option<&f32>, search: &str) {
    match (actual, expected) {
        (Some(actual), Some(expected)) => assert!(
            (actual - expected).abs() <= SCORE_TOLERANCE,
            "{search} score of pk {pk}: CQL returned {actual}, vector-store returned {expected}"
        ),
        (None, None) => {}
        _ => panic!(
            "{search} score of pk {pk}: CQL returned {actual:?}, vector-store returned {expected:?}"
        ),
    }
}

#[e2etest::test(group = hybrid)]
async fn rrf_returns_union_of_both_searches_in_fused_order(fixture: Arc<Fixture>) {
    info!("started");

    let pks = fused_pks(&fixture, "fox", 10).await;

    assert_eq!(
        pks,
        vec![1, 0, 4, 2, 3, 5],
        "Expected every row once, ordered by RRF of the ANN and BM25 ranks"
    );

    info!("finished");
}

#[e2etest::test(group = hybrid)]
async fn rrf_applies_limit_after_fusion(fixture: Arc<Fixture>) {
    info!("started");

    let pks = fused_pks(&fixture, "fox", 3).await;

    assert_eq!(
        pks,
        vec![1, 0, 4],
        "Expected the top 3 rows of the fused order, including pk 4 found by BM25 only"
    );

    info!("finished");
}

#[e2etest::test(group = hybrid)]
async fn rrf_reports_rank_of_each_search(fixture: Arc<Fixture>) {
    info!("started");

    let rows = fused_rows::<(i32, Option<i32>, Option<i32>)>(
        &fixture,
        &format!("pk, ANN_RANK(v, {QUERY_VECTOR_CQL}), BM25_RANK(doc, 'fox')"),
        "fox",
        3,
    )
    .await;

    assert_eq!(
        rows,
        vec![
            (1, Some(2), Some(1)),
            (0, Some(1), Some(3)),
            (4, None, Some(2))
        ],
        "Expected the rank each search gave a row, null where the search did not find it"
    );

    info!("finished");
}

#[e2etest::test(group = hybrid)]
async fn rrf_preserves_raw_search_scores(fixture: Arc<Fixture>) {
    info!("started");

    let rows = fused_rows::<(i32, Option<f32>, Option<f32>)>(
        &fixture,
        &format!("pk, ANN_SCORE(v, {QUERY_VECTOR_CQL}), BM25_SCORE(doc, 'fox')"),
        "fox",
        3,
    )
    .await;
    let ann_scores = ann_scores_by_pk(&fixture, 3).await;
    let bm25_scores = bm25_scores_by_pk(&fixture, "fox", 3).await;

    assert_eq!(
        rows.iter().map(|(pk, _, _)| *pk).collect::<Vec<_>>(),
        vec![1, 0, 4],
        "Expected the top 3 rows of the fused order"
    );
    for (pk, ann_score, bm25_score) in rows {
        assert_score_close(pk, ann_score, ann_scores.get(&pk), "ANN");
        assert_score_close(pk, bm25_score, bm25_scores.get(&pk), "BM25");
    }

    info!("finished");
}

#[e2etest::test(group = hybrid)]
async fn rrf_with_no_fulltext_match_follows_vector_order(fixture: Arc<Fixture>) {
    info!("started");

    let pks = fused_pks(&fixture, "unicorn", 3).await;

    assert_eq!(
        pks,
        vec![0, 1, 2],
        "Expected the ANN order when the full-text search finds nothing"
    );

    info!("finished");
}
