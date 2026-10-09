/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Tests for hybrid search: a vector search and a full-text search fused by `RRF()`.

use crate::TestActors;
use crate::common::*;
use async_backtrace::framed;
use httpapi::IndexInfo;
use httpapi::KeyspaceName;
use httpclient::HttpClient;
use scylla::client::session::Session;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = hybrid,
    fixtures = (Cluster),
    parent = crate::validator
);

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
    session: Arc<Session>,
    keyspace: KeyspaceName,
    table: TableName,
    clients: Vec<HttpClient>,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let cluster = setup.setup::<Cluster>().await?;
        let (session, clients, keyspace, table) = Self::init_hybrid_table(&cluster.actors).await;
        Some(Self {
            session,
            keyspace,
            table,
            clients,
        })
    }

    async fn teardown(self) {
        self.session
            .query_unpaged(format!("DROP KEYSPACE {}", self.keyspace), ())
            .await
            .expect("failed to drop keyspace");
    }
}

impl Fixture {
    #[framed]
    async fn insert_documents<'a>(&self, docs: impl IntoIterator<Item = (i32, [f32; 2], &'a str)>) {
        let stmt = self
            .session
            .prepare(format!(
                "INSERT INTO {} (pk, v, doc) VALUES (?, ?, ?)",
                self.table
            ))
            .await
            .expect("failed to prepare insert statement");
        for (pk, vector, doc) in docs {
            self.session
                .execute_unpaged(&stmt, (pk, vector.to_vec(), doc))
                .await
                .expect("failed to insert data");
        }
    }

    #[framed]
    async fn create_hybrid_indexes(&self) {
        let vector = create_index(
            CreateIndexQuery::new(&self.session, &self.clients, &self.table, "v")
                .options([("similarity_function", "euclidean")]),
        )
        .await;
        let fulltext = create_index(
            CreateIndexQuery::new(&self.session, &self.clients, &self.table, "doc")
                .index_type("fulltext_index"),
        )
        .await;
        self.wait_for_index_on_all_nodes(&vector).await;
        self.wait_for_index_on_all_nodes(&fulltext).await;
    }

    async fn wait_for_index_on_all_nodes(&self, index: &IndexInfo) {
        for client in &self.clients {
            wait_for_index(client, index).await;
        }
    }

    async fn rrf_search_pks(
        &self,
        query_vector: &str,
        query_string: &str,
        limit: usize,
    ) -> Vec<i32> {
        self.rrf_search::<(i32,)>("pk", query_vector, query_string, limit)
            .await
            .into_iter()
            .map(|(pk,)| pk)
            .collect()
    }

    #[framed]
    async fn rrf_search<T>(
        &self,
        select: &str,
        query_vector: &str,
        query_string: &str,
        limit: usize,
    ) -> Vec<T>
    where
        T: for<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata>,
    {
        get_query_results(
            self.rrf_select_query(select, query_vector, query_string, limit),
            &self.session,
        )
        .await
        .rows::<T>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect()
    }

    fn rrf_select_query(
        &self,
        select: &str,
        query_vector: &str,
        query_string: &str,
        limit: usize,
    ) -> String {
        format!(
            "SELECT {select} FROM {} \
             ORDER BY RRF(ANN(v, {query_vector}), BM25(doc, '{query_string}')) LIMIT {limit}",
            self.table
        )
    }

    #[framed]
    async fn init_hybrid_table(
        actors: &TestActors,
    ) -> (Arc<Session>, Vec<HttpClient>, KeyspaceName, TableName) {
        let (session, clients) = prepare_connection(actors).await;

        let keyspace = create_keyspace(&session).await;
        let table = create_table(
            &session,
            "pk INT PRIMARY KEY, v VECTOR<FLOAT, 2>, doc TEXT",
            None,
        )
        .await;

        (session, clients, keyspace, table)
    }
}

fn assert_scores_close(
    (pk, ann_score, bm25_score): (i32, Option<f32>, Option<f32>),
    expected_ann: Option<f32>,
    expected_bm25: Option<f32>,
) {
    assert!(
        scores_close(ann_score, expected_ann),
        "ANN score of pk {pk}: CQL returned {ann_score:?}, expected {expected_ann:?}"
    );
    assert!(
        scores_close(bm25_score, expected_bm25),
        "BM25 score of pk {pk}: CQL returned {bm25_score:?}, expected {expected_bm25:?}"
    );
}

fn scores_close(actual: Option<f32>, expected: Option<f32>) -> bool {
    const SCORE_TOLERANCE: f32 = 0.01;
    match (actual, expected) {
        (Some(actual), Some(expected)) => (actual - expected).abs() <= SCORE_TOLERANCE,
        _ => actual.is_none() && expected.is_none(),
    }
}

#[e2etest::test(group = hybrid)]
async fn rrf_returns_union_of_both_searches_in_fused_order(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (
                0,
                [1.0, 0.0],
                "a fox among many other animals in the wide green meadow",
            ),
            (1, [2.0, 0.0], "fox fox fox"),
            (2, [3.0, 0.0], "lazy cat sleeps"),
            (3, [4.0, 0.0], "quick brown dog"),
            (4, [5.0, 0.0], "fox fox and a hound"),
            (5, [6.0, 0.0], "vector store search"),
        ])
        .await;
    fixture.create_hybrid_indexes().await;

    // ANN returns pk 0, 1, 2, 3, 4, 5
    // BM25 returns pk 1, 4, 0
    let pks = fixture.rrf_search_pks("[1.0, 0.0]", "fox", 10).await;

    assert_eq!(
        pks,
        vec![1, 0, 4, 2, 3, 5],
        "Expected every row once, ordered by RRF of the ANN and BM25 ranks"
    );

    info!("finished");
}

#[e2etest::test(group = hybrid)]
async fn rrf_reports_rank_of_each_search(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (
                0,
                [1.0, 0.0],
                "a fox among many other animals in the wide green meadow",
            ),
            (1, [2.0, 0.0], "fox fox fox"),
            (2, [3.0, 0.0], "lazy cat sleeps"),
            (3, [4.0, 0.0], "quick brown dog"),
            (4, [5.0, 0.0], "fox fox and a hound"),
            (5, [6.0, 0.0], "vector store search"),
        ])
        .await;
    fixture.create_hybrid_indexes().await;

    // ANN returns pk 0, 1, 2
    // BM25 returns pk 1, 4, 0
    let rows = fixture
        .rrf_search::<(i32, Option<i32>, Option<i32>)>(
            "pk, ANN_RANK(v, [1.0, 0.0]), BM25_RANK(doc, 'fox')",
            "[1.0, 0.0]",
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

    fixture
        .insert_documents([
            (
                0,
                [1.0, 0.0],
                "a fox among many other animals in the wide green meadow",
            ),
            (1, [2.0, 0.0], "fox fox fox"),
            (2, [3.0, 0.0], "lazy cat sleeps"),
            (3, [4.0, 0.0], "quick brown dog"),
            (4, [5.0, 0.0], "fox fox and a hound"),
            (5, [6.0, 0.0], "vector store search"),
        ])
        .await;
    fixture.create_hybrid_indexes().await;

    // ANN returns pk 0 (score 1.0), 1 (score 0.5), 2 (score 0.2)
    // BM25 returns pk 1 (score 1.14), 4 (score 1.02), 0 (score 0.48)
    let rows = fixture
        .rrf_search::<(i32, Option<f32>, Option<f32>)>(
            "pk, ANN_SCORE(v, [1.0, 0.0]), BM25_SCORE(doc, 'fox')",
            "[1.0, 0.0]",
            "fox",
            3,
        )
        .await;

    assert_eq!(
        rows.iter().map(|(pk, _, _)| *pk).collect::<Vec<_>>(),
        vec![1, 0, 4],
        "Expected the top 3 rows of the fused order"
    );
    assert_scores_close(rows[0], Some(0.5), Some(1.14));
    assert_scores_close(rows[1], Some(1.0), Some(0.48));
    assert_scores_close(rows[2], None, Some(1.02));

    info!("finished");
}

#[e2etest::test(group = hybrid)]
async fn rrf_with_no_fulltext_match_follows_vector_order(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (0, [1.0, 0.0], "lazy cat sleeps"),
            (1, [2.0, 0.0], "quick brown dog"),
            (2, [3.0, 0.0], "vector store search"),
            (3, [4.0, 0.0], "fox fox fox"),
        ])
        .await;
    fixture.create_hybrid_indexes().await;

    // ANN returns pk 0, 1, 2
    // BM25 returns no rows
    let pks = fixture.rrf_search_pks("[1.0, 0.0]", "unicorn", 3).await;

    assert_eq!(
        pks,
        vec![0, 1, 2],
        "Expected the ANN order when the full-text search finds nothing"
    );

    info!("finished");
}
