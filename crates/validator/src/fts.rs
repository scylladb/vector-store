/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::common::*;
use async_backtrace::framed;
use httpapi::IndexInfo;
use httpapi::KeyspaceName;
use httpclient::HttpClient;
use scylla::client::session::Session;
use scylla::response::query_result::QueryRowsResult;
use std::sync::Arc;
use tracing::info;

e2etest::group!(name = fts, fixtures = (Cluster), parent = crate::validator);

struct Cluster {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for Cluster {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        setup.setup::<TestActors>().await;
        let actors = setup.get::<TestActors>().await.unwrap();
        init(&actors).await;
        Self { actors }
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

struct Fixture {
    session: Arc<Session>,
    keyspace: KeyspaceName,
    table: TableName,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        setup.setup::<Cluster>().await;
        let actors = setup.get::<TestActors>().await.unwrap();
        let (session, _clients, keyspace, table, _index) = Self::init_fts_table(&actors).await;
        Self {
            session,
            keyspace,
            table,
        }
    }

    async fn teardown(self) {
        drop_fts_keyspace(&self.session, &self.keyspace).await;
    }
}

impl Fixture {
    async fn insert_documents(&self, docs: &[(i32, &str)]) {
        for (pk, content) in docs {
            self.session
                .query_unpaged(
                    format!("INSERT INTO {} (pk, content) VALUES (?, ?)", self.table),
                    (pk, content),
                )
                .await
                .expect("failed to insert data");
        }
    }

    async fn delete_document(&self, pk: i32) {
        self.session
            .query_unpaged(format!("DELETE FROM {} WHERE pk = ?", self.table), (pk,))
            .await
            .expect("failed to delete data");
    }

    #[framed]
    async fn bm25_search(&self, query: &str, expected_count: usize) -> QueryRowsResult {
        let cql = self.bm25_select_query_with_limit(query, 100);
        let session = &self.session;
        wait_for_value(
            || async {
                let result = get_opt_query_results(&cql, session).await;
                result.filter(|r| r.rows_num() == expected_count)
            },
            format!("BM25 query '{query}' to return {expected_count} documents"),
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await
    }

    fn bm25_select_query_with_limit(&self, query: &str, limit: usize) -> String {
        let table = &self.table;
        format!(
            "SELECT pk FROM {table} WHERE BM25(content, '{query}') > 0 \
             ORDER BY BM25(content, '{query}') LIMIT {limit}"
        )
    }

    fn extract_pks(&self, result: &QueryRowsResult) -> Vec<i32> {
        result
            .rows::<(i32,)>()
            .expect("failed to get rows")
            .map(|row| row.expect("failed to get row").0)
            .collect()
    }

    async fn bm25_search_pks(&self, query: &str, expected_count: usize) -> Vec<i32> {
        let result = self.bm25_search(query, expected_count).await;
        self.extract_pks(&result)
    }

    #[framed]
    async fn init_fts_table(
        actors: &TestActors,
    ) -> (
        Arc<Session>,
        Vec<HttpClient>,
        KeyspaceName,
        TableName,
        IndexInfo,
    ) {
        let (session, clients) = prepare_connection(actors).await;

        let keyspace = create_keyspace(&session).await;
        let table = create_table(&session, "pk INT PRIMARY KEY, content TEXT", None).await;

        let index = create_index(
            CreateIndexQuery::new(&session, &clients, &table, "content")
                .index_type("fulltext_index"),
        )
        .await;

        (session, clients, keyspace, table, index)
    }
}

#[framed]
async fn drop_fts_keyspace(session: &Session, keyspace: &KeyspaceName) {
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");
}

#[e2etest::test(group = fts)]
async fn fts_index_lifecycle(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, content TEXT", None).await;

    info!("Creating fulltext index");
    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "content").index_type("fulltext_index"),
    )
    .await;

    info!("Verifying index is SERVING on all nodes");
    for client in &clients {
        wait_for_index(client, &index).await;
    }

    info!("Dropping fulltext index");
    session
        .query_unpaged(format!("DROP INDEX {}", index.index), ())
        .await
        .expect("failed to drop index");

    info!("Verifying index is removed on all nodes");
    for client in &clients {
        wait_for_no_index(client, &index).await;
    }

    drop_fts_keyspace(&session, &keyspace).await;

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_search_returns_matching_docs(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents(&[
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;

    let pks = fixture.bm25_search_pks("fox", 2).await;

    assert!(pks.contains(&1), "expected pk 1 in results");
    assert!(pks.contains(&3), "expected pk 3 in results");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_boolean_and_query(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents(&[
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;

    let pks = fixture.bm25_search_pks("fox AND meadow", 1).await;

    assert_eq!(pks, vec![3], "expected only pk 3 for 'fox AND meadow'");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_boolean_or_query(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents(&[
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;

    let pks = fixture.bm25_search_pks("fox OR turtle", 3).await;

    assert!(pks.contains(&1), "expected pk 1 in results");
    assert!(pks.contains(&2), "expected pk 2 in results");
    assert!(pks.contains(&3), "expected pk 3 in results");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_boolean_not_query(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents(&[
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;

    let pks = fixture.bm25_search_pks("fox NOT meadow", 1).await;

    assert_eq!(pks, vec![1], "expected only pk 1 for 'fox NOT meadow'");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_phrase_query(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents(&[
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;

    let pks = fixture.bm25_search_pks(r#""quick brown fox""#, 1).await;

    assert_eq!(
        pks,
        vec![1],
        "expected only pk 1 for phrase '\"quick brown fox\"'"
    );

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn fts_crud_insert(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents(&[(1, "searchable document about databases")])
        .await;

    let pks = fixture.bm25_search_pks("databases", 1).await;

    assert_eq!(pks, vec![1], "inserted document should be searchable");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn fts_crud_update(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents(&[(1, "original content about alpha")])
        .await;

    info!("Updating document content");
    fixture
        .insert_documents(&[(1, "updated content about beta")])
        .await;

    let pks = fixture.bm25_search_pks("beta", 1).await;

    assert_eq!(pks, vec![1], "updated term 'beta' should be searchable");

    let pks = fixture.bm25_search_pks("alpha", 0).await;

    assert!(pks.is_empty(), "old term 'alpha' should no longer match");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn fts_crud_delete(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents(&[
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;

    fixture.bm25_search("fox", 2).await;

    info!("Deleting document pk=1");
    fixture.delete_document(1).await;

    let pks = fixture.bm25_search_pks("fox", 1).await;

    assert_eq!(pks, vec![3], "only pk 3 should remain after deleting pk 1");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_empty_results_for_nonexistent_term(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents(&[
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;

    fixture.bm25_search("fox", 2).await;

    let pks = fixture.bm25_search_pks("xyznonexistent", 0).await;

    assert!(pks.is_empty(), "expected no results for non-existent term");

    info!("finished");
}
