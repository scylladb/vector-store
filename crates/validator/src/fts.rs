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
use std::sync::RwLock;
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
    clients: Vec<HttpClient>,
    index: RwLock<Option<Arc<IndexInfo>>>,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        setup.setup::<Cluster>().await;
        let actors = setup.get::<TestActors>().await.unwrap();
        let (session, clients, keyspace, table) = Self::init_fts_table(&actors).await;
        Self {
            session,
            keyspace,
            table,
            clients,
            index: RwLock::new(None),
        }
    }

    async fn teardown(self) {
        drop_fts_keyspace(&self.session, &self.keyspace).await;
    }
}

impl Fixture {
    #[framed]
    async fn insert_documents<S: Into<String>>(&self, docs: impl IntoIterator<Item = (i32, S)>) {
        let stmt = self
            .session
            .prepare(format!(
                "INSERT INTO {} (pk, content) VALUES (?, ?)",
                self.table
            ))
            .await
            .expect("failed to prepare insert statement");
        for (pk, content) in docs {
            self.session
                .execute_unpaged(&stmt, (pk, content.into()))
                .await
                .expect("failed to insert data");
        }
    }

    #[framed]
    async fn delete_document(&self, pk: i32) {
        self.session
            .query_unpaged(format!("DELETE FROM {} WHERE pk = ?", self.table), (pk,))
            .await
            .expect("failed to delete data");
    }

    async fn bm25_search_pks(&self, query: &str) -> Vec<i32> {
        let result = self.bm25_search(query).await;
        Self::extract_pks(&result)
    }

    #[framed]
    async fn bm25_search(&self, query: &str) -> QueryRowsResult {
        self.bm25_search_impl(query, 100).await
    }

    async fn bm25_search_with_limit(&self, query: &str, limit: usize) -> QueryRowsResult {
        self.bm25_search_impl(query, limit).await
    }

    #[framed]
    async fn bm25_search_impl(&self, query: &str, cql_limit: usize) -> QueryRowsResult {
        let cql = self.bm25_select_query(query, cql_limit);
        get_opt_query_results(&cql, &self.session)
            .await
            .unwrap_or_else(|| panic!("BM25 query '{query}' failed to return results"))
    }

    #[framed]
    async fn create_fts_index(&self) {
        let index = create_fts_index(&self.session, &self.clients, &self.table).await;
        for client in &self.clients {
            wait_for_index(client, &index).await;
        }
        *self.index.write().unwrap() = Some(Arc::new(index));
    }

    fn index(&self) -> Arc<IndexInfo> {
        Arc::clone(
            self.index
                .read()
                .unwrap()
                .as_ref()
                .expect("fts index has not been created yet, call create_fts_index() first"),
        )
    }

    #[framed]
    async fn drop_index(&self) {
        let index = self.index();
        self.session
            .query_unpaged(format!("DROP INDEX {}", index.index), ())
            .await
            .expect("failed to drop index");
        for client in &self.clients {
            wait_for_no_index(client, &index).await;
        }
    }

    fn bm25_select_query(&self, query: &str, limit: usize) -> String {
        format!(
            "SELECT pk FROM {} WHERE BM25(content, '{query}') > 0 \
             ORDER BY BM25(content, '{query}') LIMIT {limit}",
            self.table
        )
    }

    fn extract_pks(result: &QueryRowsResult) -> Vec<i32> {
        result
            .rows::<(i32,)>()
            .expect("failed to get rows")
            .map(|row| row.expect("failed to get row").0)
            .collect()
    }

    #[framed]
    async fn init_fts_table(
        actors: &TestActors,
    ) -> (Arc<Session>, Vec<HttpClient>, KeyspaceName, TableName) {
        let (session, clients) = prepare_connection(actors).await;

        let keyspace = create_keyspace(&session).await;
        let table = create_table(&session, "pk INT PRIMARY KEY, content TEXT", None).await;

        (session, clients, keyspace, table)
    }
}

async fn create_fts_index(
    session: &Session,
    clients: &[HttpClient],
    table: &TableName,
) -> IndexInfo {
    create_index(
        CreateIndexQuery::new(session, clients, table, "content").index_type("fulltext_index"),
    )
    .await
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
    let index = create_fts_index(&session, &clients, &table).await;

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
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let pks = fixture.bm25_search_pks("fox").await;

    assert_eq!(pks.len(), 2, "expected exactly 2 results for 'fox'");
    assert!(pks.contains(&1), "expected pk 1 in results");
    assert!(pks.contains(&3), "expected pk 3 in results");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_boolean_and_query(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let pks = fixture.bm25_search_pks("fox AND meadow").await;

    assert_eq!(pks, vec![3], "expected only pk 3 for 'fox AND meadow'");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_boolean_or_query(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let pks = fixture.bm25_search_pks("fox OR turtle").await;

    assert_eq!(
        pks.len(),
        3,
        "expected exactly 3 results for 'fox OR turtle'"
    );
    assert!(pks.contains(&1), "expected pk 1 in results");
    assert!(pks.contains(&2), "expected pk 2 in results");
    assert!(pks.contains(&3), "expected pk 3 in results");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_boolean_not_query(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let pks = fixture.bm25_search_pks("fox NOT meadow").await;

    assert_eq!(pks, vec![1], "expected only pk 1 for 'fox NOT meadow'");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_phrase_query(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let pks = fixture.bm25_search_pks(r#""quick brown fox""#).await;

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

    fixture.create_fts_index().await;
    fixture
        .insert_documents([(1, "searchable document about databases")])
        .await;

    wait_for(
        || async { fixture.bm25_search_pks("databases").await == vec![1] },
        "index to reflect inserted document",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn fts_crud_update(fixture: Arc<Fixture>) {
    info!("started");

    fixture.create_fts_index().await;
    fixture
        .insert_documents([(1, "original content about alpha")])
        .await;

    fixture
        .insert_documents([(1, "updated content about beta")])
        .await;

    wait_for(
        || async { fixture.bm25_search_pks("alpha").await.is_empty() },
        "index to stop matching old term 'alpha'",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    wait_for(
        || async { fixture.bm25_search_pks("beta").await == vec![1] },
        "index to reflect updated term 'beta'",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn fts_crud_delete(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    fixture.delete_document(1).await;

    wait_for(
        || async { fixture.bm25_search_pks("fox").await == vec![3] },
        "index to reflect deletion of pk 1",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_empty_results_for_nonexistent_term(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let pks = fixture.bm25_search_pks("xyznonexistent").await;

    assert!(pks.is_empty(), "expected no results for non-existent term");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_case_insensitive_search(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let uppercase_pks = fixture.bm25_search_pks("FOX").await;

    assert_eq!(
        uppercase_pks.len(),
        2,
        "expected exactly 2 results for 'FOX', got {uppercase_pks:?}"
    );
    assert!(
        uppercase_pks.contains(&1),
        "uppercase 'FOX' should match pk 1, got {uppercase_pks:?}"
    );
    assert!(
        uppercase_pks.contains(&3),
        "uppercase 'FOX' should match pk 3, got {uppercase_pks:?}"
    );

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_stop_word_filtering(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let with_stop_word_pks = fixture.bm25_search_pks("the fox").await;

    assert_eq!(
        with_stop_word_pks.len(),
        2,
        "expected exactly 2 results for 'the fox'"
    );
    assert!(
        with_stop_word_pks.contains(&1),
        "stop word 'the' should not change the result set for 'the fox'"
    );
    assert!(
        with_stop_word_pks.contains(&3),
        "stop word 'the' should not change the result set for 'the fox'"
    );

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_tokenizes_by_punctuation(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([(1, "fox-runs fast, or: jumps!")])
        .await;
    fixture.create_fts_index().await;

    let fox_pks = fixture.bm25_search_pks("fox").await;
    let runs_pks = fixture.bm25_search_pks("runs").await;
    let jumps_pks = fixture.bm25_search_pks("jumps").await;

    assert_eq!(fox_pks, vec![1], "term 'fox' should be tokenized out");
    assert_eq!(runs_pks, vec![1], "term 'runs' should be tokenized out");
    assert_eq!(jumps_pks, vec![1], "term 'jumps' should be tokenized out");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_relevance_ranking_order(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "fox fox fox jumps"),
            (2, "fox runs across the meadow"),
            (3, "a slow turtle walks through the garden"),
        ])
        .await;
    fixture.create_fts_index().await;

    let pks = fixture.bm25_search_pks("fox").await;

    assert_eq!(
        pks,
        vec![1, 2],
        "document with higher term frequency should rank first"
    );

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_limit_restricts_result_count(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents((1..=10).map(|pk| (pk, "searchable document about databases")))
        .await;
    fixture.create_fts_index().await;

    let result = fixture.bm25_search_with_limit("databases", 3).await;

    assert_eq!(
        result.rows_num(),
        3,
        "LIMIT 3 should restrict results to 3 rows"
    );

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn bm25_grouped_boolean_query(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a slow turtle walks through the meadow"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let pks = fixture.bm25_search_pks("(fox OR turtle) AND meadow").await;

    assert!(
        pks.contains(&2),
        "expected pk 2 (turtle + meadow) in results"
    );
    assert!(pks.contains(&3), "expected pk 3 (fox + meadow) in results");
    assert!(
        !pks.contains(&1),
        "pk 1 has no 'meadow' and must be excluded"
    );

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn fts_recreate_index_serves_existing_data(fixture: Arc<Fixture>) {
    info!("started");

    info!("Inserting documents");
    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "a fox walks through the garden"),
            (3, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    fixture.drop_index().await;
    fixture.create_fts_index().await;

    let pks = fixture.bm25_search_pks("fox").await;

    assert!(pks.contains(&1), "expected pk 1 after index recreate");
    assert!(pks.contains(&2), "expected pk 2 after index recreate");
    assert!(pks.contains(&3), "expected pk 3 after index recreate");

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn fts_query_without_limit_returns_error(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "the quick brown fox jumps over the lazy dog"),
            (2, "the fox runs across the meadow"),
        ])
        .await;
    fixture.create_fts_index().await;

    let err = fixture
        .session
        .query_unpaged(
            format!(
                "SELECT pk FROM {} WHERE BM25(content, 'fox') > 0 \
         ORDER BY BM25(content, 'fox')",
                fixture.table
            ),
            (),
        )
        .await
        .expect_err("BM25 ordered query without LIMIT should fail");

    assert!(
        err.to_string()
            .contains("Full-text search queries require a LIMIT"),
        "unexpected error message: {err}"
    );

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn fts_index_on_int_column_returns_error(actors: Arc<TestActors>) {
    info!("started");

    let (session, _clients) = prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, num INT", None).await;
    let index = unique_index_name();

    let err = session
        .query_unpaged(
            format!("CREATE CUSTOM INDEX {index} ON {table}(num) USING 'fulltext_index'"),
            (),
        )
        .await
        .expect_err("fulltext_index on an INT column should fail");

    assert!(
        err.to_string()
            .contains("Fulltext index is only supported on text, varchar, or ascii columns"),
        "unexpected error message: {err}"
    );

    drop_fts_keyspace(&session, &keyspace).await;

    info!("finished");
}

#[e2etest::test(group = fts)]
async fn fts_large_document_set(fixture: Arc<Fixture>) {
    info!("started");

    let needle_pks = [1234, 5678, 9012];
    const LARGE_DATASET_SIZE: i32 = 10_000;
    const MAX_SEARCH_LIMIT: usize = 1000;

    info!("Inserting {LARGE_DATASET_SIZE} documents");
    let docs = (1..=LARGE_DATASET_SIZE).map(|pk| {
        let content = if needle_pks.contains(&pk) {
            format!("haystack needle document number {pk}")
        } else {
            format!("haystack document number {pk}")
        };
        (pk, content)
    });
    fixture.insert_documents(docs).await;

    info!("Creating fulltext index over {LARGE_DATASET_SIZE} documents");
    measure_duration(
        format!("Index build for {LARGE_DATASET_SIZE} documents"),
        || fixture.create_fts_index(),
    )
    .await;

    info!("Step 1: Verifying rare term matches only the expected documents");
    let rare_hits_pks = measure_duration(
        format!("BM25 needle search over {LARGE_DATASET_SIZE} documents"),
        || fixture.bm25_search_pks("needle"),
    )
    .await;
    let needle_pks_len = needle_pks.len();
    let rare_hits_pks_len = rare_hits_pks.len();

    assert_eq!(
        rare_hits_pks_len, needle_pks_len,
        "Expected exactly {needle_pks_len} rare-term results, got {rare_hits_pks_len}"
    );
    assert!(
        rare_hits_pks.contains(&needle_pks[0]),
        "expected pk {} in results",
        needle_pks[0]
    );
    assert!(
        rare_hits_pks.contains(&needle_pks[1]),
        "expected pk {} in results",
        needle_pks[1]
    );
    assert!(
        rare_hits_pks.contains(&needle_pks[2]),
        "expected pk {} in results",
        needle_pks[2]
    );

    info!("Step 2: Verifying common term matches all documents, but is capped by LIMIT");
    let common_hits = measure_duration(
        format!("BM25 haystack search over {LARGE_DATASET_SIZE} documents"),
        || fixture.bm25_search_with_limit("haystack", MAX_SEARCH_LIMIT),
    )
    .await;
    let common_hits_rows_num = common_hits.rows_num();

    assert_eq!(
        common_hits_rows_num, MAX_SEARCH_LIMIT,
        "Expected common-term results capped at {MAX_SEARCH_LIMIT}, got {common_hits_rows_num}"
    );

    info!("finished");
}
