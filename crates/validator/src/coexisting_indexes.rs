/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Tests for a table with a secondary index, a vector index, and a fulltext index
//! coexisting on different columns.

use crate::TestActors;
use crate::common::*;
use httpapi::KeyspaceName;
use scylla::client::session::Session;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = coexisting_indexes,
    fixtures = (Cluster, Fixture),
    parent = crate::validator
);

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
    _cluster: Arc<Cluster>,
    session: Arc<Session>,
    keyspace: KeyspaceName,
    table: TableName,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        let cluster = setup.setup::<Cluster>().await;
        let actors = setup.get::<TestActors>().await.unwrap();
        let (session, keyspace, table) = setup_table(&actors).await;
        Self {
            _cluster: cluster,
            session,
            keyspace,
            table,
        }
    }

    async fn teardown(self) {
        self.session
            .query_unpaged(format!("DROP KEYSPACE {}", self.keyspace), ())
            .await
            .expect("failed to drop keyspace");
    }
}

/// Creates a table with 20 rows:
/// - `pk`  = 0..20
/// - `v`   = `[pk, pk, pk]`
/// - `doc` = a document containing "fox" for `pk % 10 == {0, 1, 7}`
/// - `s`   = `pk % 3`
///
/// A vector index is created on `v`, a fulltext index on `doc`, and a secondary index on `s`.
async fn setup_table(actors: &TestActors) -> (Arc<Session>, KeyspaceName, TableName) {
    let (session, clients) = prepare_connection(actors).await;
    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>, doc TEXT, s INT",
        None,
    )
    .await;

    session
        .query_unpaged(format!("CREATE INDEX ON {table}(s)"), ())
        .await
        .expect("failed to create secondary index on s");

    let documents = [
        "the quick brown fox jumps over the lazy dog",
        "a fast red fox runs through the forest",
        "the lazy cat sleeps all day long",
        "rust programming language is fast and safe",
        "vector store provides semantic search",
        "fulltext search indexes text documents",
        "scylladb is a fast nosql database",
        "the fox is a cunning and agile animal",
        "machine learning models produce embeddings",
        "approximate nearest neighbor search",
    ];

    for i in 0..20 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v, doc, s) VALUES (?, ?, ?, ?)"),
                (
                    i,
                    vec![i as f32; 3],
                    documents[i as usize % documents.len()],
                    i % 3,
                ),
            )
            .await
            .expect("failed to insert data");
    }

    let vector_index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .options([("similarity_function", "euclidean")]),
    )
    .await;
    let fulltext_index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "doc").index_type("fulltext_index"),
    )
    .await;

    for client in &clients {
        wait_for_index(client, &vector_index).await;
        wait_for_index(client, &fulltext_index).await;
    }

    (session, keyspace, table)
}

#[e2etest::test(group = coexisting_indexes)]
async fn query_secondary_index_only(fixture: Arc<Fixture>) {
    info!("started");

    let results = get_query_results(
        format!("SELECT pk FROM {} WHERE s = 0", fixture.table),
        &fixture.session,
    )
    .await;
    let rows: Vec<(i32,)> = results
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|r| r.expect("failed to get row"))
        .collect();

    assert_eq!(rows.len(), 7, "Expected 7 rows with s=0");
    assert!(rows.iter().all(|(pk,)| pk % 3 == 0));

    info!("finished");
}

#[e2etest::test(group = coexisting_indexes)]
async fn query_vector_index_only(fixture: Arc<Fixture>) {
    info!("started");

    let results = get_query_results(
        format!(
            "SELECT pk FROM {} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 3",
            fixture.table
        ),
        &fixture.session,
    )
    .await;
    let rows: Vec<(i32,)> = results
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|r| r.expect("failed to get row"))
        .collect();
    assert_eq!(
        rows,
        vec![(0,), (1,), (2,)],
        "Expected the closest 3 rows to [0.0, 0.0, 0.0]"
    );

    info!("finished");
}

#[e2etest::test(group = coexisting_indexes)]
async fn query_fulltext_index_only(fixture: Arc<Fixture>) {
    info!("started");

    let results = get_query_results(
        format!(
            "SELECT pk FROM {} WHERE BM25(doc, 'fox') > 0 ORDER BY BM25(doc, 'fox') LIMIT 10",
            fixture.table
        ),
        &fixture.session,
    )
    .await;
    let mut rows: Vec<(i32,)> = results
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|r| r.expect("failed to get row"))
        .collect();

    rows.sort();
    assert_eq!(
        rows,
        vec![(0,), (1,), (7,), (10,), (11,), (17,)],
        "Expected all rows whose doc contains 'fox'"
    );

    info!("finished");
}

#[e2etest::test(group = coexisting_indexes)]
async fn query_vector_and_secondary_index_rejected(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .session
        .query_unpaged(
            format!(
                "SELECT pk FROM {} WHERE s = 0 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 5",
                fixture.table
            ),
            (),
        )
        .await
        .expect_err("combining secondary index filter with ANN order should fail");

    info!("finished");
}

#[e2etest::test(group = coexisting_indexes)]
async fn query_fulltext_and_secondary_index_rejected(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .session
        .query_unpaged(
            format!(
                "SELECT pk FROM {} WHERE s = 0 AND BM25(doc, 'fox') > 0 \
                 ORDER BY BM25(doc, 'fox') LIMIT 5",
                fixture.table
            ),
            (),
        )
        .await
        .expect_err("combining secondary index filter with BM25 order should fail");

    info!("finished");
}

#[e2etest::test(group = coexisting_indexes)]
async fn query_vector_and_fulltext_index_rejected(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .session
        .query_unpaged(
            format!(
                "SELECT pk FROM {} WHERE BM25(doc, 'fox') > 0 \
                 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 5",
                fixture.table
            ),
            (),
        )
        .await
        .expect_err("combining BM25 filter with ANN order should fail");

    info!("finished");
}
