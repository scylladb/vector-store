/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Tests for the pattern index: `LIKE` queries on a column with a `pattern_index`.

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
    name = pattern,
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
        let (session, clients) = prepare_connection(&cluster.actors).await;
        let keyspace = create_keyspace(&session).await;
        let table = create_table(&session, "pk INT PRIMARY KEY, content TEXT", None).await;
        Some(Self {
            session,
            keyspace,
            table,
            clients,
        })
    }

    async fn teardown(self) {
        drop_keyspace(&self.session, &self.keyspace).await;
    }
}

impl Fixture {
    #[framed]
    async fn insert_documents(&self, docs: impl IntoIterator<Item = (i32, &str)>) {
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
                .execute_unpaged(&stmt, (pk, content))
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

    #[framed]
    async fn create_pattern_index(&self) -> IndexInfo {
        let index = create_index(
            CreateIndexQuery::new(&self.session, &self.clients, &self.table, "content")
                .index_type("pattern_index"),
        )
        .await;
        wait_for_index_on_all_nodes(&self.clients, &index).await;
        index
    }

    fn like_query(&self, limit: usize) -> String {
        format!(
            "SELECT pk FROM {} WHERE content LIKE ? LIMIT {limit}",
            self.table
        )
    }

    /// The pks of the rows whose content matches `pattern`, sorted, as LIKE returns rows in no
    /// particular order.
    #[framed]
    async fn like_pks(&self, pattern: &str, limit: usize) -> Vec<i32> {
        let mut pks: Vec<i32> =
            get_query_rows::<(i32,)>(self.like_query(limit), (pattern,), &self.session)
                .await
                .into_iter()
                .map(|(pk,)| pk)
                .collect();
        pks.sort_unstable();
        pks
    }
}

#[e2etest::test(group = pattern)]
async fn pattern_index_lifecycle(fixture: Arc<Fixture>) {
    info!("started");

    let index = fixture.create_pattern_index().await;

    fixture
        .session
        .query_unpaged(format!("DROP INDEX {}", index.index), ())
        .await
        .expect("failed to drop index");

    for client in &fixture.clients {
        wait_for_no_index(client, &index).await;
    }

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_matches_prefix_infix_underscore_and_exact_patterns(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "AB-1024-XL"),
            (2, "AB-2048-S"),
            (3, "ab-4096-M"),
            (4, "XAB-1024"),
            (5, "CD-1024-AB"),
            (6, "AB-1024"),
        ])
        .await;
    fixture.create_pattern_index().await;

    let prefix_pks = fixture.like_pks("AB-%", 10).await;
    let infix_pks = fixture.like_pks("%024%", 10).await;
    let underscore_pks = fixture.like_pks("%-_", 10).await;
    let exact_pks = fixture.like_pks("AB-1024", 10).await;

    // Prefix: neither the lowercase ab-4096-M nor XAB-1024, which holds AB- but does not start
    // with it.
    assert_eq!(prefix_pks, [1, 2, 6]);
    // Infix: every value containing 024.
    assert_eq!(infix_pks, [1, 4, 5, 6]);
    // Underscore: only values ending with '-' and exactly one character.
    assert_eq!(underscore_pks, [2, 3]);
    // Exact: neither AB-1024-XL nor XAB-1024, which contain AB-1024.
    assert_eq!(exact_pks, [6]);

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_matches_a_cjk_keyword(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "宇将军"),
            (2, "李将军"),
            (3, "将军来了"),
            (4, "将领"),
            (5, "元帅"),
        ])
        .await;
    fixture.create_pattern_index().await;

    let pks = fixture.like_pks("%将军%", 10).await;

    assert_eq!(pks, [1, 2, 3]);

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_returns_an_inserted_row(fixture: Arc<Fixture>) {
    info!("started");

    fixture.create_pattern_index().await;

    fixture.insert_documents([(1, "AB-1024-XL")]).await;

    wait_for(
        || async { fixture.like_pks("%1024%", 10).await == [1] },
        "index to reflect insertion of pk 1",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_reflects_an_updated_value(fixture: Arc<Fixture>) {
    info!("started");

    fixture.insert_documents([(1, "AB-1024-XL")]).await;
    fixture.create_pattern_index().await;

    fixture.insert_documents([(1, "CD-2048-S")]).await;

    // Wait until the old value no longer matches and the new one does.
    wait_for(
        || async {
            fixture.like_pks("AB-%", 10).await.is_empty()
                && fixture.like_pks("CD-%", 10).await == [1]
        },
        "index to reflect update of pk 1",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_skips_a_deleted_row(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([(1, "AB-1024-XL"), (2, "AB-2048-S")])
        .await;
    fixture.create_pattern_index().await;

    fixture.delete_document(1).await;

    wait_for(
        || async { fixture.like_pks("AB-%", 10).await == [2] },
        "index to reflect deletion of pk 1",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_limit_restricts_result_count(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "AB-1024-XL"),
            (2, "AB-2048-S"),
            (3, "AB-4096-M"),
            (4, "AB-8192-L"),
        ])
        .await;
    fixture.create_pattern_index().await;

    let pks = fixture.like_pks("AB-%", 3).await;

    assert_eq!(pks.len(), 3);

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_without_literal_characters_returns_error(fixture: Arc<Fixture>) {
    info!("started");

    fixture.insert_documents([(1, "AB-1024-XL")]).await;
    fixture.create_pattern_index().await;

    let err = fixture
        .session
        .query_unpaged(fixture.like_query(10), ("%_%",))
        .await
        .expect_err("LIKE '%_%' should fail");

    assert!(
        err.to_string()
            .contains("a non-empty pattern must contain at least one literal character"),
        "unexpected error message: {err}"
    );

    info!("finished");
}
