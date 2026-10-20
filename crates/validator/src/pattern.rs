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
        self.create_pattern_index_with_options(&[]).await
    }

    #[framed]
    async fn create_pattern_index_with_options(&self, options: &[(&str, &str)]) -> IndexInfo {
        let index = create_index(
            CreateIndexQuery::new(&self.session, &self.clients, &self.table, "content")
                .index_type("pattern_index")
                .options(options.iter().copied()),
        )
        .await;
        wait_for_index_on_all_nodes(&self.clients, &index).await;
        index
    }

    /// The pks of the rows whose content matches `pattern`, sorted, as LIKE returns rows in no
    /// particular order.
    #[framed]
    async fn like_pks(&self, pattern: &str, limit: usize) -> Vec<i32> {
        let mut pks: Vec<i32> = get_query_rows::<(i32,)>(
            format!(
                "SELECT pk FROM {} WHERE content LIKE ? LIMIT {limit}",
                self.table
            ),
            (pattern,),
            &self.session,
        )
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
async fn like_matches_prefix_infix_underscore_escape_and_exact_patterns(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "AB-1024-XL"),
            (2, "AB-2048-S"),
            (3, "ab-4096-M"),
            (4, "XAB-1024"),
            (5, "CD-1024-AB"),
            (6, "AB-1024"),
            (7, "AB_4096"),
        ])
        .await;
    fixture.create_pattern_index().await;

    let prefix_pks = fixture.like_pks("AB-%", 10).await;
    let infix_pks = fixture.like_pks("%024%", 10).await;
    let underscore_pks = fixture.like_pks("%-_", 10).await;
    let exact_pks = fixture.like_pks("AB-1024", 10).await;
    let escape_pks = fixture.like_pks(r"AB\_%", 10).await;

    // Prefix: neither the lowercase ab-4096-M nor XAB-1024, which holds AB- but does not start
    // with it.
    assert_eq!(prefix_pks, [1, 2, 6]);
    // Infix: every value containing 024.
    assert_eq!(infix_pks, [1, 4, 5, 6]);
    // Underscore: only values ending with '-' and exactly one character.
    assert_eq!(underscore_pks, [2, 3]);
    // Exact: neither AB-1024-XL nor XAB-1024, which contain AB-1024.
    assert_eq!(exact_pks, [6]);
    // Escape: '\_' is a literal underscore, so no AB- row matches.
    assert_eq!(escape_pks, [7]);

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_matches_a_cjk_keyword(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([
            (1, "热咖啡"),     // Hot coffee
            (2, "冰咖啡"),     // Iced coffee
            (3, "咖啡很好喝"), // The coffee tastes good
            (4, "拿铁"),       // Latte
            (5, "绿茶"),       // Green tea
        ])
        .await;
    fixture.create_pattern_index().await;

    // 咖啡 means coffee.
    let pks = fixture.like_pks("%咖啡%", 10).await;

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
async fn like_underscore_does_not_match_the_indexed_empty_value(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([(1, "AB-1024-XL"), (2, ""), (3, "X")])
        .await;
    fixture.create_pattern_index().await;

    let any_pks = fixture.like_pks("%", 10).await;
    let non_empty_pks = fixture.like_pks("%_%", 10).await;

    // '%' matches every value, so the empty value of pk 2 is in the index.
    assert_eq!(any_pks, [1, 2, 3]);
    // '%_%' needs at least one character, so only the empty value does not match.
    assert_eq!(non_empty_pks, [1, 3]);

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_ignores_letter_case_when_not_case_sensitive(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_documents([(1, "AB-1024-XL"), (2, "ab-2048-S"), (3, "CD-1024-AB")])
        .await;
    fixture
        .create_pattern_index_with_options(&[("case_sensitive", "false")])
        .await;

    let pks = fixture.like_pks("aB-%", 10).await;

    // The mixed-case pattern matches both the upper-case and the lower-case prefix.
    assert_eq!(pks, [1, 2]);

    info!("finished");
}

#[e2etest::test(group = pattern)]
async fn like_matches_a_mark_character_in_the_pattern_only_in_the_value(fixture: Arc<Fixture>) {
    info!("started");

    // Vector store uses '\u{2}' and '\u{3}' internally to mark the start and end of a value.
    // The same characters in user data must still match only as plain text.
    fixture
        .insert_documents([(1, "abc"), (2, "x\u{2}abc"), (3, "abc\u{3}")])
        .await;
    fixture.create_pattern_index().await;

    let start_mark_pks = fixture.like_pks("%\u{2}ab%", 10).await;
    let end_mark_pks = fixture.like_pks("%c\u{3}", 10).await;

    // Rows 1 and 3 start with ab, but only pk 2 holds '\u{2}' in its value.
    assert_eq!(start_mark_pks, [2]);
    // Rows 1 and 2 end with c, but only pk 3 holds '\u{3}' in its value.
    assert_eq!(end_mark_pks, [3]);

    info!("finished");
}
