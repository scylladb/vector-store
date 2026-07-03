/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use crate::common::CreateIndexQuery;
use crate::common::DEFAULT_OPERATION_TIMEOUT;
use crate::common::TableName;
use async_backtrace::framed;
use httpapi::IndexInfo;
use httpapi::KeyspaceName;
use httpclient::HttpClient;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::serialize::row::SerializeRow;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = index_modify,
    fixtures = (GroupFixture),
    parent = crate::validator
);

struct GroupFixture {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for GroupFixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        setup.setup::<TestActors>().await;
        let actors = setup.get::<TestActors>().await.unwrap();
        common::init(&actors).await;
        Self { actors }
    }

    async fn teardown(self) {
        common::cleanup(&self.actors).await;
    }
}

struct Fixture {
    session: Arc<Session>,
    clients: Vec<HttpClient>,
    keyspace: KeyspaceName,
    table: TableName,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        let actors = setup.get::<TestActors>().await.unwrap();

        let (session, clients) = common::prepare_connection(&actors).await;

        info!("Creating keyspace and table");
        let keyspace = common::create_keyspace(&session).await;
        let table = common::create_table(
            &session,
            "pk INT, ck INT, v VECTOR<FLOAT, 1>, rc INT, fc INT, PRIMARY KEY(pk, ck)",
            None,
        )
        .await;
        Self {
            session,
            clients,
            keyspace,
            table,
        }
    }

    async fn teardown(self) {
        info!("Dropping keyspace");
        self.session
            .query_unpaged(
                format!("DROP KEYSPACE {keyspace}", keyspace = self.keyspace),
                (),
            )
            .await
            .expect("failed to drop a keyspace");
    }
}

impl Fixture {
    #[framed]
    async fn insert_row(&self, columns: &str, values: impl SerializeRow) {
        self.session
            .query_unpaged(
                format!(
                    "INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                    table = self.table,
                    columns = columns,
                    placeholders = columns.split(",").map(|_| "?").join(","),
                ),
                values,
            )
            .await
            .expect("failed to insert row");
    }

    #[framed]
    async fn delete_column(&self, column: &str, pk_ck: impl SerializeRow) {
        self.session
            .query_unpaged(
                format!(
                    "DELETE {column} FROM {table} WHERE pk = ? AND ck = ?",
                    table = self.table,
                ),
                pk_ck,
            )
            .await
            .expect("failed to delete column");
    }

    #[framed]
    async fn delete_row(&self, pk_ck: impl SerializeRow) {
        self.session
            .query_unpaged(
                format!(
                    "DELETE FROM {table} WHERE pk = ? AND ck = ?",
                    table = self.table,
                ),
                pk_ck,
            )
            .await
            .expect("failed to delete row");
    }

    fn create_index_query(&self) -> CreateIndexQuery<'_> {
        CreateIndexQuery::new(&self.session, &self.clients, &self.table, "v")
            .options([("similarity_function", "euclidean")])
    }

    #[framed]
    async fn create_index(&self, query: CreateIndexQuery<'_>) -> IndexInfo {
        info!("Create an index");
        common::create_index(query).await
    }

    #[framed]
    async fn wait_for_index(&self, index: &IndexInfo) {
        info!("Wait for the index to be created");
        for client in &self.clients {
            common::wait_for_index(client, index).await;
        }
    }

    #[framed]
    async fn wait_for_index_count(&self, index: &IndexInfo, expected_size: usize) {
        common::wait_for_index_count(&self.clients, index, expected_size).await;
    }

    #[framed]
    async fn query_where(&self, filter: &str, expected_size: usize) -> Vec<(i32, i32)> {
        common::wait_for_value(
            || async {
                let mut result: Vec<_> = common::get_query_results(
                    format!(
                        "SELECT pk, ck FROM {table} WHERE {filter} \
                ORDER BY v ANN OF [0.0] LIMIT 1000",
                        table = self.table
                    ),
                    &self.session,
                )
                .await
                .rows::<(i32, i32)>()
                .expect("failed to get rows")
                .collect::<Result<_, _>>()
                .unwrap();
                result.sort();
                (result.len() == expected_size).then_some(result)
            },
            format!("query: WHERE {filter}"),
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await
    }

    #[framed]
    async fn query_filtering_where(&self, filter: &str, expected_size: usize) -> Vec<(i32, i32)> {
        common::wait_for_value(
            || async {
                let mut result: Vec<_> = common::get_query_results(
                    format!(
                        "SELECT pk, ck FROM {table} WHERE {filter} \
                ORDER BY v ANN OF [0.0] LIMIT 1000 \
                ALLOW FILTERING",
                        table = self.table
                    ),
                    &self.session,
                )
                .await
                .rows::<(i32, i32)>()
                .expect("failed to get rows")
                .collect::<Result<_, _>>()
                .unwrap();
                result.sort();
                (result.len() == expected_size).then_some(result)
            },
            format!("query: WHERE {filter} ALLOW FILTERING"),
            DEFAULT_OPERATION_TIMEOUT,
        )
        .await
    }
}

#[e2etest::test(group = index_modify)]
async fn local_index_based_on_regular_column(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_row("pk, ck, rc, v", (1, 1, 1, vec![1.0f32]))
        .await;
    // Insert a row without the regular column to test that the fullscan omits it
    fixture.insert_row("pk, ck, v", (1, 2, vec![2.0f32])).await;
    fixture
        .insert_row("pk, ck, rc, v", (1, 3, 2, vec![3.0f32]))
        .await;
    let index = fixture
        .create_index(fixture.create_index_query().partition_columns(["rc"]))
        .await;

    fixture.wait_for_index_count(&index, 2).await;

    assert_eq!(fixture.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(fixture.query_where("rc = 2", 1).await, vec![(1, 3)],);

    info!("Moving row from rc=2 to rc=1");
    fixture
        .insert_row("pk, ck, rc, v", (1, 3, 1, vec![3.0f32]))
        .await;
    assert_eq!(fixture.query_where("rc = 1", 2).await, vec![(1, 1), (1, 3)],);
    assert_eq!(fixture.query_where("rc = 2", 0).await, vec![],);

    info!("Moving row from rc=1 to rc=4");
    fixture
        .insert_row("pk, ck, rc, v", (1, 3, 4, vec![3.0f32]))
        .await;
    assert_eq!(fixture.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(fixture.query_where("rc = 4", 1).await, vec![(1, 3)],);

    info!("Deleting rc column from row (1, 3)");
    fixture.delete_column("rc", (1, 3)).await;
    assert_eq!(fixture.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(fixture.query_where("rc = 4", 0).await, vec![],);

    info!("Inserting rc = 4 into (1, 3) again");
    fixture
        .insert_row("pk, ck, rc, v", (1, 3, 4, vec![3.0f32]))
        .await;
    assert_eq!(fixture.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(fixture.query_where("rc = 4", 1).await, vec![(1, 3)],);

    info!("Moving row from rc=4 to rc=5 only by updating rc column");
    fixture.insert_row("pk, ck, rc", (1, 3, 5)).await;
    assert_eq!(fixture.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(fixture.query_where("rc = 5", 1).await, vec![(1, 3)],);

    info!("Deleting from row (1, 3)");
    fixture.delete_row((1, 3)).await;
    assert_eq!(fixture.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(fixture.query_where("rc = 4", 0).await, vec![],);

    info!("finished");
}

#[e2etest::test(group = index_modify)]
async fn global_index_with_filtering_columns(fixture: Arc<Fixture>) {
    info!("started");

    fixture
        .insert_row("pk, ck, fc, v", (1, 1, 1, vec![1.0f32]))
        .await;
    // Insert a row without the filtering column to test that the fullscan uses it
    fixture.insert_row("pk, ck, v", (1, 2, vec![2.0f32])).await;
    fixture
        .insert_row("pk, ck, fc, v", (1, 3, 2, vec![3.0f32]))
        .await;
    let index = fixture
        .create_index(fixture.create_index_query().filter_columns(["fc"]))
        .await;

    fixture.wait_for_index_count(&index, 3).await;

    assert_eq!(
        fixture.query_filtering_where("fc = 1", 1).await,
        vec![(1, 1)],
    );
    assert_eq!(
        fixture.query_filtering_where("ck = 2", 1).await,
        vec![(1, 2)],
    );
    assert_eq!(
        fixture.query_filtering_where("fc = 2", 1).await,
        vec![(1, 3)],
    );

    info!("Moving row from fc=2 to fc=1");
    fixture
        .insert_row("pk, ck, fc, v", (1, 3, 1, vec![3.0f32]))
        .await;
    assert_eq!(
        fixture.query_filtering_where("fc = 1", 2).await,
        vec![(1, 1), (1, 3)],
    );
    assert_eq!(fixture.query_filtering_where("fc = 2", 0).await, vec![],);

    info!("Moving row from fc=1 to fc=4 without updating v column");
    fixture.insert_row("pk, ck, fc", (1, 3, 4)).await;
    assert_eq!(
        fixture.query_filtering_where("fc = 1", 1).await,
        vec![(1, 1)],
    );
    assert_eq!(
        fixture.query_filtering_where("fc = 4", 1).await,
        vec![(1, 3)],
    );

    info!("Deleting fc column from row (1, 3)");
    fixture.delete_column("fc", (1, 3)).await;
    assert_eq!(
        fixture.query_filtering_where("fc = 1", 1).await,
        vec![(1, 1)],
    );
    assert_eq!(
        fixture.query_filtering_where("ck = 3", 1).await,
        vec![(1, 3)],
    );
    assert_eq!(fixture.query_filtering_where("fc = 4", 0).await, vec![],);

    info!("Inserting fc = 4 into (1, 3) again");
    fixture
        .insert_row("pk, ck, fc, v", (1, 3, 4, vec![3.0f32]))
        .await;
    assert_eq!(
        fixture.query_filtering_where("fc = 1", 1).await,
        vec![(1, 1)],
    );
    assert_eq!(
        fixture.query_filtering_where("fc = 4", 1).await,
        vec![(1, 3)],
    );

    info!("Deleting row (1, 3)");
    fixture.delete_row((1, 3)).await;
    assert_eq!(
        fixture.query_filtering_where("fc = 1", 1).await,
        vec![(1, 1)],
    );
    assert_eq!(fixture.query_filtering_where("fc = 4", 0).await, vec![],);

    info!("finished");
}
