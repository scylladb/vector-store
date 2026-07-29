/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use crate::common::CreateIndexQuery;
use crate::common::TableName;
use async_backtrace::framed;
use httpapi::IndexInfo;
use httpapi::KeyspaceName;
use httpclient::HttpClient;
use scylla::client::session::Session;
use std::sync::Arc;
use tracing::info;

const DATASET_SIZE: i32 = 100;
const OFFSET_F: i32 = DATASET_SIZE * 10;

e2etest::group!(
    name = index_create,
    fixtures = (Fixture),
    parent = crate::validator
);

struct Fixture {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for Fixture {
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

#[framed]
async fn init_keyspace_table(
    actors: &TestActors,
) -> (Arc<Session>, Vec<HttpClient>, KeyspaceName, TableName) {
    let (session, clients) = common::prepare_connection(actors).await;

    info!("Creating keyspace and table");
    let keyspace = common::create_keyspace(&session).await;
    let table = common::create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, f INT, PRIMARY KEY(pk, ck)",
        None,
    )
    .await;

    info!("Insert some vectors into the table");
    for i in 0..DATASET_SIZE {
        for j in 0..DATASET_SIZE {
            session
                .query_unpaged(
                    format!("INSERT INTO {table} (pk, ck, f, v) VALUES (?, ?, ?, ?)"),
                    (i, j, j + OFFSET_F, &vec![i as f32; 3]),
                )
                .await
                .expect("failed to insert data");
        }
    }
    (session, clients, keyspace, table)
}

#[framed]
async fn cleanup_keyspace(actors: &TestActors, keyspace: &KeyspaceName) {
    let (session, _clients) = common::prepare_connection(actors).await;

    info!("Dropping keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");
}

#[framed]
async fn wait_for_index(clients: &[HttpClient], index: &IndexInfo) {
    info!("Wait for the index to be created");
    for client in clients {
        common::wait_for_index(client, index).await;
    }
}

#[e2etest::test(group = index_create)]
async fn global_index_without_filtering_columns(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients, keyspace, table) = init_keyspace_table(&actors).await;

    info!("Create an index");
    let index = common::create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    wait_for_index(&clients, &index).await;

    info!("Query the index");
    let results = common::get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    cleanup_keyspace(&actors, &keyspace).await;

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn global_index_with_filtering_columns(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients, keyspace, table) = init_keyspace_table(&actors).await;

    info!("Create an index");
    let index = common::create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .options([("similarity_function", "euclidean")])
            .filter_columns(["f"]),
    )
    .await;

    wait_for_index(&clients, &index).await;

    info!("Query the index");
    common::wait_for(
        || async {
            let results = common::get_query_results(
                format!(
                    "SELECT pk FROM {table} WHERE f = {f} \
                    ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 100 ALLOW FILTERING",
                    f = OFFSET_F + 1
                ),
                &session,
            )
            .await;
            let rows = results.rows::<(i32,)>().expect("failed to get rows");
            rows.rows_remaining() > 10
        },
        "Wait for the query to result more than 10 items",
        common::DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    cleanup_keyspace(&actors, &keyspace).await;

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn local_index_without_filtering_columns(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients, keyspace, table) = init_keyspace_table(&actors).await;

    info!("Create an index");
    let index = common::create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v").partition_columns(["pk"]),
    )
    .await;

    wait_for_index(&clients, &index).await;

    info!("Query the index");
    let results = common::get_query_results(
        format!("SELECT ck FROM {table} WHERE pk = 1 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    cleanup_keyspace(&actors, &keyspace).await;

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn local_index_with_filtering_columns(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients, keyspace, table) = init_keyspace_table(&actors).await;

    info!("Create an index");
    let index = common::create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .partition_columns(["pk"])
            .filter_columns(["f"]),
    )
    .await;

    wait_for_index(&clients, &index).await;

    info!("Query the index");
    let results = common::get_query_results(
        format!("SELECT ck FROM {table} WHERE pk = 1 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    let results = common::get_query_results(
        format!(
            "SELECT ck FROM {table} WHERE pk = 1 AND f = {f} \
            ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING",
            f = OFFSET_F + 10
        ),
        &session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 1);

    cleanup_keyspace(&actors, &keyspace).await;

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn local_index_based_on_ck_columns(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients, keyspace, table) = init_keyspace_table(&actors).await;

    info!("Create an index");
    let index = common::create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v").partition_columns(["ck"]),
    )
    .await;

    wait_for_index(&clients, &index).await;

    info!("Query the index");
    let results = common::get_query_results(
        format!("SELECT f FROM {table} WHERE ck = 1 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    cleanup_keyspace(&actors, &keyspace).await;

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn local_index_based_on_f_columns(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients, keyspace, table) = init_keyspace_table(&actors).await;

    info!("Create an index");
    let index = common::create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v").partition_columns(["f"]),
    )
    .await;

    wait_for_index(&clients, &index).await;

    info!("Query the index");
    let results = common::get_query_results(
        format!(
            "SELECT ck FROM {table} WHERE f = {f} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10",
            f = OFFSET_F + 1
        ),
        &session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    cleanup_keyspace(&actors, &keyspace).await;

    info!("finished");
}
