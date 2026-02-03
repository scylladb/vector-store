/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use httpclient::HttpClient;
use scylla::client::session::Session;
use std::sync::Arc;
use tracing::info;
use vector_search_validator_tests::common::CreateIndexQuery;
use vector_search_validator_tests::common::DEFAULT_TEST_TIMEOUT;
use vector_search_validator_tests::*;
use vector_store::IndexInfo;
use vector_store::KeyspaceName;
use vector_store::TableName;

#[framed]
pub(crate) async fn new() -> TestCase {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, common::init)
        .with_cleanup(timeout, common::cleanup)
        .with_test(
            "global_index_without_filtering_columns",
            timeout,
            global_index_without_filtering_columns,
        )
        .with_test(
            "global_index_with_filtering_columns",
            timeout,
            global_index_with_filtering_columns,
        )
        .with_test(
            "local_index_without_filtering_columns",
            timeout,
            local_index_without_filtering_columns,
        )
        .with_test(
            "local_index_with_filtering_columns",
            timeout,
            local_index_with_filtering_columns,
        )
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

    const DATASET_SIZE: i32 = 100;
    const CK_OFFSET: i32 = 1000;
    const F_OFFSET: i32 = 2000;

    info!("Insert some vectors into the table");
    for i in 0..DATASET_SIZE {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, f, v) VALUES (?, ?, ?, ?)"),
                (i, i + CK_OFFSET, i + F_OFFSET, &vec![i as f32; 3]),
            )
            .await
            .expect("failed to insert data");
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

#[framed]
async fn global_index_without_filtering_columns(actors: TestActors) {
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

#[framed]
async fn global_index_with_filtering_columns(actors: TestActors) {
    info!("started");

    let (session, clients, keyspace, table) = init_keyspace_table(&actors).await;

    info!("Create an index");
    let index = common::create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]),
    )
    .await;

    wait_for_index(&clients, &index).await;

    // TODO: Re-enable this test after SCYLLADB-635 is solved.
    // info!("Query the index");
    // let results = common::get_query_results(
    //     format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
    //     &session,
    // )
    // .await;
    // let rows = results.rows::<(i32,)>().expect("failed to get rows");
    // assert_eq!(rows.rows_remaining(), 10);

    cleanup_keyspace(&actors, &keyspace).await;

    info!("finished");
}

#[framed]
async fn local_index_without_filtering_columns(actors: TestActors) {
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
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    cleanup_keyspace(&actors, &keyspace).await;

    info!("finished");
}

#[framed]
async fn local_index_with_filtering_columns(actors: TestActors) {
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
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    cleanup_keyspace(&actors, &keyspace).await;

    info!("finished");
}
