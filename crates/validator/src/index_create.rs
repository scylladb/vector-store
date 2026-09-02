/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use async_backtrace::framed;
use httpapi::IndexInfo;
use std::sync::Arc;
use tracing::info;

const DATASET_SIZE: i32 = 100;
const OFFSET_F: i32 = DATASET_SIZE * 10;

e2etest::group!(
    name = index_create,
    fixtures = (TestContext),
    parent = crate::standard
);

#[framed]
async fn init_table(ctx: &TestContext) -> TableName {
    info!("Creating table");
    let table = ctx
        .create_table(
            "pk INT, ck INT, v VECTOR<FLOAT, 3>, f INT, PRIMARY KEY(pk, ck)",
            None,
        )
        .await;

    info!("Insert some vectors into the table");
    let stmt = ctx
        .session
        .prepare(format!(
            "INSERT INTO {table} (pk, ck, f, v) VALUES (?, ?, ?, ?)"
        ))
        .await
        .expect("failed to prepare insert statement");
    for i in 0..DATASET_SIZE {
        let v = vec![i as f32; 3];
        for j in 0..DATASET_SIZE {
            ctx.session
                .execute_unpaged(&stmt, (i, j, j + OFFSET_F, &v))
                .await
                .expect("failed to insert data");
        }
    }
    table
}

#[framed]
async fn wait_for_index_serving(ctx: &TestContext, index: &IndexInfo) {
    info!("Wait for the index to be created");
    for client in &ctx.clients {
        wait_for_index(client, index).await;
    }
}

#[e2etest::test(group = index_create)]
async fn global_index_without_filtering_columns(ctx: Arc<TestContext>) {
    info!("started");

    let table = init_table(&ctx).await;

    info!("Create an index");
    let index = ctx.create_index(&table, "v").await;

    wait_for_index_serving(&ctx, &index).await;

    info!("Query the index");
    let results = get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &ctx.session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn global_index_with_filtering_columns(ctx: Arc<TestContext>) {
    info!("started");

    let table = init_table(&ctx).await;

    info!("Create an index");
    let index = create_index(
        ctx.index_query(&table, "v")
            .options([("similarity_function", "euclidean")])
            .filter_columns(["f"]),
    )
    .await;

    wait_for_index_serving(&ctx, &index).await;

    info!("Query the index");
    wait_for(
        || async {
            let results = get_query_results(
                format!(
                    "SELECT pk FROM {table} WHERE f = {f} \
                    ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 100 ALLOW FILTERING",
                    f = OFFSET_F + 1
                ),
                &ctx.session,
            )
            .await;
            let rows = results.rows::<(i32,)>().expect("failed to get rows");
            rows.rows_remaining() > 10
        },
        "Wait for the query to result more than 10 items",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn local_index_without_filtering_columns(ctx: Arc<TestContext>) {
    info!("started");

    let table = init_table(&ctx).await;

    info!("Create an index");
    let index = create_index(ctx.index_query(&table, "v").partition_columns(["pk"])).await;

    wait_for_index_serving(&ctx, &index).await;

    info!("Query the index");
    let results = get_query_results(
        format!("SELECT ck FROM {table} WHERE pk = 1 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &ctx.session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn local_index_with_filtering_columns(ctx: Arc<TestContext>) {
    info!("started");

    let table = init_table(&ctx).await;

    info!("Create an index");
    let index = create_index(
        ctx.index_query(&table, "v")
            .partition_columns(["pk"])
            .filter_columns(["f"]),
    )
    .await;

    wait_for_index_serving(&ctx, &index).await;

    info!("Query the index");
    let results = get_query_results(
        format!("SELECT ck FROM {table} WHERE pk = 1 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &ctx.session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    let results = get_query_results(
        format!(
            "SELECT ck FROM {table} WHERE pk = 1 AND f = {f} \
            ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING",
            f = OFFSET_F + 10
        ),
        &ctx.session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 1);

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn local_index_based_on_ck_columns(ctx: Arc<TestContext>) {
    info!("started");

    let table = init_table(&ctx).await;

    info!("Create an index");
    let index = create_index(ctx.index_query(&table, "v").partition_columns(["ck"])).await;

    wait_for_index_serving(&ctx, &index).await;

    info!("Query the index");
    let results = get_query_results(
        format!("SELECT f FROM {table} WHERE ck = 1 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &ctx.session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    info!("finished");
}

#[e2etest::test(group = index_create)]
async fn local_index_based_on_f_columns(ctx: Arc<TestContext>) {
    info!("started");

    let table = init_table(&ctx).await;

    info!("Create an index");
    let index = create_index(ctx.index_query(&table, "v").partition_columns(["f"])).await;

    wait_for_index_serving(&ctx, &index).await;

    info!("Query the index");
    let results = get_query_results(
        format!(
            "SELECT ck FROM {table} WHERE f = {f} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10",
            f = OFFSET_F + 1
        ),
        &ctx.session,
    )
    .await;
    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 10);

    info!("finished");
}

/// Test that an index on a table with a primary key type the service cannot represent is skipped.
/// Serving such an index would fail every query. Discovery must drop it and keep serving the other indexes.
#[e2etest::test(group = index_create)]
async fn index_with_unsupported_primary_key_type_is_skipped(ctx: Arc<TestContext>) {
    info!("started");

    let keyspace = &ctx.keyspace;

    info!("Create an index on a table with an unsupported primary key type");
    let unsupported_table = ctx
        .create_table(
            "pk FROZEN<TUPLE<INT, INT>> PRIMARY KEY, v VECTOR<FLOAT, 3>",
            None,
        )
        .await;
    let unsupported_index = unique_index_name();
    ctx.session
        .query_unpaged(
            format!(
                "CREATE CUSTOM INDEX {unsupported_index} ON {unsupported_table}(v) \
                USING 'vector_index'"
            ),
            (),
        )
        .await
        .expect("failed to create an index");

    // Discovery reads `system_schema.indexes`, so the skipped index must be there to be skipped.
    wait_for(
        || async {
            get_query_results(
                format!(
                    "SELECT index_name FROM system_schema.indexes \
                    WHERE keyspace_name = '{keyspace}' AND table_name = '{unsupported_table}' \
                    AND index_name = '{unsupported_index}'"
                ),
                &ctx.session,
            )
            .await
            .rows_num()
                == 1
        },
        "the unsupported index to appear in system_schema.indexes",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    info!("Create a valid index in the same keyspace");
    let table = ctx
        .create_table("pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None)
        .await;
    let index = ctx.create_index(&table, "v").await;

    // The valid index serves only after a discovery pass that already saw the skipped one.
    wait_for_index_serving(&ctx, &index).await;

    for client in &ctx.clients {
        assert!(
            !client
                .indexes()
                .await
                .iter()
                .any(|idx| idx.index == unsupported_index),
            "Expected the unsupported index to be skipped at {url}",
            url = client.url()
        );
        assert!(
            client
                .index_status(keyspace, &unsupported_index)
                .await
                .is_err(),
            "Expected no status for the skipped index at {url}",
            url = client.url()
        );
    }

    info!("finished");
}
