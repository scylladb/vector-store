/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::common::*;
use async_backtrace::framed;
use httpapi::IndexInfo;
use itertools::Itertools;
use scylla::serialize::row::SerializeRow;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = index_modify,
    fixtures = (TestContext),
    parent = crate::standard
);

/// Per-test helper that owns a uniquely-named table in the group-shared
/// keyspace and provides index/query helpers for it.
struct ModifyTable<'a> {
    ctx: &'a TestContext,
    table: TableName,
}

impl<'a> ModifyTable<'a> {
    async fn create(ctx: &'a TestContext) -> Self {
        info!("Creating table");
        let table = ctx
            .create_table(
                "pk INT, ck INT, v VECTOR<FLOAT, 1>, rc INT, fc INT, PRIMARY KEY(pk, ck)",
                None,
            )
            .await;
        Self { ctx, table }
    }

    #[framed]
    async fn insert_row(&self, columns: &str, values: impl SerializeRow) {
        self.ctx
            .session
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
        self.ctx
            .session
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
        self.ctx
            .session
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
        self.ctx
            .index_query(&self.table, "v")
            .options([("similarity_function", "euclidean")])
    }

    #[framed]
    async fn create_index(&self, query: CreateIndexQuery<'_>) -> IndexInfo {
        info!("Create an index");
        create_index(query).await
    }

    #[framed]
    async fn wait_for_index_count(&self, index: &IndexInfo, expected_size: usize) {
        wait_for_index_count(&self.ctx.clients, index, expected_size).await;
    }

    #[framed]
    async fn query_where(&self, filter: &str, expected_size: usize) -> Vec<(i32, i32)> {
        wait_for_value(
            || async {
                let mut result: Vec<_> = get_query_results(
                    format!(
                        "SELECT pk, ck FROM {table} WHERE {filter} \
                ORDER BY v ANN OF [0.0] LIMIT 1000",
                        table = self.table
                    ),
                    &self.ctx.session,
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
        wait_for_value(
            || async {
                let mut result: Vec<_> = get_query_results(
                    format!(
                        "SELECT pk, ck FROM {table} WHERE {filter} \
                ORDER BY v ANN OF [0.0] LIMIT 1000 \
                ALLOW FILTERING",
                        table = self.table
                    ),
                    &self.ctx.session,
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
async fn local_index_based_on_regular_column(ctx: Arc<TestContext>) {
    info!("started");

    let t = ModifyTable::create(&ctx).await;

    t.insert_row("pk, ck, rc, v", (1, 1, 1, vec![1.0f32])).await;
    // Insert a row without the regular column to test that the fullscan omits it
    t.insert_row("pk, ck, v", (1, 2, vec![2.0f32])).await;
    t.insert_row("pk, ck, rc, v", (1, 3, 2, vec![3.0f32])).await;
    let index = t
        .create_index(t.create_index_query().partition_columns(["rc"]))
        .await;

    t.wait_for_index_count(&index, 2).await;

    assert_eq!(t.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_where("rc = 2", 1).await, vec![(1, 3)],);

    info!("Moving row from rc=2 to rc=1");
    t.insert_row("pk, ck, rc, v", (1, 3, 1, vec![3.0f32])).await;
    assert_eq!(t.query_where("rc = 1", 2).await, vec![(1, 1), (1, 3)],);
    assert_eq!(t.query_where("rc = 2", 0).await, vec![],);

    info!("Moving row from rc=1 to rc=4");
    t.insert_row("pk, ck, rc, v", (1, 3, 4, vec![3.0f32])).await;
    assert_eq!(t.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_where("rc = 4", 1).await, vec![(1, 3)],);

    info!("Deleting rc column from row (1, 3)");
    t.delete_column("rc", (1, 3)).await;
    assert_eq!(t.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_where("rc = 4", 0).await, vec![],);

    info!("Inserting rc = 4 into (1, 3) again");
    t.insert_row("pk, ck, rc, v", (1, 3, 4, vec![3.0f32])).await;
    assert_eq!(t.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_where("rc = 4", 1).await, vec![(1, 3)],);

    info!("Moving row from rc=4 to rc=5 only by updating rc column");
    t.insert_row("pk, ck, rc", (1, 3, 5)).await;
    assert_eq!(t.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_where("rc = 5", 1).await, vec![(1, 3)],);

    info!("Deleting from row (1, 3)");
    t.delete_row((1, 3)).await;
    assert_eq!(t.query_where("rc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_where("rc = 4", 0).await, vec![],);

    info!("finished");
}

#[e2etest::test(group = index_modify)]
async fn global_index_with_filtering_columns(ctx: Arc<TestContext>) {
    info!("started");

    let t = ModifyTable::create(&ctx).await;

    t.insert_row("pk, ck, fc, v", (1, 1, 1, vec![1.0f32])).await;
    // Insert a row without the filtering column to test that the fullscan uses it
    t.insert_row("pk, ck, v", (1, 2, vec![2.0f32])).await;
    t.insert_row("pk, ck, fc, v", (1, 3, 2, vec![3.0f32])).await;
    let index = t
        .create_index(t.create_index_query().filter_columns(["fc"]))
        .await;

    t.wait_for_index_count(&index, 3).await;

    assert_eq!(t.query_filtering_where("fc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_filtering_where("ck = 2", 1).await, vec![(1, 2)],);
    assert_eq!(t.query_filtering_where("fc = 2", 1).await, vec![(1, 3)],);

    info!("Moving row from fc=2 to fc=1");
    t.insert_row("pk, ck, fc, v", (1, 3, 1, vec![3.0f32])).await;
    assert_eq!(
        t.query_filtering_where("fc = 1", 2).await,
        vec![(1, 1), (1, 3)],
    );
    assert_eq!(t.query_filtering_where("fc = 2", 0).await, vec![],);

    info!("Moving row from fc=1 to fc=4 without updating v column");
    t.insert_row("pk, ck, fc", (1, 3, 4)).await;
    assert_eq!(t.query_filtering_where("fc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_filtering_where("fc = 4", 1).await, vec![(1, 3)],);

    info!("Deleting fc column from row (1, 3)");
    t.delete_column("fc", (1, 3)).await;
    assert_eq!(t.query_filtering_where("fc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_filtering_where("ck = 3", 1).await, vec![(1, 3)],);
    assert_eq!(t.query_filtering_where("fc = 4", 0).await, vec![],);

    info!("Inserting fc = 4 into (1, 3) again");
    t.insert_row("pk, ck, fc, v", (1, 3, 4, vec![3.0f32])).await;
    assert_eq!(t.query_filtering_where("fc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_filtering_where("fc = 4", 1).await, vec![(1, 3)],);

    info!("Deleting row (1, 3)");
    t.delete_row((1, 3)).await;
    assert_eq!(t.query_filtering_where("fc = 1", 1).await, vec![(1, 1)],);
    assert_eq!(t.query_filtering_where("fc = 4", 0).await, vec![],);

    info!("finished");
}
