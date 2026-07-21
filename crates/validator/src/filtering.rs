/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common::*;
use httpapi::KeyspaceName;
use scylla::client::session::Session;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = filtering,
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
        init(&actors).await;
        Self { actors }
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

/// Test ANN search filtered by partition key equality.
///
/// Table has composite primary key (pk, ck). Insert rows across multiple
/// partitions. Query with `WHERE pk = 1` to get only rows from partition 1.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_partition_key_eq(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    // Insert 5 rows per partition for 4 partitions
    for pk in 0..4 {
        for ck in 0..5 {
            session
                .query_unpaged(
                    format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                    (pk, ck, &vec![pk as f32, ck as f32, 0.0]),
                )
                .await
                .expect("failed to insert data");
        }
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 20, "Expected 20 vectors to be indexed");
    }

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!(
                    "SELECT pk, ck FROM {table} WHERE pk = 1 ORDER BY v ANN OF [1.0, 0.0, 0.0] LIMIT 20 ALLOW FILTERING"
                ),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 5)
        },
        "Waiting for pk=1 filtered ANN query to return 5 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    let rows: Vec<(i32, i32)> = result
        .rows::<(i32, i32)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();

    assert_eq!(rows.len(), 5);
    for (pk, _ck) in &rows {
        assert_eq!(*pk, 1, "Expected all rows to have pk=1, got pk={pk}");
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by partition key using IN clause.
///
/// Query with `WHERE pk IN (0, 2)` to get rows from partitions 0 and 2 only.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_partition_key_in(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for pk in 0..4 {
        for ck in 0..5 {
            session
                .query_unpaged(
                    format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                    (pk, ck, &vec![pk as f32, ck as f32, 0.0]),
                )
                .await
                .expect("failed to insert data");
        }
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 20, "Expected 20 vectors to be indexed");
    }

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!(
                    "SELECT pk, ck FROM {table} WHERE pk IN (0, 2) ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 20 ALLOW FILTERING"
                ),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 10)
        },
        "Waiting for pk IN (0,2) filtered ANN query to return 10 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    let pks: HashSet<i32> = result
        .rows::<(i32, i32)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect();

    assert_eq!(pks, HashSet::from([0, 2]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by clustering key with less-than (<).
///
/// Restrict to a single partition with `WHERE pk = 0 AND ck < 3`.
/// Only rows with ck in {0, 1, 2} should be returned.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_clustering_key_lt(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for ck in 0..10 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (0, ck, &vec![ck as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 10, "Expected 10 vectors to be indexed");
    }

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!(
                    "SELECT ck FROM {table} WHERE pk = 0 AND ck < 3 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
                ),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 3)
        },
        "Waiting for ck < 3 filtered ANN query to return 3 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    let cks: HashSet<i32> = result
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect();

    assert_eq!(cks, HashSet::from([0, 1, 2]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by clustering key with greater-than (>).
///
/// Restrict to a single partition with `WHERE pk = 0 AND ck > 7`.
/// Only rows with ck in {8, 9} should be returned.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_clustering_key_gt(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for ck in 0..10 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (0, ck, &vec![ck as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 10, "Expected 10 vectors to be indexed");
    }

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!(
                    "SELECT ck FROM {table} WHERE pk = 0 AND ck > 7 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
                ),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 2)
        },
        "Waiting for ck > 7 filtered ANN query to return 2 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    let cks: HashSet<i32> = result
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect();

    assert_eq!(cks, HashSet::from([8, 9]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by clustering key range (>= and <=).
///
/// Restrict to a single partition with `WHERE pk = 0 AND ck >= 3 AND ck <= 5`.
/// Only rows with ck in {3, 4, 5} should be returned.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_clustering_key_range(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for ck in 0..10 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (0, ck, &vec![ck as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 10, "Expected 10 vectors to be indexed");
    }

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!(
                    "SELECT ck FROM {table} WHERE pk = 0 AND ck >= 3 AND ck <= 5 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
                ),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 3)
        },
        "Waiting for ck range filtered ANN query to return 3 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    let cks: HashSet<i32> = result
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect();

    assert_eq!(cks, HashSet::from([3, 4, 5]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by both partition key and clustering key.
///
/// Create a table with composite primary key (pk, ck1, ck2).
/// Use `WHERE pk = 1 AND ck1 = 0` to restrict on both partition and
/// first clustering column.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_pk_and_ck(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck1 INT, ck2 INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck1, ck2)",
        None,
    )
    .await;

    // Insert rows across 2 partitions, 2 ck1 values, 5 ck2 values each
    for pk in 0..2 {
        for ck1 in 0..2 {
            for ck2 in 0..5 {
                session
                    .query_unpaged(
                        format!("INSERT INTO {table} (pk, ck1, ck2, v) VALUES (?, ?, ?, ?)"),
                        (pk, ck1, ck2, &vec![pk as f32, ck1 as f32, ck2 as f32]),
                    )
                    .await
                    .expect("failed to insert data");
            }
        }
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 20, "Expected 20 vectors to be indexed");
    }

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!(
                    "SELECT pk, ck1, ck2 FROM {table} WHERE pk = 1 AND ck1 = 0 ORDER BY v ANN OF [1.0, 0.0, 0.0] LIMIT 20 ALLOW FILTERING"
                ),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 5)
        },
        "Waiting for pk=1 AND ck1=0 filtered ANN query to return 5 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    let rows: Vec<(i32, i32, i32)> = result
        .rows::<(i32, i32, i32)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();

    assert_eq!(rows.len(), 5);
    for (pk, ck1, _ck2) in &rows {
        assert_eq!(*pk, 1, "Expected pk=1, got pk={pk}");
        assert_eq!(*ck1, 0, "Expected ck1=0, got ck1={ck1}");
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test that a CQL ANN query filtering on a partition key with no matching
/// rows returns empty results.
#[e2etest::test(group = filtering)]
async fn ann_filter_returns_no_results_when_nothing_matches(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for ck in 0..10 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (0, ck, &vec![0.0_f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 10, "Expected 10 vectors to be indexed");
    }

    // Wait until the index is operational for filtered queries
    wait_for(
        || async {
            get_opt_query_results(
                format!(
                    "SELECT ck FROM {table} WHERE pk = 0 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
                ),
                &session,
            )
            .await
            .is_some()
        },
        "Waiting for filtered ANN queries to be operational",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    // Query for a partition key that does not exist
    let results = get_query_results(
        format!("SELECT ck FROM {table} WHERE pk = 999 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"),
        &session,
    )
    .await;

    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 0, "Expected no results for pk = 999");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test that filtering by the vector column in a WHERE clause fails.
///
/// `WHERE v = [...]` does not apply a filter and should be rejected.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_vector_column_fails(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    for pk in 0..5 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (pk, &vec![pk as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 5, "Expected 5 vectors to be indexed");
    }

    session
        .query_unpaged(
            format!(
                "SELECT pk FROM {table} WHERE v = [1.0, 0.0, 0.0] ORDER BY v ANN OF [1.0, 0.0, 0.0] LIMIT 5 ALLOW FILTERING"
            ),
            (),
        )
        .await
        .expect_err("WHERE on vector column should fail");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test filtering on a non-indexed column with a global index.
///
/// Steps:
/// 1. Create a table with a vector column and a non-indexed integer column.
/// 2. Create a global index on the vector column, specifying the integer column as a filtering
///    column.
/// 3. Insert rows with different values for the integer column.
/// 4. Query the table with a WHERE clause filtering on the integer column and verify that only
///   the rows matching the filter are returned.
/// 5. Drop the keyspace.
#[e2etest::test(group = filtering)]
async fn global_index_filter_by_filtering_columns(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, f INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for pk in 0..10 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, f, v) VALUES (?, ?, ?, ?)"),
                (pk, pk % 4, pk % 2, &vec![pk as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index =
        create_index(CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]))
            .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 10,
            "Expected 10 vectors to be indexed in the index"
        );
    }

    info!("Querying index for f = 0");
    let results: HashSet<_> = get_query_results(
        format!("SELECT pk FROM {table} WHERE f = 0 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"),
        &session,
    )
    .await
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();
    assert_eq!(results, HashSet::from([(0,), (2,), (4,), (6,), (8,)]));

    info!("Querying index for pk = 3 AND f = 1");
    let results: HashSet<_> = get_query_results(
        format!("SELECT pk FROM {table} WHERE pk = 3 AND f = 1 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"),
        &session,
    )
    .await
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();
    assert_eq!(results, HashSet::from([(3,)]));

    info!("Querying index for ck = 2 AND f = 0");
    let results: HashSet<_> = get_query_results(
        format!("SELECT pk FROM {table} WHERE ck = 2 AND f = 0 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"),
        &session,
    )
    .await
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();
    assert_eq!(results, HashSet::from([(2,), (6,)]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test filtering on a non-indexed column with a local index.
///
/// Steps:
/// 1. Create a table with a vector column and a non-indexed integer column.
/// 2. Create a local index on the vector column, specifying the integer column as a filtering
///    column.
/// 3. Insert rows with different values for the integer column.
/// 4. Query the table with a WHERE clause filtering on the integer column and verify that only
///   the rows matching the filter are returned.
/// 5. Drop the keyspace.
#[e2etest::test(group = filtering)]
async fn local_index_filter_by_filtering_columns(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, f INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for pk in 0..10 {
        for ck in 0..10 {
            session
                .query_unpaged(
                    format!("INSERT INTO {table} (pk, ck, f, v) VALUES (?, ?, ?, ?)"),
                    (pk, ck, ck % 2, &vec![pk as f32, ck as f32, 0.0]),
                )
                .await
                .expect("failed to insert data");
        }
    }

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .partition_columns(["pk"])
            .filter_columns(["f"]),
    )
    .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 100,
            "Expected 100 vectors to be indexed in the index"
        );
    }

    info!("Querying index for pk = 3 AND f = 1");
    let results: HashSet<_> = get_query_results(
        format!("SELECT pk, ck FROM {table} WHERE pk = 3 AND f = 1 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"),
        &session,
    )
    .await
        .rows::<(i32, i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();
    assert_eq!(
        results,
        HashSet::from([(3, 1), (3, 3), (3, 5), (3, 7), (3, 9)])
    );

    info!("Querying index for pk = 7 AND ck = 2 AND f = 0");
    let results: HashSet<_> = get_query_results(
        format!("SELECT pk, ck FROM {table} WHERE pk = 7 AND ck = 2 AND f = 0 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"),
        &session,
    )
    .await
        .rows::<(i32, i32)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();
    assert_eq!(results, HashSet::from([(7, 2),]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by partition key equality on a local index.
///
/// Create a local index partitioned by pk. Insert rows across multiple
/// partitions. Query with `WHERE pk = 1` and verify only rows from
/// partition 1 are returned.
#[e2etest::test(group = filtering)]
async fn local_index_filter_by_partition_key_eq(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for pk in 0..4 {
        for ck in 0..5 {
            session
                .query_unpaged(
                    format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                    (pk, ck, &vec![pk as f32, ck as f32, 0.0]),
                )
                .await
                .expect("failed to insert data");
        }
    }

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v").partition_columns(["pk"]),
    )
    .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 20, "Expected 20 vectors to be indexed");
    }

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!(
                    "SELECT pk, ck FROM {table} WHERE pk = 1 ORDER BY v ANN OF [1.0, 0.0, 0.0] LIMIT 20"
                ),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 5)
        },
        "Waiting for pk=1 filtered ANN query on local index to return 5 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    let rows: Vec<(i32, i32)> = result
        .rows::<(i32, i32)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect();

    assert_eq!(rows.len(), 5);
    for (pk, _ck) in &rows {
        assert_eq!(*pk, 1, "Expected all rows to have pk=1, got pk={pk}");
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by clustering key range on a local index.
///
/// Create a local index partitioned by pk. Restrict to a single partition
/// with `WHERE pk = 0 AND ck >= 3 AND ck <= 5` and verify only the matching
/// clustering keys are returned.
#[e2etest::test(group = filtering)]
async fn local_index_filter_by_clustering_key_range(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for ck in 0..10 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (0, ck, &vec![ck as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v").partition_columns(["pk"]),
    )
    .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 10, "Expected 10 vectors to be indexed");
    }

    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!(
                    "SELECT ck FROM {table} WHERE pk = 0 AND ck >= 3 AND ck <= 5 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
                ),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 3)
        },
        "Waiting for ck range filtered ANN query on local index to return 3 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    let cks: HashSet<i32> = result
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect();

    assert_eq!(cks, HashSet::from([3, 4, 5]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test that a CQL ANN query on a local index filtering on a non-existent
/// partition key returns empty results.
#[e2etest::test(group = filtering)]
async fn local_index_filter_returns_no_results_when_nothing_matches(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for ck in 0..10 {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                (0, ck, &vec![0.0_f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v").partition_columns(["pk"]),
    )
    .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 10, "Expected 10 vectors to be indexed");
    }

    wait_for(
        || async {
            get_opt_query_results(
                format!(
                    "SELECT ck FROM {table} WHERE pk = 0 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"
                ),
                &session,
            )
            .await
            .is_some()
        },
        "Waiting for filtered ANN queries on local index to be operational",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    let results = get_query_results(
        format!("SELECT ck FROM {table} WHERE pk = 999 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;

    let rows = results.rows::<(i32,)>().expect("failed to get rows");
    assert_eq!(rows.rows_remaining(), 0, "Expected no results for pk = 999");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Regression test for VECTOR-609: a global ANN query (one without a full
/// partition key equality restriction) issued against a column whose only
/// vector index is local must fail rather than silently returning empty or
/// incorrect results.
///
/// A local index can only be searched within a single partition, so the
/// routing layer cannot serve a global ANN query with it. The query must be
/// rejected end-to-end instead of returning no rows.
#[e2etest::test(group = filtering)]
async fn global_ann_query_on_local_only_index_fails(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    for pk in 0..4 {
        for ck in 0..5 {
            session
                .query_unpaged(
                    format!("INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)"),
                    (pk, ck, &vec![pk as f32, ck as f32, 0.0]),
                )
                .await
                .expect("failed to insert data");
        }
    }

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v").partition_columns(["pk"]),
    )
    .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 20, "Expected 20 vectors to be indexed");
    }

    let err = session
        .query_unpaged(
            format!("SELECT pk, ck FROM {table} ORDER BY v ANN OF [1.0, 0.0, 0.0] LIMIT 20"),
            (),
        )
        .await
        .expect_err("global ANN query on a local-only vector index should fail");

    assert!(
        err.to_string().contains(
            "Global ANN query is not supported when only a local vector index is available"
        ),
        "unexpected error message: {err}"
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Reproducer for VECTOR-593: ANN query with global index and a timestamp
/// equality filter using a space-separated CQL timestamp must not fail.
#[e2etest::test(group = filtering)]
async fn global_ann_with_timestamp_eq_filter(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk TEXT, v VECTOR<FLOAT, 3>, created_at TIMESTAMP, PRIMARY KEY (pk, created_at)",
        None,
    )
    .await;

    info!("Insert rows with various timestamps");
    let rows = [
        ("a", [0.1, 0.2, 0.3], "2024-06-15 10:00:00.000Z"),
        ("b", [0.4, 0.5, 0.6], "2005-01-01 00:01:04.000Z"),
        ("c", [0.7, 0.8, 0.9], "2024-08-20 14:30:00.000Z"),
    ];
    for (pk, vec, ts) in &rows {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v, created_at) VALUES ('{pk}', {vec:?}, '{ts}')"),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    info!("Create a global ANN index");
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;
    for client in &clients {
        wait_for_index(client, &index).await;
    }

    info!("Query with space-separated timestamp equality filter");
    let results = get_query_results(
        format!(
            "SELECT pk FROM {table} \
             WHERE created_at = '2005-01-01 00:01:04.000Z' \
             ORDER BY v ANN OF [0.4, 0.5, 0.6] LIMIT 5 \
             ALLOW FILTERING"
        ),
        &session,
    )
    .await;
    let result_rows = results.rows::<(String,)>().expect("failed to get rows");
    assert_eq!(
        result_rows.rows_remaining(),
        1,
        "Expected exactly one matching row"
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Reproducer for VECTOR-593: ANN query with local index and a timestamp
/// inequality filter using a date-only CQL timestamp must not fail.
#[e2etest::test(group = filtering)]
async fn local_ann_with_timestamp_gte_filter(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk TEXT, board_id INT, v VECTOR<FLOAT, 3>, created_at TIMESTAMP, \
         PRIMARY KEY ((pk, board_id), created_at)",
        None,
    )
    .await;

    info!("Insert rows with various timestamps");
    let rows = [
        ("alice", 42, [0.1, 0.2, 0.3], "2024-06-15 10:00:00.000Z"),
        ("alice", 42, [0.12, 0.34, 0.56], "2024-08-20 14:30:00.000Z"),
        ("alice", 42, [0.3, 0.3, 0.3], "2023-01-10 08:00:00.000Z"),
    ];
    for (pk, board, vec, ts) in &rows {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {table} (pk, board_id, v, created_at) \
                     VALUES ('{pk}', {board}, {vec:?}, '{ts}')"
                ),
                (),
            )
            .await
            .expect("failed to insert data");
    }

    info!("Create a local ANN index");
    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v")
            .partition_columns(["pk", "board_id"]),
    )
    .await;
    for client in &clients {
        wait_for_index(client, &index).await;
    }

    info!("Query with timestamp inequality filter (>= '2024-01-01')");
    let results = get_query_results(
        format!(
            "SELECT pk FROM {table} \
             WHERE pk = 'alice' AND board_id = 42 \
             AND created_at >= '2024-01-01' \
             ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5
             ALLOW FILTERING"
        ),
        &session,
    )
    .await;
    let result_rows = results.rows::<(String,)>().expect("failed to get rows");
    assert_eq!(
        result_rows.rows_remaining(),
        2,
        "Expected two rows with created_at >= 2024-01-01"
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[e2etest::test(group = filtering)]
async fn ann_filter_by_clustering_key_only_requires_allow_filtering(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "p INT, v VECTOR<FLOAT, 3>, ck INT, PRIMARY KEY (p, ck)",
        None,
    )
    .await;

    insert_ck_only_test_rows(&session, &table).await;

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 3, "Expected 3 vectors to be indexed");
    }

    info!("Verify ANN query with only ck filtering is rejected without ALLOW FILTERING");
    session
        .query_unpaged(ck_only_query(&table, false), ())
        .await
        .expect_err("ANN query with ck-only filtering should fail without ALLOW FILTERING");

    info!("Verify the same query with ALLOW FILTERING returns matching rows");
    let rows = fetch_ck_only_rows_with_retry(&session, &table, true).await;
    assert_ck_only_rows(
        &rows,
        1,
        2,
        "Expected two rows with ck=1 when using ALLOW FILTERING",
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[e2etest::test(group = filtering)]
async fn ann_filter_by_non_pk_column_rejected_without_allow_filtering(actors: Arc<TestActors>) {
    info!("started");

    let (session, keyspace, table) = prepare_non_pk_column_filter_test(&actors).await;

    info!("Test ANN query with indexed non-PK column filtering");
    let query =
        format!("SELECT * FROM {table} WHERE c = 1 ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5");

    session
        .query_unpaged(query, ())
        .await
        .expect_err("ANN query with non-PK column filtering should fail");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[e2etest::test(group = filtering)]
async fn ann_filter_by_non_pk_column_rejected_with_allow_filtering(actors: Arc<TestActors>) {
    info!("started");

    let (session, keyspace, table) = prepare_non_pk_column_filter_test(&actors).await;

    info!("Test ANN query with indexed non-PK column filtering and ALLOW FILTERING");
    let query = format!(
        "SELECT * FROM {table} WHERE c = 1 ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5 ALLOW FILTERING"
    );

    session
        .query_unpaged(query, ())
        .await
        .expect_err("ANN query with non-PK column filtering and ALLOW FILTERING should fail");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

async fn insert_ck_only_test_rows(session: &Session, table: &TableName) {
    session
        .query_unpaged(
            format!("INSERT INTO {table} (p, ck, v) VALUES (1, 1, [0.1, 0.2, 0.3])"),
            (),
        )
        .await
        .expect("failed to insert row p=1, ck=1");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (p, ck, v) VALUES (2, 1, [5.0, 5.0, 5.0])"),
            (),
        )
        .await
        .expect("failed to insert row p=2, ck=1");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (p, ck, v) VALUES (3, 2, [0.1, 0.2, 0.3])"),
            (),
        )
        .await
        .expect("failed to insert row p=3, ck=2");
}

fn ck_only_query(table: &TableName, with_allow_filtering: bool) -> String {
    let base =
        format!("SELECT p, ck FROM {table} WHERE ck = 1 ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 5");
    if with_allow_filtering {
        format!("{base} ALLOW FILTERING")
    } else {
        base
    }
}

async fn fetch_ck_only_rows_with_retry(
    session: &Session,
    table: &TableName,
    with_allow_filtering: bool,
) -> Vec<(i32, i32)> {
    let query = ck_only_query(table, with_allow_filtering);
    let wait_message = if with_allow_filtering {
        "Waiting for filtered ANN query (ck=1 with ALLOW FILTERING) to be operational"
    } else {
        "Waiting for filtered ANN query (ck=1 only) to be operational"
    };

    wait_for(
        || async {
            get_opt_query_results(query.clone(), session)
                .await
                .is_some()
        },
        wait_message,
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    get_query_results(query, session)
        .await
        .rows::<(i32, i32)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row"))
        .collect()
}

fn assert_ck_only_rows(
    rows: &[(i32, i32)],
    expected_ck: i32,
    expected_len: usize,
    expected_len_message: &str,
) {
    assert_eq!(rows.len(), expected_len, "{expected_len_message}");
    assert!(
        rows.iter().all(|(_, ck)| *ck == expected_ck),
        "Expected only rows with ck={expected_ck}"
    );
}

async fn prepare_non_pk_column_filter_test(
    actors: &TestActors,
) -> (Arc<Session>, KeyspaceName, TableName) {
    let (session, clients) = prepare_connection(actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "p INT PRIMARY KEY, c INT, v VECTOR<FLOAT, 3>",
        None,
    )
    .await;

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;
    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 0, "Index should start empty");
    }

    info!("Create index on non-PK column c");
    session
        .query_unpaged(format!("CREATE INDEX ON {table}(c)"), ())
        .await
        .expect("failed to create index on c");

    (session, keyspace, table)
}
