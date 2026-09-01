/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common::*;
use httpapi::KeyspaceName;
use scylla::client::session::Session;
use scylla::value::CqlTimeuuid;
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

e2etest::group!(
    name = filtering,
    fixtures = (Fixture),
    parent = crate::validator
);

struct Fixture {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        init(&actors).await;
        Some(Self { actors })
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

#[e2etest::test(group = filtering)]
async fn ann_filter_by_inet_clustering_key_gt(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INET, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    let addrs = [
        "0.0.0.0",
        "10.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "2001:db8::1",
    ];
    for (idx, addr) in addrs.iter().enumerate() {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, v) VALUES (0, '{addr}', ?)"),
                (&vec![idx as f32, 0.0, 0.0],),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count,
            addrs.len(),
            "Expected every address to be indexed"
        );
    }

    // ScyllaDB owns the ordering, so take the expected rows from a plain query.
    let expected: HashSet<IpAddr> = get_query_results(
        format!("SELECT ck FROM {table} WHERE pk = 0 AND ck > '::'"),
        &session,
    )
    .await
    .rows::<(IpAddr,)>()
    .expect("failed to get rows")
    .map(|row| row.expect("failed to get row").0)
    .collect();

    assert!(
        expected.iter().any(|addr| addr.is_ipv4()) && expected.iter().any(|addr| addr.is_ipv6()),
        "Expected both address families above '::', got: {expected:?}"
    );

    let result: HashSet<IpAddr> = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!(
                    "SELECT ck FROM {table} WHERE pk = 0 AND ck > '::' \
                    ORDER BY v ANN OF [-1.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
                ),
                &session,
            )
            .await;
            result.filter(|result| result.rows_num() == expected.len())
        },
        "Waiting for the inet filtered ANN query to return every matching row",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await
    .rows::<(IpAddr,)>()
    .expect("failed to get rows")
    .map(|row| row.expect("failed to get row").0)
    .collect();

    assert_eq!(result, expected);

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

/// Test ANN search filtered by equality on a BLOB filtering column.
///
/// Regression test for VECTOR-889: `cql_cmp()` had no comparison arm for
/// `CqlValue::Blob`, so a `WHERE <blob column> = <literal>` restriction
/// during ANN search always evaluated to "not equal" - silently matching
/// zero rows instead of the expected ones. VECTOR-889 reported the same bug
/// for Boolean, Uuid and Timeuuid columns too, and we have corresponding
/// tests for these types as well, below.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_blob_column_eq(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, f BLOB, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
            (0, vec![1u8, 2, 3], &vec![0.0f32, 0.0, 0.0]),
        )
        .await
        .expect("failed to insert data");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
            (1, vec![4u8, 5, 6], &vec![0.0f32, 0.0, 0.0]),
        )
        .await
        .expect("failed to insert data");

    let index =
        create_index(CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]))
            .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 2, "Expected 2 vectors to be indexed");
    }

    info!("Querying index for f = 0x010203");
    let results = get_pks(
        format!(
            "SELECT pk FROM {table} WHERE f = 0x010203 ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
        ),
        &session,
    )
    .await;
    assert_eq!(results, HashSet::from([0]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by an inequality (`>`) on a BLOB filtering
/// column, cross-checked against Scylla's own (non-ANN) filtering, to
/// verify we got the ordering right.
///
/// Blob ordering is unsigned byte-wise comparison, with a shorter blob
/// that is a prefix of a longer one sorting first. The dataset has a
/// three-way prefix family (b1 < b2 < b4, all starting [1,2]) alongside a
/// value that differs in an earlier byte (b3), so a subtly wrong
/// comparison (e.g. one that ignores length or compares lengths first)
/// would misplace some of them. b2 vs b4 also gives a first, weaker,
/// signed-byte check: their trailing tie-break byte is 0 vs 255. b5/b6
/// add a stronger version of the same check at the primary differentiator
/// instead of a tie-break: their first differing byte (0x7f vs 0x80)
/// would flip order if compared as signed i8 instead of u8.
///
/// Rather than hardcoding the expected order, this loops over every value
/// as the `>` threshold and, for each, derives the expected row set from
/// Scylla evaluating the same restriction without `ORDER BY v ANN OF` -
/// asserting the ANN-filtered query returns exactly that set. Looping the
/// threshold over the whole dataset exercises every pairwise comparison
/// among the values, not just one hand-picked split.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_blob_column_ordering_matches_scylla(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, f BLOB, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    let values: [(i32, Vec<u8>); 6] = [
        (0, vec![1, 2]),      // b1: prefix of b2
        (1, vec![1, 2, 0]),   // b2: b1 + trailing byte -> greater than b1
        (2, vec![1, 10]),     // b3: differs from b1/b2 at 2nd byte
        (3, vec![1, 2, 255]), // b4: b1 + trailing byte, greater than b2
        (4, vec![1, 127]),    // b5: 0x7f
        (5, vec![1, 128]),    // b6: 0x80 - must sort after b5, not before
    ];
    for (pk, f) in &values {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
                (pk, f, &vec![*pk as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index =
        create_index(CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]))
            .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 6, "Expected 6 vectors to be indexed");
    }

    for (threshold_pk, threshold) in &values {
        let literal = blob_literal(threshold);
        info!("Comparing ANN-filtered and plain (non-ANN) results for f > {literal}");
        let ann_results = get_pks(
            format!(
                "SELECT pk FROM {table} WHERE f > {literal} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
            ),
            &session,
        )
        .await;
        let plain_results = get_pks(
            format!("SELECT pk FROM {table} WHERE f > {literal} ALLOW FILTERING"),
            &session,
        )
        .await;

        assert_eq!(
            ann_results, plain_results,
            "ANN-filtered results must match Scylla's own (non-ANN) filtering \
             for f > {literal} (threshold pk={threshold_pk})"
        );
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by equality on a BOOLEAN filtering column.
///
/// See ann_filter_by_blob_column_eq above: this is the Boolean case of the
/// same VECTOR-889 bug.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_boolean_column_eq(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, f BOOLEAN, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
            (0, true, &vec![0.0f32, 0.0, 0.0]),
        )
        .await
        .expect("failed to insert data");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
            (1, false, &vec![0.0f32, 0.0, 0.0]),
        )
        .await
        .expect("failed to insert data");

    let index =
        create_index(CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]))
            .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 2, "Expected 2 vectors to be indexed");
    }

    info!("Querying index for f = true");
    let results = get_pks(
        format!(
            "SELECT pk FROM {table} WHERE f = true ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
        ),
        &session,
    )
    .await;
    assert_eq!(results, HashSet::from([0]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by an inequality (`<`) on a BOOLEAN filtering
/// column, cross-checked against Scylla's own (non-ANN) filtering.
///
/// There's only one possible relative order for two booleans, so unlike
/// the other ordering tests this can't catch a subtly wrong comparison -
/// but `cql_cmp()`'s `false < true` is still an assumption about Scylla's
/// own `boolean_type` comparator, not a law of nature, and this confirms
/// Scylla agrees rather than just asserting it in isolation.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_boolean_column_ordering_matches_scylla(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, f BOOLEAN, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    let values: [(i32, bool); 2] = [(0, false), (1, true)];
    for (pk, f) in &values {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
                (pk, f, &vec![*pk as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index =
        create_index(CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]))
            .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 2, "Expected 2 vectors to be indexed");
    }

    for (threshold_pk, threshold) in &values {
        info!("Comparing ANN-filtered and plain (non-ANN) results for f < {threshold}");
        let ann_results = get_pks(
            format!(
                "SELECT pk FROM {table} WHERE f < {threshold} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
            ),
            &session,
        )
        .await;
        let plain_results = get_pks(
            format!("SELECT pk FROM {table} WHERE f < {threshold} ALLOW FILTERING"),
            &session,
        )
        .await;

        assert_eq!(
            ann_results, plain_results,
            "ANN-filtered results must match Scylla's own (non-ANN) filtering \
             for f < {threshold} (threshold pk={threshold_pk})"
        );
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by equality on a UUID filtering column.
///
/// See ann_filter_by_blob_column_eq above: this is the Uuid case of the same
/// VECTOR-889 bug.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_uuid_column_eq(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, f UUID, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    let u0 = Uuid::parse_str("00000000-0000-4000-8000-000000000000").unwrap();
    let u1 = Uuid::parse_str("7fffffff-ffff-4fff-7fff-ffffffffffff").unwrap();
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
            (0, u0, &vec![0.0f32, 0.0, 0.0]),
        )
        .await
        .expect("failed to insert data");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
            (1, u1, &vec![0.0f32, 0.0, 0.0]),
        )
        .await
        .expect("failed to insert data");

    let index =
        create_index(CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]))
            .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 2, "Expected 2 vectors to be indexed");
    }

    info!("Querying index for f = {u0}");
    let results = get_pks(
        format!(
            "SELECT pk FROM {table} WHERE f = {u0} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
        ),
        &session,
    )
    .await;
    assert_eq!(results, HashSet::from([0]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by an inequality (`<`) on a UUID filtering
/// column, cross-checked against Scylla's own (non-ANN) filtering, to
/// verify we got the ordering right.
///
/// Regression coverage for VECTOR-889's `uuid_cmp()`: UUIDs order by
/// version nibble first, and version-1 (time-based) UUIDs then order by
/// their reassembled timestamp rather than raw bytes. The dataset is
/// crafted so that a naive 128-bit byte compare disagrees with that
/// ordering:
/// - `u_a` and `u_b` are both version 1, but `u_a` has a large `time_low`
///   and small `time_hi` (giving it a small true timestamp), while `u_b`
///   has a small `time_low` and large `time_hi` (giving it a large true
///   timestamp) - so byte order and timestamp order disagree on them.
/// - `u_v4` is version 4 with a `time_low` byte (0x80) that byte-sorts
///   between `u_b` and `u_a`/`u_max`, but must sort after every version-1
///   UUID regardless of its value.
/// - `u_lo` shares `u_min`'s timestamp exactly (both v1, timestamp 0), so
///   ScyllaDB's tie-break on bytes 8..16 as plain unsigned bytes decides:
///   `u_lo`'s 0x7f < `u_min`'s 0x80. `CqlTimeuuid: Ord` (the different,
///   sign-flipped tie-break ScyllaDB's *TIMEUUID*-column comparator uses)
///   would order them the other way, so reusing it here would be a bug.
///
/// Rather than hardcoding the expected order, this loops over every value
/// as the `<` threshold and, for each, derives the expected row set from
/// Scylla evaluating the same restriction without `ORDER BY v ANN OF` -
/// asserting the ANN-filtered query returns exactly that set. Looping the
/// threshold over the whole dataset exercises every pairwise comparison
/// among the values, not just one hand-picked split.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_uuid_column_ordering_matches_scylla(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, f UUID, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    let u_min = Uuid::parse_str("00000000-0000-1000-8000-000000000000").unwrap(); // v1, timestamp = 0
    let u_a = Uuid::parse_str("ffffffff-0000-1000-8000-000000000001").unwrap(); // v1, timestamp ~ 4.3e9
    let u_b = Uuid::parse_str("00000000-0000-1fff-8000-000000000002").unwrap(); // v1, timestamp ~ 1.15e18
    let u_max = Uuid::parse_str("ffffffff-ffff-1fff-bfff-ffffffffffff").unwrap(); // v1, max timestamp
    let u_v4 = Uuid::parse_str("80000000-0000-4000-8000-000000000000").unwrap(); // v4
    let u_lo = Uuid::parse_str("00000000-0000-1000-7f00-000000000003").unwrap(); // v1, timestamp = 0, same as u_min

    let values: [(i32, Uuid); 6] = [
        (0, u_min),
        (1, u_a),
        (2, u_b),
        (3, u_max),
        (4, u_v4),
        (5, u_lo),
    ];
    for (pk, u) in &values {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
                (pk, u, &vec![*pk as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index =
        create_index(CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]))
            .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 6, "Expected 6 vectors to be indexed");
    }

    for (threshold_pk, threshold) in &values {
        info!("Comparing ANN-filtered and plain (non-ANN) results for f < {threshold}");
        let ann_results = get_pks(
            format!(
                "SELECT pk FROM {table} WHERE f < {threshold} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
            ),
            &session,
        )
        .await;
        let plain_results = get_pks(
            format!("SELECT pk FROM {table} WHERE f < {threshold} ALLOW FILTERING"),
            &session,
        )
        .await;

        assert_eq!(
            ann_results, plain_results,
            "ANN-filtered results must match Scylla's own (non-ANN) filtering \
             for f < {threshold} (threshold pk={threshold_pk})"
        );
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by equality on a TIMEUUID filtering column.
///
/// See ann_filter_by_blob_column_eq above: this is the Timeuuid case of the
/// same VECTOR-889 bug.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_timeuuid_column_eq(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, f TIMEUUID, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    let t0: CqlTimeuuid = "00000000-0000-1000-8000-000000000000".parse().unwrap();
    let t1: CqlTimeuuid = "ffffffff-ffff-1fff-bfff-ffffffffffff".parse().unwrap();
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
            (0, t0, &vec![0.0f32, 0.0, 0.0]),
        )
        .await
        .expect("failed to insert data");
    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
            (1, t1, &vec![0.0f32, 0.0, 0.0]),
        )
        .await
        .expect("failed to insert data");

    let index =
        create_index(CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]))
            .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 2, "Expected 2 vectors to be indexed");
    }

    info!("Querying index for f = {t0}");
    let results = get_pks(
        format!(
            "SELECT pk FROM {table} WHERE f = {t0} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
        ),
        &session,
    )
    .await;
    assert_eq!(results, HashSet::from([0]));

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

/// Test ANN search filtered by an inequality (`<`) on a TIMEUUID filtering
/// column, cross-checked against Scylla's own (non-ANN) filtering, to
/// verify we got the ordering right.
///
/// This dataset is crafted the same way as ann_filter_by_uuid_column_ordering_matches_scylla
/// above: `t_a` has a large `time_low` and small `time_hi` (small true
/// timestamp), `t_b` has a small `time_low` and large `time_hi` (large
/// true timestamp) - so byte order and timestamp order disagree on them.
///
/// Rather than hardcoding the expected order, this loops over every value
/// as the `<` threshold and, for each, derives the expected row set from
/// Scylla evaluating the same restriction without `ORDER BY v ANN OF` -
/// asserting the ANN-filtered query returns exactly that set. Looping the
/// threshold over the whole dataset exercises every pairwise comparison
/// among the values, not just one hand-picked split.
#[e2etest::test(group = filtering)]
async fn ann_filter_by_timeuuid_column_ordering_matches_scylla(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, f TIMEUUID, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk)",
        None,
    )
    .await;

    let t_min: CqlTimeuuid = "00000000-0000-1000-8000-000000000000".parse().unwrap(); // timestamp = 0
    let t_a: CqlTimeuuid = "ffffffff-0000-1000-8000-000000000001".parse().unwrap(); // timestamp ~ 4.3e9
    let t_b: CqlTimeuuid = "00000000-0000-1fff-8000-000000000002".parse().unwrap(); // timestamp ~ 1.15e18
    let t_max: CqlTimeuuid = "ffffffff-ffff-1fff-bfff-ffffffffffff".parse().unwrap(); // max timestamp

    let values: [(i32, CqlTimeuuid); 4] = [(0, t_min), (1, t_a), (2, t_b), (3, t_max)];
    for (pk, t) in &values {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, f, v) VALUES (?, ?, ?)"),
                (pk, t, &vec![*pk as f32, 0.0, 0.0]),
            )
            .await
            .expect("failed to insert data");
    }

    let index =
        create_index(CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["f"]))
            .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 4, "Expected 4 vectors to be indexed");
    }

    for (threshold_pk, threshold) in &values {
        info!("Comparing ANN-filtered and plain (non-ANN) results for f < {threshold}");
        let ann_results = get_pks(
            format!(
                "SELECT pk FROM {table} WHERE f < {threshold} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10 ALLOW FILTERING"
            ),
            &session,
        )
        .await;
        let plain_results = get_pks(
            format!("SELECT pk FROM {table} WHERE f < {threshold} ALLOW FILTERING"),
            &session,
        )
        .await;

        assert_eq!(
            ann_results, plain_results,
            "ANN-filtered results must match Scylla's own (non-ANN) filtering \
             for f < {threshold} (threshold pk={threshold_pk})"
        );
    }

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

/// Regression test for VECTOR-892: a vector index can declare a filtering
/// column that is also one of the base table's primary-key columns. But
/// vector-store used to misalign the stored column values in that case:
/// every upserted row failed with "cannot insert value into a PrimaryKey
/// column", so the index stayed fully "built" at 0 rows and ANN queries
/// against it silently returned nothing.
///
/// The order of the declared filtering columns matters: "ck" (the
/// primary-key column) must come before "f" for the misalignment to
/// manifest - the other order happens to leave "f" correctly aligned and
/// just drops "ck", which produces no visible symptom for this test.
#[e2etest::test(group = filtering)]
async fn global_index_filter_by_filtering_column_shared_with_primary_key(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, f INT, v VECTOR<FLOAT, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, ck, f, v) VALUES (1, 2, 10, [0.1, 0.2, 0.3])"),
            (),
        )
        .await
        .expect("failed to insert data");

    // No .partition_columns() -> global index. "ck" is a clustering-key column.
    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "v").filter_columns(["ck", "f"]),
    )
    .await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(index_status.count, 1, "Expected 1 vector to be indexed");
    }

    let results = get_query_results(
        format!(
            "SELECT pk FROM {table} WHERE ck = 2 AND f = 10 \
            ORDER BY v ANN OF [0.1, 0.2, 0.3] LIMIT 1 ALLOW FILTERING"
        ),
        &session,
    )
    .await;
    assert_eq!(
        results
            .rows::<(i32,)>()
            .expect("failed to get rows")
            .rows_remaining(),
        1,
        "expected the row back"
    );

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

/// Test ANN search filtered by partition key equality or filtering column on a local index built
/// with pk or ck or regular column.
#[e2etest::test(group = filtering)]
async fn local_index_filter_by_partition_key_or_filtering(actors: Arc<TestActors>) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT, ck INT, v VECTOR<FLOAT, 1>, rc INT, fp INT, fc INT, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    const REPETITIONS: usize = 5;
    const DATASET_SIZE: usize = REPETITIONS * REPETITIONS;

    for pk in 0..REPETITIONS {
        for ck in 0..REPETITIONS {
            session
                .query_unpaged(
                    format!(
                        "INSERT INTO {table} (pk, ck, v, rc, fp, fc) VALUES (?, ?, ?, ?, ?, ?)"
                    ),
                    (
                        pk as i32,
                        ck as i32,
                        &vec![pk as f32],
                        ck as i32,
                        pk as i32,
                        ck as i32,
                    ),
                )
                .await
                .expect("failed to insert data");
        }
    }

    for (pc, oc, fc) in [("pk", "ck", "fc"), ("ck", "pk", "fp"), ("rc", "pk", "fp")] {
        info!("Testing local index with partition column {pc} and filtering column {fc}");
        let index = create_index(
            CreateIndexQuery::new(&session, &clients, &table, "v")
                .options([("similarity_function", "euclidean")])
                .partition_columns([pc])
                .filter_columns([fc]),
        )
        .await;

        for client in &clients {
            let index_status = wait_for_index(client, &index).await;
            assert_eq!(
                index_status.count, DATASET_SIZE,
                "Expected {DATASET_SIZE} vectors to be indexed in the index"
            );
        }

        info!("Querying index for {pc} = 1");
        let rows = get_query_results(
            format!(
                "SELECT {oc} FROM {table} WHERE {pc} = 1 \
                ORDER BY v ANN OF [1.0] LIMIT {DATASET_SIZE}"
            ),
            &session,
        )
        .await;
        assert_eq!(
            rows.rows::<(i32,)>()
                .expect("failed to get rows")
                .rows_remaining(),
            REPETITIONS
        );

        info!("Querying index for {pc} = 1 AND {fc} = 1");
        let rows = get_query_results(
            format!(
                "SELECT {oc} FROM {table} WHERE {pc} = 1 AND {fc} = 1 \
                ORDER BY v ANN OF [1.0] LIMIT {DATASET_SIZE} ALLOW FILTERING"
            ),
            &session,
        )
        .await;
        let rows = rows.rows::<(i32,)>().expect("failed to get rows");
        assert_eq!(rows.rows_remaining(), 1);
        assert_eq!(
            rows.into_iter().next().unwrap().expect("failed to get row"),
            (1,)
        );

        info!("Dropping index {index:?}");
        session
            .query_unpaged(
                format!("DROP INDEX IF EXISTS {index}", index = index.index.as_ref()),
                (),
            )
            .await
            .expect("failed to drop the index");
    }

    info!("Dropping keyspace {keyspace}");
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

/// Format bytes as a CQL blob literal, e.g. `0x0102ff`.
fn blob_literal(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("0x{hex}")
}

/// Run a query expected to select a `pk` column and collect the results
/// into a set, for comparing an ANN-filtered query against the same
/// restriction evaluated by Scylla without `ORDER BY v ANN OF`.
async fn get_pks(query: String, session: &Session) -> HashSet<i32> {
    get_query_results(query, session)
        .await
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect()
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
