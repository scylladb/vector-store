/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common::*;
use async_backtrace::framed;
use e2etest::TestCase;
use std::time::Duration;
use tracing::info;

const ROW_TTL_SECONDS: i32 = 10;

#[framed]
pub(crate) async fn new() -> TestCase<TestActors> {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "cql_per_row_ttl_expires_from_index",
            timeout,
            cql_per_row_ttl_expires_from_index,
        )
}

/// Test that rows inserted with CQL per-row TTL are not returned by ANN
/// queries after they expire.
///
/// 1. Create a table and insert rows — some with a short TTL, some without.
/// 2. Create an index and verify all rows are queryable via ANN.
/// 3. Wait for the TTL to expire.
/// 4. Verify that ANN queries return only the non-TTL rows.
#[framed]
async fn cql_per_row_ttl_expires_from_index(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    info!("Insert 3 rows with TTL and 2 rows without TTL");
    for pk in 0..3 {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {table} (pk, v) VALUES ({pk}, [{v}, 0.0, 0.0]) USING TTL {ROW_TTL_SECONDS}",
                    v = pk as f32,
                ),
                (),
            )
            .await
            .expect("failed to insert data with TTL");
    }
    for pk in 10..12 {
        session
            .query_unpaged(
                format!(
                    "INSERT INTO {table} (pk, v) VALUES ({pk}, [{v}, 1.0, 1.0])",
                    v = pk as f32,
                ),
                (),
            )
            .await
            .expect("failed to insert data without TTL");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 5,
            "Expected 5 vectors to be indexed before TTL expiry"
        );
    }

    info!("Verify all 5 rows are returned before TTL expiry");
    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 5)
        },
        "Waiting for ANN query to return all 5 rows",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    assert_eq!(result.rows_num(), 5, "Expected 5 rows before TTL expiry");

    info!("Wait for TTL to expire and verify ANN query returns only non-TTL rows");
    let result = wait_for_value(
        || async {
            let result = get_opt_query_results(
                format!("SELECT pk FROM {table} ORDER BY v ANN OF [10.0, 1.0, 1.0] LIMIT 10"),
                &session,
            )
            .await;
            result.filter(|r| r.rows_num() == 2)
        },
        "Waiting for ANN query to return only non-TTL rows",
        Duration::from_secs(ROW_TTL_SECONDS as u64) + DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
    let rows: Vec<i32> = result
        .rows::<(i32,)>()
        .expect("failed to get rows")
        .map(|row| row.expect("failed to get row").0)
        .collect();
    assert_eq!(rows.len(), 2, "Expected 2 rows after TTL expiry");
    for pk in &rows {
        assert!(
            *pk >= 10 && *pk < 12,
            "Expected only non-TTL rows (pk=10,11), got pk={pk}"
        );
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
