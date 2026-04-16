/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use crate::common::*;
use async_backtrace::framed;
use e2etest::TestCase;
use scylla::value::CqlValue;

#[framed]
pub(crate) async fn new() -> TestCase<TestActors> {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, common::init)
        .with_cleanup(timeout, common::cleanup)
        .with_test(
            "test_serialization_deserialization_all_types",
            timeout,
            test_serialization_deserialization_all_types,
        )
    // Enable when merged https://github.com/scylladb/scylladb/pull/29505
    //.with_test("test_varint_filter", timeout, test_varint_filter)
}

#[framed]
async fn test_serialization_deserialization_all_types(actors: TestActors) {
    let (session, clients) = common::prepare_connection(&actors).await;

    let cases = vec![
        ("ascii", "'random_text'"),
        ("bigint", "1234"),
        ("blob", "0xdeadbeef"),
        ("boolean", "true"),
        ("date", "'2023-10-01'"),
        ("double", "3.14159"),
        ("float", "2.71828"),
        ("int", "42"),
        ("smallint", "123"),
        ("tinyint", "7"),
        ("uuid", "841685b2-8803-11f0-8de9-0242ac120002"),
        ("timeuuid", "841685b2-8803-11f0-8de9-0242ac120002"),
        ("time", "'08:12:54.2137'"),
        ("timestamp", "'2023-10-01T12:34:56.789Z'"),
        ("text", "'some_text'"),
        ("varint", "-98765432109876543210"),
    ];

    let keyspace = create_keyspace(&session).await;

    for (typ, data) in &cases {
        session
            .query_unpaged(
                format!("CREATE TABLE tbl_{typ} (id {typ} PRIMARY KEY, vec vector<float, 3>)"),
                (),
            )
            .await
            .expect("failed to create a table");
        session
            .query_unpaged(
                format!("INSERT INTO tbl_{typ} (id, vec) VALUES ({data}, [1.0, 2.0, 3.0])"),
                (),
            )
            .await
            .expect("failed to insert data");

        let index = create_index(CreateIndexQuery::new(
            &session,
            &clients,
            format!("tbl_{typ}"),
            "vec",
        ))
        .await;
        for client in &clients {
            wait_for_index(client, &index).await;
        }

        let rows = session
            .query_unpaged(
                format!("SELECT * FROM tbl_{typ} ORDER BY vec ANN OF [1.0, 2.0, 3.0] LIMIT 1"),
                (),
            )
            .await
            .expect("failed to select data");
        let rows = rows.into_rows_result().unwrap();
        assert_eq!(rows.rows_num(), 1);
        let value: (CqlValue, Vec<f32>) = rows.first_row().unwrap();
        assert_eq!(value.1, vec![1.0, 2.0, 3.0]);
    }

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");
}

#[framed]
async fn test_varint_filter(actors: TestActors) {
    let (session, clients) = common::prepare_connection(&actors).await;
    let keyspace = create_keyspace(&session).await;

    let table = create_table(
        &session,
        "pk int, ck varint, vec vector<float, 3>, PRIMARY KEY (pk, ck)",
        None,
    )
    .await;

    let rows_to_insert = [
        "-42",
        "0",
        "42",
        // 98765432109876543210: exceeds i64 max (~9.2e18); requires BigInt comparison.
        "98765432109876543210",
    ];

    for ck in &rows_to_insert {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, vec) VALUES (1, {ck}, [1.0, 2.0, 3.0])"),
                (),
            )
            .await
            .unwrap_or_else(|e| panic!("failed to insert ck={ck}: {e}"));
    }

    let index = create_index(
        CreateIndexQuery::new(&session, &clients, &table, "vec")
            .partition_columns(["pk"])
            .filter_columns(["ck"]),
    )
    .await;
    for client in &clients {
        wait_for_index(client, &index).await;
    }

    // Helper: run a query with a ck restriction and return the number of rows.
    let count_rows = |restriction: &str| {
        let session = session.clone();
        let table = table.clone();
        let restriction = restriction.to_string();
        async move {
            let rows = session
                .query_unpaged(
                    format!(
                        "SELECT ck FROM {table} WHERE pk = 1 AND {restriction} ORDER BY vec ANN OF [1.0, 2.0, 3.0] LIMIT 10"
                    ),
                    (),
                )
                .await
                .unwrap_or_else(|e| panic!("query failed for restriction '{restriction}': {e}"));
            rows.into_rows_result().unwrap().rows_num()
        }
    };

    // Small range: excludes the beyond-i64 value.
    assert_eq!(
        count_rows("ck > -100 AND ck < 100").await,
        3,
        "ck > -100 AND ck < 100 should return {{-42, 0, 42}}"
    );

    // Lower-bounded: includes the beyond-i64 value.
    assert_eq!(
        count_rows("ck >= 0").await,
        3,
        "ck >= 0 should return {{0, 42, 98765432109876543210}}"
    );

    // Tight range: nothing between 42 and the big value.
    assert_eq!(
        count_rows("ck > 42 AND ck < 98765432109876543210").await,
        0,
        "ck > 42 AND ck < 98765432109876543210 should return no rows"
    );

    // Beyond-i64 boundary: naive i64 cast of 98765432109876543210 overflows.
    // Must use BigInt comparison to return exactly 1 row.
    assert_eq!(
        count_rows("ck > 98765432109876543209").await,
        1,
        "ck > 98765432109876543209 should return {{98765432109876543210}} (requires BigInt, not i64)"
    );

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");
}
