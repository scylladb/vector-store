/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::common::*;
use scylla::statement::{Consistency, Statement};
use scylla::value::CqlValue;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

const CDC_WAIT: Duration = Duration::from_secs(10);

e2etest::group!(
    name = serde,
    fixtures = (TestContext),
    parent = crate::standard
);

/// Indexes a table whose primary key has the given CQL type and checks that a
/// row inserted with `data` as its key survives the round trip through the
/// index: an ANN query must return it with its vector intact.
async fn assert_key_type_round_trip(ctx: &TestContext, typ: &str, data: &str) {
    let table = ctx
        .create_table(&format!("id {typ} PRIMARY KEY, vec vector<float, 3>"), None)
        .await;
    ctx.session
        .query_unpaged(
            format!("INSERT INTO {table} (id, vec) VALUES ({data}, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert data");

    let index = ctx.create_index(&table, "vec").await;
    for client in &ctx.clients {
        wait_for_index(client, &index).await;
    }

    let rows = ctx
        .session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY vec ANN OF [1.0, 2.0, 3.0] LIMIT 1"),
            (),
        )
        .await
        .expect("failed to select data");
    let rows = rows.into_rows_result().unwrap();
    assert_eq!(rows.rows_num(), 1);
    let value: (CqlValue, Vec<f32>) = rows.first_row().unwrap();
    assert_eq!(value.1, vec![1.0, 2.0, 3.0]);
}

/// Declares one test per CQL primary-key type.
///
/// A single test looping over the types would serialise the cases, each
/// waiting out its own index build, which made this group the slowest one in
/// the suite. As separate tests the framework runs them concurrently, within
/// its configured concurrency limit, and names the failing type on its own.
macro_rules! key_type_round_trip_tests {
    ($($name:ident: $typ:literal = $data:literal,)*) => {
        $(
            #[e2etest::test(group = serde)]
            async fn $name(ctx: Arc<TestContext>) {
                assert_key_type_round_trip(&ctx, $typ, $data).await;
            }
        )*
    };
}

key_type_round_trip_tests! {
    serialization_ascii: "ascii" = "'random_text'",
    serialization_bigint: "bigint" = "1234",
    serialization_blob: "blob" = "0xdeadbeef",
    serialization_boolean: "boolean" = "true",
    serialization_date: "date" = "'2023-10-01'",
    serialization_decimal: "decimal" = "-98765432109876543210.123456789",
    serialization_double: "double" = "3.14159",
    serialization_float: "float" = "2.71828",
    serialization_inet: "inet" = "'10.0.0.1'",
    serialization_int: "int" = "42",
    serialization_smallint: "smallint" = "123",
    serialization_tinyint: "tinyint" = "7",
    serialization_uuid: "uuid" = "841685b2-8803-11f0-8de9-0242ac120002",
    serialization_timeuuid: "timeuuid" = "841685b2-8803-11f0-8de9-0242ac120002",
    serialization_time: "time" = "'08:12:54.2137'",
    serialization_timestamp: "timestamp" = "'2023-10-01T12:34:56.789Z'",
    serialization_text: "text" = "'some_text'",
    serialization_varint: "varint" = "-98765432109876543210",
}

#[e2etest::test(group = serde)]
async fn test_varint_filter(ctx: Arc<TestContext>) {
    let table = ctx
        .create_table(
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
        ctx.session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, ck, vec) VALUES (1, {ck}, [1.0, 2.0, 3.0])"),
                (),
            )
            .await
            .unwrap_or_else(|e| panic!("failed to insert ck={ck}: {e}"));
    }

    let index = create_index(
        ctx.index_query(&table, "vec")
            .partition_columns(["pk"])
            .filter_columns(["ck"]),
    )
    .await;
    for client in &ctx.clients {
        wait_for_index(client, &index).await;
    }

    // Helper: run a query with a ck restriction and return the number of rows.
    let count_rows = |restriction: &str| {
        let session = ctx.session.clone();
        let table = table.clone();
        let restriction = restriction.to_string();
        async move {
            let rows = session
                .query_unpaged(
                    format!(
                        "SELECT ck FROM {table} WHERE pk = 1 AND {restriction} \
                        ORDER BY vec ANN OF [1.0, 2.0, 3.0] LIMIT 10 ALLOW FILTERING"
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
}

/// Test the fundamental PK vs CK asymmetry for decimal types:
///
/// - PK is byte-identity: `1.0` and `1.00` are different partitions.
/// - CK is semantic: `3.14` and `3.140` are the same row (overwrite).
///
/// Verified via index_status.count and direct VS ANN queries (not CQL).
#[e2etest::test(group = serde)]
async fn test_decimal_key(ctx: Arc<TestContext>) {
    let client = ctx.clients.first().unwrap();

    let table = ctx
        .create_table(
            "pk decimal, ck decimal, vec vector<float, 3>, PRIMARY KEY (pk, ck)",
            None,
        )
        .await;

    let insert = |pk: &'static str, ck: &'static str, v: [f32; 3]| {
        let table = table.clone();
        let session = ctx.session.clone();
        async move {
            let mut query = Statement::new(format!(
                "INSERT INTO {table} (pk, ck, vec) VALUES ({pk}, {ck}, [{}, {}, {}])",
                v[0], v[1], v[2]
            ));
            query.set_consistency(Consistency::All);
            session
                .query_unpaged(query, ())
                .await
                .unwrap_or_else(|e| panic!("insert pk={pk}, ck={ck}: {e}"));
        }
    };

    // Phase 1: before index creation (full scan path).
    // CK: inserting ck=3.140 after ck=3.14 in pk=1.0 is an upsert
    // (same row after lookup normalization).
    // PK byte-identity: pk=1.00 is a separate partition (no normalization for PK).
    insert("1.0", "3.14", [1.0, 2.0, 3.0]).await;
    insert("1.0", "3.140", [4.0, 5.0, 6.0]).await;
    insert("1.00", "3.140", [7.0, 8.0, 9.0]).await;

    let index = ctx.create_index(&table, "vec").await;
    for c in &ctx.clients {
        wait_for_index(c, &index).await;
    }

    let get_keys = || async {
        let (primary_keys, _, _) = client
            .ann(
                &index.keyspace,
                &index.index,
                vec![1.0, 2.0, 3.0].into(),
                None,
                std::num::NonZeroUsize::new(10).unwrap().into(),
            )
            .await;
        let pks = primary_keys.get(&"pk".into()).expect("pk column");
        let cks = primary_keys.get(&"ck".into()).expect("ck column");
        let mut keys: Vec<(String, String)> = pks
            .iter()
            .zip(cks.iter())
            .map(|(pk, ck)| {
                (
                    pk.as_str().unwrap().to_string(),
                    ck.as_str().unwrap().to_string(),
                )
            })
            .collect();
        keys.sort();
        keys
    };

    let status = client
        .index_status(&index.keyspace, &index.index)
        .await
        .unwrap();
    assert_eq!(
        status.count, 2,
        "full scan: 2 rows (CK overwrite + PK identity)"
    );

    let keys = get_keys().await;
    let mut expected_pks: HashSet<_> = ["1.0".to_string(), "1.00".to_string()]
        .into_iter()
        .collect();
    for (pk, ck) in &keys {
        assert!(expected_pks.remove(pk), "PK byte-identity preserved");
        match pk.as_str() {
            "1.0" => assert!(ck == "3.14" || ck == "3.140"),
            "1.00" => assert_eq!(ck, "3.140"),
            _ => panic!("unexpected pk={pk}"),
        }
    }
    assert!(expected_pks.is_empty());

    // Phase 2: after index creation (CDC path).
    // Same pattern as Phase 1.
    insert("2.0", "7.5", [1.0, 2.0, 3.0]).await;
    insert("2.0", "7.50", [4.0, 5.0, 6.0]).await;
    insert("2.00", "7.50", [7.0, 8.0, 9.0]).await;

    for client in &ctx.clients {
        wait_for(
            || async {
                let status = client.index_status(&index.keyspace, &index.index).await;
                matches!(status, Ok(s) if s.count == 4)
            },
            "Waiting for CDC (count == 4)",
            CDC_WAIT,
        )
        .await;
    }

    let keys = get_keys().await;
    let mut expected_pks: HashSet<_> = [
        "1.0".to_string(),
        "1.00".to_string(),
        "2.0".to_string(),
        "2.00".to_string(),
    ]
    .into_iter()
    .collect();
    for (pk, ck) in &keys {
        assert!(expected_pks.remove(pk), "PK byte-identity preserved");
        match pk.as_str() {
            "1.0" => assert!(ck == "3.14" || ck == "3.140"),
            "1.00" => assert_eq!(ck, "3.140"),
            "2.0" => assert!(ck == "7.5" || ck == "7.50"),
            "2.00" => assert_eq!(ck, "7.50"),
            _ => panic!("unexpected pk={pk}"),
        }
    }
    assert!(expected_pks.is_empty());
}

/// Verify decimal filters use semantic BigDecimal comparison (not byte/unscaled comparison).
/// Tests PK byte-identity (pk=1.0 vs pk=1.00 are separate partitions) and CK semantic
/// comparison (cross-scale ordering, unnormalized CK match, beyond-i64 values).
#[e2etest::test(group = serde)]
async fn test_decimal_filter(ctx: Arc<TestContext>) {
    let table = ctx
        .create_table(
            "pk decimal, ck decimal, vec vector<float, 3>, PRIMARY KEY (pk, ck)",
            None,
        )
        .await;

    let insert = |pk: &'static str, ck: &'static str| {
        let table = table.clone();
        let session = ctx.session.clone();
        async move {
            session
                .query_unpaged(
                    format!(
                        "INSERT INTO {table} (pk, ck, vec) VALUES ({pk}, {ck}, [1.0, 2.0, 3.0])"
                    ),
                    (),
                )
                .await
                .unwrap_or_else(|e| panic!("insert pk={pk}, ck={ck}: {e}"));
        }
    };

    // pk=1.0 partition: CK values with different scales.
    insert("1.0", "3.14").await;
    insert("1.0", "3.9").await;
    insert("1.0", "7.50").await; // unnormalized representation
    insert("1.0", "98765432109876543210.1").await; // beyond i64

    // pk=1.00 partition: separate from pk=1.0 (byte-identity).
    insert("1.00", "5.0").await;

    let index = create_index(
        ctx.index_query(&table, "vec")
            .partition_columns(["pk"])
            .filter_columns(["ck"]),
    )
    .await;
    for client in &ctx.clients {
        wait_for_index(client, &index).await;
    }

    let count_rows = |pk: &str, restriction: &str| {
        let session = ctx.session.clone();
        let table = table.clone();
        let pk = pk.to_string();
        let restriction = restriction.to_string();
        async move {
            let rows = session
                .query_unpaged(
                    format!(
                        "SELECT ck FROM {table} WHERE pk = {pk} AND {restriction} \
                         ORDER BY vec ANN OF [1.0, 2.0, 3.0] LIMIT 10 ALLOW FILTERING"
                    ),
                    (),
                )
                .await
                .unwrap_or_else(|e| panic!("query failed: {e}"));
            rows.into_rows_result().unwrap().rows_num()
        }
    };

    // Cross-scale ordering: 3.14 < 3.9 (naive unscaled: 314 > 39).
    assert_eq!(
        count_rows("1.0", "ck > 3.0 AND ck < 4.0").await,
        2,
        "should find both 3.14 and 3.9"
    );

    // Unnormalized CK: 7.50 was inserted, ck = 7.5 should match semantically.
    assert_eq!(
        count_rows("1.0", "ck >= 7.5 AND ck <= 7.5").await,
        1,
        "unnormalized CK 7.50 should match filter for 7.5"
    );

    // Unnormalized filter value: querying with ck = 7.50 should also match.
    assert_eq!(
        count_rows("1.0", "ck >= 7.50 AND ck <= 7.50").await,
        1,
        "unnormalized filter value 7.50 should match stored 7.50"
    );

    // Beyond-i64 range filter.
    assert_eq!(
        count_rows(
            "1.0",
            "ck > 98765432109876543210.0 AND ck < 98765432109876543210.9"
        )
        .await,
        1,
        "beyond-i64 value should be comparable"
    );

    // PK byte-identity: pk=1.00 is a separate partition from pk=1.0.
    assert_eq!(
        count_rows("1.00", "ck > 0.0").await,
        1,
        "pk=1.00 partition should have only its own row"
    );
}
