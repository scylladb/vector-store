/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use crate::tests::*;
use scylla::value::CqlValue;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, crate::common::init)
        .with_cleanup(timeout, crate::common::cleanup)
        .with_test(
            "serialization_deserialization_all_types",
            timeout,
            test_serialization_deserialization_all_types,
        )
}

async fn test_serialization_deserialization_all_types(actors: TestActors) {
    let (session, _client) = crate::common::prepare_connection(&actors).await;

    let cases = vec![
        ("ascii", "'random_text'"),
        ("bigint", "1234"),
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

        session
            .query_unpaged(
                format!("CREATE INDEX idx_{typ} ON tbl_{typ}(vec) USING 'vector_index'"),
                (),
            )
            .await
            .expect("failed to create an index");
    }

    for (typ, _) in &cases {
        wait_for(
            || async {
                session
                    .query_unpaged(
                        format!(
                            "SELECT * FROM tbl_{typ} ORDER BY vec ANN OF [1.0, 2.0, 3.0] LIMIT 1"
                        ),
                        (),
                    )
                    .await
                    .is_ok()
            },
            "Waiting for index build",
            Duration::from_secs(10),
        )
        .await;
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
        .expect("failed to drop a keyspace");
}
