/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use crate::tests::*;
use std::time::Duration;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "full_scan_is_completed_when_responding_to_messages_concurrently",
            timeout,
            full_scan_is_completed_when_responding_to_messages_concurrently,
        )
}

async fn full_scan_is_completed_when_responding_to_messages_concurrently(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(actors).await;

    session.query_unpaged(
        "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        (),
    ).await.expect("failed to create a keyspace");

    session
        .use_keyspace("ks", false)
        .await
        .expect("failed to use a keyspace");

    session
        .query_unpaged(
            "
            CREATE TABLE tbl (id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>)
            WITH cdc = {'enabled': true }
            ",
            (),
        )
        .await
        .expect("failed to create a table");

    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..1000 {
        session
            .query_unpaged(
                "INSERT INTO tbl (id, embedding) VALUES (?, ?)",
                (i, embedding.clone()),
            )
            .await
            .expect("failed to insert data");
    }

    session
        .query_unpaged(
            "CREATE INDEX idx ON tbl(embedding) USING 'vector_index'",
            (),
        )
        .await
        .expect("failed to create an index");

    while client.indexes().await.is_empty() {}
    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].keyspace.as_ref(), "ks");
    assert_eq!(indexes[0].index.as_ref(), "idx");

    let result = session
        .query_unpaged(
            "SELECT * FROM tbl ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5",
            (),
        )
        .await;

    match &result {
        Err(e) if format!("{e:?}").contains("503 Service Unavailable") => {}
        _ => panic!("Expected SERVICE_UNAVAILABLE error, got: {result:?}"),
    }

    wait_for(
        || async { client.count_from_info(&indexes[0]).await == Some(1000) },
        "Waiting for 1000 vectors to be indexed",
    )
    .await;

    session
        .query_unpaged(
            "SELECT * FROM tbl ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5",
            (),
        )
        .await
        .expect("failed to query ANN search");

    session
        .query_unpaged("DROP INDEX idx", ())
        .await
        .expect("failed to drop an index");

    while !client.indexes().await.is_empty() {}

    session
        .query_unpaged("DROP KEYSPACE ks", ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
