/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use crate::scylla_cluster::ScyllaClusterExt;
use crate::tests::*;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "reconnect_doesnt_break_fullscan",
            timeout,
            test_reconnect_doesnt_break_fullscan,
        )
}

async fn test_reconnect_doesnt_break_fullscan(actors: TestActors) {
    info!("started");

    let (session, client) = prepare_connection(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        Some("CDC = {'enabled': true}"),
    )
    .await;

    let stmt = session
        .prepare(format!(
            "INSERT INTO {table} (id, embedding) VALUES (?, [1.0, 2.0, 3.0])"
        ))
        .await
        .expect("failed to prepare a statement");

    for id in 0..1000 {
        session
            .execute_unpaged(&stmt, (id,))
            .await
            .expect("failed to insert a row");
    }

    let index = create_index(&session, &client, &table, "embedding", None).await;

    let result = session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 1"),
            (),
        )
        .await;

    match &result {
        Err(e) if format!("{e:?}").contains("503 Service Unavailable") => {}
        _ => panic!("Expected SERVICE_UNAVAILABLE error, got: {result:?}"),
    }

    actors.db.down().await;

    sleep(Duration::from_secs(1)).await;
    let count = client.count(&index.keyspace, &index.index).await;
    assert!(count.is_some() && count.unwrap() < 1000);
    actors.db.up(get_default_vs_url(&actors).await, None).await;

    assert!(actors.db.wait_for_ready().await);

    wait_for(
        || async {
            session
                .query_unpaged(
                    format!(
                        "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 1"
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

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
