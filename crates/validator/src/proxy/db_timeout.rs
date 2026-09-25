/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use super::ProxyTestContext;
use crate::common::*;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::info;

e2etest::group!(name = db_timeout, fixtures = (), parent = super::proxy);

/// Test that CDC is working after rust driver session's client timeout.
///
/// Steps:
/// - Create a table with a vector column.
/// - Create a vector index on the vector column (table without data).
/// - Wait until Vector Store creates the index.
/// - Insert 10 vectors into the table that will be picked up by CDC.
/// - Simulate a client timeout using proxy.
/// - Insert 10 vectors into the table that will be picked up by CDC.
/// - Wait until Vector Store finishes the CDC reader.
/// - Bring back simulator to normal operation.
/// - Wait until Vector Store updates the index using CDC.
#[e2etest::test(group = db_timeout)]
async fn client_timeout_doesnt_stop_cdc(ctx: Arc<ProxyTestContext>) {
    info!("started");
    let client = ctx.client();
    let table = ctx
        .create_table("pk INT, v VECTOR<FLOAT, 1>, PRIMARY KEY (pk)", None)
        .await;

    info!("Initially, the index should have 0 vectors");
    let index = ctx.create_index(&table, "v").await;

    let index_status = wait_for_index(client, &index).await;
    assert_eq!(index_status.count, 0, "Expected 0 vectors to be indexed");

    info!("Insert initial data that will be picked up by CDC");
    const DATA_SIZE: usize = 10;
    let insert_vectors = async |start| {
        for i in start..start + DATA_SIZE {
            ctx.session
                .query_unpaged(
                    format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                    (i as i32, vec![i as f32]),
                )
                .await
                .expect("failed to insert data");
        }
    };
    insert_vectors(0).await;

    const INSERT_FROM_CDC_TIMEOUT: Duration = Duration::from_secs(30);
    wait_for(
        async || {
            client
                .index_status(&index.keyspace, &index.index)
                .await
                .unwrap()
                .count
                == DATA_SIZE
        },
        "Vector Store updates the index using CDC",
        INSERT_FROM_CDC_TIMEOUT,
    )
    .await;

    info!("Restart internals counter for cdc handler errors");
    let counter = format!("{}.{}-cdc-handler-errors", index.keyspace, index.index);
    client.internals_clear_counters().await.unwrap();
    client
        .internals_start_counter(counter.clone())
        .await
        .unwrap();

    info!("Simulate client timeout using proxy");
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel();
    let (timestamp_tx, timestamp_rx) = watch::channel(Instant::now());
    tokio::spawn(async move {
        // For each dropped frame, update the timestamp.
        while frame_rx.recv().await.is_some() {
            _ = timestamp_tx.send(Instant::now());
        }
    });
    ctx.actors()
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::drop_frame().with_feedback_when_performed(frame_tx),
        )]))
        .await;

    let index_status = wait_for_index(client, &index).await;
    info!("Current index status: {index_status:?}");

    info!("Insert new data that will be picked up by CDC");
    insert_vectors(DATA_SIZE).await;

    let index_status = client
        .index_status(&index.keyspace, &index.index)
        .await
        .unwrap();
    info!("Current index status: {index_status:?}");

    const DROP_FRAME_TIMEOUT: Duration = Duration::from_secs(5);
    wait_for(
        async || {
            if *timestamp_rx.borrow() + DROP_FRAME_TIMEOUT < Instant::now() {
                // There are no flowing frames through the proxy, it means that CDC reader
                // has finished session and cdc driver tries to reconnect (which is not
                // visible in scylla-proxy feedback). We can assume that current Session is
                // finished without finishing cdc handler (cdc waits till connection is
                // restored). So we can stop waiting for cdc handler error.
                info!("No dropped frames detected recently");
                return true;
            }
            let counters = client.internals_counters().await.unwrap();
            *counters.get(&counter).unwrap() > 0
        },
        "Vector Store's CDC handler error or no dropped frames detected",
        INSERT_FROM_CDC_TIMEOUT,
    )
    .await;

    info!("Stop timeout simulation");
    ctx.actors().db_proxy.turn_off_rules().await;

    wait_for(
        async || {
            client
                .index_status(&index.keyspace, &index.index)
                .await
                .unwrap()
                .count
                == 2 * DATA_SIZE
        },
        "Vector Store updates the index with new data using CDC",
        Duration::from_secs(30),
    )
    .await;

    info!("finished");
}
