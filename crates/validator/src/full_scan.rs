/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::common::*;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use httpapi::IndexInfo;
use httpapi::IndexStatus;
use httpclient::HttpClient;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::RequestFrame;
use scylla_proxy::RequestOpcode;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

e2etest::group!(
    name = full_scan,
    fixtures = (Fixture),
    parent = crate::validator
);

struct Fixture {
    actors: TestActors,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<crate::TestEnv>().await?.new_cluster().await;
        init_with_proxy_single_vs(&actors).await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }

    // This cluster is independent of every other one, so its groups may run
    // alongside groups owning a different cluster.
    fn group_can_run_concurrently() -> bool {
        true
    }

    // Its nodes claim loopback addresses, ports and routes, which every other
    // cluster claims too, so it runs in a network namespace of its own, where
    // it is alone in claiming them.
    fn group_needs_own_namespace() -> bool {
        true
    }
}

#[e2etest::test(group = full_scan)]
async fn full_scan_is_completed_when_responding_to_messages_concurrently(fixture: Arc<Fixture>) {
    let actors = &fixture.actors;
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(actors).await;
    let client = clients.first().unwrap();

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        Some("CDC = {'enabled': true}"),
    )
    .await;

    info!("Inserting data to the table");
    const DATASET_SIZE: i32 = 100;
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..DATASET_SIZE {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (id, embedding) VALUES (?, ?)"),
                (i, embedding.clone()),
            )
            .await
            .expect("failed to insert data");
    }

    info!("Slow communication between vector-store and scylla using proxy");
    const FRAME_DELAY: Duration = Duration::from_millis(100);
    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    info!("Creating index");
    let index = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;

    info!("Checking that full scan isn't completed");
    let result = session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await;
    match &result {
        Err(e) if format!("{e:?}").contains("503 Service Unavailable") => {}
        _ => panic!("Expected SERVICE_UNAVAILABLE error, got: {result:?}"),
    }

    info!("Recovering normal communication");
    actors.db_proxy.turn_off_rules().await;

    info!("Waiting for index to be built");
    let index_status = wait_for_index(client, &index).await;
    assert_eq!(
        index_status.count, DATASET_SIZE as usize,
        "Expected {DATASET_SIZE} vectors to be indexed"
    );

    info!("Checking that ANN search works after index is built");
    session
        .query_unpaged(
            format!("SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 5"),
            (),
        )
        .await
        .expect("failed to query ANN search");

    info!("Dropping index");
    session
        .query_unpaged(format!("DROP INDEX {}", index.index), ())
        .await
        .expect("failed to drop an index");

    wait_for_no_index(client, &index).await;

    info!("Dropping keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

#[e2etest::test(group = full_scan)]
async fn full_scan_stops_when_index_is_dropped(fixture: Arc<Fixture>) {
    let actors = &fixture.actors;
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(actors).await;
    let client = clients.first().unwrap();

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        None,
    )
    .await;

    info!("Inserting data to the table");
    const DATASET_SIZE: i32 = 1000;
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    for i in 0..DATASET_SIZE {
        session
            .query_unpaged(
                format!("INSERT INTO {table} (id, embedding) VALUES (?, ?)"),
                (i, embedding.clone()),
            )
            .await
            .expect("failed to insert data");
    }

    info!("Slowing down communication to keep fullscan in progress");
    const FRAME_DELAY: Duration = Duration::from_millis(50);
    let (tx_feedback_before_drop, mut rx_feedback_before_drop) = mpsc::unbounded_channel();
    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY)
                .with_feedback_when_performed(tx_feedback_before_drop),
        )]))
        .await;

    info!("Creating index");
    let index = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;

    info!("Waiting for fullscan to reach BOOTSTRAPPING state");
    wait_for_bootstrapping(client, &index).await;

    let fullscan_statement_id = tokio::time::timeout(Duration::from_secs(10), async {
        let mut accumulated_counts: HashMap<Vec<u8>, usize> = HashMap::new();
        while let Some((frame, _)) = rx_feedback_before_drop.recv().await {
            if frame.opcode != RequestOpcode::Execute {
                continue;
            }
            if let Some(id) = execute_statement_id(&frame) {
                *accumulated_counts.entry(id).or_insert(0) += 1;
            }
            if let Some((statement_id, _)) = accumulated_counts
                .iter()
                .max_by_key(|(_, count)| *count)
                .filter(|(_, count)| **count >= 5)
            {
                return statement_id.clone();
            }
        }
        panic!("feedback channel closed before observing enough fullscan frames");
    })
    .await
    .expect("expected to observe at least one fullscan EXECUTE before drop");

    info!("Dropping index while fullscan is still running");
    session
        .query_unpaged(format!("DROP INDEX {}", index.index), ())
        .await
        .expect("failed to drop an index");

    info!("Waiting for index to be removed from vector-store");
    wait_for_no_index(client, &index).await;

    info!("Verifying no DB execute traffic continues after index drop");
    let (tx_feedback, mut rx_feedback) = mpsc::unbounded_channel();
    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::RequestOpcode(RequestOpcode::Execute),
            RequestReaction::noop().with_feedback_when_performed(tx_feedback),
        )]))
        .await;

    // We cannot deterministically prove that fullscan has stopped — we can only
    // observe that no matching execute frames arrive within a bounded window.
    // A 500ms settle period absorbs any in-flight queries from the transition,
    // and the remaining 1.5s is long enough to detect ongoing fullscan traffic
    // (which sends queries at high frequency when active).
    let mut fullscan_execute_after_drop = 0usize;
    let settle_until = tokio::time::Instant::now() + Duration::from_millis(500);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while let Ok(Some((frame, _))) = tokio::time::timeout_at(deadline, rx_feedback.recv()).await {
        let Some(statement_id) = execute_statement_id(&frame) else {
            continue;
        };
        if statement_id == fullscan_statement_id && tokio::time::Instant::now() > settle_until {
            fullscan_execute_after_drop += 1;
        }
    }

    assert_eq!(
        fullscan_execute_after_drop, 0,
        "fullscan still sends DB execute requests after index drop: {fullscan_execute_after_drop}",
    );

    info!("Recovering normal communication");
    actors.db_proxy.turn_off_rules().await;

    info!("Dropping keyspace");
    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}

fn execute_statement_id(frame: &RequestFrame) -> Option<Vec<u8>> {
    let body = frame.body.as_ref();
    if body.len() < 2 {
        return None;
    }

    let id_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let end = 2 + id_len;
    if body.len() < end {
        return None;
    }

    Some(body[2..end].to_vec())
}

async fn wait_for_bootstrapping(client: &HttpClient, index: &IndexInfo) {
    wait_for_value(
        || async {
            match client.index_status(&index.keyspace, &index.index).await {
                Ok(status) if status.status == IndexStatus::Bootstrapping => Some(status),
                _ => None,
            }
        },
        format!(
            "index '{}' must reach BOOTSTRAPPING at {}",
            index.index,
            client.url()
        ),
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;
}
