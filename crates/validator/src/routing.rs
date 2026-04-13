/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common::*;
use async_backtrace::framed;
use e2etest::TestCase;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use httpapi::IndexInfo;
use httpapi::IndexStatus;
use httpclient::HttpClient;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use std::time::Duration;
use tracing::info;

const ANN_LIMIT: usize = 5;
const FRAME_DELAY: Duration = Duration::from_millis(100);

#[framed]
pub(crate) async fn new() -> TestCase<TestActors> {
    let timeout = DEFAULT_TEST_TIMEOUT;
    TestCase::empty()
        .with_init(timeout, init_with_proxy_single_vs)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "ann_routes_to_serving_index_while_replacement_is_bootstrapping",
            timeout,
            ann_routes_to_serving_index_while_replacement_is_bootstrapping,
        )
        .with_test(
            "ann_does_not_route_between_columns_while_requested_index_is_bootstrapping",
            timeout,
            ann_does_not_route_between_columns_while_requested_index_is_bootstrapping,
        )
        .with_test(
            "ann_returns_not_found_for_nonexistent_index",
            timeout,
            ann_returns_not_found_for_nonexistent_index,
        )
        .with_test(
            "ann_returns_unavailable_when_only_index_is_bootstrapping",
            timeout,
            ann_returns_unavailable_when_only_index_is_bootstrapping,
        )
        .with_test(
            "ann_returns_not_found_after_index_is_dropped",
            timeout,
            ann_returns_not_found_after_index_is_dropped,
        )
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
            "index '{}' must stay bootstrapping at {}",
            index.index,
            client.url()
        ),
        Duration::from_secs(60),
    )
    .await;
}

#[framed]
async fn ann_routes_to_serving_index_while_replacement_is_bootstrapping(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().expect("expected a single HTTP client");

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        None,
    )
    .await;

    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, embedding) VALUES (0, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert test embedding");

    let oldest = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;
    let oldest_status = wait_for_index(client, &oldest).await;
    assert_eq!(oldest_status.count, 1);

    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    let replacement = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;

    wait_for_bootstrapping(client, &replacement).await;

    get_query_results(
        format!(
            "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
        ),
        &session,
    )
    .await;

    actors.db_proxy.turn_off_rules().await;
    let replacement_status = wait_for_index(client, &replacement).await;
    assert_eq!(replacement_status.count, 1);

    session
        .query_unpaged(format!("DROP INDEX {}", oldest.index), ())
        .await
        .expect("failed to drop oldest index");

    wait_for(
        || async {
            client
                .indexes()
                .await
                .iter()
                .all(|i| i.index != oldest.index)
        },
        "oldest index must be removed",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    get_query_results(
        format!(
            "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
        ),
        &session,
    )
    .await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[framed]
async fn ann_does_not_route_between_columns_while_requested_index_is_bootstrapping(
    actors: TestActors,
) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().expect("expected a single HTTP client");

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>, embedding2 VECTOR<FLOAT, 3>",
        None,
    )
    .await;
    session
        .query_unpaged(
            format!(
                "INSERT INTO {table} (pk, embedding, embedding2) VALUES (0, [1.0, 2.0, 3.0], [1.0, 2.0, 3.0])"
            ),
            (),
        )
        .await
        .expect("failed to insert test embedding");

    let source = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;
    let source_status = wait_for_index(client, &source).await;
    assert_eq!(source_status.count, 1);

    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    let requested = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding2",
    ))
    .await;

    wait_for_bootstrapping(client, &requested).await;

    session
        .query_unpaged(
            format!(
                "SELECT * FROM {table} ORDER BY embedding2 ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
            ),
            (),
        )
        .await
        .expect_err("ANN query should fail when index is bootstrapping");

    actors.db_proxy.turn_off_rules().await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[framed]
async fn ann_returns_not_found_for_nonexistent_index(actors: TestActors) {
    info!("started");

    let (session, _clients) = prepare_connection_single_vs_no_tls(&actors).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        None,
    )
    .await;

    session
        .query_unpaged(
            format!(
                "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
            ),
            (),
        )
        .await
        .expect_err("ANN query should fail when no index exists");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[framed]
async fn ann_returns_unavailable_when_only_index_is_bootstrapping(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().expect("expected a single HTTP client");

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        None,
    )
    .await;

    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, embedding) VALUES (0, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert test embedding");

    actors
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    let index = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;

    wait_for_bootstrapping(client, &index).await;

    session
        .query_unpaged(
            format!(
                "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
            ),
            (),
        )
        .await
        .expect_err("ANN query should fail when index is bootstrapping");

    actors.db_proxy.turn_off_rules().await;

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}

#[framed]
async fn ann_returns_not_found_after_index_is_dropped(actors: TestActors) {
    info!("started");

    let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;
    let client = clients.first().expect("expected a single HTTP client");

    let keyspace = create_keyspace(&session).await;
    let table = create_table(
        &session,
        "pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
        None,
    )
    .await;

    session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, embedding) VALUES (0, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert test embedding");

    let index = create_index(CreateIndexQuery::new(
        &session,
        &clients,
        &table,
        "embedding",
    ))
    .await;
    let index_status = wait_for_index(client, &index).await;
    assert_eq!(index_status.count, 1);

    get_query_results(
        format!(
            "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
        ),
        &session,
    )
    .await;

    session
        .query_unpaged(format!("DROP INDEX {}", index.index), ())
        .await
        .expect("failed to drop index");

    wait_for(
        || async { client.indexes().await.is_empty() },
        "index must be removed",
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    session
        .query_unpaged(
            format!(
                "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
            ),
            (),
        )
        .await
        .expect_err("ANN query should fail after index is dropped");

    session
        .query_unpaged(format!("DROP KEYSPACE {keyspace}"), ())
        .await
        .expect("failed to drop keyspace");

    info!("finished");
}
