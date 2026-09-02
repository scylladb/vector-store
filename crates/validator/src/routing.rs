/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use httpapi::IndexInfo;
use httpapi::IndexStatus;
use httpclient::HttpClient;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

const ANN_LIMIT: usize = 5;
const FRAME_DELAY: Duration = Duration::from_millis(500);

e2etest::group!(
    name = routing,
    fixtures = (ProxyTestContext),
    parent = crate::proxy
);

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

#[e2etest::test(group = routing)]
async fn ann_routes_to_serving_index_while_replacement_is_bootstrapping(
    ctx: Arc<ProxyTestContext>,
) {
    info!("started");

    let client = ctx.client();
    let table = ctx
        .create_table("pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>", None)
        .await;

    ctx.session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, embedding) VALUES (0, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert test embedding");

    let oldest = ctx.create_index(&table, "embedding").await;
    let oldest_status = wait_for_index(client, &oldest).await;
    assert_eq!(oldest_status.count, 1);

    ctx.actors()
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    let replacement = ctx.create_index(&table, "embedding").await;

    wait_for_bootstrapping(client, &replacement).await;

    get_query_results(
        format!(
            "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
        ),
        &ctx.session,
    )
    .await;

    ctx.actors().db_proxy.turn_off_rules().await;
    let replacement_status = wait_for_index(client, &replacement).await;
    assert_eq!(replacement_status.count, 1);

    ctx.session
        .query_unpaged(format!("DROP INDEX {}", oldest.index), ())
        .await
        .expect("failed to drop oldest index");

    wait_for_no_index(client, &oldest).await;

    get_query_results(
        format!(
            "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
        ),
        &ctx.session,
    )
    .await;

    info!("finished");
}

#[e2etest::test(group = routing)]
async fn ann_does_not_route_between_columns_while_requested_index_is_bootstrapping(
    ctx: Arc<ProxyTestContext>,
) {
    info!("started");

    let client = ctx.client();
    let table = ctx
        .create_table(
            "pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>, embedding2 VECTOR<FLOAT, 3>",
            None,
        )
        .await;
    ctx.session
        .query_unpaged(
            format!(
                "INSERT INTO {table} (pk, embedding, embedding2) VALUES (0, [1.0, 2.0, 3.0], [1.0, 2.0, 3.0])"
            ),
            (),
        )
        .await
        .expect("failed to insert test embedding");

    let source = ctx.create_index(&table, "embedding").await;
    let source_status = wait_for_index(client, &source).await;
    assert_eq!(source_status.count, 1);

    ctx.actors()
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    let requested = ctx.create_index(&table, "embedding2").await;

    wait_for_bootstrapping(client, &requested).await;

    ctx.session
        .query_unpaged(
            format!(
                "SELECT * FROM {table} ORDER BY embedding2 ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
            ),
            (),
        )
        .await
        .expect_err("ANN query should fail when index is bootstrapping");

    ctx.actors().db_proxy.turn_off_rules().await;

    info!("finished");
}

#[e2etest::test(group = routing)]
async fn ann_returns_not_found_for_nonexistent_index(ctx: Arc<ProxyTestContext>) {
    info!("started");

    let table = ctx
        .create_table("pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>", None)
        .await;

    ctx.session
        .query_unpaged(
            format!(
                "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
            ),
            (),
        )
        .await
        .expect_err("ANN query should fail when no index exists");

    info!("finished");
}

#[e2etest::test(group = routing)]
async fn ann_returns_unavailable_when_only_index_is_bootstrapping(ctx: Arc<ProxyTestContext>) {
    info!("started");

    let client = ctx.client();
    let table = ctx
        .create_table("pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>", None)
        .await;

    ctx.session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, embedding) VALUES (0, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert test embedding");

    ctx.actors()
        .db_proxy
        .change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::delay(FRAME_DELAY),
        )]))
        .await;

    let index = ctx.create_index(&table, "embedding").await;

    wait_for_bootstrapping(client, &index).await;

    ctx.session
        .query_unpaged(
            format!(
                "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
            ),
            (),
        )
        .await
        .expect_err("ANN query should fail when index is bootstrapping");

    ctx.actors().db_proxy.turn_off_rules().await;

    info!("finished");
}

#[e2etest::test(group = routing)]
async fn ann_returns_not_found_after_index_is_dropped(ctx: Arc<ProxyTestContext>) {
    info!("started");

    let client = ctx.client();
    let table = ctx
        .create_table("pk INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>", None)
        .await;

    ctx.session
        .query_unpaged(
            format!("INSERT INTO {table} (pk, embedding) VALUES (0, [1.0, 2.0, 3.0])"),
            (),
        )
        .await
        .expect("failed to insert test embedding");

    let index = ctx.create_index(&table, "embedding").await;
    let index_status = wait_for_index(client, &index).await;
    assert_eq!(index_status.count, 1);

    get_query_results(
        format!(
            "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
        ),
        &ctx.session,
    )
    .await;

    ctx.session
        .query_unpaged(format!("DROP INDEX {}", index.index), ())
        .await
        .expect("failed to drop index");

    wait_for_no_index(client, &index).await;

    ctx.session
        .query_unpaged(
            format!(
                "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT {ANN_LIMIT}"
            ),
            (),
        )
        .await
        .expect_err("ANN query should fail after index is dropped");

    info!("finished");
}
