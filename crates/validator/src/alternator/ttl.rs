/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

//! Alternator TTL integration tests.
//!
//! These tests verify that when DynamoDB TTL expires a row, the resulting CDC
//! tombstone is consumed by Vector Store and the expired vector is removed
//! from the index.

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::Select;
use aws_sdk_dynamodb::types::TimeToLiveSpecification;
use e2etest::TestCase;
use tracing::info;

use super::Item;
use super::TableContext;
use super::TableShape;
use super::query::QueryBuilderExt;

/// Enables DynamoDB TTL on `ttl_attribute` for the given table.
async fn enable_ttl(client: &aws_sdk_dynamodb::Client, table_name: &str, ttl_attribute: &str) {
    client
        .update_time_to_live()
        .table_name(table_name)
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name(ttl_attribute)
                .build()
                .expect("failed to build TimeToLiveSpecification"),
        )
        .send()
        .await
        .expect("UpdateTimeToLive should succeed");
}

/// Returns a Unix epoch timestamp `seconds_from_now` seconds in the future.
fn ttl_epoch(seconds_from_now: i64) -> i64 {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    now + seconds_from_now
}

/// Inserts 3 items (2 permanent + 1 expiring) via the initial-scan path,
/// then enables TTL.  Waits for the TTL-expired row to be reaped and the
/// index count to drop to 2.
///
/// Covers both HASH-only and HASH+RANGE key schemas.
#[framed]
async fn ttl_expiration_removes_vector(actors: TestActors) {
    info!("started");

    const SHAPES: &[TableShape] = &[
        TableShape {
            table_prefix: "",
            index_prefix: "",
            pk: "Pk-TTL",
            sk: None,
            vec: Some("Vec-TTL"),
            pk_type: super::ScalarAttributeType::S,
        },
        TableShape {
            table_prefix: "",
            index_prefix: "",
            pk: "Pk-TTLHR",
            sk: Some("Sk-TTLHR"),
            vec: Some("Vec-TTLHR"),
            pk_type: super::ScalarAttributeType::S,
        },
    ];

    let ttl_attribute = "ttl_expiry";

    for shape in SHAPES {
        info!(?shape, "testing shape");

        let perm1 =
            Item::key(shape.pk, shape.sk, "pk", "1").vec(shape.vec.unwrap(), [1.0, 1.0, 1.0]);
        let perm2 =
            Item::key(shape.pk, shape.sk, "pk", "2").vec(shape.vec.unwrap(), [1.0, 2.0, 4.0]);
        let expiring = Item::key(shape.pk, shape.sk, "pk", "expiring")
            .vec(shape.vec.unwrap(), [1.0, 4.0, 8.0])
            .attr(ttl_attribute, AttributeValue::N(ttl_epoch(2).to_string()));

        let ctx = TableContext::create_with_data(
            &actors,
            shape,
            &[perm1.clone(), perm2.clone(), expiring],
        )
        .await;

        info!(
            "Enabling TTL on attribute '{ttl_attribute}' for '{}'",
            ctx.table_name
        );
        enable_ttl(&ctx.client, &ctx.table_name, ttl_attribute).await;

        info!("Waiting for TTL expiration to propagate to VS index");
        ctx.wait_for_count(2).await;

        ctx.wait_for_ann([1.0, 1.0, 1.0], &[perm1.clone(), perm2.clone()])
            .await;

        info!("TTL expiration correctly removed expired item from index");
        ctx.done().await;
    }

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: TTL expiration verified via Alternator Query with AllProjectedAttributes
// ---------------------------------------------------------------------------

/// Combines TTL expiration with an Alternator `Query` using
/// `Select::AllProjectedAttributes` to verify that both the VS index and the
/// Alternator Query endpoint agree that the expired item is gone.
///
/// Inserts 3 items (2 permanent + 1 expiring) via the initial-scan path,
/// then enables TTL.  After the expired row disappears, issues a VectorSearch
/// `Query` with `Select::AllProjectedAttributes` — an index-only read that
/// skips the base table — and asserts that the expired item is absent and
/// only the 2 permanent items are returned.
#[framed]
async fn ttl_expiration_verified_via_query_with_all_projected(actors: TestActors) {
    info!("started");

    let shape = TableShape {
        table_prefix: "",
        index_prefix: "",
        pk: "Pk-TTLProj",
        sk: None,
        vec: Some("Vec-TTLProj"),
        pk_type: super::ScalarAttributeType::S,
    };
    let ttl_attribute = "ttl_expiry";

    let perm1 = Item::key("Pk-TTLProj", shape.sk, "pk", "1").vec("Vec-TTLProj", [1.0, 1.0, 1.0]);
    let perm2 = Item::key("Pk-TTLProj", shape.sk, "pk", "2").vec("Vec-TTLProj", [1.0, 2.0, 4.0]);
    let expiring = Item::key("Pk-TTLProj", shape.sk, "pk", "expiring")
        .vec("Vec-TTLProj", [1.0, 4.0, 8.0])
        .attr(ttl_attribute, AttributeValue::N(ttl_epoch(2).to_string()));

    let ctx =
        TableContext::create_with_data(&actors, &shape, &[perm1.clone(), perm2.clone(), expiring])
            .await;

    info!(
        "Enabling TTL on attribute '{ttl_attribute}' for '{}'",
        ctx.table_name
    );
    enable_ttl(&ctx.client, &ctx.table_name, ttl_attribute).await;

    info!("Waiting for TTL to expire the item and for VS to remove it");
    ctx.wait_for_count(2).await;

    info!("Querying via Alternator with Select::AllProjectedAttributes after TTL expiration");
    common::wait_for(
        || {
            let ctx = &ctx;
            async move {
                let items = ctx
                    .client
                    .query()
                    .table_name(&ctx.table_name)
                    .index_name(ctx.index.index.as_ref())
                    .limit(5)
                    .select(Select::AllProjectedAttributes)
                    .vector_search([1.0, 1.0, 1.0])
                    .send()
                    .await
                    .expect("Query with VectorSearch should succeed")
                    .items()
                    .to_vec();

                items.len() == 2
                    && items.iter().all(|item| {
                        !matches!(item.get(ctx.pk.as_str()), Some(AttributeValue::S(s)) if s == "pk-expiring")
                    })
            }
        },
        "AllProjectedAttributes query to return only 2 permanent items after TTL expiration",
        common::DEFAULT_TEST_TIMEOUT,
    )
    .await;

    ctx.done().await;

    info!("finished");
}

pub(super) fn register(test_case: TestCase<TestActors>) -> TestCase<TestActors> {
    test_case
        .with_test(
            "ttl_expiration_removes_vector",
            common::DEFAULT_TEST_TIMEOUT,
            ttl_expiration_removes_vector,
        )
        .with_test(
            "ttl_expiration_verified_via_query_with_all_projected",
            common::DEFAULT_TEST_TIMEOUT,
            ttl_expiration_verified_via_query_with_all_projected,
        )
}
