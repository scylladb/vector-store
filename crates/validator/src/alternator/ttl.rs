/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Alternator TTL integration tests.
//!
//! These tests verify that when DynamoDB TTL expires a row, the resulting CDC
//! tombstone is consumed by Vector Store and the expired vector is removed
//! from the index.

use crate::alternator;
use crate::alternator::AlternatorContext;
use crate::alternator::Item;
use crate::alternator::TableContext;
use crate::alternator::TableShape;
use crate::alternator::query::QueryBuilderExt;
use crate::common;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::ScalarAttributeType;
use aws_sdk_dynamodb::types::Select;
use aws_sdk_dynamodb::types::TimeToLiveSpecification;
use std::sync::Arc;
use tracing::info;

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
#[e2etest::test(group = ttl)]
async fn ttl_expiration_removes_vector(ctx: Arc<AlternatorContext>) {
    info!("started");

    let shapes: &[TableShape] = &[
        TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "Pk-TTL".into(),
            sk_name: None,
            vec_name: Some("Vec-TTL".into()),
            pk_type: ScalarAttributeType::S,
        },
        TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "Pk-TTLHR".into(),
            sk_name: Some("Sk-TTLHR".into()),
            vec_name: Some("Vec-TTLHR".into()),
            pk_type: ScalarAttributeType::S,
        },
    ];

    let ttl_attribute = "ttl_expiry";

    for shape in shapes {
        info!(?shape, "testing shape");

        let perm1 =
            Item::key(shape.pk(), shape.sk(), "pk", "1").vec(shape.vec().unwrap(), [1.0, 1.0, 1.0]);
        let perm2 =
            Item::key(shape.pk(), shape.sk(), "pk", "2").vec(shape.vec().unwrap(), [1.0, 2.0, 4.0]);
        let expiring = Item::key(shape.pk(), shape.sk(), "pk", "expiring")
            .vec(shape.vec().unwrap(), [1.0, 4.0, 8.0])
            .attr(ttl_attribute, AttributeValue::N(ttl_epoch(2).to_string()));

        let ctx = TableContext::create_with_data(
            ctx.actors(),
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

/// Like [`ttl_expiration_removes_vector`] but uses `Select::AllProjectedAttributes`
/// - an index-only read that skips the base table.
#[e2etest::test(group = ttl)]
async fn ttl_expiration_verified_via_query_with_all_projected(ctx: Arc<AlternatorContext>) {
    info!("started");

    let shape = TableShape {
        table_prefix: None,
        index_prefix: None,
        pk_name: "Pk-TTLProj".into(),
        sk_name: None,
        vec_name: Some("Vec-TTLProj".into()),
        pk_type: ScalarAttributeType::S,
    };
    let ttl_attribute = "ttl_expiry";

    let vec_attr = shape.vec().unwrap();

    let perm1 = Item::key(shape.pk(), shape.sk(), "pk", "1").vec(vec_attr, [1.0, 1.0, 1.0]);
    let perm2 = Item::key(shape.pk(), shape.sk(), "pk", "2").vec(vec_attr, [1.0, 2.0, 4.0]);
    let expiring = Item::key(shape.pk(), shape.sk(), "pk", "expiring")
        .vec(vec_attr, [1.0, 4.0, 8.0])
        .attr(ttl_attribute, AttributeValue::N(ttl_epoch(2).to_string()));

    let ctx = TableContext::create_with_data(
        ctx.actors(),
        &shape,
        &[perm1.clone(), perm2.clone(), expiring],
    )
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
                        !matches!(item.get(ctx.shape.pk()), Some(AttributeValue::S(s)) if s == "pk-expiring")
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

e2etest::group!(
    name = ttl,
    fixtures = (AlternatorContext),
    parent = alternator::alternator
);
