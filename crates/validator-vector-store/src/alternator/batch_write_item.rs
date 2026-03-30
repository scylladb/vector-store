/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;

use super::Item;
use super::TableContext;

async fn batch_write_items(ctx: &TableContext, puts: &[Item], deletes: &[Item]) {
    let mut write_requests = puts
        .iter()
        .map(|item| {
            let mut builder = aws_sdk_dynamodb::types::PutRequest::builder();
            for (attr_name, attr_val) in &item.0 {
                builder = builder.item(attr_name, attr_val.clone());
            }
            aws_sdk_dynamodb::types::WriteRequest::builder()
                .put_request(builder.build().expect("failed to build PutRequest"))
                .build()
        })
        .collect::<Vec<_>>();

    write_requests.extend(deletes.iter().map(|item| {
        let mut builder = aws_sdk_dynamodb::types::DeleteRequest::builder();
        // DeleteRequest only needs the key attributes (pk, and sk if present).
        let key_attrs: Vec<&str> = std::iter::once(ctx.pk.as_str())
            .chain(ctx.sk.as_deref())
            .collect();
        for attr_name in key_attrs {
            if let Some(attr_val) = item.0.get(attr_name) {
                builder = builder.key(attr_name, attr_val.clone());
            }
        }
        aws_sdk_dynamodb::types::WriteRequest::builder()
            .delete_request(builder.build().expect("failed to build DeleteRequest"))
            .build()
    }));

    ctx.client
        .batch_write_item()
        .request_items(&ctx.table_name, write_requests)
        .send()
        .await
        .expect("BatchWriteItem should succeed");
}

/// Exercises `BatchWriteItem` (puts + deletes) and verifies the VS index is
/// updated correctly.
///
/// Loops [`NAME_PATTERNS`](super::NAME_PATTERNS) so that every combination
/// of key schema (HASH-only / HASH+RANGE) and naming style (plain /
/// special) is covered.
///
/// Three batch phases are tested:
/// 1. Initial batch of 3 puts.
/// 2. Mixed batch: 1 put + 1 delete.
/// 3. Delete-only batch: 2 deletes → 1 item remains.
///
/// Every item uses a distinct partition key so the test works for both
/// HASH-only and HASH+RANGE tables (a HASH-only table would reject two
/// items sharing the same PK inside a single `BatchWriteItem` call).
#[framed]
async fn batch_write_item_updates_index(actors: TestActors) {
    info!("started");

    for shape in super::NAME_PATTERNS {
        info!("Testing shape: {shape:?}");

        let ctx = TableContext::create(&actors, shape).await;
        let vec_attr = ctx
            .vec_attr
            .as_deref()
            .expect("TableContext has no vec_attr");

        let a = Item::key(&ctx.pk, ctx.sk.as_deref(), "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]);
        let b = Item::key(&ctx.pk, ctx.sk.as_deref(), "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]);
        let c = Item::key(&ctx.pk, ctx.sk.as_deref(), "pk", "c").vec(vec_attr, [1.0, 4.0, 8.0]);

        // Phase 1: batch put 3 items.
        info!(
            "Batch writing initial put requests into '{}'",
            ctx.table_name
        );
        batch_write_items(&ctx, &[a.clone(), b.clone(), c.clone()], &[]).await;
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[a.clone(), b.clone(), c.clone()])
            .await;

        // Phase 2: mixed put + delete — add d, remove a.
        info!(
            "Batch writing mixed put and delete requests into '{}'",
            ctx.table_name
        );
        let d = Item::key(&ctx.pk, ctx.sk.as_deref(), "pk", "d").vec(vec_attr, [1.0, 1.0, 1.0]);
        batch_write_items(&ctx, std::slice::from_ref(&d), &[a]).await;
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[d.clone(), b.clone(), c.clone()])
            .await;

        // Phase 3: delete-only — remove b and c, leaving d.
        info!("Batch writing delete requests into '{}'", ctx.table_name);
        batch_write_items(&ctx, &[], &[b, c]).await;
        ctx.wait_for_count(1).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], std::slice::from_ref(&d))
            .await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case.with_test(
        "batch_write_item_updates_index",
        common::DEFAULT_TEST_TIMEOUT,
        batch_write_item_updates_index,
    )
}
