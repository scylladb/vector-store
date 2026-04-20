/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;

use super::Item;
use super::TableContext;
use super::dynamo_float_list;

fn build_put_requests(items: &[Item]) -> Vec<aws_sdk_dynamodb::types::WriteRequest> {
    items
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
        .collect()
}

async fn batch_write_items(
    ctx: &TableContext,
    puts: &[Item],
    deletes: &[Item],
) -> Result<BatchWriteItemOutput, SdkError<BatchWriteItemError>> {
    let mut write_requests = build_put_requests(puts);

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
}

/// Exercises `BatchWriteItem` (puts + deletes) and verifies the VS index is
/// updated correctly.
///
/// Loops [`NAME_PATTERNS`](super::NAME_PATTERNS) so that every combination
/// of key schema (HASH-only / HASH+RANGE) and naming style (plain /
/// special) is covered.
///
/// Four batch operations are tested:
/// 1. Initial batch of 3 puts.
/// 2. Replace an existing item with a new vector.
/// 3. Mixed batch: 1 put + 1 delete.
/// 4. Delete-only batch: 2 deletes → 1 item remains.
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

        // Batch put 3 items.
        info!(
            "Batch writing initial put requests into '{}'",
            ctx.table_name
        );
        batch_write_items(&ctx, &[a.clone(), b.clone(), c.clone()], &[])
            .await
            .expect("BatchWriteItem should succeed");
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[a.clone(), b.clone(), c.clone()])
            .await;

        // Replace a with a_replaced=[-1,-1,-1] — same key, vector points in the
        // opposite direction so it falls to last position in ANN results.
        // b=[1,2,4] and c=[1,4,8] retain their order.
        let a_replaced =
            Item::key(&ctx.pk, ctx.sk.as_deref(), "pk", "a").vec(vec_attr, [-1.0, -1.0, -1.0]);
        batch_write_items(&ctx, &[a_replaced.clone()], &[])
            .await
            .expect("BatchWriteItem should succeed");
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[b.clone(), c.clone(), a_replaced.clone()])
            .await;

        // Mixed put + delete — add d, remove a (using a_replaced which holds the
        // current state of that key).
        info!(
            "Batch writing mixed put and delete requests into '{}'",
            ctx.table_name
        );
        let d = Item::key(&ctx.pk, ctx.sk.as_deref(), "pk", "d").vec(vec_attr, [1.0, 1.0, 1.0]);
        batch_write_items(&ctx, std::slice::from_ref(&d), &[a_replaced])
            .await
            .expect("BatchWriteItem should succeed");
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[d.clone(), b.clone(), c.clone()])
            .await;

        // Delete-only — remove b and c, leaving d.
        info!("Batch writing delete requests into '{}'", ctx.table_name);
        batch_write_items(&ctx, &[], &[b, c])
            .await
            .expect("BatchWriteItem should succeed");
        ctx.wait_for_count(1).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], std::slice::from_ref(&d))
            .await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

/// Tests that `BatchWriteItem` containing an item with an invalid vector type
/// is rejected by Scylla and does not update the VS index.
///
/// Uses only [`NAME_PATTERNS`](super::NAME_PATTERNS)[0] (plain names, HASH-only).
///
/// Sequential scenarios on the same table:
///
/// 1. **Scenario 1** — batch put `[valid (vec=[1,1,1]), no_vec (no vec attr)]`.
///    Succeeds.  Only the valid item is indexed: count=1, ANN=[valid].
/// 2. **Scenario 2** — batch put `[valid2, wrong_type (vec=S("bad"))]`.
///    Scylla rejects the whole batch.  Count stays at 1.
/// 3. **Scenario 3** — batch put `[valid3, wrong_type (vec=L([N,N,S("3.0")]))]`.
///    Mostly-float list, last element is S — wrong element type.  Rejected.  Count stays at 1.
/// 4. **Scenario 4** — batch put `[valid4, wrong_type (vec=L([N,N,NULL]))]`.
///    Mostly-float list, last element is NULL.  Rejected.  Count stays at 1.
/// 5. **Scenario 5** — batch put `[valid5, wrong_type (vec=L([1.0, 1.0]))]`.
///    List of floats, too short (2 instead of 3).  Rejected.  Count stays at 1.
/// 6. **Scenario 6** — batch put `[valid6, wrong_type (vec=L([1.0,1.0,1.0,1.0]))]`.
///    List of floats, too long (4 instead of 3).  Rejected.  Count stays at 1.
#[framed]
async fn batch_write_item_with_invalid_vector(actors: TestActors) {
    info!("started");

    let shape = &super::NAME_PATTERNS[0]; // plain names, HASH-only
    let ctx = TableContext::create(&actors, shape).await;
    let vec_attr = ctx
        .vec_attr
        .as_deref()
        .expect("TableContext has no vec_attr");

    let pk = shape.pk;

    // Almost-valid vectors used to test wrong-element-type rejection.
    let vec_with_string_elem = AttributeValue::L(vec![
        AttributeValue::N("1.0".into()),
        AttributeValue::N("2.0".into()),
        AttributeValue::S("3.0".into()),
    ]);
    let vec_with_null_elem = AttributeValue::L(vec![
        AttributeValue::N("1.0".into()),
        AttributeValue::N("2.0".into()),
        AttributeValue::Null(true),
    ]);

    // Scenario 1: batch put a valid item and a no-vec item.  Only valid is
    // indexed.
    info!(
        "Scenario 1: batch put valid + no_vec into '{}'",
        ctx.table_name
    );
    let valid = Item::key(pk, None, "pk", "valid").vec(vec_attr, [1.0, 1.0, 1.0]);
    let no_vec = Item::key(pk, None, "pk", "no-vec");
    batch_write_items(&ctx, &[valid.clone(), no_vec], &[])
        .await
        .expect("BatchWriteItem should succeed");
    ctx.wait_for_count(1).await;
    ctx.wait_for_ann([1.0, 1.0, 1.0], &[valid.clone()]).await;

    // Scenario 2: batch put a valid item together with a String-typed vector —
    // Scylla rejects the whole batch.  Count stays at 1.
    info!(
        "Scenario 2: batch put valid2 + wrong_type(String) into '{}'",
        ctx.table_name
    );
    let valid2 = Item::key(pk, None, "pk", "valid2").vec(vec_attr, [1.0, 2.0, 4.0]);
    let wrong_type_string = Item::key(pk, None, "pk", "wrong-type-string")
        .attr(vec_attr, AttributeValue::S("bad".into()));
    super::assert_service_error(
        batch_write_items(&ctx, &[valid2, wrong_type_string], &[]).await,
        "ValidationException",
    );
    ctx.wait_for_count(1).await;

    // Scenario 3: batch put a valid item together with a mostly-float vector
    // where the last element is an S (numeric-looking) — wrong element type,
    // Scylla rejects the batch.
    info!(
        "Scenario 3: batch put valid3 + wrong_type(mostly-float, one S) into '{}'",
        ctx.table_name
    );
    let valid3 = Item::key(pk, None, "pk", "valid3").vec(vec_attr, [1.0, 4.0, 8.0]);
    let wrong_type_mostly_float_one_s = Item::key(pk, None, "pk", "wrong-type-mostly-float-one-s")
        .attr(vec_attr, vec_with_string_elem);
    super::assert_service_error(
        batch_write_items(&ctx, &[valid3, wrong_type_mostly_float_one_s], &[]).await,
        "ValidationException",
    );
    ctx.wait_for_count(1).await;

    // Scenario 4: batch put a valid item together with a mostly-float vector
    // where the last element is NULL — Scylla rejects the batch.
    info!(
        "Scenario 4: batch put valid4 + wrong_type(mostly-float, one NULL) into '{}'",
        ctx.table_name
    );
    let valid4 = Item::key(pk, None, "pk", "valid4").vec(vec_attr, [-1.0, -1.0, -1.0]);
    let wrong_type_mostly_float_one_null =
        Item::key(pk, None, "pk", "wrong-type-mostly-float-one-null")
            .attr(vec_attr, vec_with_null_elem);
    super::assert_service_error(
        batch_write_items(&ctx, &[valid4, wrong_type_mostly_float_one_null], &[]).await,
        "ValidationException",
    );
    ctx.wait_for_count(1).await;

    // Scenario 5: batch put a valid item together with a too-short float list
    // (2 elements instead of 3) — Scylla rejects the batch.
    info!(
        "Scenario 5: batch put valid5 + wrong_type(too-short) into '{}'",
        ctx.table_name
    );
    let valid5 = Item::key(pk, None, "pk", "valid5").vec(vec_attr, [1.0, 1.0, 2.0]);
    let wrong_type_too_short = Item::key(pk, None, "pk", "wrong-type-too-short")
        .attr(vec_attr, dynamo_float_list([1.0_f32, 1.0]));
    super::assert_service_error(
        batch_write_items(&ctx, &[valid5, wrong_type_too_short], &[]).await,
        "ValidationException",
    );
    ctx.wait_for_count(1).await;

    // Scenario 6: batch put a valid item together with a too-long float list
    // (4 elements instead of 3) — Scylla rejects the batch.
    info!(
        "Scenario 6: batch put valid6 + wrong_type(too-long) into '{}'",
        ctx.table_name
    );
    let valid6 = Item::key(pk, None, "pk", "valid6").vec(vec_attr, [2.0, 1.0, 1.0]);
    let wrong_type_too_long = Item::key(pk, None, "pk", "wrong-type-too-long")
        .attr(vec_attr, dynamo_float_list([1.0_f32, 1.0, 1.0, 1.0]));
    super::assert_service_error(
        batch_write_items(&ctx, &[valid6, wrong_type_too_long], &[]).await,
        "ValidationException",
    );
    ctx.wait_for_count(1).await;

    ctx.done().await;
    info!("finished");
}

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case
        .with_test(
            "batch_write_item_updates_index",
            common::DEFAULT_TEST_TIMEOUT,
            batch_write_item_updates_index,
        )
        .with_test(
            "batch_write_item_with_invalid_vector",
            common::DEFAULT_TEST_TIMEOUT,
            batch_write_item_with_invalid_vector,
        )
}
