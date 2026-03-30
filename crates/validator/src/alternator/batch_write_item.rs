/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput;
use aws_sdk_dynamodb::types::AttributeValue;
use e2etest::TestCase;
use tracing::info;

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
        let key_attrs: Vec<&str> = std::iter::once(ctx.shape.pk())
            .chain(ctx.shape.sk())
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

/// Exercises `BatchWriteItem` (puts, replaces, mixed put+delete, delete-only)
/// and verifies the VS index is updated correctly.
///
/// Loops [`NAME_PATTERNS`](super::NAME_PATTERNS) to cover all key schema and
/// naming combinations.
#[framed]
async fn batch_write_item_updates_index(actors: TestActors) {
    info!("started");

    for shape in &super::name_patterns() {
        info!("Testing shape: {shape:?}");

        let ctx = TableContext::create(&actors, shape).await;
        let vec_attr = ctx.shape.vec().expect("TableContext has no vec_attr");

        let a = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]);
        let b = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]);
        let c = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "c").vec(vec_attr, [1.0, 4.0, 8.0]);

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

        // Replace a with a_replaced=[-1,-1,-1] - same key, vector points in the
        // opposite direction so it falls to last position in ANN results.
        // b=[1,2,4] and c=[1,4,8] retain their order.
        let a_replaced =
            Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "a").vec(vec_attr, [-1.0, -1.0, -1.0]);
        batch_write_items(&ctx, std::slice::from_ref(&a_replaced), &[])
            .await
            .expect("BatchWriteItem should succeed");
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[b.clone(), c.clone(), a_replaced.clone()])
            .await;

        // Mixed put + delete - add d, remove a (using a_replaced which holds the
        // current state of that key).
        info!(
            "Batch writing mixed put and delete requests into '{}'",
            ctx.table_name
        );
        let d = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "d").vec(vec_attr, [1.0, 1.0, 1.0]);
        batch_write_items(&ctx, std::slice::from_ref(&d), &[a_replaced])
            .await
            .expect("BatchWriteItem should succeed");
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[d.clone(), b.clone(), c.clone()])
            .await;

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
/// is rejected by Scylla and does not update the VS index. Covers string
/// values, mixed-type lists, NULL elements, and wrong dimensions.
#[framed]
async fn batch_write_item_with_invalid_vector(actors: TestActors) {
    info!("started");

    let shape = &super::name_patterns()[0]; // plain names, HASH-only
    let ctx = TableContext::create(&actors, shape).await;
    let vec_attr = ctx.shape.vec().expect("TableContext has no vec_attr");

    let pk = shape.pk();

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
    ctx.wait_for_ann([1.0, 1.0, 1.0], std::slice::from_ref(&valid))
        .await;

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

pub(super) async fn new() -> TestCase<TestActors> {
    TestCase::empty()
        .with_init(common::DEFAULT_TEST_TIMEOUT, super::init)
        .with_cleanup(common::DEFAULT_TEST_TIMEOUT, common::cleanup)
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
