/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemOutput;
use aws_sdk_dynamodb::types::AttributeValue;
use e2etest::TestCase;
use tracing::info;

use super::Item;
use super::TableContext;
use super::dynamo_float_list;

/// Issues an `UpdateItem` on the vector column of `item`.
///
/// The expression is supplied by the caller and may reference:
/// - `#vec` - the vector attribute name (always bound to `ctx.shape.vec_name`)
/// - `:val` - the new value (bound when `value` is `Some`)
async fn update_item_expr(
    ctx: &TableContext,
    item: &Item,
    update_expr: &str,
    value: Option<AttributeValue>,
) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
    let va = ctx.shape.vec().expect("TableContext has no vec_attr");
    let mut req = ctx
        .client
        .update_item()
        .table_name(&ctx.table_name)
        .update_expression(update_expr)
        .expression_attribute_names("#vec", va);
    for attr_name in std::iter::once(ctx.shape.pk()).chain(ctx.shape.sk()) {
        if let Some(attr_val) = item.0.get(attr_name) {
            req = req.key(attr_name, attr_val.clone());
        }
    }
    if let Some(val) = value {
        req = req.expression_attribute_values(":val", val);
    }
    req.send().await
}

/// Updates a vector via `UpdateItem` and verifies the VS index reflects the
/// change. Covers replacing an existing vector, adding a vector to a
/// previously unindexed item, and conditional UpdateItem (LWT path).
///
/// Starting state: `a` and `b` indexed, `c` has no vector (count=2).
#[framed]
async fn update_item_updates_index(actors: TestActors) {
    info!("started");

    for shape in &super::name_patterns() {
        info!("Testing shape: {shape:?}");

        let vec_attr = shape.vec().expect("NAME_PATTERNS entries always have vec");

        let a = Item::key(shape.pk(), shape.sk(), "pk", "a").vec(vec_attr, [1.0, 2.0, 4.0]);
        let b = Item::key(shape.pk(), shape.sk(), "pk", "b").vec(vec_attr, [1.0, 4.0, 8.0]);
        let c_no_vec = Item::key(shape.pk(), shape.sk(), "pk", "c");

        let ctx = TableContext::create_with_invalid_data(
            &actors,
            shape,
            &[a.clone(), b.clone()],
            std::slice::from_ref(&c_no_vec),
        )
        .await;

        info!("Step 1: updating 'b' vector in '{}'", ctx.table_name);
        let b_vec = [1.0, 1.0, 1.0];
        let b_updated = Item::key(shape.pk(), shape.sk(), "pk", "b").vec(vec_attr, b_vec);
        update_item_expr(
            &ctx,
            &b_updated,
            "SET #vec = :val",
            Some(dynamo_float_list(b_vec)),
        )
        .await
        .expect("UpdateItem should succeed");

        ctx.wait_for_ann([1.0, 1.0, 1.0], &[b_updated.clone(), a.clone()])
            .await;

        info!("Step 2: adding vector to 'c' in '{}'", ctx.table_name);
        let c_vec = [-1.0, -1.0, -1.0];
        let c_with_vec = Item::key(shape.pk(), shape.sk(), "pk", "c").vec(vec_attr, c_vec);
        update_item_expr(
            &ctx,
            &c_with_vec,
            "SET #vec = :val",
            Some(dynamo_float_list(c_vec)),
        )
        .await
        .expect("UpdateItem should succeed");

        // c=[-1,-1,-1] is identical to query [-1,-1,-1] (similarity=1, first).
        // a=[1,2,4] has cosine similarity ≈ -0.882 (second).
        // b_updated=[1,1,1] is antipodal to [-1,-1,-1] (similarity=-1, last).
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann([-1.0, -1.0, -1.0], &[c_with_vec, a, b_updated])
            .await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

/// Tests that `UpdateItem` operations which result in an absent or invalid
/// vector are handled correctly: REMOVE of the vector column de-indexes the
/// item (the row remains in the table but without a vector - analogous to
/// `put_item_with_invalid_vector_is_not_indexed` where a PutItem without a
/// vector column is accepted but not indexed), and wrong-type updates are
/// rejected by Scylla with `ValidationException`.
#[framed]
async fn update_item_with_invalid_vector_is_not_indexed(actors: TestActors) {
    info!("started");

    let shape = &super::name_patterns()[0]; // plain names, HASH-only
    let vec_attr = shape.vec().expect("NAME_PATTERNS[0] always has vec");

    let a = Item::key(shape.pk(), shape.sk(), "pk", "a").vec(vec_attr, [1.0, 2.0, 4.0]);
    let b = Item::key(shape.pk(), shape.sk(), "pk", "b").vec(vec_attr, [1.0, 1.0, 1.0]);

    let ctx = TableContext::create_with_data(&actors, shape, &[a.clone(), b.clone()]).await;

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

    update_item_expr(&ctx, &b, "REMOVE #vec", None)
        .await
        .expect("UpdateItem should succeed");
    ctx.wait_for_count(1).await;
    ctx.wait_for_ann([1.0, 1.0, 1.0], std::slice::from_ref(&a))
        .await;

    super::assert_service_error(
        update_item_expr(
            &ctx,
            &a,
            "SET #vec = :val",
            Some(AttributeValue::S("not-a-vector".into())),
        )
        .await,
        "ValidationException",
    );

    super::assert_service_error(
        update_item_expr(&ctx, &a, "SET #vec = :val", Some(vec_with_string_elem)).await,
        "ValidationException",
    );

    super::assert_service_error(
        update_item_expr(&ctx, &a, "SET #vec = :val", Some(vec_with_null_elem)).await,
        "ValidationException",
    );

    super::assert_service_error(
        update_item_expr(
            &ctx,
            &a,
            "SET #vec = :val",
            Some(dynamo_float_list([1.0_f32, 1.0])),
        )
        .await,
        "ValidationException",
    );

    super::assert_service_error(
        update_item_expr(
            &ctx,
            &a,
            "SET #vec = :val",
            Some(dynamo_float_list([1.0_f32, 1.0, 1.0, 1.0])),
        )
        .await,
        "ValidationException",
    );

    ctx.done().await;
    info!("finished");
}

pub(super) async fn new() -> TestCase<TestActors> {
    TestCase::empty()
        .with_init(common::DEFAULT_TEST_TIMEOUT, super::init)
        .with_cleanup(common::DEFAULT_TEST_TIMEOUT, common::cleanup)
        .with_test(
            "update_item_updates_index",
            common::DEFAULT_TEST_TIMEOUT,
            update_item_updates_index,
        )
        .with_test(
            "update_item_with_invalid_vector_is_not_indexed",
            common::DEFAULT_TEST_TIMEOUT,
            update_item_with_invalid_vector_is_not_indexed,
        )
}
