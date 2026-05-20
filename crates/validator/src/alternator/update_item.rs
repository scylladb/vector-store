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
/// - `#pk`  - the partition-key attribute name (bound to `ctx.pk` only when
///   `condition_expr` is `Some`; Alternator rejects spurious bindings)
///
/// An optional `condition_expr` may be supplied; when `Some` it is set as the
/// `ConditionExpression` on the request, allowing the caller to exercise the
/// LWT/Paxos code path under `only_rmw_uses_lwt` write isolation.
async fn update_item_expr(
    ctx: &TableContext,
    item: &Item,
    update_expr: &str,
    value: Option<AttributeValue>,
    condition_expr: Option<&str>,
) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
    let va = ctx.shape.vec().expect("TableContext has no vec_attr");
    let mut req = ctx
        .client
        .update_item()
        .table_name(&ctx.table_name)
        .update_expression(update_expr)
        .expression_attribute_names("#vec", va);
    // Only bind #pk when a condition expression actually references it,
    // otherwise Alternator rejects the request as having a spurious binding.
    if condition_expr.is_some() {
        req = req.expression_attribute_names("#pk", ctx.shape.pk());
    }
    for attr_name in std::iter::once(ctx.shape.pk()).chain(ctx.shape.sk()) {
        if let Some(attr_val) = item.0.get(attr_name) {
            req = req.key(attr_name, attr_val.clone());
        }
    }
    if let Some(val) = value {
        req = req.expression_attribute_values(":val", val);
    }
    if let Some(cond) = condition_expr {
        req = req.condition_expression(cond);
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
            None,
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
            None,
        )
        .await
        .expect("UpdateItem should succeed");

        // c=[-1,-1,-1] is identical to query [-1,-1,-1] (similarity=1, first).
        // a=[1,2,4] has cosine similarity ≈ -0.882 (second).
        // b_updated=[1,1,1] is antipodal to [-1,-1,-1] (similarity=-1, last).
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann(
            [-1.0, -1.0, -1.0],
            &[c_with_vec.clone(), a.clone(), b_updated.clone()],
        )
        .await;

        // Conditional UpdateItem exercises the LWT/Paxos path under
        // `only_rmw_uses_lwt` (ConditionExpression makes it RMW).
        info!(
            "Step 3: conditional UpdateItem (passing condition) in '{}'",
            ctx.table_name
        );
        let a_new_vec = [4.0_f32, 2.0, 1.0];
        let a_updated = Item::key(shape.pk(), shape.sk(), "pk", "a").vec(vec_attr, a_new_vec);
        update_item_expr(
            &ctx,
            &a_updated,
            "SET #vec = :val",
            Some(dynamo_float_list(a_new_vec)),
            Some("attribute_exists(#pk)"),
        )
        .await
        .expect("conditional UpdateItem with passing condition should succeed");
        // a_updated=[4,2,1]: ANN([4,2,1]) ->
        //   a_updated first (sim=1.0), b_updated=[1,1,1] second (sim≈0.882),
        //   c=[-1,-1,-1] last (sim≈-0.882).
        ctx.wait_for_ann([4.0, 2.0, 1.0], &[a_updated, b_updated, c_with_vec])
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

    update_item_expr(&ctx, &b, "REMOVE #vec", None, None)
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
            None,
        )
        .await,
        "ValidationException",
    );

    super::assert_service_error(
        update_item_expr(
            &ctx,
            &a,
            "SET #vec = :val",
            Some(vec_with_string_elem),
            None,
        )
        .await,
        "ValidationException",
    );

    super::assert_service_error(
        update_item_expr(&ctx, &a, "SET #vec = :val", Some(vec_with_null_elem), None).await,
        "ValidationException",
    );

    super::assert_service_error(
        update_item_expr(
            &ctx,
            &a,
            "SET #vec = :val",
            Some(dynamo_float_list([1.0_f32, 1.0])),
            None,
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
            None,
        )
        .await,
        "ValidationException",
    );

    ctx.done().await;
    info!("finished");
}

/// Tests element-level `UpdateItem` operations on the vector column
/// (`SET #vec[i]`, `REMOVE #vec[i]`, `list_append`, `ADD #vec[i]`) and
/// verifies that VS reindexes or de-indexes items correctly.
///
/// Starts with 2 valid items and 3 invalid-vector items (mixed types, wrong
/// dimensions). Steps 1-5 exercise mutations on a valid item (accepted and
/// rejected). Steps 6-8 fix the invalid items. Step 9 tests ADD (LWT path).
#[framed]
async fn update_item_vector_element_operations(actors: TestActors) {
    info!("started");

    let patterns = super::name_patterns();
    let shape = &patterns[0]; // plain names, HASH-only
    let vec_attr = shape.vec().expect("NAME_PATTERNS[0] always has vec");
    let pk = shape.pk();

    // Two valid items so ANN order is meaningful and vector changes are
    // detectable.  Invalid items are pre-inserted before the index exists.
    let valid_a = Item::key(pk, None, "pk", "valid-a").vec(vec_attr, [1.0, 2.0, 4.0]);
    let valid_b = Item::key(pk, None, "pk", "valid-b").vec(vec_attr, [4.0, 2.0, 1.0]);
    let mixed = Item::key(pk, None, "pk", "mixed").attr(
        vec_attr,
        AttributeValue::L(vec![
            AttributeValue::N("1.0".into()),
            AttributeValue::N("2.0".into()),
            AttributeValue::S("3.0".into()),
        ]),
    );
    let too_short =
        Item::key(pk, None, "pk", "too-short").attr(vec_attr, dynamo_float_list([1.0_f32, 2.0]));
    let too_long = Item::key(pk, None, "pk", "too-long")
        .attr(vec_attr, dynamo_float_list([1.0_f32, 2.0, 4.0, 8.0]));

    let ctx = TableContext::create_with_invalid_data(
        &actors,
        shape,
        &[valid_a.clone(), valid_b.clone()],
        &[mixed.clone(), too_short.clone(), too_long.clone()],
    )
    .await;
    info!("Step 1: SET #vec[0] on 'valid_a'");
    update_item_expr(
        &ctx,
        &valid_a,
        "SET #vec[0] = :val",
        Some(AttributeValue::N("5.0".into())),
        None,
    )
    .await
    .expect("UpdateItem should succeed");
    let valid_a_step1 = Item::key(pk, None, "pk", "valid-a").vec(vec_attr, [5.0, 2.0, 4.0]);
    ctx.wait_for_ann([5.0, 2.0, 4.0], &[valid_a_step1.clone(), valid_b.clone()])
        .await;

    info!("Step 2: SET #vec[0] = S on 'valid_a' (rejected)");
    super::assert_service_error(
        update_item_expr(
            &ctx,
            &valid_a,
            "SET #vec[0] = :val",
            Some(AttributeValue::S("bad".into())),
            None,
        )
        .await,
        "ValidationException",
    );

    info!("Step 3: SET #vec[2] = NULL on 'valid_a' (rejected)");
    super::assert_service_error(
        update_item_expr(
            &ctx,
            &valid_a,
            "SET #vec[2] = :val",
            Some(AttributeValue::Null(true)),
            None,
        )
        .await,
        "ValidationException",
    );

    info!("Step 4: REMOVE #vec[0] on 'valid_a' (rejected - wrong dimension)");
    super::assert_service_error(
        update_item_expr(&ctx, &valid_a, "REMOVE #vec[0]", None, None).await,
        "ValidationException",
    );

    info!("Step 5: list_append on 'valid_a' (rejected - wrong dimension)");
    super::assert_service_error(
        update_item_expr(
            &ctx,
            &valid_a,
            "SET #vec = list_append(#vec, :val)",
            Some(AttributeValue::L(vec![AttributeValue::N("1.0".into())])),
            None,
        )
        .await,
        "ValidationException",
    );
    info!("Step 6: SET #vec[2] on 'mixed'");
    update_item_expr(
        &ctx,
        &mixed,
        "SET #vec[2] = :val",
        Some(AttributeValue::N("3.0".into())),
        None,
    )
    .await
    .expect("UpdateItem should succeed");
    ctx.wait_for_count(3).await;
    info!("Step 7: SET #vec[2] on 'too_short' (out-of-range index appends)");
    update_item_expr(
        &ctx,
        &too_short,
        "SET #vec[2] = :val",
        Some(AttributeValue::N("4.0".into())),
        None,
    )
    .await
    .expect("UpdateItem should succeed");
    ctx.wait_for_count(4).await;
    info!("Step 8: REMOVE #vec[3] on 'too_long'");
    update_item_expr(&ctx, &too_long, "REMOVE #vec[3]", None, None)
        .await
        .expect("UpdateItem should succeed");
    ctx.wait_for_count(5).await;
    info!("Step 9: ADD #vec[0] on 'valid_b' (ADD is always RMW -> LWT path)");
    update_item_expr(
        &ctx,
        &valid_b,
        "ADD #vec[0] :val",
        Some(AttributeValue::N("1.0".into())),
        None,
    )
    .await
    .expect("UpdateItem ADD #vec[0] should succeed");
    let valid_b_step9 = Item::key(pk, None, "pk", "valid-b").vec(vec_attr, [5.0, 2.0, 1.0]);
    ctx.wait_for_count(5).await;
    ctx.wait_for_ann([5.0, 2.0, 1.0], &[valid_b_step9]).await;

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
        .with_test(
            "update_item_vector_element_operations",
            common::DEFAULT_TEST_TIMEOUT,
            update_item_vector_element_operations,
        )
}
