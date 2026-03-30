/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemOutput;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;

use super::Item;
use super::TableContext;
use super::dynamo_float_list;

/// Issues an `UpdateItem` on the vector column of `item`.
///
/// The expression is supplied by the caller and may reference:
/// - `#vec` — the vector attribute name (always bound to `ctx.vec_attr`)
/// - `:val` — the new value (bound when `value` is `Some`)
///
/// Examples of supported expressions:
/// - `"SET #vec = :val"` — replace the entire vector
/// - `"SET #vec[0] = :val"` — replace a single element
/// - `"REMOVE #vec"` — remove the entire vector attribute
/// - `"REMOVE #vec[0]"` — remove a single element
/// - `"SET #vec = list_append(#vec, :val)"` — append elements
///
/// Only the key attributes of `item` (pk, and sk when present) are used to
/// identify the row; all other attributes in `item` are ignored.
async fn update_item_expr(
    ctx: &TableContext,
    item: &Item,
    update_expr: &str,
    value: Option<AttributeValue>,
) -> Result<UpdateItemOutput, SdkError<UpdateItemError>> {
    let va = ctx
        .vec_attr
        .as_deref()
        .expect("TableContext has no vec_attr");
    let mut req = ctx
        .client
        .update_item()
        .table_name(&ctx.table_name)
        .update_expression(update_expr)
        .expression_attribute_names("#vec", va);
    // Set key attributes (pk, and sk if present).
    for attr_name in std::iter::once(ctx.pk.as_str()).chain(ctx.sk.as_deref()) {
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
/// new vector.
///
/// Loops [`NAME_PATTERNS`](super::NAME_PATTERNS) so that every combination
/// of key schema (HASH-only / HASH+RANGE) and naming style (plain /
/// special) is covered.
///
/// Starting state (created via `create_with_invalid_data`):
/// - `a = [1, 2, 4]` — indexed
/// - `b = [1, 4, 8]` — indexed
/// - `c` — **no vector attribute**, pre-inserted as an invalid item so it is
///   in the table from the very first scan but is not indexed (count = 2).
///
/// Two update steps are then exercised:
///
/// 1. **Replace an existing vector**: SET `b` → `[1, 1, 1]`.
///    ANN(`[1,1,1]`) should return `[b_updated, a]`.
///
/// 2. **Add a vector to an item that had none**: SET `c` → `[-1, -1, -1]`.
///    VS should add `c` to the index (count rises to 3).
///    ANN(`[-1,-1,-1]`) should return `[c, a, b_updated]`:
///    - `c = [-1,-1,-1]` is identical to the query vector (similarity = 1).
///    - `a = [1,2,4]`  has cosine similarity ≈ −0.882 to `[-1,-1,-1]`.
///    - `b_updated = [1,1,1]` is antipodal (similarity = −1), so it is last.
#[framed]
async fn update_item_updates_index(actors: TestActors) {
    info!("started");

    for shape in super::NAME_PATTERNS {
        info!("Testing shape: {shape:?}");

        let vec_attr = shape.vec.expect("NAME_PATTERNS entries always have vec");

        let a = Item::key(shape.pk, shape.sk, "pk", "a").vec(vec_attr, [1.0, 2.0, 4.0]);
        let b = Item::key(shape.pk, shape.sk, "pk", "b").vec(vec_attr, [1.0, 4.0, 8.0]);
        // c has no vector yet — it is present in the table from the initial
        // scan but should not be indexed until we SET its vector below.
        let c_no_vec = Item::key(shape.pk, shape.sk, "pk", "c");

        // create_with_invalid_data counts only `items` (a and b); c_no_vec is
        // pre-inserted but has no vector so VS skips it (initial count = 2).
        let ctx = TableContext::create_with_invalid_data(
            &actors,
            shape,
            &[a.clone(), b.clone()],
            &[c_no_vec.clone()],
        )
        .await;

        // Step 1: replace b's vector.  VS must update the indexed vector.
        info!("Step 1: updating 'b' vector in '{}'", ctx.table_name);
        let b_vec = [1.0, 1.0, 1.0];
        let b_updated = Item::key(shape.pk, shape.sk, "pk", "b").vec(vec_attr, b_vec);
        update_item_expr(
            &ctx,
            &b_updated,
            "SET #vec = :val",
            Some(dynamo_float_list(b_vec)),
        )
        .await
        .expect("UpdateItem should succeed");

        // b_updated=[1,1,1] is closest to query [1,1,1]; a=[1,2,4] follows.
        // Count remains 2 (c still has no vector).
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[b_updated.clone(), a.clone()])
            .await;

        // Step 2: give c a vector via UpdateItem SET — VS should index it.
        info!("Step 2: adding vector to 'c' in '{}'", ctx.table_name);
        let c_vec = [-1.0, -1.0, -1.0];
        let c_with_vec = Item::key(shape.pk, shape.sk, "pk", "c").vec(vec_attr, c_vec);
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

/// Tests that `UpdateItem` with an invalid vector (wrong type or REMOVE) is
/// handled correctly by VS: wrong-type updates are rejected by Scylla, and
/// REMOVE of the vector column de-indexes the item.
///
/// Uses only [`NAME_PATTERNS`](super::NAME_PATTERNS)[0] (plain names, HASH-only).
///
/// Starting state: two items with valid vectors — `a=[1,2,4]` and `b=[1,1,1]`
/// — so the index begins with `count=2`.
///
/// Two phases are tested:
///
/// 1. REMOVE the vector from `b` via `UpdateItem`.  VS should de-index `b`,
///    leaving count=1 (only `a`).
/// 2. Attempt to SET the vector on `a` to each of the following invalid values
///    — Scylla rejects all of them with `ValidationException`, count stays at 1:
///    - a DynamoDB `String` (`S`)
///    - a `List` with two valid `N` elements and one `S` element
///    - a `List` with two valid `N` elements and one `NULL` element
///    - a `List` of floats with too few elements (2 instead of 3)
///    - a `List` of floats with too many elements (4 instead of 3)
#[framed]
async fn update_item_with_invalid_vector_is_not_indexed(actors: TestActors) {
    info!("started");

    let shape = &super::NAME_PATTERNS[0]; // plain names, HASH-only
    let vec_attr = shape.vec.expect("NAME_PATTERNS[0] always has vec");

    let a = Item::key(shape.pk, shape.sk, "pk", "a").vec(vec_attr, [1.0, 2.0, 4.0]);
    let b = Item::key(shape.pk, shape.sk, "pk", "b").vec(vec_attr, [1.0, 1.0, 1.0]);

    let ctx = TableContext::create_with_data(&actors, shape, &[a.clone(), b.clone()]).await;
    // Starting state: count=2 (a and b both have valid vectors).

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

    // Phase 1: REMOVE the vector from b — VS should de-index b.  Count drops
    // to 1 (only a remains).
    update_item_expr(&ctx, &b, "REMOVE #vec", None)
        .await
        .expect("UpdateItem should succeed");
    ctx.wait_for_count(1).await;
    ctx.wait_for_ann([1.0, 1.0, 1.0], &[a.clone()]).await;

    // Phase 2: attempt to SET the vector on a to various invalid values —
    // Scylla rejects all of them at write time with ValidationException so
    // the writes never reach VS.  No count check needed here.

    // String value.
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

    // List with two N elements and one S element — last element wrong type.
    super::assert_service_error(
        update_item_expr(&ctx, &a, "SET #vec = :val", Some(vec_with_string_elem)).await,
        "ValidationException",
    );

    // List with two N elements and one NULL element.
    super::assert_service_error(
        update_item_expr(&ctx, &a, "SET #vec = :val", Some(vec_with_null_elem)).await,
        "ValidationException",
    );

    // List of floats, too short (2 elements instead of 3).
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

    // List of floats, too long (4 elements instead of 3).
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

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case
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
