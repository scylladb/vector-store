/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_sdk_dynamodb::types::AttributeValue;
use e2etest::TestCase;
use tracing::info;

use super::Item;
use super::TableContext;
use super::dynamo_float_list;

/// Inserts items via `PutItem` and verifies the VS index is updated.
///
/// Loops [`NAME_PATTERNS`](super::NAME_PATTERNS) so that every combination
/// of key schema (HASH-only / HASH+RANGE) and naming style (plain /
/// special) is covered.
#[framed]
async fn put_item_updates_index(actors: TestActors) {
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

        for item in [&a, &b, &c] {
            ctx.put(item).await;
        }
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[a, b.clone(), c.clone()])
            .await;

        // Replace item a with a vector pointing in the opposite direction —
        // a_replaced=[-1,-1,-1] has cosine=-1.0 (antipodal to [1,1,1]) so it falls
        // to last position.  b=[1,2,4] and c=[1,4,8] retain their order.
        let a_replaced =
            Item::key(&ctx.pk, ctx.sk.as_deref(), "pk", "a").vec(vec_attr, [-1.0, -1.0, -1.0]);
        ctx.put(&a_replaced).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[b, c, a_replaced]).await;

        // Step 3: conditional PutItem (LWT path).
        //
        // Under `only_rmw_uses_lwt`, a PutItem with a ConditionExpression is
        // treated as RMW and goes through the LWT/Paxos path.  This exercises
        // the same `learn_decision` CDC path that the `always_use_lwt` test
        // covers, but inside the normal alternator suite.
        //
        // Write a fresh item `d` with `attribute_not_exists(#pk)` — the
        // condition passes (d does not exist) → VS must index it (count = 4).
        info!(
            "Step 3: conditional PutItem (passing condition) in '{}'",
            ctx.table_name
        );
        let d = Item::key(&ctx.pk, ctx.sk.as_deref(), "pk", "d").vec(vec_attr, [1.0, 1.0, 1.0]);
        let mut req = ctx
            .client
            .put_item()
            .table_name(&ctx.table_name)
            .condition_expression("attribute_not_exists(#pk)")
            .expression_attribute_names("#pk", ctx.pk.as_str());
        for (attr_name, attr_val) in &d.0 {
            req = req.item(attr_name, attr_val.clone());
        }
        req.send()
            .await
            .expect("conditional PutItem with passing condition should succeed");
        ctx.wait_for_count(4).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

/// Inserts items with an invalid vector attribute and verifies VS does not
/// index them.
///
/// The following invalid cases are tested:
/// - an item with **no** vector attribute at all — Scylla accepts this; VS
///   should skip it during CDC processing.
/// - vector attribute is a DynamoDB `String` (`S`) — rejected by Scylla.
/// - vector attribute is a `List` with two valid `N` elements and one `S`
///   element (numeric-looking) — rejected by Scylla; the last element has the
///   wrong DynamoDB type.
/// - vector attribute is a `List` with two valid `N` elements and one `NULL`
///   element — rejected by Scylla.
/// - vector attribute is a `List` of floats with too few elements (2 instead
///   of 3) — rejected by Scylla.
/// - vector attribute is a `List` of floats with too many elements (4 instead
///   of 3) — rejected by Scylla.
///
/// Only the valid item should appear in the index (`count == 1`).
#[framed]
async fn put_item_with_invalid_vector_is_not_indexed(actors: TestActors) {
    info!("started");

    let shape = &super::NAME_PATTERNS[0]; // plain names, HASH-only
    let pk = shape.pk;
    let vec_attr = shape.vec.expect("NAME_PATTERNS[0] always has vec");

    let ctx = TableContext::create(&actors, shape).await;

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

    // Item without the vector attribute — Scylla accepts this; VS should
    // not index it.
    let no_vec = Item::key(pk, None, "pk", "no-vec");
    ctx.put(&no_vec).await;

    // Vector attribute is a String — Scylla rejects at write time.
    let wrong_type_string = Item::key(pk, None, "pk", "wrong-type-string")
        .attr(vec_attr, AttributeValue::S("not-a-vector".into()));
    ctx.put_expecting_error(&wrong_type_string, "ValidationException")
        .await;

    // Vector attribute is a List with two valid N elements and one S element
    // (numeric-looking) — the last element has the wrong type.  Scylla rejects
    // at write time; element type must be N, not S.
    let wrong_type_mostly_float_one_s = Item::key(pk, None, "pk", "wrong-type-mostly-float-one-s")
        .attr(vec_attr, vec_with_string_elem);
    ctx.put_expecting_error(&wrong_type_mostly_float_one_s, "ValidationException")
        .await;

    // Vector attribute is a List with two valid N elements and one NULL element
    // — Scylla rejects at write time.
    let wrong_type_mostly_float_one_null =
        Item::key(pk, None, "pk", "wrong-type-mostly-float-one-null")
            .attr(vec_attr, vec_with_null_elem);
    ctx.put_expecting_error(&wrong_type_mostly_float_one_null, "ValidationException")
        .await;

    // Vector attribute is a List of floats with too few elements — Scylla
    // rejects at write time.
    let wrong_type_too_short = Item::key(pk, None, "pk", "wrong-type-too-short")
        .attr(vec_attr, dynamo_float_list([1.0_f32, 1.0]));
    ctx.put_expecting_error(&wrong_type_too_short, "ValidationException")
        .await;

    // Vector attribute is a List of floats with too many elements — Scylla
    // rejects at write time.
    let wrong_type_too_long = Item::key(pk, None, "pk", "wrong-type-too-long")
        .attr(vec_attr, dynamo_float_list([1.0_f32, 1.0, 1.0, 1.0]));
    ctx.put_expecting_error(&wrong_type_too_long, "ValidationException")
        .await;

    // valid is put last so that wait_for_ann acts as a sequencing barrier:
    // when VS has indexed valid it must have already processed the no_vec CDC
    // event before it (CDC events are ordered), proving that the item without
    // a vector attribute was correctly ignored.
    let valid = Item::key(pk, None, "pk", "valid").vec(vec_attr, [1.0, 1.0, 1.0]);
    ctx.put(&valid).await;
    ctx.wait_for_ann([1.0, 1.0, 1.0], &[valid]).await;

    // Replace the valid item with a version that has no vector attribute —
    // Scylla accepts this and removes the vector column from the row, so VS should
    // de-index it.  Count must drop to zero.
    let valid_no_vec = Item::key(pk, None, "pk", "valid");
    ctx.put(&valid_no_vec).await;
    ctx.wait_for_count(0).await;

    // Attempt to put the same key again with a String vector — Scylla
    // rejects this at write time with ValidationException.  Count stays at zero.
    let wrong_type2 =
        Item::key(pk, None, "pk", "valid").attr(vec_attr, AttributeValue::S("bad".into()));
    ctx.put_expecting_error(&wrong_type2, "ValidationException")
        .await;
    ctx.wait_for_count(0).await;

    ctx.done().await;
    info!("finished");
}

pub(super) fn register(test_case: TestCase<TestActors>) -> TestCase<TestActors> {
    test_case
        .with_test(
            "put_item_updates_index",
            common::DEFAULT_TEST_TIMEOUT,
            put_item_updates_index,
        )
        .with_test(
            "put_item_with_invalid_vector_is_not_indexed",
            common::DEFAULT_TEST_TIMEOUT,
            put_item_with_invalid_vector_is_not_indexed,
        )
}
