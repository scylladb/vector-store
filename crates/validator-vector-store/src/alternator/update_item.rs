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
/// - `#pk`  — the partition-key attribute name (bound to `ctx.pk` only when
///   `condition_expr` is `Some`; Alternator rejects spurious bindings)
///
/// An optional `condition_expr` may be supplied; when `Some` it is set as the
/// `ConditionExpression` on the request, allowing the caller to exercise the
/// LWT/Paxos code path under `only_rmw_uses_lwt` write isolation.
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
    condition_expr: Option<&str>,
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
    // Only bind #pk when a condition expression actually references it,
    // otherwise Alternator rejects the request as having a spurious binding.
    if condition_expr.is_some() {
        req = req.expression_attribute_names("#pk", ctx.pk.as_str());
    }
    // Set key attributes (pk, and sk if present).
    for attr_name in std::iter::once(ctx.pk.as_str()).chain(ctx.sk.as_deref()) {
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
            None,
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

        // Step 3: conditional UpdateItem (LWT path).
        //
        // Under `only_rmw_uses_lwt`, UpdateItem with a ConditionExpression is
        // treated as RMW and goes through the LWT/Paxos path.  This exercises
        // the same `learn_decision` CDC path as the `always_use_lwt` test,
        // but inside the normal alternator suite.
        //
        // Update `a`'s vector to [2,2,2] with `attribute_exists(#pk)` —
        // the condition passes (a is present) → VS must re-index a with
        // the new vector.
        info!(
            "Step 3: conditional UpdateItem (passing condition) in '{}'",
            ctx.table_name
        );
        let a_new_vec = [4.0_f32, 2.0, 1.0];
        let a_updated = Item::key(shape.pk, shape.sk, "pk", "a").vec(vec_attr, a_new_vec);
        update_item_expr(
            &ctx,
            &a_updated,
            "SET #vec = :val",
            Some(dynamo_float_list(a_new_vec)),
            Some("attribute_exists(#pk)"),
        )
        .await
        .expect("conditional UpdateItem with passing condition should succeed");
        // a_updated=[4,2,1]: ANN([4,2,1]) →
        //   a_updated first (sim=1.0), b_updated=[1,1,1] second (sim≈0.882),
        //   c=[-1,-1,-1] last (sim≈-0.882).
        ctx.wait_for_ann([4.0, 2.0, 1.0], &[a_updated, b_updated, c_with_vec])
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
    update_item_expr(&ctx, &b, "REMOVE #vec", None, None)
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
            None,
        )
        .await,
        "ValidationException",
    );

    // List with two N elements and one S element — last element wrong type.
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

    // List with two N elements and one NULL element.
    super::assert_service_error(
        update_item_expr(&ctx, &a, "SET #vec = :val", Some(vec_with_null_elem), None).await,
        "ValidationException",
    );

    // List of floats, too short (2 elements instead of 3).
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

    // List of floats, too long (4 elements instead of 3).
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

/// Tests element-level `UpdateItem` operations on the vector column and
/// verifies that VS reindexes (or de-indexes) the item correctly.
///
/// Uses only [`NAME_PATTERNS`](super::NAME_PATTERNS)[0] (plain names, HASH-only).
///
/// Items are pre-inserted **before** the index is created (via
/// `create_with_data`) so Scylla accepts any vector type.  After the index is
/// created the initial state is:
///
/// | Item | Vector | Indexed? |
/// |---|---|---|
/// | `valid` | `[1.0, 2.0, 4.0]` | yes (count=1) |
/// | `mixed` | `L([N("1.0"), N("2.0"), S("3.0")])` | no |
/// | `null_elem` | `L([N("1.0"), N("2.0"), NULL])` | no |
/// | `too_short` | `[1.0, 2.0]` | no |
/// | `too_long` | `[1.0, 2.0, 4.0, 8.0]` | no |
///
/// 9 steps exercise `SET #vec[i] = :val`, `REMOVE #vec[i]`,
/// `SET #vec = list_append(#vec, :val)`, and `ADD #vec[i] :val`:
///
/// - Steps 1-5: operate on `valid_a`.
///   - Step 1: `SET #vec[0] = N("5.0")` → `[5.0, 2.0, 4.0]`. VS re-indexes;
///     ANN order confirms the new vector is in effect.
///   - Step 2: `SET #vec[0] = S("bad")` → rejected (wrong element type).
///   - Step 3: `SET #vec[2] = NULL` → rejected (wrong element type).
///   - Step 4: `REMOVE #vec[0]` → rejected (would leave 2 elements).
///   - Step 5: `list_append([1.0])` → rejected (would make 4 elements).
/// - Step 6: fix `mixed` by setting last element to valid N.
/// - Step 7: fix `too_short` via out-of-range `SET #vec[2]` (appends).
/// - Step 8: fix `too_long` via `REMOVE #vec[3]`.
/// - Step 9: `ADD #vec[0] N("1.0")` on `valid_b` — increments the first
///   component from 4.0 to 5.0, giving `[5.0, 2.0, 1.0]`.  `ADD` on a
///   sub-path is RMW → LWT/Paxos path under `only_rmw_uses_lwt`; VS
///   re-indexes `valid_b` with the new vector.
#[framed]
async fn update_item_vector_element_operations(actors: TestActors) {
    info!("started");

    let shape = &super::NAME_PATTERNS[0]; // plain names, HASH-only
    let vec_attr = shape.vec.expect("NAME_PATTERNS[0] always has vec");
    let pk = shape.pk;

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

    // `valid_a` and `valid_b` are well-formed 3-element float lists (count=2).
    // The other three have invalid vector types and are skipped by VS.
    let ctx = TableContext::create_with_invalid_data(
        &actors,
        shape,
        &[valid_a.clone(), valid_b.clone()],
        &[mixed.clone(), too_short.clone(), too_long.clone()],
    )
    .await;

    // -----------------------------------------------------------------------
    // Steps 1-5: operate on `valid_a`
    // -----------------------------------------------------------------------

    // Step 1: SET #vec[0] = N("5.0") → [5.0, 2.0, 4.0].
    // VS re-indexes valid_a with the new vector.
    // ANN([5,2,4]): valid_a=[5,2,4] is identical to query → first;
    // valid_b=[4,2,1] is second.  This confirms the re-index used the new vector.
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

    // Step 2: SET #vec[0] = S("bad") → rejected by Scylla (wrong element type).
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

    // Step 3: SET #vec[2] = NULL → rejected by Scylla (wrong element type).
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

    // Step 4: REMOVE #vec[0] would leave [2.0, 4.0] (2 elements — wrong
    // dimension) → rejected by Scylla.
    info!("Step 4: REMOVE #vec[0] on 'valid_a' (rejected — would leave 2 elements)");
    super::assert_service_error(
        update_item_expr(&ctx, &valid_a, "REMOVE #vec[0]", None, None).await,
        "ValidationException",
    );

    // Step 5: list_append would make [5.0, 2.0, 4.0, 1.0] (4 elements — wrong
    // dimension) → rejected by Scylla.
    info!("Step 5: list_append on 'valid_a' (rejected — would make 4 elements)");
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

    // -----------------------------------------------------------------------
    // Step 6: fix `mixed` (L([N,N,S]) → [1.0, 2.0, 3.0])
    // -----------------------------------------------------------------------

    // Step 6: SET #vec[2] = N("3.0") → [1.0, 2.0, 3.0] (all N).
    // VS indexes mixed; count rises to 3.
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

    // -----------------------------------------------------------------------
    // Step 7: fix `too_short` ([1.0, 2.0])
    // -----------------------------------------------------------------------

    // Step 7: SET #vec[2] = N("4.0") — index 2 is out-of-range for a 2-element
    // list so DynamoDB appends → [1.0, 2.0, 4.0] (3 elements).
    // VS indexes too_short; count rises to 4.
    info!("Step 7: SET #vec[2] (out-of-range append) on 'too_short'");
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

    // -----------------------------------------------------------------------
    // Step 8: fix `too_long` ([1.0, 2.0, 4.0, 8.0])
    // -----------------------------------------------------------------------

    // Step 8: REMOVE #vec[3] → [1.0, 2.0, 4.0] (3 elements).
    // VS indexes too_long; count rises to 5.
    info!("Step 8: REMOVE #vec[3] on 'too_long'");
    update_item_expr(&ctx, &too_long, "REMOVE #vec[3]", None, None)
        .await
        .expect("UpdateItem should succeed");
    ctx.wait_for_count(5).await;

    // -----------------------------------------------------------------------
    // Step 9: ADD #vec[0] on `valid_b` (LWT RMW path — ADD is always RMW)
    // -----------------------------------------------------------------------

    // Step 9: `ADD #vec[0] :val` increments the first component of `valid_b`
    // from 4.0 by 1.0, giving [5.0, 2.0, 1.0].  `ADD` is always RMW, so
    // under `only_rmw_uses_lwt` it goes through the LWT/Paxos path.
    // VS must re-index valid_b with the new vector; an ANN query for
    // [5.0, 2.0, 1.0] must return valid_b as the first result.
    info!("Step 9: ADD #vec[0] on 'valid_b' (LWT RMW path, vector changes)");
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
    // Count stays at 5; ANN([5,2,1]) returns valid_b_step9 first.
    ctx.wait_for_count(5).await;
    ctx.wait_for_ann([5.0, 2.0, 1.0], &[valid_b_step9]).await;

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
        .with_test(
            "update_item_vector_element_operations",
            common::DEFAULT_TEST_TIMEOUT,
            update_item_vector_element_operations,
        )
}
