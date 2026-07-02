/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::alternator;
use crate::alternator::Item;
use crate::alternator::TableContext;
use crate::common;
use aws_sdk_dynamodb::types::AttributeValue;
use std::sync::Arc;
use tracing::info;

/// Inserts items via `PutItem` and verifies the VS index is updated.
///
/// Loops [`alternator::name_patterns`] so that every combination
/// of key schema (HASH-only / HASH+RANGE) and naming style (plain /
/// special) is covered.
#[e2etest::test(group = put_item)]
async fn put_item_updates_index(actors: Arc<TestActors>) {
    info!("started");

    for shape in &alternator::name_patterns() {
        info!("Testing shape: {shape:?}");

        let ctx = TableContext::create(&actors, shape).await;

        let vec_attr = ctx.shape.vec().expect("TableContext has no vec_attr");

        let a = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]);
        let b = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]);
        let c = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "c").vec(vec_attr, [1.0, 4.0, 8.0]);

        info!("Step 1: inserting initial items into '{}'", ctx.table_name);
        for item in [&a, &b, &c] {
            ctx.put(item).send().await.expect("PutItem should succeed");
        }
        ctx.wait_for_count(3).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[a, b.clone(), c.clone()])
            .await;

        // Replace item 'a' with a vector pointing in the opposite direction -
        // a_replaced=[-1,-1,-1] has cosine=-1.0 (antipodal to [1,1,1]) so it falls
        // to last position.  b=[1,2,4] and c=[1,4,8] retain their order.
        info!("Step 2: replacing item in '{}'", ctx.table_name);
        let a_replaced =
            Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "a").vec(vec_attr, [-1.0, -1.0, -1.0]);
        ctx.put(&a_replaced)
            .send()
            .await
            .expect("PutItem should succeed");
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[b, c, a_replaced]).await;

        // Conditional PutItem exercises the LWT/Paxos path under
        // `only_rmw_uses_lwt` (ConditionExpression makes it RMW).
        info!(
            "Step 3: conditional PutItem (passing condition) in '{}'",
            ctx.table_name
        );
        let d = Item::key(ctx.shape.pk(), ctx.shape.sk(), "pk", "d").vec(vec_attr, [1.0, 1.0, 1.0]);
        ctx.put(&d)
            .condition_expression("attribute_not_exists(#pk)")
            .expression_attribute_names("#pk", ctx.shape.pk())
            .send()
            .await
            .expect("conditional PutItem with passing condition should succeed");
        ctx.wait_for_count(4).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

/// Inserts items with various invalid vector attributes and verifies VS does
/// not index them.  Only the valid item should appear in the index.
#[e2etest::test(group = put_item)]
async fn put_item_with_invalid_vector_is_not_indexed(actors: Arc<TestActors>) {
    info!("started");

    let shape = &alternator::name_patterns()[0]; // plain names, HASH-only
    let pk = shape.pk();
    let vec_attr = shape.vec().expect("NAME_PATTERNS[0] always has vec");

    let ctx = TableContext::create(&actors, shape).await;

    let no_vec = Item::key(pk, None, "pk", "no-vec");
    ctx.put(&no_vec)
        .send()
        .await
        .expect("PutItem should succeed");

    let try_replace_valid =
        Item::key(pk, None, "pk", "try-replace-valid").vec(vec_attr, [1.0, 1.0, 1.0]);
    ctx.put(&try_replace_valid)
        .send()
        .await
        .expect("PutItem should succeed");
    ctx.wait_for_ann([1.0, 1.0, 1.0], std::slice::from_ref(&try_replace_valid))
        .await;

    let invalid_items = [
        Item::key(pk, None, "pk", "null-vec").attr(vec_attr, AttributeValue::Null(true)),
        Item::key(pk, None, "pk", "wrong-type-string")
            .attr(vec_attr, AttributeValue::S("not-a-vector".into())),
        Item::key(pk, None, "pk", "wrong-type-mostly-float-one-s").attr(
            vec_attr,
            AttributeValue::L(vec![
                AttributeValue::N("1.0".into()),
                AttributeValue::N("2.0".into()),
                AttributeValue::S("3.0".into()),
            ]),
        ),
        Item::key(pk, None, "pk", "wrong-type-mostly-float-one-null").attr(
            vec_attr,
            AttributeValue::L(vec![
                AttributeValue::N("1.0".into()),
                AttributeValue::N("2.0".into()),
                AttributeValue::Null(true),
            ]),
        ),
        Item::key(pk, None, "pk", "wrong-type-too-short")
            .attr(vec_attr, alternator::float_list([1.0_f32, 1.0])),
        Item::key(pk, None, "pk", "wrong-type-too-long")
            .attr(vec_attr, alternator::float_list([1.0_f32, 1.0, 1.0, 1.0])),
        Item::key(pk, None, "pk", "try-replace-valid").attr(vec_attr, AttributeValue::Null(true)),
        Item::key(pk, None, "pk", "try-replace-valid")
            .attr(vec_attr, AttributeValue::S("bad".into())),
    ];

    for invalid_item in &invalid_items {
        info!(?invalid_item, "expecting invalid PutItem to fail");
        alternator::assert_service_error(ctx.put(invalid_item).send().await, "ValidationException");
    }
    ctx.wait_for_count(1).await;
    ctx.wait_for_ann([1.0, 1.0, 1.0], std::slice::from_ref(&try_replace_valid))
        .await;

    let try_replace_valid_no_vec = Item::key(pk, None, "pk", "try-replace-valid");
    ctx.put(&try_replace_valid_no_vec)
        .send()
        .await
        .expect("PutItem should succeed");
    ctx.wait_for_count(0).await;

    ctx.done().await;
    info!("finished");
}

e2etest::group!(
    name = put_item,
    fixtures = (Fixture),
    parent = alternator::alternator
);

struct Fixture {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Self {
        setup.setup::<TestActors>().await;
        let actors = setup.get::<TestActors>().await.unwrap();
        alternator::init(&actors).await;
        Self { actors }
    }

    async fn teardown(self) {
        common::cleanup(&self.actors).await;
    }
}
