/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::alternator;
use crate::alternator::AlternatorContext;
use crate::alternator::Item;
use crate::alternator::TableContext;
use aws_sdk_dynamodb::operation::delete_item::builders::DeleteItemFluentBuilder;
use std::sync::Arc;
use tracing::info;

fn ctx_delete_item(ctx: &TableContext, item: &Item) -> DeleteItemFluentBuilder {
    let mut req = ctx.client.delete_item().table_name(&ctx.table_name);
    let key_attrs: Vec<&str> = std::iter::once(ctx.shape.pk())
        .chain(ctx.shape.sk())
        .collect();
    for attr_name in key_attrs {
        if let Some(attr_val) = item.0.get(attr_name) {
            req = req.key(attr_name, attr_val.clone());
        }
    }
    req
}

/// Inserts items, deletes one via `DeleteItem`, and verifies the VS index
/// reflects the deletion.
///
/// Loops [`alternator::name_patterns`] so that every combination
/// of key schema (HASH-only / HASH+RANGE) and naming style (plain /
/// special) is covered.
#[e2etest::test(group = delete_item)]
async fn delete_item_updates_index(ctx: Arc<AlternatorContext>) {
    info!("started");

    for shape in &alternator::name_patterns() {
        info!("Testing shape: {shape:?}");

        let vec_attr = shape.vec().expect("NAME_PATTERNS entries always have vec");

        let a = Item::key(shape.pk(), shape.sk(), "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]);
        let b = Item::key(shape.pk(), shape.sk(), "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]);
        let c = Item::key(shape.pk(), shape.sk(), "pk", "c").vec(vec_attr, [1.0, 4.0, 8.0]);

        let ctx =
            TableContext::create_with_data(ctx.actors(), shape, &[a.clone(), b.clone(), c.clone()])
                .await;

        info!("Step 1: deleting item from '{}'", ctx.table_name);
        ctx_delete_item(&ctx, &a)
            .send()
            .await
            .expect("DeleteItem should succeed");

        ctx.wait_for_count(2).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[b.clone(), c]).await;

        // Conditional DeleteItem exercises the LWT/Paxos path under
        // `only_rmw_uses_lwt` (ConditionExpression makes it RMW).
        info!(
            "Step 2: conditional DeleteItem (passing condition) in '{}'",
            ctx.table_name
        );
        ctx_delete_item(&ctx, &b)
            .expression_attribute_names("#pk", ctx.shape.pk())
            .condition_expression("attribute_exists(#pk)")
            .send()
            .await
            .expect("conditional DeleteItem with passing condition should succeed");

        ctx.wait_for_count(1).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

e2etest::group!(
    name = delete_item,
    fixtures = (AlternatorContext),
    parent = alternator::alternator
);
