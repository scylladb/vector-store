/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use e2etest::TestCase;
use tracing::info;

use super::Item;
use super::TableContext;

async fn delete_item(ctx: &TableContext, item: &Item) {
    let mut req = ctx.client.delete_item().table_name(&ctx.table_name);
    // Write every key attribute from the item into the DeleteItem key map.
    // Key attributes are: always pk, and sk when the table has a sort key.
    let key_attrs: Vec<&str> = std::iter::once(ctx.pk.as_str())
        .chain(ctx.sk.as_deref())
        .collect();
    for attr_name in key_attrs {
        if let Some(attr_val) = item.0.get(attr_name) {
            req = req.key(attr_name, attr_val.clone());
        }
    }
    req.send().await.expect("DeleteItem should succeed");
}

/// Inserts items, deletes one via `DeleteItem`, and verifies the VS index
/// reflects the deletion.
///
/// Loops [`NAME_PATTERNS`](super::NAME_PATTERNS) so that every combination
/// of key schema (HASH-only / HASH+RANGE) and naming style (plain /
/// special) is covered.
#[framed]
async fn delete_item_updates_index(actors: TestActors) {
    info!("started");

    for shape in super::NAME_PATTERNS {
        info!("Testing shape: {shape:?}");

        let vec_attr = shape.vec.expect("NAME_PATTERNS entries always have vec");

        let a = Item::key(shape.pk, shape.sk, "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]);
        let b = Item::key(shape.pk, shape.sk, "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]);
        let c = Item::key(shape.pk, shape.sk, "pk", "c").vec(vec_attr, [1.0, 4.0, 8.0]);

        let ctx =
            TableContext::create_with_data(&actors, shape, &[a.clone(), b.clone(), c.clone()])
                .await;

        info!("Deleting item from '{}'", ctx.table_name);
        delete_item(&ctx, &a).await;

        ctx.wait_for_count(2).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[b.clone(), c]).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

pub(super) fn register(test_case: TestCase<TestActors>) -> TestCase<TestActors> {
    test_case.with_test(
        "delete_item_updates_index",
        common::DEFAULT_TEST_TIMEOUT,
        delete_item_updates_index,
    )
}
