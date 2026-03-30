/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;

use super::Item;
use super::TableContext;

async fn update_item_vector(ctx: &TableContext, item: &Item) {
    let va = ctx
        .vec_attr
        .as_deref()
        .expect("TableContext has no vec_attr");
    let vec_av = item
        .0
        .get(va)
        .expect("Item has no vector for update_item_vector")
        .clone();
    let mut req = ctx.client.update_item().table_name(&ctx.table_name);
    // Set key attributes (pk, and sk if present).
    let key_attrs: Vec<&str> = std::iter::once(ctx.pk.as_str())
        .chain(ctx.sk.as_deref())
        .collect();
    for attr_name in key_attrs {
        if let Some(attr_val) = item.0.get(attr_name) {
            req = req.key(attr_name, attr_val.clone());
        }
    }
    req = req
        .update_expression("SET #vector = :vector")
        .expression_attribute_names("#vector", va)
        .expression_attribute_values(":vector", vec_av);
    req.send().await.expect("UpdateItem should succeed");
}

/// Updates a vector via `UpdateItem` and verifies the VS index reflects the
/// new vector.
///
/// Loops [`NAME_PATTERNS`](super::NAME_PATTERNS) so that every combination
/// of key schema (HASH-only / HASH+RANGE) and naming style (plain /
/// special) is covered.
#[framed]
async fn update_item_updates_index(actors: TestActors) {
    info!("started");

    for shape in super::NAME_PATTERNS {
        info!("Testing shape: {shape:?}");

        let vec_attr = shape.vec.expect("NAME_PATTERNS entries always have vec");

        let a = Item::key(shape.pk, shape.sk, "pk", "a").vec(vec_attr, [1.0, 2.0, 4.0]);
        let b = Item::key(shape.pk, shape.sk, "pk", "b").vec(vec_attr, [1.0, 4.0, 8.0]);

        let ctx = TableContext::create_with_data(&actors, shape, &[a.clone(), b.clone()]).await;

        info!("Updating item 'pk-b' vector in '{}'", ctx.table_name);
        let b_updated = Item::key(shape.pk, shape.sk, "pk", "b").vec(vec_attr, [1.0, 1.0, 1.0]);
        update_item_vector(&ctx, &b_updated).await;

        ctx.wait_for_count(2).await;
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[a, b_updated.clone()])
            .await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case.with_test(
        "update_item_updates_index",
        common::DEFAULT_TEST_TIMEOUT,
        update_item_updates_index,
    )
}
