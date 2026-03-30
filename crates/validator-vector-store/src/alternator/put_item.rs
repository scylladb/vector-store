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
        ctx.wait_for_ann([1.0, 1.0, 1.0], &[a.clone(), b, c]).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case.with_test(
        "put_item_updates_index",
        common::DEFAULT_TEST_TIMEOUT,
        put_item_updates_index,
    )
}
