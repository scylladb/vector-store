/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use serde_json::Value;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;

use super::Item;
use super::NAME_PATTERNS;
use super::TableContext;
use super::TableShape;
use super::dynamo_float_list;
use super::issue_update_table;
use super::wait_for_index_count;
use super::wait_for_no_index;

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

/// Builds the `VectorIndexUpdates` JSON for creating a single vector index.
fn vector_index_create_update(index_name: &str, vec_attr: &str, dimensions: usize) -> Value {
    serde_json::json!([
        {
            "Create": {
                "IndexName": index_name,
                "VectorAttribute": {
                    "AttributeName": vec_attr,
                    "Dimensions": dimensions
                }
            }
        }
    ])
}

/// Builds the `VectorIndexUpdates` JSON for deleting a single vector index.
fn vector_index_delete_update(index_name: &str) -> Value {
    serde_json::json!([
        {
            "Delete": {
                "IndexName": index_name
            }
        }
    ])
}

// ---------------------------------------------------------------------------
// Test: create a vector index via UpdateTable (no preexisting data)
// ---------------------------------------------------------------------------

/// Creates a plain Alternator table (no vector index), then issues an
/// `UpdateTable` with `VectorIndexUpdates[{Create: ...}]` and waits for the
/// Vector Store to start serving the newly created index.
///
/// Loops [`NAME_PATTERNS`](NAME_PATTERNS) using
/// `TableShape { vec: None, ..*shape }` to create the table without an index,
/// then adds the index via UpdateTable using the shape's naming conventions.
#[framed]
async fn create_vector_index_via_update_table(actors: TestActors) {
    info!("started");

    for shape in NAME_PATTERNS {
        let no_vec_shape = TableShape {
            vec: None,
            pk_type: shape.pk_type.clone(),
            ..*shape
        };
        let vec_attr = shape.vec.unwrap_or("vec");
        info!("Testing shape: {shape:?}");

        let ctx = TableContext::create(&actors, &no_vec_shape).await;

        info!("Confirming no index exists yet for '{}'", ctx.table_name);
        wait_for_no_index(&ctx.vs_client, &ctx.index).await;

        info!(
            "Issuing UpdateTable for '{}' to add vector index '{}'",
            ctx.table_name, ctx.index.index
        );
        issue_update_table(
            &ctx.client,
            &ctx.table_name,
            vector_index_create_update(ctx.index.index.as_ref(), vec_attr, 3),
        )
        .await;

        info!(
            "Waiting for Vector Store to serve index '{}/{}'",
            ctx.index.keyspace, ctx.index.index
        );
        common::wait_for_index(&ctx.vs_client, &ctx.index).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: create a vector index via UpdateTable on preexisting data
// ---------------------------------------------------------------------------

/// Creates a plain Alternator table (no vector index), inserts data
/// (including the vector attribute), then adds the index with `UpdateTable`
/// and verifies that Vector Store indexed the already-present rows via the
/// initial full-scan path.
///
/// The vector values are stored as extra attributes on each item (via
/// [`Item::attr`] + [`dynamo_float_list`]) so that [`TableContext::create_with_data`]
/// can be called with `shape.vec = None` — this inserts the rows (with
/// vectors already present) without creating an index.  After confirming no
/// index exists yet, the test issues `UpdateTable` with
/// `VectorIndexUpdates[{Create: ...}]` and waits for VS to serve the index
/// with the correct item count.
///
/// Items are constructed with [`Item::key`] so the same dataset works for
/// both HASH-only and HASH+RANGE shapes.  Loops
/// [`NAME_PATTERNS`](NAME_PATTERNS).
#[framed]
async fn create_vector_index_via_update_table_with_preexisting_data(actors: TestActors) {
    info!("started");

    for shape in NAME_PATTERNS {
        let vec_attr = shape.vec.unwrap_or("vec");
        info!("Testing shape: {shape:?}");

        let no_vec_shape = TableShape {
            vec: None,
            pk_type: shape.pk_type.clone(),
            ..*shape
        };

        // Vectors are stored as extra attributes so create_with_data inserts
        // them even though no_vec_shape.vec is None (no index created yet).
        let dataset = vec![
            Item::key(shape.pk, shape.sk, "pk", "a")
                .attr(vec_attr, dynamo_float_list([1.0, 1.0, 1.0])),
            Item::key(shape.pk, shape.sk, "pk", "b")
                .attr(vec_attr, dynamo_float_list([1.0, 2.0, 4.0])),
            Item::key(shape.pk, shape.sk, "pk", "c")
                .attr(vec_attr, dynamo_float_list([1.0, 4.0, 8.0])),
        ];

        let ctx = TableContext::create_with_data(&actors, &no_vec_shape, &dataset).await;

        info!("Confirming no index exists yet for '{}'", ctx.table_name);
        wait_for_no_index(&ctx.vs_client, &ctx.index).await;

        info!(
            "Issuing UpdateTable for '{}' to add vector index '{}'",
            ctx.table_name, ctx.index.index
        );
        issue_update_table(
            &ctx.client,
            &ctx.table_name,
            vector_index_create_update(ctx.index.index.as_ref(), vec_attr, 3),
        )
        .await;

        info!(
            "Waiting for Vector Store to serve index '{}/{}'",
            ctx.index.keyspace, ctx.index.index
        );
        common::wait_for_index(&ctx.vs_client, &ctx.index).await;
        wait_for_index_count(&ctx.vs_client, &ctx.index, dataset.len()).await;

        ctx.wait_for_ann([1.0, 1.0, 1.0], &dataset).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: delete a vector index via UpdateTable
// ---------------------------------------------------------------------------

/// Creates an Alternator table (with a vector index) for each shape in
/// [`NAME_PATTERNS`], waits for the Vector Store to serve it, then issues
/// `UpdateTable` with `VectorIndexUpdates[{Delete: ...}]` and confirms the
/// index disappears.  Loops [`NAME_PATTERNS`](NAME_PATTERNS).
#[framed]
async fn delete_vector_index_via_update_table(actors: TestActors) {
    info!("started");

    for shape in NAME_PATTERNS {
        info!("Testing shape: {shape:?}");

        let ctx = TableContext::create(&actors, shape).await;

        info!(
            "Issuing UpdateTable for '{}' to delete vector index '{}'",
            ctx.table_name, ctx.index.index
        );
        issue_update_table(
            &ctx.client,
            &ctx.table_name,
            vector_index_delete_update(ctx.index.index.as_ref()),
        )
        .await;

        info!(
            "Waiting for Vector Store to drop index '{}/{}'",
            ctx.index.keyspace, ctx.index.index
        );
        wait_for_no_index(&ctx.vs_client, &ctx.index).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case
        .with_test(
            "create_vector_index_via_update_table",
            common::DEFAULT_TEST_TIMEOUT,
            create_vector_index_via_update_table,
        )
        .with_test(
            "create_vector_index_via_update_table_with_preexisting_data",
            common::DEFAULT_TEST_TIMEOUT,
            create_vector_index_via_update_table_with_preexisting_data,
        )
        .with_test(
            "delete_vector_index_via_update_table",
            common::DEFAULT_TEST_TIMEOUT,
            delete_vector_index_via_update_table,
        )
}
