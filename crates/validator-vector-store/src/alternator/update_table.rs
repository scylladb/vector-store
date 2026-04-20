/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use aws_sdk_dynamodb::types::AttributeValue;
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
/// In addition to the valid rows, two rows with non-indexable vector values
/// are pre-filled before the index is created:
/// - one row **without** the vector attribute at all
/// - one row whose vector attribute is a DynamoDB `String` (`S`) instead of
///   a `List` of numbers
/// - one row whose vector attribute is a `List` with two valid `N` elements
///   and one `S` element (numeric-looking) — almost valid but wrong element type
/// - one row whose vector attribute is a `List` with two valid `N` elements
///   and one `NULL` element
///
/// The full-scan should skip those rows so the final count equals only the
/// number of valid items.
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

        // Build the full dataset: valid items (will be indexed) plus
        // non-indexable rows (no vector, wrong type) that the full-scan
        // should skip.  No index exists yet so Alternator accepts any
        // vector type at write time; all puts succeed.
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
        let valid_items = [
            Item::key(shape.pk, shape.sk, "pk", "a")
                .attr(vec_attr, dynamo_float_list([1.0, 1.0, 1.0])),
            Item::key(shape.pk, shape.sk, "pk", "b")
                .attr(vec_attr, dynamo_float_list([1.0, 2.0, 4.0])),
            Item::key(shape.pk, shape.sk, "pk", "c")
                .attr(vec_attr, dynamo_float_list([1.0, 4.0, 8.0])),
        ];
        let mut all_items = valid_items.to_vec();
        all_items.extend([
            // No vector attribute — full-scan should skip this row.
            Item::key(shape.pk, shape.sk, "pk", "no-vec"),
            // Vector attribute is a String, not a List — full-scan should skip.
            Item::key(shape.pk, shape.sk, "pk", "wrong-type-string")
                .attr(vec_attr, AttributeValue::S("not-a-vector".into())),
            // Vector attribute is a List with two valid N elements and one S
            // element (numeric-looking) — full-scan should skip this row.
            Item::key(shape.pk, shape.sk, "pk", "wrong-type-mostly-float-one-s")
                .attr(vec_attr, vec_with_string_elem),
            // Vector attribute is a List with two valid N elements and one NULL
            // element — full-scan should skip this row.
            Item::key(shape.pk, shape.sk, "pk", "wrong-type-mostly-float-one-null")
                .attr(vec_attr, vec_with_null_elem),
            // Vector attribute is a List of floats but with too few elements
            // (2 instead of 3) — full-scan should skip.
            Item::key(shape.pk, shape.sk, "pk", "wrong-type-too-short")
                .attr(vec_attr, dynamo_float_list([1.0_f32, 1.0])),
            // Vector attribute is a List of floats but with too many elements
            // (4 instead of 3) — full-scan should skip.
            Item::key(shape.pk, shape.sk, "pk", "wrong-type-too-long")
                .attr(vec_attr, dynamo_float_list([1.0_f32, 1.0, 1.0, 1.0])),
        ]);

        let ctx = TableContext::create_with_data(&actors, &no_vec_shape, &all_items).await;

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
        wait_for_index_count(&ctx.vs_client, &ctx.index, valid_items.len()).await;

        ctx.wait_for_ann([1.0, 1.0, 1.0], &valid_items).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: delete a vector index via UpdateTable
// ---------------------------------------------------------------------------

/// Creates an Alternator table (with a vector index) for each shape in
/// [`NAME_PATTERNS`], inserts data and waits for VS to index it, then issues
/// `UpdateTable` with `VectorIndexUpdates[{Delete: ...}]` and confirms the
/// index disappears.  After deletion, verifies that Scylla accepts a `PutItem`
/// with a non-vector value for the previously-indexed attribute (which would
/// have been rejected as a `ValidationException` while the index was active).
/// Loops [`NAME_PATTERNS`](NAME_PATTERNS).
#[framed]
async fn delete_vector_index_via_update_table(actors: TestActors) {
    info!("started");

    for shape in NAME_PATTERNS {
        info!("Testing shape: {shape:?}");

        let vec_attr = shape.vec.unwrap();
        let items = [
            Item::key(shape.pk, shape.sk, "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]),
            Item::key(shape.pk, shape.sk, "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]),
        ];
        let ctx = TableContext::create_with_data(&actors, shape, &items).await;

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

        info!("Confirming arbitrary writes are accepted after index deletion");
        ctx.put(
            &Item::key(shape.pk, shape.sk, "pk", "c")
                .attr(vec_attr, AttributeValue::S("not-a-vector".into())),
        )
        .await;

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
