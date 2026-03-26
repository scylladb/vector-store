/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_sdk_dynamodb::types::AttributeValue;
use e2etest::TestCase;
use serde_json::Value;
use tracing::info;

use crate::alternator;
use crate::alternator::Item;
use crate::alternator::TableContext;
use crate::alternator::TableShape;

fn vector_index_create_update(index_name: &str, vec_attr: &str) -> Value {
    serde_json::json!([
        {
            "Create": {
                "IndexName": index_name,
                "VectorAttribute": {
                    "AttributeName": vec_attr,
                    "Dimensions": Item::VEC_DIMS
                }
            }
        }
    ])
}

#[framed]
async fn create_vector_index_via_update_table(actors: TestActors) {
    info!("started");

    for shape in &alternator::name_patterns() {
        let no_vec_shape = TableShape {
            vec_name: None,
            ..shape.clone()
        };
        let vec_attr = shape.vec().unwrap_or("vec");
        info!("Testing shape: {shape:?}");

        let ctx = TableContext::create(&actors, &no_vec_shape).await;

        info!("Confirming no index exists yet for '{}'", ctx.table_name);
        alternator::wait_for_no_index(&ctx.vs_clients, &ctx.index).await;

        info!(
            "Issuing UpdateTable for '{}' to add vector index '{}'",
            ctx.table_name, ctx.index.index
        );
        alternator::update_table_vector_indexes(
            &ctx.client,
            &ctx.table_name,
            vector_index_create_update(ctx.index.index.as_ref(), vec_attr),
        )
        .await;

        info!(
            "Waiting for Vector Store to serve index '{}/{}'",
            ctx.index.keyspace, ctx.index.index
        );
        alternator::wait_for_index(&ctx.vs_clients, &ctx.index).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

/// Like above but with pre-existing data in the table before adding the index.
#[framed]
async fn create_vector_index_via_update_table_with_preexisting_data(actors: TestActors) {
    info!("started");

    for shape in &alternator::name_patterns() {
        let vec_attr = shape.vec().unwrap_or("vec");
        info!("Testing shape: {shape:?}");

        let no_vec_shape = TableShape {
            vec_name: None,
            ..shape.clone()
        };

        let items = [
            Item::key(shape.pk(), shape.sk(), "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]),
            Item::key(shape.pk(), shape.sk(), "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]),
            Item::key(shape.pk(), shape.sk(), "pk", "c").vec(vec_attr, [1.0, 4.0, 8.0]),
        ];

        let ctx = TableContext::create_with_data(&actors, &no_vec_shape, &items).await;

        info!("Confirming no index exists yet for '{}'", ctx.table_name);
        alternator::wait_for_no_index(&ctx.vs_clients, &ctx.index).await;

        info!(
            "Issuing UpdateTable for '{}' to add vector index '{}'",
            ctx.table_name, ctx.index.index
        );
        alternator::update_table_vector_indexes(
            &ctx.client,
            &ctx.table_name,
            vector_index_create_update(ctx.index.index.as_ref(), vec_attr),
        )
        .await;

        info!(
            "Waiting for Vector Store to serve index '{}/{}'",
            ctx.index.keyspace, ctx.index.index
        );
        alternator::wait_for_index_count(&ctx.vs_clients, &ctx.index, items.len()).await;

        ctx.wait_for_ann([1.0, 1.0, 1.0], &items).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

/// Verifies that the initial-scan path skips non-indexable rows.
#[framed]
async fn create_vector_index_via_update_table_with_invalid_data(actors: TestActors) {
    info!("started");

    for shape in &alternator::name_patterns() {
        let vec_attr = shape.vec().unwrap_or("vec");
        info!("Testing shape: {shape:?}");

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
            Item::key(shape.pk(), shape.sk(), "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]),
            Item::key(shape.pk(), shape.sk(), "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]),
            Item::key(shape.pk(), shape.sk(), "pk", "c").vec(vec_attr, [1.0, 4.0, 8.0]),
        ];
        let invalid_items = [
            Item::key(shape.pk(), shape.sk(), "pk", "no-vec"),
            Item::key(shape.pk(), shape.sk(), "pk", "wrong-type-string")
                .attr(vec_attr, AttributeValue::S("not-a-vector".into())),
            Item::key(
                shape.pk(),
                shape.sk(),
                "pk",
                "wrong-type-mostly-float-one-s",
            )
            .attr(vec_attr, vec_with_string_elem),
            Item::key(
                shape.pk(),
                shape.sk(),
                "pk",
                "wrong-type-mostly-float-one-null",
            )
            .attr(vec_attr, vec_with_null_elem),
            Item::key(shape.pk(), shape.sk(), "pk", "wrong-type-too-short")
                .attr(vec_attr, alternator::float_list([1.0_f32, 1.0])),
            Item::key(shape.pk(), shape.sk(), "pk", "wrong-type-too-long")
                .attr(vec_attr, alternator::float_list([1.0_f32, 1.0, 1.0, 1.0])),
        ];

        let ctx =
            TableContext::create_with_invalid_data(&actors, shape, &valid_items, &invalid_items)
                .await;

        ctx.wait_for_ann([1.0, 1.0, 1.0], &valid_items).await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

/// Deletes a vector index via UpdateTable and verifies writes succeed after.
#[framed]
async fn delete_vector_index_via_update_table(actors: TestActors) {
    info!("started");

    for shape in &alternator::name_patterns() {
        info!("Testing shape: {shape:?}");

        let vec_attr = shape.vec().unwrap();
        let items = [
            Item::key(shape.pk(), shape.sk(), "pk", "a").vec(vec_attr, [1.0, 1.0, 1.0]),
            Item::key(shape.pk(), shape.sk(), "pk", "b").vec(vec_attr, [1.0, 2.0, 4.0]),
        ];
        let ctx = TableContext::create_with_data(&actors, shape, &items).await;

        info!(
            "Issuing UpdateTable for '{}' to delete vector index '{}'",
            ctx.table_name, ctx.index.index
        );
        alternator::update_table_vector_indexes(
            &ctx.client,
            &ctx.table_name,
            serde_json::json!([{
                "Delete": {
                    "IndexName": ctx.index.index.as_ref()
                }
            }]),
        )
        .await;

        info!(
            "Waiting for Vector Store to drop index '{}/{}'",
            ctx.index.keyspace, ctx.index.index
        );
        alternator::wait_for_no_index(&ctx.vs_clients, &ctx.index).await;

        info!("Confirming arbitrary writes are accepted after index deletion");
        ctx.put(
            &Item::key(shape.pk(), shape.sk(), "pk", "c")
                .attr(vec_attr, AttributeValue::S("not-a-vector".into())),
        )
        .await;

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

pub(super) async fn new() -> TestCase<TestActors> {
    TestCase::empty()
        .with_init(common::DEFAULT_TEST_TIMEOUT, alternator::init)
        .with_cleanup(common::DEFAULT_TEST_TIMEOUT, common::cleanup)
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
            "create_vector_index_via_update_table_with_invalid_data",
            common::DEFAULT_TEST_TIMEOUT,
            create_vector_index_via_update_table_with_invalid_data,
        )
        .with_test(
            "delete_vector_index_via_update_table",
            common::DEFAULT_TEST_TIMEOUT,
            delete_vector_index_via_update_table,
        )
}
