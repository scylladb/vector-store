/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::ScalarAttributeType;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;

use super::Item;
use super::TableContext;
use super::TableShape;
use super::query::QueryBuilderExt;

// ---------------------------------------------------------------------------
// Test: String partition key (baseline)
// ---------------------------------------------------------------------------

#[framed]
async fn query_with_string_key(actors: TestActors) {
    info!("started");

    let shape = TableShape {
        table_prefix: "",
        index_prefix: "",
        pk: "Pk-StrKey",
        sk: None,
        vec: Some("Vec-StrKey"),
        pk_type: ScalarAttributeType::S,
    };

    let initial = [
        Item::new(shape.pk, AttributeValue::S("str-a".into()))
            .vec(shape.vec.unwrap(), [1.0, 1.0, 1.0]),
        Item::new(shape.pk, AttributeValue::S("str-b".into()))
            .vec(shape.vec.unwrap(), [1.0, 2.0, 4.0]),
    ];

    let ctx = TableContext::create_with_data(&actors, &shape, &initial).await;

    let extra = Item::new(shape.pk, AttributeValue::S("str-c".into()))
        .vec(shape.vec.unwrap(), [1.0, 4.0, 8.0]);
    ctx.put(&extra).await;
    ctx.wait_for_count(3).await;

    info!("Querying with keys-only ProjectionExpression on String PK table");
    let items = ctx
        .client
        .query()
        .table_name(&ctx.table_name)
        .index_name(ctx.index.index.as_ref())
        .limit(3)
        .projection_expression("#pk")
        .expression_attribute_names("#pk", shape.pk)
        .vector_search([1.0, 1.0, 1.0])
        .send()
        .await
        .expect("Query with VectorSearch should succeed")
        .items()
        .to_vec();

    assert!(
        !items.is_empty(),
        "keys-only query should return at least one item"
    );
    for item in &items {
        assert!(
            item.contains_key(shape.pk),
            "projected item should contain pk '{}', got: {:?}",
            shape.pk,
            item.keys().collect::<Vec<_>>()
        );
        assert!(
            !item.contains_key(shape.vec.unwrap()),
            "projected item should NOT contain vector '{}', got: {:?}",
            shape.vec.unwrap(),
            item.keys().collect::<Vec<_>>()
        );
    }

    ctx.done().await;
    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Number partition key
// ---------------------------------------------------------------------------

#[framed]
async fn query_with_number_key(actors: TestActors) {
    info!("started");

    let shape = TableShape {
        table_prefix: "",
        index_prefix: "",
        pk: "Pk-NumKey",
        sk: None,
        vec: Some("Vec-NumKey"),
        pk_type: ScalarAttributeType::N,
    };

    let initial = [
        Item::new(shape.pk, AttributeValue::N("1".into())).vec(shape.vec.unwrap(), [1.0, 1.0, 1.0]),
        Item::new(shape.pk, AttributeValue::N("2".into())).vec(shape.vec.unwrap(), [1.0, 2.0, 4.0]),
    ];

    let ctx = TableContext::create_with_data(&actors, &shape, &initial).await;

    let extra =
        Item::new(shape.pk, AttributeValue::N("3".into())).vec(shape.vec.unwrap(), [1.0, 4.0, 8.0]);
    ctx.put(&extra).await;
    ctx.wait_for_count(3).await;

    info!("Querying with keys-only ProjectionExpression on Number PK table");
    let items = ctx
        .client
        .query()
        .table_name(&ctx.table_name)
        .index_name(ctx.index.index.as_ref())
        .limit(3)
        .projection_expression("#pk")
        .expression_attribute_names("#pk", shape.pk)
        .vector_search([1.0, 1.0, 1.0])
        .send()
        .await
        .expect("Query with VectorSearch should succeed")
        .items()
        .to_vec();

    assert!(
        !items.is_empty(),
        "keys-only query should return at least one item"
    );
    for item in &items {
        assert!(
            item.contains_key(shape.pk),
            "projected item should contain pk '{}', got: {:?}",
            shape.pk,
            item.keys().collect::<Vec<_>>()
        );
        assert!(
            !item.contains_key(shape.vec.unwrap()),
            "projected item should NOT contain vector '{}', got: {:?}",
            shape.vec.unwrap(),
            item.keys().collect::<Vec<_>>()
        );
    }

    ctx.done().await;
    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Binary partition key
// ---------------------------------------------------------------------------

#[framed]
async fn query_with_binary_key(actors: TestActors) {
    info!("started");

    let shape = TableShape {
        table_prefix: "",
        index_prefix: "",
        pk: "Pk-BinKey",
        sk: None,
        vec: Some("Vec-BinKey"),
        pk_type: ScalarAttributeType::B,
    };

    let initial = [
        Item::new(shape.pk, AttributeValue::B(Blob::new(vec![0x01u8])))
            .vec(shape.vec.unwrap(), [1.0, 1.0, 1.0]),
        Item::new(shape.pk, AttributeValue::B(Blob::new(vec![0x02u8])))
            .vec(shape.vec.unwrap(), [1.0, 2.0, 4.0]),
    ];

    let ctx = TableContext::create_with_data(&actors, &shape, &initial).await;

    let extra = Item::new(shape.pk, AttributeValue::B(Blob::new(vec![0x03u8])))
        .vec(shape.vec.unwrap(), [1.0, 4.0, 8.0]);
    ctx.put(&extra).await;
    ctx.wait_for_count(3).await;

    info!("Querying with keys-only ProjectionExpression on Binary PK table");
    let items = ctx
        .client
        .query()
        .table_name(&ctx.table_name)
        .index_name(ctx.index.index.as_ref())
        .limit(3)
        .projection_expression("#pk")
        .expression_attribute_names("#pk", shape.pk)
        .vector_search([1.0, 1.0, 1.0])
        .send()
        .await
        .expect("Query with VectorSearch should succeed")
        .items()
        .to_vec();

    assert!(
        !items.is_empty(),
        "keys-only query should return at least one item"
    );
    for item in &items {
        assert!(
            item.contains_key(shape.pk),
            "projected item should contain pk '{}', got: {:?}",
            shape.pk,
            item.keys().collect::<Vec<_>>()
        );
        assert!(
            !item.contains_key(shape.vec.unwrap()),
            "projected item should NOT contain vector '{}', got: {:?}",
            shape.vec.unwrap(),
            item.keys().collect::<Vec<_>>()
        );
    }

    ctx.done().await;
    info!("finished");
}

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case
        .with_test(
            "query_with_string_key",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_string_key,
        )
        .with_test(
            "query_with_number_key",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_number_key,
        )
        .with_test(
            "query_with_binary_key",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_binary_key,
        )
}
