/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::ScalarAttributeType;
use e2etest::TestCase;
use std::collections::HashSet;
use tracing::info;

use super::Item;
use super::TableContext;
use super::TableShape;
use super::query::QueryBuilderExt;

/// Shared logic for all key-type tests: creates a table with 2 initial items,
/// adds a 3rd via PutItem, then queries with a keys-only projection and asserts
/// that only the pk attribute is returned.
async fn query_with_key_type(
    actors: &TestActors,
    shape: &TableShape,
    initial: &[Item],
    extra: Item,
) {
    let vec_attr = shape.vec.unwrap();
    let ctx = TableContext::create_with_data(actors, shape, initial).await;

    ctx.put(&extra).await;
    ctx.wait_for_count(3).await;

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
    let expected_keys: HashSet<&str> = [shape.pk].into();
    for item in &items {
        let got_keys: HashSet<&str> = item.keys().map(String::as_str).collect();
        assert_eq!(
            got_keys, expected_keys,
            "projected item should contain only pk '{}'",
            shape.pk
        );
        assert!(
            !item.contains_key(vec_attr),
            "projected item should NOT contain vector '{vec_attr}'"
        );
    }

    ctx.done().await;
}

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
    let v = shape.vec.unwrap();
    let initial = [
        Item::new(shape.pk, AttributeValue::S("str-a".into())).vec(v, [1.0, 1.0, 1.0]),
        Item::new(shape.pk, AttributeValue::S("str-b".into())).vec(v, [1.0, 2.0, 4.0]),
    ];
    let extra = Item::new(shape.pk, AttributeValue::S("str-c".into())).vec(v, [1.0, 4.0, 8.0]);
    query_with_key_type(&actors, &shape, &initial, extra).await;
    info!("finished");
}

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
    let v = shape.vec.unwrap();
    let initial = [
        Item::new(shape.pk, AttributeValue::N("1".into())).vec(v, [1.0, 1.0, 1.0]),
        Item::new(shape.pk, AttributeValue::N("2".into())).vec(v, [1.0, 2.0, 4.0]),
    ];
    let extra = Item::new(shape.pk, AttributeValue::N("3".into())).vec(v, [1.0, 4.0, 8.0]);
    query_with_key_type(&actors, &shape, &initial, extra).await;
    info!("finished");
}

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
    let v = shape.vec.unwrap();
    let initial = [
        Item::new(shape.pk, AttributeValue::B(Blob::new(vec![0x01u8]))).vec(v, [1.0, 1.0, 1.0]),
        Item::new(shape.pk, AttributeValue::B(Blob::new(vec![0x02u8]))).vec(v, [1.0, 2.0, 4.0]),
    ];
    let extra =
        Item::new(shape.pk, AttributeValue::B(Blob::new(vec![0x03u8]))).vec(v, [1.0, 4.0, 8.0]);
    query_with_key_type(&actors, &shape, &initial, extra).await;
    info!("finished");
}

pub(super) fn register(test_case: TestCase<TestActors>) -> TestCase<TestActors> {
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
