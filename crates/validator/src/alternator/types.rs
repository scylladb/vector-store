/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::alternator;
use crate::alternator::AlternatorContext;
use crate::alternator::Item;
use crate::alternator::TableContext;
use crate::alternator::TableShape;
use crate::alternator::query::QueryBuilderExt;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::ScalarAttributeType;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Shared logic for all key-type tests: creates a table with initial items,
/// adds extra items via PutItem, then queries with both L-type and FLOAT32VECTOR
/// encodings and asserts that all expected pk values are returned with correct
/// projection.
async fn query_with_key_type(
    actors: &TestActors,
    shape: &TableShape,
    initial: &[Item],
    extra: &[Item],
) {
    let ctx = TableContext::create_with_data(actors, shape, initial).await;

    for item in extra {
        ctx.put(item).send().await.expect("PutItem should succeed");
    }
    ctx.wait_for_count(initial.len() + extra.len()).await;

    let expected_pks: Vec<&AttributeValue> = initial
        .iter()
        .chain(extra.iter())
        .map(|item| item.0.get(shape.pk()).expect("item has no pk"))
        .collect();

    let base_query = || {
        ctx.client
            .query()
            .table_name(&ctx.table_name)
            .index_name(ctx.index.index.as_ref())
            .limit((initial.len() + extra.len()) as i32)
            .projection_expression("#pk")
            .expression_attribute_names("#pk", shape.pk())
    };

    let assert_results = |items: &[HashMap<String, AttributeValue>], label: &str| {
        let mut expected: Vec<HashMap<String, AttributeValue>> = expected_pks
            .iter()
            .map(|pk| HashMap::from([(shape.pk().to_string(), (*pk).clone())]))
            .collect();

        let mut got = items.to_vec();
        got.sort_by_key(|m| format!("{:?}", m.get(shape.pk())));
        expected.sort_by_key(|m| format!("{:?}", m.get(shape.pk())));

        assert_eq!(got, expected, "{label} query returned unexpected results");
    };

    let items = base_query()
        .vector_search([1.0, 1.0, 1.0])
        .send()
        .await
        .expect("Query with `L-type vector` should succeed")
        .items()
        .to_vec();
    assert_results(&items, "L-type");

    let items = base_query()
        .vector_search_optimized([1.0, 1.0, 1.0])
        .send()
        .await
        .expect("Query with `FLOAT32VECTOR` should succeed")
        .items()
        .to_vec();
    assert_results(&items, "FLOAT32VECTOR");

    ctx.done().await;
}

#[e2etest::test(group = types)]
async fn query_with_string_key(ctx: Arc<AlternatorContext>) {
    info!("started");
    let shape = TableShape {
        table_prefix: None,
        index_prefix: None,
        pk_name: "Pk-StrKey".into(),
        sk_name: None,
        vec_name: Some("Vec-StrKey".into()),
        pk_type: ScalarAttributeType::S,
    };
    let v = shape.vec().unwrap();
    let initial = [
        Item::new(shape.pk(), AttributeValue::S("str-a".into())).vec(v, [1.0, 1.0, 1.0]),
        Item::new(shape.pk(), AttributeValue::S("str-b".into())).vec(v, [1.0, 2.0, 4.0]),
    ];
    let extra = [Item::new(shape.pk(), AttributeValue::S("str-c".into())).vec(v, [1.0, 4.0, 8.0])];
    query_with_key_type(ctx.actors(), &shape, &initial, &extra).await;
    info!("finished");
}

#[e2etest::test(group = types)]
async fn query_with_number_key(ctx: Arc<AlternatorContext>) {
    info!("started");
    let shape = TableShape {
        table_prefix: None,
        index_prefix: None,
        pk_name: "Pk-NumKey".into(),
        sk_name: None,
        vec_name: Some("Vec-NumKey".into()),
        pk_type: ScalarAttributeType::N,
    };
    let v = shape.vec().unwrap();
    let initial = [
        Item::new(shape.pk(), AttributeValue::N("1".into())).vec(v, [1.0, 1.0, 1.0]),
        Item::new(shape.pk(), AttributeValue::N("2".into())).vec(v, [1.0, 2.0, 4.0]),
    ];
    let extra = [Item::new(shape.pk(), AttributeValue::N("3".into())).vec(v, [1.0, 4.0, 8.0])];
    query_with_key_type(ctx.actors(), &shape, &initial, &extra).await;
    info!("finished");
}

#[e2etest::test(group = types)]
async fn query_with_binary_key(ctx: Arc<AlternatorContext>) {
    info!("started");
    let shape = TableShape {
        table_prefix: None,
        index_prefix: None,
        pk_name: "Pk-BinKey".into(),
        sk_name: None,
        vec_name: Some("Vec-BinKey".into()),
        pk_type: ScalarAttributeType::B,
    };
    let v = shape.vec().unwrap();
    let initial = [
        Item::new(shape.pk(), AttributeValue::B(Blob::new(vec![0x01u8]))).vec(v, [1.0, 1.0, 1.0]),
        Item::new(shape.pk(), AttributeValue::B(Blob::new(vec![0x02u8]))).vec(v, [1.0, 2.0, 4.0]),
    ];
    let extra = [
        Item::new(shape.pk(), AttributeValue::B(Blob::new(vec![0x03u8]))).vec(v, [1.0, 4.0, 8.0]),
    ];
    query_with_key_type(ctx.actors(), &shape, &initial, &extra).await;
    info!("finished");
}

/// Verifies queries work with both FLOAT32VECTOR-encoded items and query vectors.
#[e2etest::test(group = types)]
async fn query_with_optimized_vector_type(ctx: Arc<AlternatorContext>) {
    info!("started");

    let shape = TableShape {
        table_prefix: None,
        index_prefix: None,
        pk_name: "Pk-VecType".into(),
        sk_name: None,
        vec_name: Some("Vec-VecType".into()),
        pk_type: ScalarAttributeType::S,
    };

    let v = shape.vec().unwrap();
    let initial = [
        Item::new(shape.pk(), AttributeValue::S("pk-l-scan".into())).vec(v, [1.0, 1.0, 1.0]),
        Item::new(shape.pk(), AttributeValue::S("pk-v-scan".into()))
            .vec_optimized(v, [1.0, 1.0, 1.0]),
    ];
    let extra = [
        Item::new(shape.pk(), AttributeValue::S("pk-l-live".into())).vec(v, [1.0, 1.0, 1.0]),
        Item::new(shape.pk(), AttributeValue::S("pk-v-live".into()))
            .vec_optimized(v, [1.0, 1.0, 1.0]),
    ];
    query_with_key_type(ctx.actors(), &shape, &initial, &extra).await;

    info!("finished");
}

e2etest::group!(
    name = types,
    fixtures = (AlternatorContext),
    parent = alternator::alternator
);
