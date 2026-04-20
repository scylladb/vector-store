/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_sdk_dynamodb::client::customize::CustomizableOperation;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::query::QueryOutput;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::ScalarAttributeType;
use aws_sdk_dynamodb::types::Select;
use e2etest::TestCase;
use httpapi::ColumnName;
use httpapi::IndexName;
use httpapi::Limit;
use httpapi::Vector;
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use tracing::info;

use super::Item;
use super::JsonBodyInjectInterceptor;
use super::TableContext;
use super::alternator_keyspace;
use super::create_alternator_table;
use super::delete_alternator_table;
use super::dynamo_float_list;
use super::issue_update_table;
use super::make_clients;
use super::unique_alternator_index_name;
use super::unique_alternator_table_name;
use super::wait_for_index_count;

/// Extracts a string attribute value from an item map, panicking with a clear
/// message if the key is absent or the value is not `AttributeValue::S`.
fn extract_s(item: &HashMap<String, AttributeValue>, attr: &str) -> String {
    match item.get(attr) {
        Some(AttributeValue::S(value)) => value.clone(),
        other => panic!("expected string attribute '{attr}', got {other:?}"),
    }
}

async fn put_item_with_optimized_vector(
    client: &aws_sdk_dynamodb::Client,
    table_name: &str,
    pk_attr: &str,
    pk_value: &str,
    vec_attr: &str,
    vector: [f32; 3],
) {
    let item_json = serde_json::json!({
        pk_attr: { "S": pk_value },
        vec_attr: { "V": vector },
    });

    client
        .put_item()
        .table_name(table_name)
        .item(pk_attr, AttributeValue::S(pk_value.to_string()))
        .customize()
        .interceptor(JsonBodyInjectInterceptor::new([("Item", item_json)]))
        .send()
        .await
        .expect("PutItem with optimized vector should succeed");
}
/// Extension trait that adds Alternator `VectorSearch` to any [`QueryFluentBuilder`].
///
/// Call `.vector_search(vector)` as the **last** builder step before `.send()`:
///
/// ```ignore
/// // Simple — just the items:
/// let items = ctx.client.query()
///     .table_name(&ctx.table_name)
///     .index_name(ctx.index.index.as_ref())
///     .limit(10)
///     .vector_search([1.0, 1.0, 1.0])
///     .send().await.expect("Query with VectorSearch should succeed")
///     .items().to_vec();
///
/// // Full output (e.g. for Select::Count):
/// let resp = ctx.client.query()
///     .table_name(&ctx.table_name)
///     .index_name(ctx.index.index.as_ref())
///     .limit(5)
///     .select(Select::Count)
///     .vector_search([1.0_f32, 1.0, 1.0])
///     .send().await.expect("Query with Select::Count should succeed");
/// ```
pub(super) trait QueryBuilderExt {
    fn vector_search(
        self,
        vector: impl IntoIterator<Item = f32>,
    ) -> CustomizableOperation<QueryOutput, QueryError, QueryFluentBuilder>;
}

impl QueryBuilderExt for QueryFluentBuilder {
    fn vector_search(
        self,
        vector: impl IntoIterator<Item = f32>,
    ) -> CustomizableOperation<QueryOutput, QueryError, QueryFluentBuilder> {
        let json = serde_json::json!({
            "QueryVector": {
                "L": vector
                    .into_iter()
                    .map(|v| serde_json::json!({ "N": v.to_string() }))
                    .collect::<Vec<_>>()
            }
        });
        self.customize()
            .interceptor(JsonBodyInjectInterceptor::new([("VectorSearch", json)]))
    }
}

// ---------------------------------------------------------------------------
// Test: Query with VectorSearch works and respects Limit
// ---------------------------------------------------------------------------

/// Creates Alternator tables with a vector index (both HASH-only and
/// HASH+RANGE schemas), inserts items, waits for the index to be SERVING, then
/// issues a `Query` extended with the Alternator `VectorSearch` attribute (no
/// `KeyConditionExpression`).  Asserts that results are returned, the limit is
/// respected, and the closest item is ranked first.
#[framed]
async fn query_with_vector_search(actors: TestActors) {
    info!("started");

    let shapes = [
        super::TableShape {
            table_prefix: "",
            index_prefix: "",
            pk: "Pk-QVS",
            sk: None,
            vec: Some("Vec-QVS"),
            pk_type: super::ScalarAttributeType::S,
        },
        super::TableShape {
            table_prefix: "",
            index_prefix: "",
            pk: "Pk-QVS",
            sk: Some("Sk-QVS"),
            vec: Some("Vec-QVS"),
            pk_type: super::ScalarAttributeType::S,
        },
    ];

    for shape in &shapes {
        info!("Testing shape: {shape:?}");

        let dataset = [
            Item::new(shape.pk, AttributeValue::S("pk-a".into()))
                .maybe_sk(shape.sk, AttributeValue::S("sk-1".into()))
                .vec(shape.vec.unwrap(), [1.0, 1.0, 1.0]),
            Item::new(shape.pk, AttributeValue::S("pk-b".into()))
                .maybe_sk(shape.sk, AttributeValue::S("sk-2".into()))
                .vec(shape.vec.unwrap(), [1.0, 2.0, 4.0]),
            Item::new(shape.pk, AttributeValue::S("pk-c".into()))
                .maybe_sk(shape.sk, AttributeValue::S("sk-3".into()))
                .vec(shape.vec.unwrap(), [4.0, 1.0, 2.0]),
        ];

        let ctx = TableContext::create_with_data(&actors, shape, &dataset).await;

        info!(
            "Issuing Query with VectorSearch Limit=2 on '{}'",
            ctx.table_name
        );
        let items = ctx
            .client
            .query()
            .table_name(&ctx.table_name)
            .index_name(ctx.index.index.as_ref())
            .limit(2)
            .vector_search([1.0, 1.0, 1.0])
            .send()
            .await
            .expect("Query with VectorSearch should succeed")
            .items()
            .to_vec();

        assert!(
            !items.is_empty(),
            "Query with VectorSearch should return at least one item"
        );
        assert!(
            items.len() <= 2,
            "Query with VectorSearch Limit=2 should return at most 2 items, got {}",
            items.len()
        );
        // Verify the closest result is pk-a (exact query vector match).
        assert_eq!(
            items[0].get(shape.pk),
            Some(&AttributeValue::S("pk-a".into())),
            "closest result should be pk-a"
        );
        if let Some(sk_name) = shape.sk {
            assert_eq!(
                items[0].get(sk_name),
                Some(&AttributeValue::S("sk-1".into())),
                "closest result should have sk-1"
            );
        }

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

#[framed]
async fn query_uses_selected_vector_index(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = super::make_clients(&actors).await;

    let table_name = super::unique_alternator_table_name();
    let partition_key_name = "Pk-Query-Case";
    let unique_index_name = super::unique_alternator_index_name();
    let lower_index_name: IndexName = unique_index_name.as_ref().to_ascii_lowercase().into();
    let upper_index_name: IndexName = unique_index_name.as_ref().to_ascii_uppercase().into();
    let lower_vector_attribute_name = "samevector";
    let upper_vector_attribute_name = "SAMEVECTOR";
    let lower_index = httpapi::IndexInfo::new(
        super::alternator_keyspace(&table_name).as_ref(),
        lower_index_name.as_ref(),
    );
    let upper_index = httpapi::IndexInfo::new(
        super::alternator_keyspace(&table_name).as_ref(),
        upper_index_name.as_ref(),
    );
    let dataset = [
        ("pk-a", [1.0, 1.0, 1.0], [4.0, 1.0, 2.0]),
        ("pk-b", [1.0, 2.0, 4.0], [2.0, 1.0, 3.0]),
        ("pk-c", [3.0, 1.0, 2.0], [1.0, 1.0, 1.0]),
    ];

    info!(
        "Creating Alternator table '{table_name}' with case-distinct VectorIndexes '{}' and '{}'",
        lower_index.index, upper_index.index
    );
    super::create_alternator_table(
        &client,
        &table_name,
        partition_key_name,
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[
            (lower_index.index.as_ref(), lower_vector_attribute_name, 3),
            (upper_index.index.as_ref(), upper_vector_attribute_name, 3),
        ],
    )
    .await
    .expect("CreateTable with case-distinct VectorIndexes should succeed");

    info!("Inserting items with different vectors for each indexed column into '{table_name}'");
    for (partition_key, lower_vector, upper_vector) in dataset {
        client
            .put_item()
            .table_name(&table_name)
            .item(
                partition_key_name,
                AttributeValue::S(partition_key.to_string()),
            )
            .item(lower_vector_attribute_name, dynamo_float_list(lower_vector))
            .item(upper_vector_attribute_name, dynamo_float_list(upper_vector))
            .send()
            .await
            .expect("PutItem should succeed");
    }

    info!(
        "Waiting for Vector Store to index all {} items for '{}/{}' and '{}/{}'",
        dataset.len(),
        lower_index.keyspace,
        lower_index.index,
        upper_index.keyspace,
        upper_index.index
    );
    super::wait_for_index_count(&vs_clients[0], &lower_index, dataset.len()).await;
    super::wait_for_index_count(&vs_clients[0], &upper_index, dataset.len()).await;

    info!(
        "Querying lower-case index '{}' on '{table_name}'",
        lower_index.index
    );
    let lower_items = client
        .query()
        .table_name(&table_name)
        .index_name(lower_index.index.as_ref())
        .limit(1)
        .vector_search([1.0, 1.0, 1.0])
        .send()
        .await
        .expect("Query with VectorSearch should succeed")
        .items()
        .to_vec();
    assert_eq!(
        lower_items[0].get(partition_key_name),
        Some(&AttributeValue::S("pk-a".into())),
        "lower-case index should return pk-a as nearest"
    );

    info!(
        "Querying upper-case index '{}' on '{table_name}'",
        upper_index.index
    );
    let upper_items = client
        .query()
        .table_name(&table_name)
        .index_name(upper_index.index.as_ref())
        .limit(1)
        .vector_search([1.0, 1.0, 1.0])
        .send()
        .await
        .expect("Query with VectorSearch should succeed")
        .items()
        .to_vec();
    assert_eq!(
        upper_items[0].get(partition_key_name),
        Some(&AttributeValue::S("pk-c".into())),
        "upper-case index should return pk-c as nearest"
    );

    super::delete_alternator_table(&client, &table_name).await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query sees both optimized `V` and standard `L` vector encodings
// ---------------------------------------------------------------------------

/// Mirrors upstream Alternator coverage for mixed vector encodings.
///
/// One item is written with the optimized Alternator vector type (`{"V": [...]}`),
/// while another is written through the standard DynamoDB list-of-numbers
/// representation (`{"L": [{"N": ...}, ...]}`). After the vector index is
/// created and fully prefills, a query must return both items.
#[framed]
async fn query_with_vector_v_type_and_l_type(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;
    let vs_client = &vs_clients[0];

    let table_name = unique_alternator_table_name();
    let index_name = unique_alternator_index_name();
    let pk_name = "p";
    let vec_name = "v";
    let index = vector_store::IndexInfo::new(
        alternator_keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );

    create_alternator_table(
        &client,
        &table_name,
        pk_name,
        ScalarAttributeType::S,
        None,
        &[],
    )
    .await
    .expect("CreateTable should succeed");

    let pk_v = "pk-v";
    let pk_l = "pk-l";

    put_item_with_optimized_vector(
        &client,
        &table_name,
        pk_name,
        pk_v,
        vec_name,
        [1.0, 0.0, 0.0],
    )
    .await;
    client
        .put_item()
        .table_name(&table_name)
        .item(pk_name, AttributeValue::S(pk_l.to_string()))
        .item(vec_name, dynamo_float_list([1.0_f32, 0.0, 0.0]))
        .send()
        .await
        .expect("PutItem with list-of-numbers vector should succeed");

    issue_update_table(
        &client,
        &table_name,
        serde_json::json!([{
            "Create": {
                "IndexName": index.index.as_ref(),
                "VectorAttribute": {
                    "AttributeName": vec_name,
                    "Dimensions": 3
                }
            }
        }]),
    )
    .await;

    common::wait_for_index(vs_client, &index).await;
    wait_for_index_count(vs_client, &index, 2).await;

    let items = client
        .query()
        .table_name(&table_name)
        .index_name(index.index.as_ref())
        .limit(2)
        .vector_search([1.0_f32, 0.0, 0.0])
        .send()
        .await
        .expect("Query with mixed vector encodings should succeed")
        .items()
        .to_vec();

    let expected = HashSet::from([pk_v.to_string(), pk_l.to_string()]);
    let got = items
        .iter()
        .map(|item| extract_s(item, pk_name))
        .collect::<HashSet<_>>();
    assert_eq!(
        got, expected,
        "query should return both V and L encoded vectors"
    );

    delete_alternator_table(&client, &table_name).await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query works end-to-end with optimized `V` vector encoding
// ---------------------------------------------------------------------------

/// Writes vectors using Alternator's optimized `V` type and verifies that a
/// vector-search query returns all indexed items and ranks the nearest one
/// first.
#[framed]
async fn query_with_vector_v_type(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;
    let vs_client = &vs_clients[0];

    let table_name = unique_alternator_table_name();
    let index_name = unique_alternator_index_name();
    let pk_name = "p";
    let vec_name = "v";
    let index = vector_store::IndexInfo::new(
        alternator_keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );

    create_alternator_table(
        &client,
        &table_name,
        pk_name,
        ScalarAttributeType::S,
        None,
        &[],
    )
    .await
    .expect("CreateTable should succeed");

    let pk_nearest = "pk-nearest";
    let pk_farther = "pk-farther";

    put_item_with_optimized_vector(
        &client,
        &table_name,
        pk_name,
        pk_nearest,
        vec_name,
        [1.0, 0.0, 0.0],
    )
    .await;
    put_item_with_optimized_vector(
        &client,
        &table_name,
        pk_name,
        pk_farther,
        vec_name,
        [0.0, 1.0, 0.0],
    )
    .await;

    issue_update_table(
        &client,
        &table_name,
        serde_json::json!([{
            "Create": {
                "IndexName": index.index.as_ref(),
                "VectorAttribute": {
                    "AttributeName": vec_name,
                    "Dimensions": 3
                }
            }
        }]),
    )
    .await;

    common::wait_for_index(vs_client, &index).await;
    wait_for_index_count(vs_client, &index, 2).await;

    let items = client
        .query()
        .table_name(&table_name)
        .index_name(index.index.as_ref())
        .limit(2)
        .vector_search([1.0_f32, 0.0, 0.0])
        .send()
        .await
        .expect("Query with optimized vector encoding should succeed")
        .items()
        .to_vec();

    assert_eq!(
        items.len(),
        2,
        "query should return both optimized-vector items"
    );
    assert_eq!(
        extract_s(&items[0], pk_name),
        pk_nearest,
        "nearest optimized-vector item should rank first"
    );
    let got = items
        .iter()
        .map(|item| extract_s(item, pk_name))
        .collect::<HashSet<_>>();
    assert_eq!(
        got,
        HashSet::from([pk_nearest.to_string(), pk_farther.to_string()]),
        "query should return all optimized-vector items"
    );

    delete_alternator_table(&client, &table_name).await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query with VectorSearch returns results in distance order
// ---------------------------------------------------------------------------

/// Creates an Alternator table with 5 items at known cosine distances from a
/// query vector, retrieves all of them via VectorSearch with Limit=5, and
/// asserts that the returned items are ordered by ascending cosine distance
/// (nearest first).
///
/// Alternator vector indexes always use **cosine** similarity (the default;
/// there is currently no way to select another metric via the Alternator API).
/// Cosine distance = 1 - cos(theta), so vectors pointing in the same direction as
/// the query vector are "nearest" regardless of magnitude.
#[framed]
async fn query_with_vector_search_multiple_results_ordering(actors: TestActors) {
    info!("started");

    let dataset = [
        Item::new("Pk-Ord", AttributeValue::S("pk-nearest".into())).vec("Vec-Ord", [1.0, 0.0, 0.0]),
        Item::new("Pk-Ord", AttributeValue::S("pk-near".into())).vec("Vec-Ord", [1.0, 0.1, 0.0]),
        Item::new("Pk-Ord", AttributeValue::S("pk-mid".into())).vec("Vec-Ord", [1.0, 1.0, 0.0]),
        Item::new("Pk-Ord", AttributeValue::S("pk-far".into())).vec("Vec-Ord", [0.0, 1.0, 0.0]),
        Item::new("Pk-Ord", AttributeValue::S("pk-farthest".into()))
            .vec("Vec-Ord", [-1.0, 0.0, 0.0]),
    ];

    let ctx = TableContext::create_with_data(
        &actors,
        &super::TableShape {
            table_prefix: "",
            index_prefix: "",
            pk: "Pk-Ord",
            sk: None,
            vec: Some("Vec-Ord"),
            pk_type: super::ScalarAttributeType::S,
        },
        &dataset,
    )
    .await;

    let expected_order: Vec<AttributeValue> = dataset
        .iter()
        .map(|i| i.0.get("Pk-Ord").expect("item has no Pk-Ord").clone())
        .collect();

    info!(
        "Issuing Query with VectorSearch Limit=5 on '{}'",
        ctx.table_name
    );
    let raw = ctx
        .client
        .query()
        .table_name(&ctx.table_name)
        .index_name(ctx.index.index.as_ref())
        .limit(5)
        .vector_search([1.0, 0.0, 0.0])
        .send()
        .await
        .expect("Query with VectorSearch should succeed")
        .items()
        .to_vec();
    let actual_order: Vec<AttributeValue> = raw
        .iter()
        .filter_map(|item| item.get("Pk-Ord"))
        .cloned()
        .collect();

    assert_eq!(
        actual_order, expected_order,
        "Results should be ordered by ascending cosine distance from query vector"
    );

    ctx.done().await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query with ProjectionExpression — all name patterns and key schemas
// ---------------------------------------------------------------------------

/// Creates tables for every entry in [`NAME_PATTERNS`](super::NAME_PATTERNS)
/// (covering HASH-only and HASH+RANGE schemas, as well as special characters in
/// attribute names) and issues a VectorSearch query with a
/// `ProjectionExpression` that requests only the key attribute(s), excluding the
/// vector and an extra "data" attribute.
///
/// Verifies for every shape that:
/// - the partition key is present in each returned item,
/// - the sort key is present when the shape has one,
/// - the vector attribute is absent,
/// - the extra data attribute is absent.
#[framed]
async fn query_with_projection_special_names(actors: TestActors) {
    info!("started");

    for shape in super::NAME_PATTERNS {
        info!("Testing shape: {shape:?}");

        let data_attribute_name = "data:extra";
        let sk = shape.sk;

        let dataset = [Item::new(shape.pk, AttributeValue::S("pk-1".into()))
            .maybe_sk(sk, AttributeValue::S("sk-1".into()))
            .vec(
                shape.vec.expect("NAME_PATTERNS entries always have vec"),
                [1.0, 1.0, 1.0],
            )
            .attr(
                data_attribute_name,
                AttributeValue::S("extra-val".to_string()),
            )];

        let ctx = TableContext::create_with_data(&actors, shape, &dataset).await;
        let pk_name = ctx.pk.as_str();

        // Build projection over key(s) only — exclude vector and data.
        let proj_expr: &str = if shape.sk.is_some() {
            "#pk, #sk"
        } else {
            "#pk"
        };

        info!("Querying with ProjectionExpression = '{proj_expr}'");
        let mut builder = ctx
            .client
            .query()
            .table_name(&ctx.table_name)
            .index_name(ctx.index.index.as_ref())
            .limit(1)
            .projection_expression(proj_expr)
            .expression_attribute_names("#pk", pk_name);
        if let Some(sk_name) = shape.sk {
            builder = builder.expression_attribute_names("#sk", sk_name);
        }
        let items = builder
            .vector_search([1.0, 1.0, 1.0])
            .send()
            .await
            .expect("Query with VectorSearch should succeed")
            .items()
            .to_vec();

        // Expected: only the key attributes from the dataset item.
        let mut expected_item: HashMap<String, AttributeValue> = HashMap::new();
        expected_item.insert(pk_name.to_string(), AttributeValue::S("pk-1".into()));
        if let Some(sk_name) = shape.sk {
            expected_item.insert(sk_name.to_string(), AttributeValue::S("sk-1".into()));
        }
        assert_eq!(
            items,
            vec![expected_item],
            "projected item should contain only key attributes"
        );

        ctx.done().await;
        info!("Shape {shape:?} passed");
    }

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query with Select::AllAttributes returns all attributes
// ---------------------------------------------------------------------------

/// Creates a table, inserts items with a partition key, vector, and extra
/// "data" attribute.  Queries with `Select::AllAttributes` and verifies
/// that all three attributes are returned for each item.
#[framed]
async fn query_with_select_all_attributes(actors: TestActors) {
    info!("started");

    let shape = super::TableShape {
        table_prefix: "",
        index_prefix: "",
        pk: "Pk-SelAll",
        sk: None,
        vec: Some("Vec-SelAll"),
        pk_type: super::ScalarAttributeType::S,
    };
    let data_attribute_name = "Data-SelAll";

    let dataset = [Item::new("Pk-SelAll", AttributeValue::S("pk-a".into()))
        .vec("Vec-SelAll", [1.0, 1.0, 1.0])
        .attr(
            data_attribute_name,
            AttributeValue::S("some-data".to_string()),
        )];

    let ctx = TableContext::create_with_data(&actors, &shape, &dataset).await;

    info!("Querying with Select::AllAttributes");
    let items = ctx
        .client
        .query()
        .table_name(&ctx.table_name)
        .index_name(ctx.index.index.as_ref())
        .limit(1)
        .select(Select::AllAttributes)
        .vector_search([1.0, 1.0, 1.0])
        .send()
        .await
        .expect("Query with VectorSearch should succeed")
        .items()
        .to_vec();

    assert_eq!(items.len(), 1, "should return 1 item");
    let item = &items[0];
    assert!(
        item.contains_key("Pk-SelAll"),
        "AllAttributes should return partition key"
    );
    assert!(
        item.contains_key("Vec-SelAll"),
        "AllAttributes should return vector attribute"
    );
    assert!(
        item.contains_key(data_attribute_name),
        "AllAttributes should return data attribute"
    );

    ctx.done().await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query with Select::Count returns count but no items
// ---------------------------------------------------------------------------

/// Creates a table with items, then queries with `Select::Count`. Verifies
/// that the response has a count > 0 but returns no item data (the DynamoDB
/// API returns Count/ScannedCount in the response but Items is empty).
#[framed]
async fn query_with_select_count(actors: TestActors) {
    info!("started");

    let dataset = [
        Item::new("Pk-SelCnt", AttributeValue::S("pk-a".into())).vec("Vec-SelCnt", [1.0, 1.0, 1.0]),
        Item::new("Pk-SelCnt", AttributeValue::S("pk-b".into())).vec("Vec-SelCnt", [1.0, 2.0, 4.0]),
    ];

    let ctx = TableContext::create_with_data(
        &actors,
        &super::TableShape {
            table_prefix: "",
            index_prefix: "",
            pk: "Pk-SelCnt",
            sk: None,
            vec: Some("Vec-SelCnt"),
            pk_type: super::ScalarAttributeType::S,
        },
        &dataset,
    )
    .await;

    info!("Querying with Select::Count");
    let resp = ctx
        .client
        .query()
        .table_name(&ctx.table_name)
        .index_name(ctx.index.index.as_ref())
        .limit(5)
        .select(Select::Count)
        .vector_search([1.0_f32, 1.0, 1.0])
        .send()
        .await
        .expect("Query with Select::Count should succeed");

    assert!(
        resp.count() > 0,
        "Select::Count should report count > 0, got {}",
        resp.count()
    );
    assert!(
        resp.items().is_empty(),
        "Select::Count should return empty items list, got {} items",
        resp.items().len()
    );

    ctx.done().await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query with Limit larger than dataset size
// ---------------------------------------------------------------------------

/// Creates a table with 3 items, queries with `Limit=1000` (far larger than
/// the dataset), and verifies that all 3 items are returned without error or
/// truncation. This exercises the case where `Limit > dataset_size`, which
/// should return all available results rather than erroring.
#[framed]
async fn query_with_limit_larger_than_dataset(actors: TestActors) {
    info!("started");

    let dataset = [
        Item::new("Pk-BigLim", AttributeValue::S("pk-a".into())).vec("Vec-BigLim", [1.0, 1.0, 1.0]),
        Item::new("Pk-BigLim", AttributeValue::S("pk-b".into())).vec("Vec-BigLim", [1.0, 2.0, 4.0]),
        Item::new("Pk-BigLim", AttributeValue::S("pk-c".into())).vec("Vec-BigLim", [2.0, 1.0, 3.0]),
    ];

    let ctx = TableContext::create_with_data(
        &actors,
        &super::TableShape {
            table_prefix: "",
            index_prefix: "",
            pk: "Pk-BigLim",
            sk: None,
            vec: Some("Vec-BigLim"),
            pk_type: super::ScalarAttributeType::S,
        },
        &dataset,
    )
    .await;

    info!(
        "Issuing Query with VectorSearch Limit=1000 on '{}' (dataset has {} items)",
        ctx.table_name,
        dataset.len()
    );
    let items = ctx
        .client
        .query()
        .table_name(&ctx.table_name)
        .index_name(ctx.index.index.as_ref())
        .limit(1000)
        .vector_search([1.0, 1.0, 1.0])
        .send()
        .await
        .expect("Query with VectorSearch should succeed")
        .items()
        .to_vec();

    assert_eq!(
        items.len(),
        dataset.len(),
        "Query with Limit=1000 should return all {} items, got {}",
        dataset.len(),
        items.len()
    );
    info!(
        "Query returned {} item(s) with Limit=1000 (dataset has {})",
        items.len(),
        dataset.len()
    );

    ctx.done().await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query with large Dimensions (1536 — OpenAI embedding size) e2e
// ---------------------------------------------------------------------------

/// Creates a table with `Dimensions=1536` (the dimensionality of OpenAI
/// text-embedding-ada-002 vectors), inserts one item with a 1536-element
/// vector, waits for VS to index it, then issues an ANN query and asserts
/// the inserted item is returned.
///
/// This exercises the full create → insert → index → query → delete lifecycle
/// with a non-trivial dimensionality.
#[framed]
async fn query_with_large_dimensions(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let index_name = unique_alternator_index_name();
    let partition_key_name = "pk";
    let vector_attribute_name = "vec";
    let dimensions: usize = 1536;
    let index = httpapi::IndexInfo::new(
        alternator_keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );

    info!("Creating Alternator table '{table_name}' with Dimensions={dimensions}");
    super::create_alternator_table(
        &client,
        &table_name,
        partition_key_name,
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(index.index.as_ref(), vector_attribute_name, dimensions)],
    )
    .await
    .expect("CreateTable with large Dimensions should succeed");

    info!(
        "Waiting for VS to discover index '{}/{}'",
        index.keyspace, index.index
    );
    common::wait_for_index(&vs_clients[0], &index).await;

    // Build a 1536-dim vector: [1.0, 0.0, 0.0, ..., 0.0]
    let mut vector_data: Vec<f32> = vec![0.0; dimensions];
    vector_data[0] = 1.0;

    info!("Inserting item with {dimensions}-dim vector into '{table_name}'");
    client
        .put_item()
        .table_name(&table_name)
        .item(
            partition_key_name,
            aws_sdk_dynamodb::types::AttributeValue::S("pk-1".to_string()),
        )
        .item(
            vector_attribute_name,
            dynamo_float_list(vector_data.iter().copied()),
        )
        .send()
        .await
        .expect("PutItem with large-dimension vector should succeed");

    info!("Waiting for VS to index the item");
    wait_for_index_count(&vs_clients[0], &index, 1).await;

    info!("Querying VS with a {dimensions}-dim ANN query");
    let (pks, _, _) = vs_clients[0]
        .ann(
            &index.keyspace,
            &index.index,
            Vector::from(vector_data),
            None,
            Limit::from(NonZeroUsize::new(1).unwrap()),
        )
        .await;
    let pk_column = ColumnName::from(partition_key_name);
    let pk_values: Vec<&str> = pks
        .get(&pk_column)
        .expect("ANN result should contain the pk column")
        .iter()
        .map(|v| v.as_str().expect("pk value should be a string"))
        .collect();
    assert_eq!(
        pk_values,
        vec!["pk-1"],
        "ANN should return the inserted item"
    );

    super::delete_alternator_table(&client, &table_name).await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query with KeyConditionExpression alongside VectorSearch
// ---------------------------------------------------------------------------

/// Creates a HASH+RANGE Alternator table, inserts items for two distinct
/// partition keys, then issues a VectorSearch `Query` with a
/// `KeyConditionExpression` that restricts results to a single partition key.
///
/// Alternator evaluates the `KeyConditionExpression` server-side after the
/// Vector Store returns ANN results, so only items belonging to the requested
/// partition key should appear in the response.  The test confirms that:
/// - all returned items carry the expected partition-key value,
/// - items from the other partition key are absent,
/// - the correct number of items is returned.
#[framed]
async fn query_with_key_condition_expression(actors: TestActors) {
    info!("started");

    let pk_name = "Pk-KCE";
    let sk_name = "Sk-KCE";
    let vec_name = "Vec-KCE";

    // Two items for "pk-a", one item for "pk-b" (same vector — would rank
    // equally, but should be excluded by the KeyConditionExpression).
    let dataset = [
        Item::new(pk_name, AttributeValue::S("pk-a".into()))
            .sk(sk_name, AttributeValue::S("sk-1".into()))
            .vec(vec_name, [1.0, 1.0, 1.0]),
        Item::new(pk_name, AttributeValue::S("pk-a".into()))
            .sk(sk_name, AttributeValue::S("sk-2".into()))
            .vec(vec_name, [1.0, 2.0, 4.0]),
        Item::new(pk_name, AttributeValue::S("pk-b".into()))
            .sk(sk_name, AttributeValue::S("sk-1".into()))
            .vec(vec_name, [1.0, 1.0, 1.0]),
    ];

    let ctx = TableContext::create_with_data(
        &actors,
        &super::TableShape {
            table_prefix: "",
            index_prefix: "",
            pk: pk_name,
            sk: Some(sk_name),
            vec: Some(vec_name),
            pk_type: super::ScalarAttributeType::S,
        },
        &dataset,
    )
    .await;

    info!(
        "Issuing Query with KeyConditionExpression '{}' = :pk on '{}'",
        pk_name, ctx.table_name
    );
    let resp = ctx
        .client
        .query()
        .table_name(&ctx.table_name)
        .index_name(ctx.index.index.as_ref())
        .limit(10)
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", pk_name)
        .expression_attribute_values(":pk", AttributeValue::S("pk-a".into()))
        .vector_search([1.0_f32, 1.0, 1.0])
        .send()
        .await
        .expect("Query with KeyConditionExpression should succeed");

    // pk-a has two items (sk-1 nearest, sk-2 farther); pk-b must be excluded.
    let expected: Vec<HashMap<String, AttributeValue>> = vec![
        HashMap::from([
            (pk_name.to_string(), AttributeValue::S("pk-a".into())),
            (sk_name.to_string(), AttributeValue::S("sk-1".into())),
        ]),
        HashMap::from([
            (pk_name.to_string(), AttributeValue::S("pk-a".into())),
            (sk_name.to_string(), AttributeValue::S("sk-2".into())),
        ]),
    ];
    assert_eq!(
        resp.items(),
        expected,
        "unexpected KeyConditionExpression results"
    );

    ctx.done().await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: Query with FilterExpression alongside VectorSearch
// ---------------------------------------------------------------------------

/// Creates a HASH-only Alternator table with items that carry a
/// `Category` attribute, then issues a VectorSearch `Query` with a
/// `FilterExpression` that keeps only items whose `Category` equals `"keep"`.
///
/// DynamoDB/Alternator applies `FilterExpression` **after** the Vector Store
/// returns the ANN result set (no pre-filtering optimisation), so all items
/// are fetched from the index first and then narrowed client-side by
/// Alternator.  The test uses `Limit` larger than the dataset to ensure all
/// items are considered before the filter is applied.
///
/// Verifies that:
/// - only items with `Category = "keep"` appear in the response,
/// - the item with `Category = "drop"` is absent,
/// - the remaining items are still returned in cosine-distance order.
#[framed]
async fn query_with_filter_expression(actors: TestActors) {
    info!("started");

    let pk_name = "Pk-Flt";
    let vec_name = "Vec-Flt";
    let cat_name = "Category";

    // Three items close to query vector [1, 1, 1].  "pk-drop" sits between
    // the two "keep" items in cosine distance but must be absent from results.
    let dataset = [
        Item::new(pk_name, AttributeValue::S("pk-keep-1".into()))
            .vec(vec_name, [1.0, 1.0, 1.0])
            .attr(cat_name, AttributeValue::S("keep".into())),
        Item::new(pk_name, AttributeValue::S("pk-drop".into()))
            .vec(vec_name, [1.0, 1.05, 1.0])
            .attr(cat_name, AttributeValue::S("drop".into())),
        Item::new(pk_name, AttributeValue::S("pk-keep-2".into()))
            .vec(vec_name, [1.0, 1.1, 1.0])
            .attr(cat_name, AttributeValue::S("keep".into())),
    ];

    let ctx = TableContext::create_with_data(
        &actors,
        &super::TableShape {
            table_prefix: "",
            index_prefix: "",
            pk: pk_name,
            sk: None,
            vec: Some(vec_name),
            pk_type: super::ScalarAttributeType::S,
        },
        &dataset,
    )
    .await;

    // Use Limit larger than the dataset so all items are read from the index
    // before FilterExpression is applied (DynamoDB applies Limit first, then
    // FilterExpression — a Limit smaller than the dataset could exclude items
    // before the filter ever sees them).
    info!(
        "Issuing Query with FilterExpression '#cat = :cat' on '{}'",
        ctx.table_name
    );
    let resp = ctx
        .client
        .query()
        .table_name(&ctx.table_name)
        .index_name(ctx.index.index.as_ref())
        .limit(100)
        .filter_expression("#cat = :cat")
        .projection_expression("#pk, #cat")
        .expression_attribute_names("#pk", pk_name)
        .expression_attribute_names("#cat", cat_name)
        .expression_attribute_values(":cat", AttributeValue::S("keep".into()))
        .vector_search([1.0_f32, 1.0, 1.0])
        .send()
        .await
        .expect("Query with FilterExpression should succeed");

    // ANN ordering is preserved after filtering: pk-keep-1 is nearest to
    // [1,1,1] (exact match), pk-keep-2 is slightly farther.  "pk-drop" must
    // be excluded by the FilterExpression.
    let expected: Vec<HashMap<String, AttributeValue>> = vec![
        HashMap::from([
            (pk_name.to_string(), AttributeValue::S("pk-keep-1".into())),
            (cat_name.to_string(), AttributeValue::S("keep".into())),
        ]),
        HashMap::from([
            (pk_name.to_string(), AttributeValue::S("pk-keep-2".into())),
            (cat_name.to_string(), AttributeValue::S("keep".into())),
        ]),
    ];
    assert_eq!(
        resp.items(),
        expected,
        "unexpected FilterExpression results"
    );

    ctx.done().await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test-case registration
// ---------------------------------------------------------------------------

pub(super) fn register(test_case: TestCase<TestActors>) -> TestCase<TestActors> {
    test_case
        .with_test(
            "query_with_vector_search",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_vector_search,
        )
        .with_test(
            "query_uses_selected_vector_index",
            common::DEFAULT_TEST_TIMEOUT,
            query_uses_selected_vector_index,
        )
        .with_test(
            "query_with_vector_v_type_and_l_type",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_vector_v_type_and_l_type,
        )
        .with_test(
            "query_with_vector_v_type",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_vector_v_type,
        )
        .with_test(
            "query_with_vector_search_multiple_results_ordering",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_vector_search_multiple_results_ordering,
        )
        .with_test(
            "query_with_projection_special_names",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_projection_special_names,
        )
        .with_test(
            "query_with_select_all_attributes",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_select_all_attributes,
        )
        .with_test(
            "query_with_select_count",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_select_count,
        )
        .with_test(
            "query_with_limit_larger_than_dataset",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_limit_larger_than_dataset,
        )
        .with_test(
            "query_with_large_dimensions",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_large_dimensions,
        )
        // Not implemented yet
        //.with_test(
        //    "query_with_key_condition_expression",
        //    common::DEFAULT_TEST_TIMEOUT,
        //    query_with_key_condition_expression,
        //)
        .with_test(
            "query_with_filter_expression",
            common::DEFAULT_TEST_TIMEOUT,
            query_with_filter_expression,
        )
}
