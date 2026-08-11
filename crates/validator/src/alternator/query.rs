/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::alternator;
use crate::alternator::Item;
use crate::alternator::JsonBodyInjectInterceptor;
use crate::alternator::TableContext;
use crate::alternator::TableShape;
use crate::common;
use aws_sdk_dynamodb::client::customize::CustomizableOperation;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::query::QueryOutput;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::ScalarAttributeType;
use aws_sdk_dynamodb::types::Select;
use httpapi::IndexName;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Extension trait that adds Alternator `VectorSearch` to [`QueryFluentBuilder`].
pub(super) trait QueryBuilderExt {
    fn vector_search(
        self,
        vector: impl IntoIterator<Item = f32>,
    ) -> CustomizableOperation<QueryOutput, QueryError, QueryFluentBuilder>;

    fn vector_search_optimized(
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

    fn vector_search_optimized(
        self,
        vector: impl IntoIterator<Item = f32>,
    ) -> CustomizableOperation<QueryOutput, QueryError, QueryFluentBuilder> {
        let json = serde_json::json!({
            "QueryVector": {
                "FLOAT32VECTOR": vector.into_iter().collect::<Vec<_>>()
            }
        });
        self.customize()
            .interceptor(JsonBodyInjectInterceptor::new([("VectorSearch", json)]))
    }
}

/// Verifies basic VectorSearch query: results returned, limit respected, nearest item first.
#[e2etest::test(group = query)]
async fn query_with_vector_search(actors: Arc<TestActors>) {
    info!("started");

    let shapes = [
        TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "Pk-QVS".into(),
            sk_name: None,
            vec_name: Some("Vec-QVS".into()),
            pk_type: ScalarAttributeType::S,
        },
        TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "Pk-QVS".into(),
            sk_name: Some("Sk-QVS".into()),
            vec_name: Some("Vec-QVS".into()),
            pk_type: ScalarAttributeType::S,
        },
    ];

    for shape in &shapes {
        info!("Testing shape: {shape:?}");

        let dataset = [
            Item::new(shape.pk(), AttributeValue::S("pk-a".into()))
                .maybe_sk(shape.sk(), AttributeValue::S("sk-1".into()))
                .vec(shape.vec().unwrap(), [1.0, 1.0, 1.0]),
            Item::new(shape.pk(), AttributeValue::S("pk-b".into()))
                .maybe_sk(shape.sk(), AttributeValue::S("sk-2".into()))
                .vec(shape.vec().unwrap(), [1.0, 2.0, 4.0]),
            Item::new(shape.pk(), AttributeValue::S("pk-c".into()))
                .maybe_sk(shape.sk(), AttributeValue::S("sk-3".into()))
                .vec(shape.vec().unwrap(), [4.0, 1.0, 2.0]),
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
        assert_eq!(
            items[0].get(shape.pk()),
            Some(&AttributeValue::S("pk-a".into())),
            "closest result should be pk-a"
        );
        if let Some(sk_name) = shape.sk() {
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

#[e2etest::test(group = query)]
async fn query_uses_selected_vector_index(actors: Arc<TestActors>) {
    info!("started");

    let (client, vs_clients) = alternator::make_clients(&actors).await;

    let table_name = alternator::unique_table_name();
    let partition_key_name = "Pk-Query-Case";
    let unique_index_name = alternator::unique_index_name();
    let lower_index_name: IndexName = unique_index_name.as_ref().to_ascii_lowercase().into();
    let upper_index_name: IndexName = unique_index_name.as_ref().to_ascii_uppercase().into();
    let lower_vector_attribute_name = "samevector";
    let upper_vector_attribute_name = "SAMEVECTOR";
    let lower_index = httpapi::IndexInfo::new(
        alternator::keyspace(&table_name).as_ref(),
        lower_index_name.as_ref(),
    );
    let upper_index = httpapi::IndexInfo::new(
        alternator::keyspace(&table_name).as_ref(),
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
    alternator::create_table(
        &client,
        &table_name,
        partition_key_name,
        ScalarAttributeType::S,
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
            .item(
                lower_vector_attribute_name,
                alternator::float_list(lower_vector),
            )
            .item(
                upper_vector_attribute_name,
                alternator::float_list(upper_vector),
            )
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
    common::wait_for_index_count(&vs_clients, &lower_index, dataset.len()).await;
    common::wait_for_index_count(&vs_clients, &upper_index, dataset.len()).await;

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

    alternator::delete_table(&client, &table_name).await;

    info!("finished");
}

/// Verifies ANN results are ordered by ascending cosine distance.
#[e2etest::test(group = query)]
async fn query_with_vector_search_multiple_results_ordering(actors: Arc<TestActors>) {
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
        &TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "Pk-Ord".into(),
            sk_name: None,
            vec_name: Some("Vec-Ord".into()),
            pk_type: ScalarAttributeType::S,
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

/// Verifies ProjectionExpression returns only requested key attributes across name_patterns.
#[e2etest::test(group = query)]
async fn query_with_projection_special_names(actors: Arc<TestActors>) {
    info!("started");

    for shape in &alternator::name_patterns() {
        info!("Testing shape: {shape:?}");

        let data_attribute_name = "data:extra";
        let sk = shape.sk();

        let dataset = [Item::new(shape.pk(), AttributeValue::S("pk-1".into()))
            .maybe_sk(sk, AttributeValue::S("sk-1".into()))
            .vec(
                shape.vec().expect("NAME_PATTERNS entries always have vec"),
                [1.0, 1.0, 1.0],
            )
            .attr(
                data_attribute_name,
                AttributeValue::S("extra-val".to_string()),
            )];

        let ctx = TableContext::create_with_data(&actors, shape, &dataset).await;
        let pk_name = ctx.shape.pk();

        let proj_expr: &str = if shape.sk_name.is_some() {
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
        if let Some(sk_name) = shape.sk() {
            builder = builder.expression_attribute_names("#sk", sk_name);
        }
        let items = builder
            .vector_search([1.0, 1.0, 1.0])
            .send()
            .await
            .expect("Query with VectorSearch should succeed")
            .items()
            .to_vec();

        let mut expected_item: HashMap<String, AttributeValue> = HashMap::new();
        expected_item.insert(pk_name.to_string(), AttributeValue::S("pk-1".into()));
        if let Some(sk_name) = shape.sk() {
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

/// Verifies Select::AllAttributes returns all item attributes.
#[e2etest::test(group = query)]
async fn query_with_select_all_attributes(actors: Arc<TestActors>) {
    info!("started");

    let shape = TableShape {
        table_prefix: None,
        index_prefix: None,
        pk_name: "Pk-SelAll".into(),
        sk_name: None,
        vec_name: Some("Vec-SelAll".into()),
        pk_type: ScalarAttributeType::S,
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

/// Verifies Select::Count returns count > 0 with empty items list.
#[e2etest::test(group = query)]
async fn query_with_select_count(actors: Arc<TestActors>) {
    info!("started");

    let dataset = [
        Item::new("Pk-SelCnt", AttributeValue::S("pk-a".into())).vec("Vec-SelCnt", [1.0, 1.0, 1.0]),
        Item::new("Pk-SelCnt", AttributeValue::S("pk-b".into())).vec("Vec-SelCnt", [1.0, 2.0, 4.0]),
    ];

    let ctx = TableContext::create_with_data(
        &actors,
        &TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "Pk-SelCnt".into(),
            sk_name: None,
            vec_name: Some("Vec-SelCnt".into()),
            pk_type: ScalarAttributeType::S,
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

/// Verifies Limit larger than dataset returns all items without error.
#[e2etest::test(group = query)]
async fn query_with_limit_larger_than_dataset(actors: Arc<TestActors>) {
    info!("started");

    let dataset = [
        Item::new("Pk-BigLim", AttributeValue::S("pk-a".into())).vec("Vec-BigLim", [1.0, 1.0, 1.0]),
        Item::new("Pk-BigLim", AttributeValue::S("pk-b".into())).vec("Vec-BigLim", [1.0, 2.0, 4.0]),
        Item::new("Pk-BigLim", AttributeValue::S("pk-c".into())).vec("Vec-BigLim", [2.0, 1.0, 3.0]),
    ];

    let ctx = TableContext::create_with_data(
        &actors,
        &TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "Pk-BigLim".into(),
            sk_name: None,
            vec_name: Some("Vec-BigLim".into()),
            pk_type: ScalarAttributeType::S,
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

/// Verifies VectorSearch works with 1536-dimensional vectors.
#[e2etest::test(group = query)]
async fn query_with_large_dimensions(actors: Arc<TestActors>) {
    info!("started");

    let (client, vs_clients) = alternator::make_clients(&actors).await;

    let table_name = alternator::unique_table_name();
    let index_name = alternator::unique_index_name();
    let partition_key_name = "pk";
    let vector_attribute_name = "vec";
    let dimensions: usize = 1536;
    let index = httpapi::IndexInfo::new(
        alternator::keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );

    info!("Creating Alternator table '{table_name}' with Dimensions={dimensions}");
    alternator::create_table(
        &client,
        &table_name,
        partition_key_name,
        ScalarAttributeType::S,
        None,
        &[(index.index.as_ref(), vector_attribute_name, dimensions)],
    )
    .await
    .expect("CreateTable with large Dimensions should succeed");

    info!(
        "Waiting for VS to discover index '{}/{}'",
        index.keyspace, index.index
    );
    alternator::wait_for_index(&vs_clients, &index).await;

    let mut vector_data: Vec<f32> = vec![0.0; dimensions];
    vector_data[0] = 1.0;

    info!("Inserting item with {dimensions}-dim vector into '{table_name}'");
    client
        .put_item()
        .table_name(&table_name)
        .item(partition_key_name, AttributeValue::S("pk-1".to_string()))
        .item(
            vector_attribute_name,
            alternator::float_list(vector_data.iter().copied()),
        )
        .send()
        .await
        .expect("PutItem with large-dimension vector should succeed");

    info!("Waiting for VS to index the item");
    common::wait_for_index_count(&vs_clients, &index, 1).await;

    info!("Querying with VectorSearch on {dimensions}-dim index");
    let items = client
        .query()
        .table_name(&table_name)
        .index_name(index.index.as_ref())
        .limit(1)
        .vector_search(vector_data)
        .send()
        .await
        .expect("Query with VectorSearch should succeed")
        .items()
        .to_vec();

    assert_eq!(items.len(), 1, "should return 1 item");
    assert_eq!(
        items[0].get(partition_key_name),
        Some(&AttributeValue::S("pk-1".into())),
        "VectorSearch should return the inserted item"
    );

    alternator::delete_table(&client, &table_name).await;

    info!("finished");
}

/// Verifies FilterExpression excludes non-matching items while preserving ANN ordering.
#[e2etest::test(group = query)]
async fn query_with_filter_expression(actors: Arc<TestActors>) {
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
        &TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: pk_name.into(),
            sk_name: None,
            vec_name: Some(vec_name.into()),
            pk_type: ScalarAttributeType::S,
        },
        &dataset,
    )
    .await;

    // Use Limit larger than the dataset so all items are read from the index
    // before FilterExpression is applied (DynamoDB applies Limit first, then
    // FilterExpression - a Limit smaller than the dataset could exclude items
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

e2etest::group!(
    name = query,
    fixtures = (Fixture),
    parent = alternator::alternator
);

struct Fixture {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        alternator::init(&actors).await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        common::cleanup(&self.actors).await;
    }
}
