/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::alternator;
use crate::common;
use aws_sdk_dynamodb::types::IndexStatus;
use aws_sdk_dynamodb::types::ProjectionType;
use aws_sdk_dynamodb::types::VectorDistanceFunction;
use httpapi::IndexInfo;
use httpapi::IndexName;
use std::sync::Arc;
use tracing::info;

/// Creates, describes, and deletes Alternator tables for every entry in
/// [`alternator::name_patterns`] (the 2x2 matrix of plain/special x HASH-only/HASH+RANGE).
/// For each shape the test:
/// 1. Calls `CreateTable` with a vector index.
/// 2. Waits for Vector Store to discover the index.
/// 3. Calls `DescribeTable` and verifies the reported `VectorIndexes`.
/// 4. Calls `DeleteTable` and waits for Vector Store to drop the index.
#[e2etest::test(group = create_table)]
async fn create_describe_and_delete_table_with_vector_index(actors: Arc<TestActors>) {
    info!("started");

    let (client, vs_clients) = alternator::make_clients(&actors).await;

    let patterns = alternator::name_patterns();
    for (i, shape) in patterns.iter().enumerate() {
        info!("NAME_PATTERNS[{i}]: {shape:?}");

        let vec_attr = shape.vec().expect("NAME_PATTERNS entries always have vec");
        let (table_name, index) = alternator::resolve_table_names(shape);

        info!(
            "Creating Alternator table '{table_name}' with VectorIndex '{}'",
            index.index
        );
        alternator::create_table(
            &client,
            &table_name,
            shape.pk(),
            shape.pk_type.clone(),
            shape.sk(),
            &[(index.index.as_ref(), vec_attr, 3)],
        )
        .await
        .expect("CreateTable with VectorIndex should succeed");

        info!(
            "Waiting for Vector Store to discover index '{}/{}'",
            index.keyspace, index.index
        );
        alternator::wait_for_index(&vs_clients, &index).await;

        info!("Describing Alternator table '{table_name}' and asserting VectorIndexes");
        let description = client
            .describe_table()
            .table_name(&table_name)
            .send()
            .await
            .expect("DescribeTable should succeed");

        let ctx = format!("NAME_PATTERNS[{i}]");
        let vector_indexes = description
            .table()
            .expect("DescribeTable should return a table description")
            .vector_indexes();
        assert_eq!(vector_indexes.len(), 1, "{ctx}: expected one vector index");
        let vector_index = &vector_indexes[0];
        assert_eq!(
            vector_index.index_name(),
            Some(index.index.as_ref()),
            "{ctx}: IndexName"
        );
        assert_eq!(
            vector_index.vector_attribute().map(|a| a.attribute_name()),
            Some(vec_attr),
            "{ctx}: VectorAttribute"
        );
        assert_eq!(vector_index.dimensions(), Some(3), "{ctx}: Dimensions");
        assert_eq!(
            vector_index.distance_function(),
            Some(&VectorDistanceFunction::Cosine),
            "{ctx}: DistanceFunction"
        );
        assert_eq!(
            vector_index.index_status(),
            Some(&IndexStatus::Active),
            "{ctx}: IndexStatus"
        );
        assert_eq!(
            vector_index.projection().and_then(|p| p.projection_type()),
            Some(&ProjectionType::KeysOnly),
            "{ctx}: Projection"
        );

        info!("Deleting Alternator table '{table_name}'");
        alternator::delete_table(&client, &table_name).await;

        info!(
            "Waiting for Vector Store to drop index '{}/{}'",
            index.keyspace, index.index
        );
        alternator::wait_for_no_index(&vs_clients, &index).await;

        info!("NAME_PATTERNS[{i}] passed");
    }

    info!("finished");
}

#[e2etest::test(group = create_table)]
async fn create_table_with_two_case_distinct_vector_indexes(actors: Arc<TestActors>) {
    info!("started");

    let (client, vs_clients) = alternator::make_clients(&actors).await;

    let table_name = alternator::unique_table_name();
    let partition_key_name = "Pk-Case";
    let unique_index_name = alternator::unique_index_name();
    let lower_index_name: IndexName = unique_index_name.as_ref().to_ascii_lowercase().into();
    let upper_index_name: IndexName = unique_index_name.as_ref().to_ascii_uppercase().into();
    let lower_vector_attribute_name = "samevector";
    let upper_vector_attribute_name = "SAMEVECTOR";
    let lower_index = IndexInfo::new(
        alternator::keyspace(&table_name).as_ref(),
        lower_index_name.as_ref(),
    );
    let upper_index = IndexInfo::new(
        alternator::keyspace(&table_name).as_ref(),
        upper_index_name.as_ref(),
    );

    info!(
        "Creating Alternator table '{table_name}' with case-distinct VectorIndexes '{}' and '{}'",
        lower_index.index, upper_index.index
    );
    alternator::create_table(
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
    info!(
        "Created Alternator table '{table_name}' with case-distinct VectorIndexes '{}' and '{}'",
        lower_index.index, upper_index.index
    );

    alternator::wait_for_index(&vs_clients, &lower_index).await;
    alternator::wait_for_index(&vs_clients, &upper_index).await;

    alternator::delete_table(&client, &table_name).await;

    alternator::wait_for_no_index(&vs_clients, &lower_index).await;
    alternator::wait_for_no_index(&vs_clients, &upper_index).await;

    info!("finished");
}

/// Index names are scoped to a CQL keyspace (`alternator_<table>`), so the
/// same index name on case-distinct tables should be independent.
#[e2etest::test(group = create_table)]
async fn create_table_with_same_index_name_on_case_distinct_tables(actors: Arc<TestActors>) {
    info!("started");

    let (client, vs_clients) = alternator::make_clients(&actors).await;

    let base_name = alternator::unique_table_name();
    let table_a = base_name.to_uppercase();
    let table_b = base_name.to_lowercase();
    let shared_index_name = alternator::unique_index_name();
    let vec_attr = "vec";

    let index_a = IndexInfo::new(
        alternator::keyspace(&table_a).as_ref(),
        shared_index_name.as_ref(),
    );
    let index_b = IndexInfo::new(
        alternator::keyspace(&table_b).as_ref(),
        shared_index_name.as_ref(),
    );

    info!(
        "Creating table '{table_a}' with index '{}'",
        shared_index_name
    );
    alternator::create_table(
        &client,
        &table_a,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(shared_index_name.as_ref(), vec_attr, 3)],
    )
    .await
    .expect("CreateTable for table_a should succeed");

    info!(
        "Creating table '{table_b}' with the same index name '{}'",
        shared_index_name
    );
    alternator::create_table(
        &client,
        &table_b,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(shared_index_name.as_ref(), vec_attr, 3)],
    )
    .await
    .expect("CreateTable for table_b with same index name should succeed");

    alternator::wait_for_index(&vs_clients, &index_a).await;
    alternator::wait_for_index(&vs_clients, &index_b).await;

    alternator::delete_table(&client, &table_a).await;
    alternator::delete_table(&client, &table_b).await;

    alternator::wait_for_no_index(&vs_clients, &index_a).await;
    alternator::wait_for_no_index(&vs_clients, &index_b).await;

    info!("finished");
}

/// Alternator forbids two vector indexes on the same column with different
/// `Dimensions`.
#[e2etest::test(group = create_table)]
async fn create_table_with_two_indexes_on_same_vector_column(actors: Arc<TestActors>) {
    info!("started");

    let (client, _vs_clients) = alternator::make_clients(&actors).await;

    let table_name = alternator::unique_table_name();
    let index_a_name = alternator::unique_index_name();
    let index_b_name = alternator::unique_index_name();
    let vec_attr = "vec";

    info!(
        "Attempting CreateTable '{table_name}' with two indexes ('{}', '{}') on the same \
         column '{vec_attr}' with different Dimensions (expecting failure)",
        index_a_name, index_b_name
    );
    let result = alternator::create_table(
        &client,
        &table_name,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[
            (index_a_name.as_ref(), vec_attr, 3),
            (index_b_name.as_ref(), vec_attr, 4),
        ],
    )
    .await;

    match result {
        Err(err) => {
            info!(
                "CreateTable with two indexes on the same column with different Dimensions \
                 correctly rejected: {err}"
            );
        }
        Ok(_) => {
            alternator::delete_table(&client, &table_name).await;
            panic!(
                "Expected CreateTable with two indexes on the same vector column with \
                 different Dimensions to fail, but it succeeded."
            );
        }
    }

    info!("finished");
}

/// Positive case (192-char name) is covered by `alternator::name_patterns`.
#[e2etest::test(group = create_table)]
async fn create_table_with_over_max_length_index_name(actors: Arc<TestActors>) {
    info!("started");

    let (client, _vs_clients) = alternator::make_clients(&actors).await;

    let table_name = alternator::unique_table_name();
    let over_len = alternator::MAX_ALTERNATOR_INDEX_NAME_LEN + 1;
    let index_name =
        alternator::pad_to_len(alternator::unique_index_name().as_ref(), over_len, 'X');
    assert_eq!(index_name.len(), over_len);

    info!("Creating table with {over_len}-char index name (should be rejected)");
    let result = alternator::create_table(
        &client,
        &table_name,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(&index_name, "vec", 3)],
    )
    .await;

    match result {
        Err(err) => {
            info!("CreateTable with {over_len}-char index name correctly rejected: {err}");
        }
        Ok(_) => {
            alternator::delete_table(&client, &table_name).await;
            panic!(
                "Expected CreateTable with {over_len}-char index name to fail, but it succeeded. \
                 The actual Alternator index name limit may be higher than {}.",
                alternator::MAX_ALTERNATOR_INDEX_NAME_LEN
            );
        }
    }

    info!("finished");
}

#[e2etest::test(group = create_table)]
async fn create_table_with_boundary_dimensions(actors: Arc<TestActors>) {
    info!("started");

    let (client, vs_clients) = alternator::make_clients(&actors).await;

    let table_name = alternator::unique_table_name();
    let index_name = alternator::unique_index_name();

    let max_dimensions: usize = 16_000;

    // -- Negative: max_dimensions + 1 must be rejected --------------------------
    info!(
        "Attempting CreateTable '{table_name}' with Dimensions={} (expecting failure)",
        max_dimensions + 1
    );
    let result = alternator::create_table(
        &client,
        &table_name,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(index_name.as_ref(), "vec", max_dimensions + 1)],
    )
    .await;

    match result {
        Err(err) => {
            info!(
                "CreateTable with Dimensions={} was correctly rejected: {err}",
                max_dimensions + 1
            );
        }
        Ok(_) => {
            alternator::delete_table(&client, &table_name).await;
            panic!(
                "Expected CreateTable with Dimensions={} to fail, but it succeeded.",
                max_dimensions + 1
            );
        }
    }

    // -- Positive: max_dimensions must succeed (same table/index names) ----------
    info!("Retrying with Dimensions={max_dimensions} (expecting success)");
    alternator::create_table(
        &client,
        &table_name,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(index_name.as_ref(), "vec", max_dimensions)],
    )
    .await
    .expect("CreateTable with Dimensions=16000 should succeed");

    let index = IndexInfo::new(
        alternator::keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );
    alternator::wait_for_index(&vs_clients, &index).await;

    alternator::delete_table(&client, &table_name).await;
    info!("finished");
}

e2etest::group!(
    name = create_table,
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
