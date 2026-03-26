/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::AfterDeserializationInterceptorContextRef;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;
use vector_store::IndexInfo;
use vector_store::IndexName;

use super::alternator_keyspace;
use super::make_clients;
use super::resolve_table_names;
use super::unique_alternator_index_name;
use super::unique_alternator_table_name;
use super::wait_for_no_index;

// ---------------------------------------------------------------------------
// VectorIndexesAssertInterceptor
// ---------------------------------------------------------------------------

/// An SDK interceptor that asserts the raw `DescribeTable` JSON response
/// contains the expected `VectorIndexes` entry.
///
/// Attach this to a `client.describe_table().customize().interceptor(...)` call.
/// It fires in [`read_after_deserialization`], at which point the response body
/// has already been buffered by the SDK (via its internal `read_body()` call
/// that precedes deserialization of non-streaming operations).  The raw bytes
/// are parsed as JSON and `Table.VectorIndexes` is verified before the
/// interceptor returns.
///
/// [`read_after_deserialization`]: Intercept::read_after_deserialization
#[derive(Debug, Clone)]
struct VectorIndexesAssertInterceptor {
    expected_index_name: String,
    expected_vec_attr: String,
    expected_dimensions: u64,
    context_label: String,
}

impl VectorIndexesAssertInterceptor {
    fn new(
        expected_index_name: &str,
        expected_vec_attr: &str,
        expected_dimensions: u64,
        context_label: &str,
    ) -> Self {
        Self {
            expected_index_name: expected_index_name.to_string(),
            expected_vec_attr: expected_vec_attr.to_string(),
            expected_dimensions,
            context_label: context_label.to_string(),
        }
    }
}

impl Intercept for VectorIndexesAssertInterceptor {
    fn name(&self) -> &'static str {
        "VectorIndexesAssertInterceptor"
    }

    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let bytes = context
            .response()
            .body()
            .bytes()
            .ok_or("expected buffered response body for DescribeTable")?;

        let json: serde_json::Value = serde_json::from_slice(bytes)?;
        let ctx = &self.context_label;

        let table = json
            .get("Table")
            .ok_or_else(|| format!("raw DescribeTable should contain 'Table' key ({ctx})"))?;

        let vector_indexes = table
            .get("VectorIndexes")
            .ok_or_else(|| format!("Table should contain 'VectorIndexes' extension field ({ctx})"))?
            .as_array()
            .ok_or_else(|| format!("VectorIndexes should be an array ({ctx})"))?;

        assert_eq!(
            vector_indexes.len(),
            1,
            "should have exactly 1 VectorIndex ({ctx})"
        );

        let vi = &vector_indexes[0];
        assert_eq!(
            vi.get("IndexName").and_then(|v| v.as_str()),
            Some(self.expected_index_name.as_str()),
            "VectorIndex IndexName should match ({ctx})"
        );

        let va = vi
            .get("VectorAttribute")
            .ok_or_else(|| format!("VectorIndex should contain VectorAttribute ({ctx})"))?;
        assert_eq!(
            va.get("AttributeName").and_then(|v| v.as_str()),
            Some(self.expected_vec_attr.as_str()),
            "VectorAttribute AttributeName should match ({ctx})"
        );
        assert_eq!(
            va.get("Dimensions").and_then(|v| v.as_u64()),
            Some(self.expected_dimensions),
            "VectorAttribute Dimensions should be {} ({ctx})",
            self.expected_dimensions
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Test: CreateTable + DescribeTable + DeleteTable across the NAME_PATTERNS 2×2 matrix
// ---------------------------------------------------------------------------

/// Creates, describes, and deletes Alternator tables for every entry in
/// [`super::NAME_PATTERNS`] (the 2×2 matrix of plain/special × HASH-only/HASH+RANGE).
///
/// For each shape the test:
/// 1. Calls `CreateTable` with a vector index.
/// 2. Waits for Vector Store to discover the index.
/// 3. Calls `DescribeTable` with [`VectorIndexesAssertInterceptor`] to verify
///    the `VectorIndexes` extension field in the raw response.
/// 4. Calls `DeleteTable`.
/// 5. Waits for Vector Store to drop the index.
///
/// This covers the create/describe/delete lifecycle, the `VectorIndexes` field
/// round-trip, and the full name-pattern matrix (plain names, max-length names
/// with special characters, HASH-only and HASH+RANGE schemas) in a single test.
#[framed]
async fn create_describe_and_delete_table_with_vector_index(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    for (i, shape) in super::NAME_PATTERNS.iter().enumerate() {
        info!("NAME_PATTERNS[{i}]: {shape:?}");

        let vec_attr = shape.vec.expect("NAME_PATTERNS entries always have vec");
        let (table_name, index_name, index) = resolve_table_names(shape);

        info!("Creating Alternator table '{table_name}' with VectorIndex '{index_name}'");
        super::create_alternator_table(
            &client,
            &table_name,
            shape.pk,
            aws_sdk_dynamodb::types::ScalarAttributeType::S,
            shape.sk,
            &[(index.index.as_ref(), vec_attr, 3)],
        )
        .await
        .expect("CreateTable with VectorIndex should succeed");

        info!(
            "Waiting for Vector Store to discover index '{}/{}'",
            index.keyspace, index.index
        );
        common::wait_for_index(&vs_clients[0], &index).await;

        info!("Describing Alternator table '{table_name}' and asserting VectorIndexes");
        client
            .describe_table()
            .table_name(&table_name)
            .customize()
            .interceptor(VectorIndexesAssertInterceptor::new(
                index.index.as_ref(),
                vec_attr,
                3,
                &format!("NAME_PATTERNS[{i}]"),
            ))
            .send()
            .await
            .expect("DescribeTable should succeed");

        info!("Deleting Alternator table '{table_name}'");
        super::delete_alternator_table(&client, &table_name).await;

        info!(
            "Waiting for Vector Store to drop index '{}/{}'",
            index.keyspace, index.index
        );
        wait_for_no_index(&vs_clients[0], &index).await;

        info!("NAME_PATTERNS[{i}] passed");
    }

    info!("finished");
}

#[framed]
async fn create_table_with_two_case_distinct_vector_indexes(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let partition_key_name = "Pk-Case";
    let unique_index_name = unique_alternator_index_name();
    let lower_index_name: IndexName = unique_index_name.as_ref().to_ascii_lowercase().into();
    let upper_index_name: IndexName = unique_index_name.as_ref().to_ascii_uppercase().into();
    let lower_vector_attribute_name = "samevector";
    let upper_vector_attribute_name = "SAMEVECTOR";
    let lower_index = IndexInfo::new(
        alternator_keyspace(&table_name).as_ref(),
        lower_index_name.as_ref(),
    );
    let upper_index = IndexInfo::new(
        alternator_keyspace(&table_name).as_ref(),
        upper_index_name.as_ref(),
    );

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
    info!(
        "Created Alternator table '{table_name}' with case-distinct VectorIndexes '{}' and '{}'",
        lower_index.index, upper_index.index
    );

    common::wait_for_index(&vs_clients[0], &lower_index).await;
    common::wait_for_index(&vs_clients[0], &upper_index).await;

    super::delete_alternator_table(&client, &table_name).await;

    wait_for_no_index(&vs_clients[0], &lower_index).await;
    wait_for_no_index(&vs_clients[0], &upper_index).await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: same index name on two case-distinct tables
// ---------------------------------------------------------------------------

/// Verifies that two separate tables can each carry a vector index with the
/// **same `IndexName`** string.
///
/// In Alternator every table lives in its own CQL keyspace
/// (`alternator_<table>`), so index names are scoped to their table.  Two
/// indexes with the same name on different tables are therefore fully
/// independent, and both should be discovered and served by Vector Store.
#[framed]
async fn create_table_with_same_index_name_on_case_distinct_tables(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_a = unique_alternator_table_name();
    let table_b = unique_alternator_table_name();
    let shared_index_name = unique_alternator_index_name();
    let vec_attr = "vec";

    let index_a = IndexInfo::new(
        alternator_keyspace(&table_a).as_ref(),
        shared_index_name.as_ref(),
    );
    let index_b = IndexInfo::new(
        alternator_keyspace(&table_b).as_ref(),
        shared_index_name.as_ref(),
    );

    info!(
        "Creating table '{table_a}' with index '{}'",
        shared_index_name
    );
    super::create_alternator_table(
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
    super::create_alternator_table(
        &client,
        &table_b,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(shared_index_name.as_ref(), vec_attr, 3)],
    )
    .await
    .expect("CreateTable for table_b with same index name should succeed");

    common::wait_for_index(&vs_clients[0], &index_a).await;
    common::wait_for_index(&vs_clients[0], &index_b).await;

    super::delete_alternator_table(&client, &table_a).await;
    super::delete_alternator_table(&client, &table_b).await;

    wait_for_no_index(&vs_clients[0], &index_a).await;
    wait_for_no_index(&vs_clients[0], &index_b).await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: two indexes on the same vector column (negative test)
// ---------------------------------------------------------------------------

/// Verifies that Alternator rejects a `CreateTable` request that specifies
/// **two vector indexes both pointing at the same vector attribute** (same
/// `AttributeName` and `Dimensions`, different `IndexName`).
///
/// The positive case — two indexes on *distinct* vector columns — is covered by
/// `create_table_with_two_case_distinct_vector_indexes`.
#[framed]
async fn create_table_with_two_indexes_on_same_vector_column(actors: TestActors) {
    info!("started");

    let (client, _vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let index_a_name = unique_alternator_index_name();
    let index_b_name = unique_alternator_index_name();
    let vec_attr = "vec";

    info!(
        "Attempting CreateTable '{table_name}' with two indexes ('{}', '{}') on the same \
         column '{vec_attr}' (expecting failure)",
        index_a_name, index_b_name
    );
    let result = super::create_alternator_table(
        &client,
        &table_name,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[
            (index_a_name.as_ref(), vec_attr, 3),
            (index_b_name.as_ref(), vec_attr, 3),
        ],
    )
    .await;

    match result {
        Err(err) => {
            info!("CreateTable with two indexes on the same column correctly rejected: {err}");
        }
        Ok(_) => {
            super::delete_alternator_table(&client, &table_name).await;
            panic!(
                "Expected CreateTable with two indexes on the same vector column to fail, \
                 but it succeeded — Alternator now allows this; convert to a positive test."
            );
        }
    }

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: CreateTable with over-max-length index name (negative test)
// ---------------------------------------------------------------------------

/// Verifies that Alternator rejects an index name exactly one character longer
/// than the maximum allowed length.  The maximum is
/// [`MAX_ALTERNATOR_INDEX_NAME_LEN`] (192 chars); a 193-char index name must be
/// rejected.
///
/// The positive side (192-char index name succeeds) is covered by the special
/// entries in [`super::NAME_PATTERNS`], exercised via
/// `create_describe_and_delete_table_with_vector_index`.
#[framed]
async fn create_table_with_over_max_length_index_name(actors: TestActors) {
    info!("started");

    let (client, _vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let over_len = super::MAX_ALTERNATOR_INDEX_NAME_LEN + 1;
    let prefix = unique_alternator_index_name();
    let index_name = if prefix.as_ref().len() >= over_len {
        prefix.as_ref()[..over_len].to_string()
    } else {
        format!("{prefix}{}", "X".repeat(over_len - prefix.as_ref().len()))
    };
    assert_eq!(index_name.len(), over_len);

    info!("Creating table with {over_len}-char index name (should be rejected)");
    let result = super::create_alternator_table(
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
            super::delete_alternator_table(&client, &table_name).await;
            panic!(
                "Expected CreateTable with {over_len}-char index name to fail, but it succeeded. \
                 The actual Alternator index name limit may be higher than {}.",
                super::MAX_ALTERNATOR_INDEX_NAME_LEN
            );
        }
    }

    info!("finished");
}

// ---------------------------------------------------------------------------
// Test: CreateTable with over-max Dimensions (negative test)
// ---------------------------------------------------------------------------

/// Attempts to create a table with `Dimensions=65536` and expects the request
/// to be rejected by Alternator or ScyllaDB.  If it unexpectedly succeeds, the
/// test fails so we know the boundary is higher than expected and the test
/// needs updating.
///
/// The positive side (large but valid dimensions) is covered by
/// `query_with_large_dimensions` in `query.rs`, which creates a table,
/// inserts items, and issues ANN queries against it.
#[framed]
async fn create_table_with_over_max_dimensions(actors: TestActors) {
    info!("started");

    let (client, _vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let index_name = unique_alternator_index_name();
    let partition_key_name = "pk";
    let vector_attribute_name = "vec";
    let over_max_dimensions: usize = 65_536;

    info!(
        "Attempting CreateTable '{table_name}' with Dimensions={over_max_dimensions} (expecting failure)"
    );
    let result = super::create_alternator_table(
        &client,
        &table_name,
        partition_key_name,
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(
            index_name.as_ref(),
            vector_attribute_name,
            over_max_dimensions,
        )],
    )
    .await;

    match result {
        Err(err) => {
            info!(
                "CreateTable with Dimensions={over_max_dimensions} was correctly rejected: {err}"
            );
        }
        Ok(_) => {
            // Clean up the accidentally-created table before failing.
            super::delete_alternator_table(&client, &table_name).await;
            panic!(
                "Expected CreateTable with Dimensions={over_max_dimensions} to fail, but it \
                 succeeded. The upper bound for Dimensions is higher than expected — update \
                 this test."
            );
        }
    }

    info!("finished");
}

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case
        .with_test(
            "create_describe_and_delete_table_with_vector_index",
            common::DEFAULT_TEST_TIMEOUT,
            create_describe_and_delete_table_with_vector_index,
        )
        .with_test(
            "create_table_with_two_case_distinct_vector_indexes",
            common::DEFAULT_TEST_TIMEOUT,
            create_table_with_two_case_distinct_vector_indexes,
        )
        .with_test(
            "create_table_with_same_index_name_on_case_distinct_tables",
            common::DEFAULT_TEST_TIMEOUT,
            create_table_with_same_index_name_on_case_distinct_tables,
        )
        .with_test(
            "create_table_with_two_indexes_on_same_vector_column",
            common::DEFAULT_TEST_TIMEOUT,
            create_table_with_two_indexes_on_same_vector_column,
        )
        .with_test(
            "create_table_with_over_max_dimensions",
            common::DEFAULT_TEST_TIMEOUT,
            create_table_with_over_max_dimensions,
        )
        .with_test(
            "create_table_with_over_max_length_index_name",
            common::DEFAULT_TEST_TIMEOUT,
            create_table_with_over_max_length_index_name,
        )
}
