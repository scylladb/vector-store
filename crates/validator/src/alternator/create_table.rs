/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::TestActors;
use crate::common;
use crate::common::wait_for_no_index;
use async_backtrace::framed;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::AfterDeserializationInterceptorContextRef;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use e2etest::TestCase;
use httpapi::IndexInfo;
use httpapi::IndexName;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::info;

use super::alternator_keyspace;
use super::make_clients;
use super::resolve_table_names;
use super::unique_alternator_index_name;
use super::unique_alternator_table_name;
/// An SDK interceptor that captures the `VectorIndexes` extension field from
/// the raw `DescribeTable` JSON response as a [`serde_json::Value`].  The
/// caller retrieves it via [`into_captured`] after the SDK call completes and
/// performs the assertions explicitly.
///
/// Attach this to a `client.describe_table().customize().interceptor(...)` call.
/// It fires in [`read_after_deserialization`], at which point the response body
/// has already been buffered by the SDK (via its internal `read_body()` call
/// that precedes deserialization of non-streaming operations).
///
/// [`into_captured`]: VectorIndexesCaptureInterceptor::into_captured
/// [`read_after_deserialization`]: Intercept::read_after_deserialization
#[derive(Debug, Clone)]
struct VectorIndexesCaptureInterceptor {
    captured: Arc<Mutex<Option<serde_json::Value>>>,
}

impl VectorIndexesCaptureInterceptor {
    fn new() -> Self {
        Self {
            captured: Arc::new(Mutex::new(None)),
        }
    }

    /// Consume the interceptor and return the `VectorIndexes` JSON value
    /// captured during the last `DescribeTable` call.  Returns `None` if
    /// `read_after_deserialization` has not fired yet (i.e. the call has not
    /// completed or failed before the response body was available).
    fn into_captured(self) -> Option<serde_json::Value> {
        // `unwrap`: poisoning here means a panic already occurred in the
        // interceptor thread, which would surface as a test failure anyway.
        Arc::try_unwrap(self.captured)
            .expect("no other Arc clones should remain after the SDK call")
            .into_inner()
            .unwrap()
    }
}

impl Intercept for VectorIndexesCaptureInterceptor {
    fn name(&self) -> &'static str {
        "VectorIndexesCaptureInterceptor"
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

        let table = json
            .get("Table")
            .ok_or("raw DescribeTable should contain 'Table' key")?;

        // `VectorIndexes` is our extension field - capture it as-is so the
        // test can assert on the full value.  Absence is stored as None and
        // surfaced by the test assertion rather than aborting the SDK call.
        *self.captured.lock().unwrap() = table.get("VectorIndexes").cloned();

        Ok(())
    }
}

/// Asserts that `actual` JSON contains all fields from `expected`, allowing
/// additional attributes in objects. Arrays must match in length and each
/// element is checked recursively. Scalar values must be exactly equal.
fn assert_json_includes(actual: &serde_json::Value, expected: &serde_json::Value, ctx: &str) {
    match (actual, expected) {
        (serde_json::Value::Object(a), serde_json::Value::Object(e)) => {
            for (k, v) in e {
                let a_v = a
                    .get(k)
                    .unwrap_or_else(|| panic!("{ctx}: missing key {k:?}"));
                assert_json_includes(a_v, v, &format!("{ctx}.{k}"));
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(e)) => {
            assert_eq!(a.len(), e.len(), "{ctx}: array length mismatch");
            for (i, (a_v, e_v)) in a.iter().zip(e.iter()).enumerate() {
                assert_json_includes(a_v, e_v, &format!("{ctx}[{i}]"));
            }
        }
        _ => assert_eq!(actual, expected, "{ctx}: value mismatch"),
    }
}

/// Creates, describes, and deletes Alternator tables for every entry in
/// [`super::NAME_PATTERNS`] (the 2x2 matrix of plain/special x HASH-only/HASH+RANGE).
/// For each shape the test:
/// 1. Calls `CreateTable` with a vector index.
/// 2. Waits for Vector Store to discover the index.
/// 3. Calls `DescribeTable` and verifies the `VectorIndexes` extension field.
/// 4. Calls `DeleteTable` and waits for Vector Store to drop the index.
#[framed]
async fn create_describe_and_delete_table_with_vector_index(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let patterns = super::name_patterns();
    for (i, shape) in patterns.iter().enumerate() {
        info!("NAME_PATTERNS[{i}]: {shape:?}");

        let vec_attr = shape.vec().expect("NAME_PATTERNS entries always have vec");
        let (table_name, index_name, index) = resolve_table_names(shape);

        info!("Creating Alternator table '{table_name}' with VectorIndex '{index_name}'");
        super::create_alternator_table(
            &client,
            &table_name,
            shape.pk(),
            aws_sdk_dynamodb::types::ScalarAttributeType::S,
            shape.sk(),
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
        let interceptor = VectorIndexesCaptureInterceptor::new();
        client
            .describe_table()
            .table_name(&table_name)
            .customize()
            .interceptor(interceptor.clone())
            .send()
            .await
            .expect("DescribeTable should succeed");

        let ctx = format!("NAME_PATTERNS[{i}]");
        let captured = interceptor
            .into_captured()
            .expect("VectorIndexesCaptureInterceptor should have fired");
        assert_json_includes(
            &captured,
            &serde_json::json!([{
                "IndexName": index.index.as_ref(),
                "VectorAttribute": { "AttributeName": vec_attr, "Dimensions": 3 },
                "IndexStatus": "ACTIVE",
                "Projection": { "ProjectionType": "KEYS_ONLY" }
            }]),
            &ctx,
        );

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

/// Verifies that two tables whose names differ **only in letter case** can
/// each carry a vector index with the **same `IndexName`** string.
///
/// In Alternator every table lives in its own CQL keyspace
/// (`alternator_<table>`), so index names are scoped to their table.  Two
/// indexes with the same name on case-distinct tables are therefore fully
/// independent, and both should be discovered and served by Vector Store.
///
/// This also confirms that Alternator table names are case-sensitive (i.e.
/// `MyTable` and `mytable` are two separate tables).
#[framed]
async fn create_table_with_same_index_name_on_case_distinct_tables(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let base_name = unique_alternator_table_name();
    let table_a = base_name.to_uppercase();
    let table_b = base_name.to_lowercase();
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

/// Verifies that Alternator rejects a `CreateTable` request that specifies
/// **two vector indexes both pointing at the same vector attribute** (same
/// `AttributeName` and `Dimensions`, different `IndexName`).
///
/// The positive case - two indexes on *distinct* vector columns - is covered by
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
                 but it succeeded - Alternator now allows this; convert to a positive test."
            );
        }
    }

    info!("finished");
}

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
    let index_name = super::pad_to_len(unique_alternator_index_name().as_ref(), over_len, 'X');
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

/// The maximum supported vector dimension is 16000.  This test verifies:
/// 1. `Dimensions=16001` is rejected (negative case).
/// 2. `Dimensions=16000` is accepted using the same table/index names (positive case).
#[framed]
async fn create_table_with_boundary_dimensions(actors: TestActors) {
    info!("started");

    let (client, vs_clients) = make_clients(&actors).await;

    let table_name = unique_alternator_table_name();
    let index_name = unique_alternator_index_name();

    let max_dimensions: usize = 16_000;

    // -- Negative: max_dimensions + 1 must be rejected --------------------------
    info!(
        "Attempting CreateTable '{table_name}' with Dimensions={} (expecting failure)",
        max_dimensions + 1
    );
    let result = super::create_alternator_table(
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
            super::delete_alternator_table(&client, &table_name).await;
            panic!(
                "Expected CreateTable with Dimensions={} to fail, but it succeeded.",
                max_dimensions + 1
            );
        }
    }

    // -- Positive: max_dimensions must succeed (same table/index names) ----------
    info!("Retrying with Dimensions={max_dimensions} (expecting success)");
    super::create_alternator_table(
        &client,
        &table_name,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(index_name.as_ref(), "vec", max_dimensions)],
    )
    .await
    .expect("CreateTable with Dimensions=16000 should succeed");

    let index = super::IndexInfo::new(
        super::alternator_keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );
    crate::common::wait_for_index(&vs_clients[0], &index).await;

    super::delete_alternator_table(&client, &table_name).await;
    info!("finished");
}

pub(super) async fn new() -> TestCase<TestActors> {
    TestCase::empty()
        .with_init(common::DEFAULT_TEST_TIMEOUT, super::init)
        .with_cleanup(common::DEFAULT_TEST_TIMEOUT, common::cleanup)
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
            "create_table_with_boundary_dimensions",
            common::DEFAULT_TEST_TIMEOUT,
            create_table_with_boundary_dimensions,
        )
        .with_test(
            "create_table_with_over_max_length_index_name",
            common::DEFAULT_TEST_TIMEOUT,
            create_table_with_over_max_length_index_name,
        )
}
