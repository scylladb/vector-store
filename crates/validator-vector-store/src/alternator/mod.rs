/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod auth;
mod batch_write_item;
mod create_table;
mod delete_item;
mod key_types;
mod lwt;
mod put_item;
mod query;
mod ttl;
mod update_item;
mod update_table;

use async_backtrace::framed;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::Region;
use aws_sdk_dynamodb::operation::delete_table::DeleteTableError;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::ScalarAttributeType;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use http::HeaderValue;
use http::header::CONTENT_LENGTH;
use httpclient::HttpClient;
use serde_json::Map;
use serde_json::Value;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::info;
use tracing::warn;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;
use vector_store::ColumnName;
use vector_store::IndexInfo;
use vector_store::IndexName;
use vector_store::KeyspaceName;
use vector_store::Limit;
use vector_store::Vector;

static TABLE_COUNTER: AtomicUsize = AtomicUsize::new(0);
static INDEX_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Returns a unique table name that is always exactly **13 characters** long.
///
/// The format is `Alt-Tbl_NNNNN` where `NNNNN` is a zero-padded counter.
/// Fixed length is important so that callers can compute exact total lengths
/// when composing table names (e.g. adding a prefix to hit the 192-char max).
pub(super) fn unique_alternator_table_name() -> String {
    format!(
        "Alt-Tbl_{:05}",
        TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Returns a unique index name that is always exactly **13 characters** long.
///
/// The format is `Alt-Idx_NNNNN` where `NNNNN` is a zero-padded counter.
/// See [`unique_alternator_table_name`] for the rationale.
pub(super) fn unique_alternator_index_name() -> IndexName {
    format!(
        "Alt-Idx_{:05}",
        INDEX_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
    .into()
}

/// The number of characters consumed by the `"_" + unique_name` suffix when
/// a [`TableShape`] prefix is composed with a unique name.
///
/// This equals `1` (the `"_"` separator) + `13` (the fixed-length unique name)
/// = **14**.  It is a `const fn` so it can be used in `const` expressions
/// (e.g. compile-time assertions).
const fn alternator_unique_name_suffix_len() -> usize {
    1 + 13 // "_" + "Alt-Tbl_NNNNN" or "Alt-Idx_NNNNN"
}

/// Maximum DynamoDB table name length accepted by ScyllaDB Alternator.
///
/// ScyllaDB keyspace names are limited to 203 characters. Alternator stores
/// each DynamoDB table `T` under the CQL keyspace `alternator_T` (11-char
/// prefix), so the effective maximum table name length is `203 - 11 = 192`.
/// This was empirically verified: 192-char names succeed, 193-char names are
/// rejected.
pub(super) const MAX_ALTERNATOR_TABLE_NAME_LEN: usize = 192;

/// Maximum DynamoDB index name length accepted by ScyllaDB Alternator.
///
/// Empirically verified: Alternator's `VectorIndexes` validation rejects
/// `IndexName` values longer than 192 characters with the error:
///
/// > VectorIndexes IndexName must be at least 3 characters long and at most
/// > 192 characters long
///
/// This matches the table-name limit ([`MAX_ALTERNATOR_TABLE_NAME_LEN`]).
pub(super) const MAX_ALTERNATOR_INDEX_NAME_LEN: usize = 192;

#[framed]
pub(crate) async fn new() -> TestCase {
    let test_case = TestCase::empty()
        .with_init(common::DEFAULT_TEST_TIMEOUT, init)
        .with_cleanup(common::DEFAULT_TEST_TIMEOUT, common::cleanup);

    let test_case = create_table::register(test_case);
    let test_case = update_table::register(test_case);
    let test_case = put_item::register(test_case);
    let test_case = delete_item::register(test_case);
    let test_case = update_item::register(test_case);
    let test_case = batch_write_item::register(test_case);
    let test_case = query::register(test_case);
    let test_case = key_types::register(test_case);
    ttl::register(test_case)
}

#[framed]
pub(crate) async fn new_with_always_lwt() -> TestCase {
    let test_case = TestCase::empty().with_cleanup(common::DEFAULT_TEST_TIMEOUT, common::cleanup);
    lwt::register(test_case)
}

pub(crate) async fn new_with_auth() -> TestCase {
    let test_case = TestCase::empty().with_cleanup(common::DEFAULT_TEST_TIMEOUT, common::cleanup);
    auth::register(test_case)
}

pub(super) const ALTERNATOR_PORT: u16 = 8000;

/// In ScyllaDB Alternator, a DynamoDB table named `T` is stored under the CQL
/// keyspace `alternator_T`. Vector Store discovers indexes by scanning
/// `system_schema.indexes`, so the keyspace name is what VS uses to identify
/// an Alternator-backed index.
pub(super) fn alternator_keyspace(table_name: &str) -> KeyspaceName {
    format!("alternator_{table_name}").into()
}

/// A DynamoDB SDK interceptor that injects arbitrary key/value pairs into the
/// JSON request body before SigV4 signing.
///
/// The standard `aws-sdk-dynamodb` crate serialises requests without knowledge
/// of ScyllaDB Alternator extension fields such as `VectorIndexes`.  This
/// interceptor fires in [`modify_before_signing`], reads the already-serialised
/// JSON body, merges the provided fields, re-serialises, replaces the body, and
/// updates the `Content-Length` header so the SigV4 signature and HTTP transport
/// both operate on the correct byte count.
///
/// # Example
/// ```ignore
/// client
///     .create_table()
///     // ...
///     .customize()
///     .interceptor(JsonBodyInjectInterceptor::new([
///         ("VectorIndexes", vector_indexes_json),
///     ]))
///     .send()
///     .await?;
/// ```
///
/// [`modify_before_signing`]: Intercept::modify_before_signing
#[derive(Debug, Clone)]
pub(super) struct JsonBodyInjectInterceptor {
    fields: Map<String, Value>,
}

impl JsonBodyInjectInterceptor {
    /// Creates a new interceptor that will inject the given `fields` into every
    /// outgoing request body.
    pub(super) fn new(fields: impl IntoIterator<Item = (impl Into<String>, Value)>) -> Self {
        Self {
            fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }
}

impl Intercept for JsonBodyInjectInterceptor {
    fn name(&self) -> &'static str {
        "JsonBodyInjectInterceptor"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let new_bytes = {
            let original = context
                .request()
                .body()
                .bytes()
                .ok_or("expected in-memory body for Alternator request")?
                .to_vec();

            let mut json: Value = serde_json::from_slice(&original)?;
            let obj = json
                .as_object_mut()
                .ok_or("expected JSON object body for Alternator request")?;
            for (key, value) in &self.fields {
                obj.insert(key.clone(), value.clone());
            }
            serde_json::to_vec(&json)?
        };

        let new_len = new_bytes.len();

        let request = context.request_mut();
        *request.body_mut() = SdkBody::from(new_bytes);
        request.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&new_len.to_string()).expect("content-length value is valid"),
        );

        Ok(())
    }
}

/// Builds a DynamoDB client pointing at the ScyllaDB Alternator endpoint on
/// `db_ip`.
///
/// Dummy AWS credentials are used because authorization is disabled in tests
/// via `--alternator-enforce-authorization=false`.
async fn make_dynamodb_client(db_ip: Ipv4Addr) -> Client {
    let creds = Credentials::new("any", "any", None, None, "test");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(creds)
        .endpoint_url(format!("http://{db_ip}:{ALTERNATOR_PORT}"))
        .region(Region::new("us-east-1"))
        .load()
        .await;
    Client::new(&config)
}

/// Polls the Alternator HTTP endpoint on `db_ip` until it responds successfully.
///
/// The Alternator port may become available slightly after the CQL port (which
/// is what `db.wait_for_ready()` checks), so `init` should call this once after
/// the cluster has started before any tests run.
pub(super) async fn wait_for_alternator(db_ip: Ipv4Addr) {
    let client = make_dynamodb_client(db_ip).await;
    common::wait_for(
        || {
            let c = client.clone();
            async move { c.list_tables().limit(1).send().await.is_ok() }
        },
        format!("Alternator endpoint at http://{db_ip}:{ALTERNATOR_PORT} to be ready"),
        common::DEFAULT_TEST_TIMEOUT,
    )
    .await;
}

/// Creates a DynamoDB client pointing at the first DB node and VS HTTP clients.
pub(super) async fn make_clients(actors: &TestActors) -> (Client, Vec<HttpClient>) {
    let db_ip = actors.services_subnet.ip(common::DB_OCTET_1);
    let dynamodb_client = make_dynamodb_client(db_ip).await;
    let vs_clients = common::get_default_vs_ips(actors)
        .into_iter()
        .map(|ip| HttpClient::new((ip, common::VS_PORT).into()))
        .collect();
    (dynamodb_client, vs_clients)
}

/// Polls the VS HTTP endpoint until the given index is no longer visible
/// (i.e. `index_status` returns an error).  Used to confirm that a delete
/// action (via `UpdateTable` or `DeleteTable`) has been processed and
/// propagated to the Vector Store.
pub(super) async fn wait_for_no_index(client: &HttpClient, index: &IndexInfo) {
    common::wait_for(
        || async {
            client
                .index_status(&index.keyspace, &index.index)
                .await
                .is_err()
        },
        format!(
            "index '{}/{}' to be gone at {}",
            index.keyspace,
            index.index,
            client.url()
        ),
        Duration::from_secs(60),
    )
    .await;
}

pub(super) fn dynamo_float_list(values: impl IntoIterator<Item = f32>) -> AttributeValue {
    AttributeValue::L(
        values
            .into_iter()
            .map(|value| AttributeValue::N(value.to_string()))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Item — unified test-item representation
// ---------------------------------------------------------------------------

/// A single DynamoDB item for test operations.
///
/// `Item` is a flat map of attribute name → `AttributeValue`.  Every field
/// that identifies a row — partition key, sort key, vector, extra attributes
/// such as TTL or data columns — is stored as a regular map entry.  There are
/// no dedicated typed fields: callers supply `AttributeValue` variants
/// directly, which makes it easy to test non-string key types (`N`, `B`) as
/// well as the usual `S` keys.
///
/// Constructed via a builder chain:
///
/// ```ignore
/// // String PK, vector, extra attribute:
/// Item::new("pk", AttributeValue::S("pk-a".into()))
///     .vec("vec", [1.0, 2.0, 3.0])
///     .attr("ttl", AttributeValue::N("1234".into()))
///
/// // Shape-aware constructor (handles HASH-only / HASH+RANGE automatically):
/// Item::key(shape.pk, shape.sk, "vec", "a")
///     .attr("ttl", AttributeValue::N(ttl_epoch(2).to_string()))
///
/// // Number PK (no helper shorthand needed):
/// Item::new("pk", AttributeValue::N("42".into()))
///     .vec("vec", [1.0, 0.0, 0.0])
/// ```
#[derive(Debug, Clone)]
pub(super) struct Item(pub HashMap<String, AttributeValue>);

impl Item {
    /// Creates a new item containing only the partition-key attribute.
    ///
    /// `pk_attr` is the attribute name (e.g. `"pk"` or `shape.pk`);
    /// `pk_val` is any `AttributeValue` — `S`, `N`, or `B`.
    pub(super) fn new(pk_attr: &str, pk_val: AttributeValue) -> Self {
        let mut map = HashMap::new();
        map.insert(pk_attr.to_string(), pk_val);
        Self(map)
    }

    /// Constructs an item whose keys follow the table-shape convention using
    /// plain `String` partition and sort-key values.
    ///
    /// - **HASH-only** (`sk_attr = None`): PK attribute = `"{pk_prefix}-{suffix}"`.
    /// - **HASH+RANGE** (`sk_attr = Some`): PK attribute = `pk_prefix`, SK
    ///   attribute = `suffix`.
    ///
    /// Using letter suffixes (`"a"`, `"b"`, `"c"`) keeps item names consistent
    /// across both table shapes:
    ///
    /// ```ignore
    /// let a = Item::key(shape.pk, shape.sk, "pk", "a").vec(shape.vec.unwrap(), [1.0, 1.0, 1.0]);
    /// let b = Item::key(shape.pk, shape.sk, "pk", "b").vec(shape.vec.unwrap(), [1.0, 2.0, 4.0]);
    /// // HASH-only:   pk attr = "pk-a" / "pk-b"  (distinct PKs, no SK)
    /// // HASH+RANGE:  pk attr = "pk", sk attr = "a" / "b"  (shared PK, distinct SKs)
    /// ```
    pub(super) fn key(pk_attr: &str, sk_attr: Option<&str>, pk_prefix: &str, suffix: &str) -> Self {
        if let Some(sk) = sk_attr {
            Self::new(pk_attr, AttributeValue::S(pk_prefix.to_string()))
                .sk(sk, AttributeValue::S(suffix.to_string()))
        } else {
            Self::new(pk_attr, AttributeValue::S(format!("{pk_prefix}-{suffix}")))
        }
    }

    /// Adds a sort-key attribute.
    pub(super) fn sk(mut self, sk_attr: &str, sk_val: AttributeValue) -> Self {
        self.0.insert(sk_attr.to_string(), sk_val);
        self
    }

    /// Conditionally adds a sort-key attribute.
    ///
    /// When `sk_attr` is `Some`, inserts `sk_val` under that attribute name.
    /// When `None` (HASH-only table), returns `self` unchanged.
    ///
    /// ```ignore
    /// let item = Item::new(shape.pk, AttributeValue::S("pk-1".into()))
    ///     .maybe_sk(shape.sk, AttributeValue::S("sk-1".into()))
    ///     .vec(shape.vec.unwrap(), [1.0, 1.0, 1.0]);
    /// ```
    pub(super) fn maybe_sk(self, sk_attr: Option<&str>, sk_val: AttributeValue) -> Self {
        if let Some(sk) = sk_attr {
            self.sk(sk, sk_val)
        } else {
            self
        }
    }

    /// Adds the vector attribute as a DynamoDB `L` (list of numbers).
    pub(super) fn vec(mut self, vec_attr: &str, v: [f32; 3]) -> Self {
        self.0.insert(vec_attr.to_string(), dynamo_float_list(v));
        self
    }

    /// Adds an arbitrary extra attribute (TTL, data column, filter column, …).
    pub(super) fn attr(mut self, name: impl Into<String>, val: AttributeValue) -> Self {
        self.0.insert(name.into(), val);
        self
    }
}

pub(super) async fn wait_for_index_count(
    client: &HttpClient,
    index: &IndexInfo,
    expected_count: usize,
) {
    common::wait_for_value(
        || async {
            match client.index_status(&index.keyspace, &index.index).await {
                Ok(resp)
                    if resp.status == vector_store::httproutes::IndexStatus::Serving
                        && resp.count == expected_count =>
                {
                    Some(())
                }
                _ => None,
            }
        },
        format!(
            "index '{}/{}' to report count {} at {}",
            index.keyspace,
            index.index,
            expected_count,
            client.url()
        ),
        Duration::from_secs(60),
    )
    .await;
}

/// Waits until ANN returns exactly the expected items in the expected order.
///
/// Polls the ANN endpoint with `Limit = expected.len()` until the returned
/// items match `expected` position-by-position.  Both HASH-only and
/// HASH+RANGE tables are handled transparently: whichever key attributes are
/// present in the expected `Item` maps (pk always, sk when the table has one)
/// are compared; other attributes are ignored.
///
/// ANN results are deterministic for the datasets used in these tests
/// (cosine distances are unambiguous), so order is always asserted.
pub(super) async fn wait_for_ann(
    client: &HttpClient,
    index: &IndexInfo,
    pk_name: &str,
    sk_name: Option<&str>,
    query_vector: [f32; 3],
    expected: &[Item],
) {
    let pk_column: ColumnName = pk_name.into();
    let sk_column: Option<ColumnName> = sk_name.map(|s| s.into());
    let expect_count = expected.len();

    // Pre-build the expected key strings for each position.
    let expected_keys: Vec<(String, Option<String>)> = expected
        .iter()
        .map(|item| {
            let pk = av_to_key_string(item.0.get(pk_name).expect("expected Item has no pk attr"));
            let sk = sk_name
                .map(|sk| av_to_key_string(item.0.get(sk).expect("expected Item has no sk attr")));
            (pk, sk)
        })
        .collect();

    common::wait_for_value(
        || async {
            let (primary_keys, _distances, _scores) = client
                .ann(
                    &index.keyspace,
                    &index.index,
                    Vector::from(query_vector.to_vec()),
                    None,
                    Limit::from(
                        NonZeroUsize::new(expect_count)
                            .expect("expected ANN result set is non-empty"),
                    ),
                )
                .await;

            let pk_values = primary_keys.get(&pk_column)?;
            if pk_values.len() != expect_count {
                return None;
            }

            let sk_values: Option<&Vec<Value>> = match sk_column.as_ref() {
                None => None,
                Some(col) => Some(primary_keys.get(col)?),
            };

            for (i, (exp_pk, exp_sk)) in expected_keys.iter().enumerate() {
                let got_pk = av_to_key_string(&json_value_to_av(&pk_values[i]));
                if got_pk != *exp_pk {
                    return None;
                }
                if let (Some(sk_vals), Some(exp_sk_str)) = (sk_values, exp_sk) {
                    let got_sk = av_to_key_string(&json_value_to_av(&sk_vals[i]));
                    if got_sk != *exp_sk_str {
                        return None;
                    }
                }
            }

            Some(())
        },
        format!(
            "index '{}/{}' to return expected ANN keys at {}",
            index.keyspace,
            index.index,
            client.url()
        ),
        Duration::from_secs(60),
    )
    .await
}

/// Converts an `AttributeValue` to a canonical string representation used
/// for set-membership comparisons in [`wait_for_ann`].
///
/// - `S(s)` → `s` (the string itself)
/// - `N(n)` → `n` (the decimal string, as DynamoDB stores it)
/// - `B(b)` → base64-encoded bytes (for set equality)
/// - Everything else → debug representation (fallback, not normally reached)
fn av_to_key_string(av: &AttributeValue) -> String {
    match av {
        AttributeValue::S(s) => s.clone(),
        AttributeValue::N(n) => n.clone(),
        AttributeValue::B(b) => {
            use std::fmt::Write;
            b.as_ref().iter().fold(String::new(), |mut s, byte| {
                let _ = write!(s, "{byte:02x}");
                s
            })
        }
        other => format!("{other:?}"),
    }
}

/// Converts a `serde_json::Value` returned by the VS ANN endpoint into an
/// `AttributeValue`.
///
/// The VS HTTP layer serialises key column values as JSON scalars:
/// - CQL `text` / `ascii` → `Value::String`  → `AttributeValue::S`
/// - CQL `int` / `bigint` / `float` / `double` → `Value::Number` → `AttributeValue::N`
///
/// CQL `blob` (DynamoDB `B` key type) is not yet serialised by the VS HTTP
/// layer (`try_to_json` hits `unimplemented!()` for `CqlValue::Blob`); a
/// test that reaches this path will panic at the server side before this
/// function is called.
fn json_value_to_av(v: &serde_json::Value) -> AttributeValue {
    match v {
        serde_json::Value::String(s) => AttributeValue::S(s.clone()),
        serde_json::Value::Number(n) => AttributeValue::N(n.to_string()),
        other => panic!("unexpected ANN key JSON value: {other:?}"),
    }
}

/// Creates an Alternator table with the given key schema and optional vector
/// indexes.
///
/// - `partition_key_name`: name of the hash key attribute.
/// - `pk_type`: DynamoDB scalar type for the partition key (`S`, `N`, or `B`).
///   Pass `ScalarAttributeType::S` for the usual string key.
/// - `sort_key_name`: pass `Some("sk")` for HASH+RANGE, `None` for HASH-only.
///   Sort keys always use `ScalarAttributeType::S`.
/// - `vector_indexes`: slice of `(index_name, vector_attribute, dimensions)`.
///   Pass `&[]` for a plain table with no vector index.
///
/// Returns the raw SDK result so callers can assert on expected errors.
/// For convenience, panicking call sites use `.await.expect(...)`.
pub(super) async fn create_alternator_table(
    client: &Client,
    table_name: &str,
    partition_key_name: &str,
    pk_type: aws_sdk_dynamodb::types::ScalarAttributeType,
    sort_key_name: Option<&str>,
    vector_indexes: &[(&str, &str, usize)],
) -> Result<
    aws_sdk_dynamodb::operation::create_table::CreateTableOutput,
    aws_sdk_dynamodb::error::SdkError<aws_sdk_dynamodb::operation::create_table::CreateTableError>,
> {
    let mut builder = client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name(partition_key_name)
                .attribute_type(pk_type)
                .build()
                .expect("failed to build AttributeDefinition"),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name(partition_key_name)
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .expect("failed to build KeySchemaElement"),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest);

    if let Some(sk) = sort_key_name {
        builder = builder
            .attribute_definitions(
                aws_sdk_dynamodb::types::AttributeDefinition::builder()
                    .attribute_name(sk)
                    .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                    .build()
                    .expect("failed to build AttributeDefinition"),
            )
            .key_schema(
                aws_sdk_dynamodb::types::KeySchemaElement::builder()
                    .attribute_name(sk)
                    .key_type(aws_sdk_dynamodb::types::KeyType::Range)
                    .build()
                    .expect("failed to build KeySchemaElement"),
            );
    }

    if vector_indexes.is_empty() {
        builder.send().await
    } else {
        let indexes_json = serde_json::json!(
            vector_indexes
                .iter()
                .map(|(index_name, vec_attr, dims)| {
                    serde_json::json!({
                        "IndexName": index_name,
                        "VectorAttribute": {
                            "AttributeName": vec_attr,
                            "Dimensions": dims
                        }
                    })
                })
                .collect::<Vec<_>>()
        );
        builder
            .customize()
            .interceptor(JsonBodyInjectInterceptor::new([(
                "VectorIndexes",
                indexes_json,
            )]))
            .send()
            .await
    }
}

/// Issues an `UpdateTable` request with the given `VectorIndexUpdates` JSON.
pub(super) async fn issue_update_table(
    client: &Client,
    table_name: &str,
    vector_index_updates: Value,
) {
    client
        .update_table()
        .table_name(table_name)
        .customize()
        .interceptor(JsonBodyInjectInterceptor::new([(
            "VectorIndexUpdates",
            vector_index_updates,
        )]))
        .send()
        .await
        .expect("UpdateTable with VectorIndexUpdates should succeed");
}

pub(super) async fn delete_alternator_table(client: &Client, table_name: &str) {
    client
        .delete_table()
        .table_name(table_name)
        .send()
        .await
        .expect("DeleteTable should succeed");
}

/// Asserts that an operation result is an error whose code or message
/// contains `expected_err`.
///
/// Accepts the raw `Result` returned by any AWS SDK send call, so call
/// sites read as a single expression:
///
/// ```ignore
/// super::assert_service_error(
///     batch_write_items(&ctx, &[valid2, wrong_type], &[]).await,
///     "ValidationException",
/// );
/// ```
pub(super) fn assert_service_error<O, E>(
    result: Result<O, aws_sdk_dynamodb::error::SdkError<E>>,
    expected_err: &str,
) where
    O: std::fmt::Debug,
    E: aws_smithy_types::error::metadata::ProvideErrorMetadata,
{
    use aws_sdk_dynamodb::error::ProvideErrorMetadata as _;
    let err = result.expect_err("operation should have been rejected");
    let code = err.code().unwrap_or("");
    let message = err.message().unwrap_or("");
    assert!(
        code.contains(expected_err) || message.contains(expected_err),
        "expected error containing {expected_err:?}, got code={code:?} message={message:?}"
    );
}

/// Standard test init: starts ScyllaDB with the Alternator endpoint enabled on
/// each node's own IP, alongside the Vector Store.
#[framed]
pub async fn init(actors: TestActors) {
    info!("started");

    let mut scylla_configs = common::get_default_scylla_node_configs(&actors).await;

    for config in &mut scylla_configs {
        let node_ip = config.db_ip;
        config.args.extend([
            format!("--alternator-port={ALTERNATOR_PORT}"),
            format!("--alternator-address={node_ip}"),
            "--alternator-write-isolation=only_rmw_uses_lwt".to_string(),
            "--alternator-enforce-authorization=false".to_string(),
            "--alternator-ttl-period-in-seconds=1".to_string(),
        ]);
    }

    // Capture db_ip before actors is moved into init_with_config.
    let db_ip = actors.services_subnet.ip(common::DB_OCTET_1);
    let vs_configs = common::get_default_vs_node_configs(&actors).await;
    common::init_with_config(actors, scylla_configs, vs_configs).await;

    wait_for_alternator(db_ip).await;
    info!("finished");
}

// ---------------------------------------------------------------------------
// TableShape — unified test-configuration type
// ---------------------------------------------------------------------------

/// Describes the full set of names and optional features for a test table.
///
/// Every integration test that creates an Alternator table goes through
/// [`TableContext::create`], which accepts a `TableShape` value.  The struct
/// controls:
///
/// - **Table and index names** — `table_prefix` / `index_prefix`.  When
///   non-empty the final name is `"{prefix}_{unique_suffix}"`, which lets
///   tests exercise long or special-character names.  When `""`, a plain
///   short unique name is used.
///
/// - **Attribute names** — `pk`, `sk`, `vec`.  These are the names of the
///   partition key, (optional) sort key, and (optional) vector attribute
///   respectively.  The `sk` and `vec` fields being `Some`/`None` also
///   controls the table **shape**: whether a sort key (HASH+RANGE) and/or
///   a vector index are created.
///
/// # Name patterns
///
/// Tests iterate [`NAME_PATTERNS`], a 2×2 matrix of 4 `TableShape` entries
/// that covers all combinations of:
///
/// | | Plain names | Special names |
/// |---|---|---|
/// | **HASH-only** | entry 0 | entry 2 |
/// | **HASH+RANGE** | entry 1 | entry 3 |
///
/// The special-name entries pack every tricky character category into one
/// name (leading digit, colon, single-quote, double-quote, backslash,
/// `@#$`, spaces, Cyrillic, emoji) and pad every name to its maximum
/// allowed length (192 chars for table/index, 255 bytes for attributes).
#[derive(Debug, Clone)]
pub(super) struct TableShape {
    pub table_prefix: &'static str,
    pub index_prefix: &'static str,
    pub pk: &'static str,
    pub sk: Option<&'static str>,
    pub vec: Option<&'static str>,
    /// Scalar type of the partition key attribute.  Use `ScalarAttributeType::S`
    /// for the default string key; `N` or `B` for numeric / binary key tests.
    pub pk_type: ScalarAttributeType,
}

// -- Special name components ------------------------------------------------
//
// Table/index name prefixes: 178 chars each so that the total name
// (prefix + "_" + 13-char unique suffix = prefix + 14) hits the
// MAX_ALTERNATOR_TABLE_NAME_LEN / MAX_ALTERNATOR_INDEX_NAME_LEN of 192.
//
// Attribute names (pk, sk, vec): padded to exactly 255 bytes (the
// DynamoDB attribute name limit).  The base contains ASCII special
// characters *plus* Unicode (Cyrillic + emoji), so the byte count of
// the base exceeds its char count.  ASCII 'X' padding fills the rest.

/// Special table-name prefix (178 chars = 33-char base + 145-char pad).
const SPECIAL_TABLE_PREFIX: &str = concat!(
    "123-With.Hyphens_UPPER.MixedCase-",
    "maxlen-pad.maxlen-pad.maxlen-pad.maxlen-pad.maxlen-pad.",
    "maxlen-pad.maxlen-pad.maxlen-pad.maxlen-pad.maxlen-pad.",
    "maxlen-pad.maxlen-pad.maxlen-pad.ma",
);

/// Special index-name prefix (178 chars = 37-char base + 141-char pad).
const SPECIAL_INDEX_PREFIX: &str = concat!(
    "123-idx.With.Hyphens_UPPER-MixedCase-",
    "maxlen-pad.maxlen-pad.maxlen-pad.maxlen-pad.maxlen-pad.",
    "maxlen-pad.maxlen-pad.maxlen-pad.maxlen-pad.maxlen-pad.",
    "maxlen-pad.maxlen-pad.maxlen-pa",
);

/// Special partition-key name (255 bytes = 47-byte base + 208 ASCII pad).
const SPECIAL_PK: &str = concat!(
    "1:pk'.\".\\@#$ with spaces кириллица🦀",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
);

/// Special sort-key name (255 bytes = 47-byte base + 208 ASCII pad).
const SPECIAL_SK: &str = concat!(
    "1:sk'.\".\\@#$ with spaces кириллица🦀",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
);

/// Special vector-attribute name (255 bytes = 48-byte base + 207 ASCII pad).
const SPECIAL_VEC: &str = concat!(
    "1:vec'.\".\\@#$ with spaces кириллица🦀",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
);

// Compile-time assertions: table/index prefixes hit the max-length limit,
// and attribute names are exactly 255 UTF-8 bytes.
const _: () = assert!(
    SPECIAL_TABLE_PREFIX.len()
        == MAX_ALTERNATOR_TABLE_NAME_LEN - alternator_unique_name_suffix_len()
);
const _: () = assert!(
    SPECIAL_INDEX_PREFIX.len()
        == MAX_ALTERNATOR_INDEX_NAME_LEN - alternator_unique_name_suffix_len()
);
const _: () = assert!(SPECIAL_PK.len() == 255);
const _: () = assert!(SPECIAL_SK.len() == 255);
const _: () = assert!(SPECIAL_VEC.len() == 255);

/// The 2×2 matrix of table shapes exercised by every `_with_names` test.
///
/// Each basic-operation test (`put_item`, `delete_item`, `update_item`,
/// `batch_write_item`, etc.) loops over this slice so that every operation
/// is verified against all four combinations:
///
/// | Entry | Names | Key schema |
/// |-------|-------|------------|
/// | 0 | plain | HASH-only |
/// | 1 | plain | HASH+RANGE |
/// | 2 | special (max-length + special chars) | HASH-only |
/// | 3 | special (max-length + special chars) | HASH+RANGE |
pub(super) const NAME_PATTERNS: &[TableShape] = &[
    // 0: plain, HASH-only
    TableShape {
        table_prefix: "",
        index_prefix: "",
        pk: "pk",
        sk: None,
        vec: Some("vec"),
        pk_type: ScalarAttributeType::S,
    },
    // 1: plain, HASH+RANGE
    TableShape {
        table_prefix: "",
        index_prefix: "",
        pk: "pk",
        sk: Some("sk"),
        vec: Some("vec"),
        pk_type: ScalarAttributeType::S,
    },
    // 2: special, HASH-only
    TableShape {
        table_prefix: SPECIAL_TABLE_PREFIX,
        index_prefix: SPECIAL_INDEX_PREFIX,
        pk: SPECIAL_PK,
        sk: None,
        vec: Some(SPECIAL_VEC),
        pk_type: ScalarAttributeType::S,
    },
    // 3: special, HASH+RANGE
    TableShape {
        table_prefix: SPECIAL_TABLE_PREFIX,
        index_prefix: SPECIAL_INDEX_PREFIX,
        pk: SPECIAL_PK,
        sk: Some(SPECIAL_SK),
        vec: Some(SPECIAL_VEC),
        pk_type: ScalarAttributeType::S,
    },
];

// ---------------------------------------------------------------------------
// TableContext — shared test fixture
// ---------------------------------------------------------------------------

/// Resolves the final table name, index name, and [`IndexInfo`] from a
/// [`TableShape`].  When a prefix is non-empty the final name is
/// `"{prefix}_{unique_suffix}"`; when empty a plain unique name is used.
///
/// This is the single source of truth for name construction — used by both
/// [`TableContext::create`] and tests that manage the table lifecycle directly.
pub(super) fn resolve_table_names(shape: &TableShape) -> (String, String, IndexInfo) {
    let table_name = if shape.table_prefix.is_empty() {
        unique_alternator_table_name().to_string()
    } else {
        format!("{}_{}", shape.table_prefix, unique_alternator_table_name())
    };
    let index_name = if shape.index_prefix.is_empty() {
        unique_alternator_index_name().to_string()
    } else {
        format!("{}_{}", shape.index_prefix, unique_alternator_index_name())
    };
    let index = IndexInfo::new(alternator_keyspace(&table_name).as_ref(), &index_name);
    (table_name, index_name, index)
}

// ---------------------------------------------------------------------------

/// A test fixture that encapsulates the repetitive create-table → wait →
/// operate → assert → cleanup cycle used by most Alternator integration tests.
///
/// # Construction
///
/// [`TableContext::create`] is the single constructor.  It accepts a
/// [`TableShape`] reference that controls:
///
/// - **Table/index names** — when `table_prefix` / `index_prefix` are
///   non-empty the final name is `"{prefix}_{unique_suffix}"`.  When `""`,
///   a plain short unique name is used.
///
/// - **Key schema** — `shape.sk` being `Some` produces a HASH+RANGE table,
///   `None` produces HASH-only.
///
/// - **Vector index** — `shape.vec` being `Some` creates a vector index and
///   waits for VS to discover it.  `None` creates a plain table with no
///   index — useful for `UpdateTable` tests that add the index themselves.
///
/// # Cleanup
///
/// [`TableContext::done`] is idempotent: it swallows
/// `ResourceNotFoundException` so that tests which explicitly call
/// `DeleteTable` (e.g. `delete_table.rs`) can still call `done()`
/// safely at the end.
pub(super) struct TableContext {
    pub client: Client,
    pub vs_client: HttpClient,
    pub table_name: String,
    pub index: IndexInfo,
    pub pk: String,
    pub sk: Option<String>,
    pub vec_attr: Option<String>,
}

impl TableContext {
    /// Creates a new Alternator table and (optionally) a vector index.
    pub(super) async fn create(actors: &TestActors, shape: &TableShape) -> Self {
        let (client, vs_clients) = make_clients(actors).await;
        let vs_client = vs_clients
            .into_iter()
            .next()
            .expect("need at least one VS client");

        let (table_name, _, index) = resolve_table_names(shape);

        let indexes: Vec<(&str, &str, usize)> = match shape.vec {
            Some(va) => vec![(index.index.as_ref(), va, 3)],
            None => vec![],
        };
        create_alternator_table(
            &client,
            &table_name,
            shape.pk,
            shape.pk_type.clone(),
            shape.sk,
            &indexes,
        )
        .await
        .expect("CreateTable should succeed");
        if shape.vec.is_some() {
            common::wait_for_index(&vs_client, &index).await;
        }

        Self {
            client,
            vs_client,
            table_name,
            index,
            pk: shape.pk.to_string(),
            sk: shape.sk.map(str::to_string),
            vec_attr: shape.vec.map(str::to_string),
        }
    }

    // -- Item operations (unified for HASH-only and HASH+RANGE) ---------------

    /// Inserts an item into the table.
    ///
    /// All attributes in the item (including PK, SK, vector, and extras) are
    /// written verbatim to DynamoDB.  The item must contain the vector
    /// attribute keyed under `ctx.vec_attr`; there is no runtime check —
    /// DynamoDB will reject a put that is missing required schema columns.
    pub(super) async fn put(&self, item: &Item) {
        let mut req = self.client.put_item().table_name(&self.table_name);
        for (attr_name, attr_val) in &item.0 {
            req = req.item(attr_name, attr_val.clone());
        }
        req.send().await.expect("PutItem should succeed");
    }

    /// Inserts an item into the table, asserting that Scylla rejects it with
    /// an error message containing `expected_err`.
    ///
    /// Use this when a write is expected to be rejected — e.g. putting an item
    /// with a wrong vector attribute type when a vector index already exists.
    pub(super) async fn put_expecting_error(&self, item: &Item, expected_err: &str) {
        let mut req = self.client.put_item().table_name(&self.table_name);
        for (attr_name, attr_val) in &item.0 {
            req = req.item(attr_name, attr_val.clone());
        }
        assert_service_error(req.send().await, expected_err);
    }

    /// Creates a table, inserts the given items, and optionally adds a vector
    /// index via `UpdateTable` (the initial-scan path).
    ///
    /// When `shape.vec` is `Some`, the method behaves exactly like before:
    /// creates the table without an index, inserts items, issues `UpdateTable`
    /// to create the index, and waits for VS to serve it with the correct
    /// count.  The returned context has `vec_attr` set.
    ///
    /// When `shape.vec` is `None`, only the first two steps are performed:
    /// the table is created and the items are inserted, but no index is added.
    /// The returned context has `vec_attr: None`.  This allows the caller to
    /// confirm that no index exists (`wait_for_no_index`), then add the index
    /// itself via `UpdateTable`.
    ///
    /// All `items` are expected to carry a valid vector and will be counted
    /// toward the expected index count.  Use [`Self::create_with_invalid_data`]
    /// when the dataset also contains items with invalid or missing vectors
    /// that VS should skip.
    ///
    /// All item attributes (PK, SK, vector, extras) are written verbatim.
    pub(super) async fn create_with_data(
        actors: &TestActors,
        shape: &TableShape,
        items: &[Item],
    ) -> Self {
        Self::create_with_invalid_data(actors, shape, items, &[]).await
    }

    /// Like [`Self::create_with_data`] but also inserts `invalid_items` into
    /// the table before the index is created.
    ///
    /// `items` are the valid rows — they carry a well-formed vector and are
    /// counted toward the expected index count that VS must reach before the
    /// function returns.
    ///
    /// `invalid_items` are inserted into the table (Scylla accepts any value
    /// before a vector index exists) but are expected to be skipped by VS
    /// during the initial scan (wrong type, missing attribute, wrong
    /// dimensions, etc.).  They are **not** counted toward the expected index
    /// count.
    pub(super) async fn create_with_invalid_data(
        actors: &TestActors,
        shape: &TableShape,
        items: &[Item],
        invalid_items: &[Item],
    ) -> Self {
        // 1. Create table without a vector index.
        let no_vec_shape = TableShape {
            vec: None,
            pk_type: shape.pk_type.clone(),
            ..*shape
        };
        let ctx = Self::create(actors, &no_vec_shape).await;

        // 2. Insert all items — write every attribute in the item verbatim.
        for item in items.iter().chain(invalid_items.iter()) {
            let mut req = ctx.client.put_item().table_name(&ctx.table_name);
            for (attr_name, attr_val) in &item.0 {
                req = req.item(attr_name, attr_val.clone());
            }
            req.send().await.expect("PutItem should succeed");
        }

        // If no vec attribute was requested, return the context as-is.
        let vec_attr = match shape.vec {
            None => return ctx,
            Some(va) => va,
        };

        // 3. Add the vector index via UpdateTable (initial-scan path).
        issue_update_table(
            &ctx.client,
            &ctx.table_name,
            serde_json::json!([{
                "Create": {
                    "IndexName": ctx.index.index.as_ref(),
                    "VectorAttribute": {
                        "AttributeName": vec_attr,
                        "Dimensions": 3
                    }
                }
            }]),
        )
        .await;

        // 4. Wait for VS to serve the index.  Only `items` (the valid ones)
        //    are counted; `invalid_items` are skipped by VS.
        common::wait_for_index(&ctx.vs_client, &ctx.index).await;
        wait_for_index_count(&ctx.vs_client, &ctx.index, items.len()).await;

        // 5. Return context with vec_attr correctly set.
        Self {
            vec_attr: Some(vec_attr.to_string()),
            ..ctx
        }
    }

    // -- Wait helpers -------------------------------------------------------

    pub(super) async fn wait_for_count(&self, n: usize) {
        wait_for_index_count(&self.vs_client, &self.index, n).await;
    }

    /// Waits until ANN returns exactly the expected items in the expected order.
    pub(super) async fn wait_for_ann(&self, qvec: [f32; 3], expected: &[Item]) {
        wait_for_ann(
            &self.vs_client,
            &self.index,
            &self.pk,
            self.sk.as_deref(),
            qvec,
            expected,
        )
        .await
    }

    // -- Cleanup ------------------------------------------------------------

    /// Deletes the Alternator table. Idempotent — swallows
    /// `ResourceNotFoundException` so that tests which explicitly call
    /// `DeleteTable` can still call this safely at the end.
    pub(super) async fn done(&self) {
        match self
            .client
            .delete_table()
            .table_name(&self.table_name)
            .send()
            .await
        {
            Ok(_) => {}
            Err(err) => {
                if err
                    .as_service_error()
                    .is_some_and(|e| matches!(e, DeleteTableError::ResourceNotFoundException(_)))
                {
                    // Already deleted — nothing to do.
                } else {
                    warn!(
                        "DeleteTable for '{}' failed unexpectedly: {err}",
                        self.table_name
                    );
                }
            }
        }
    }
}
