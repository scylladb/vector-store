/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod batch_write_item;
mod create_table;
mod delete_item;
mod put_item;
mod query;
mod ttl;
mod update_item;
mod update_table;

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::Region;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
use aws_sdk_dynamodb::operation::create_table::CreateTableOutput;
use aws_sdk_dynamodb::operation::delete_table::DeleteTableError;
use aws_sdk_dynamodb::types::AttributeDefinition;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::types::BillingMode;
use aws_sdk_dynamodb::types::KeySchemaElement;
use aws_sdk_dynamodb::types::KeyType;
use aws_sdk_dynamodb::types::ScalarAttributeType;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::config_bag::ConfigBag;
use e2etest::TestCase;
use http::HeaderValue;
use http::header::CONTENT_LENGTH;
use httpapi::ColumnName;
use httpapi::IndexInfo;
use httpapi::IndexName;
use httpapi::KeyspaceName;
use httpapi::Limit;
use httpapi::Vector;
use httpclient::HttpClient;
use serde_json::Map;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Write;
use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tracing::info;
use tracing::warn;
static TABLE_COUNTER: AtomicUsize = AtomicUsize::new(0);
static INDEX_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_alternator_table_name() -> String {
    common::unique_name("Alt-Tbl", &TABLE_COUNTER)
}

fn unique_alternator_index_name() -> IndexName {
    common::unique_name("Alt-Idx", &INDEX_COUNTER).into()
}

/// Maximum DynamoDB table name length accepted by ScyllaDB Alternator.
///
/// DynamoDB allows up to 255 characters, but ScyllaDB lowers this:
/// Scylla stores each table in a directory named `<table>-<UUID>` (33 extra
/// bytes), and Linux limits directory names to 255 bytes, capping CQL names
/// at 222. Alternator then lowers it further to 192 to leave room for the
/// `_scylla_cdc_log` suffix (15 chars) added when CDC/streams are enabled.
/// See ScyllaDB `alternator/executor_util.hh`: `max_table_name_length = 192`.
const MAX_ALTERNATOR_TABLE_NAME_LEN: usize = 192;

/// Maximum DynamoDB vector index name length accepted by ScyllaDB Alternator.
///
/// Alternator validates index names with the same function and limit as table
/// names. Documented in ScyllaDB `docs/alternator/vector-search.md`:
/// "`IndexName`: 3–192 characters, matching the regex `[a-zA-Z0-9._-]+`".
const MAX_ALTERNATOR_INDEX_NAME_LEN: usize = MAX_ALTERNATOR_TABLE_NAME_LEN;

/// Maximum DynamoDB attribute name length in bytes.
///
/// Per AWS DynamoDB naming rules: attribute names must be between 1 and 255
/// bytes long (UTF-8 encoded).
/// See <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html>.
const MAX_ALTERNATOR_ATTRIBUTE_NAME_LEN: usize = 255;

#[framed]
pub(crate) async fn test_cases() -> Vec<(String, TestCase<TestActors>)> {
    vec![
        ("alternator_create_table".into(), create_table::new().await),
        ("alternator_update_table".into(), update_table::new().await),
        ("alternator_put_item".into(), put_item::new().await),
        ("alternator_delete_item".into(), delete_item::new().await),
        ("alternator_update_item".into(), update_item::new().await),
        (
            "alternator_batch_write_item".into(),
            batch_write_item::new().await,
        ),
        ("alternator_ttl".into(), ttl::new().await),
        ("alternator_query".into(), query::new().await),
    ]
}

const ALTERNATOR_PORT: u16 = 8000;

/// In ScyllaDB Alternator, a DynamoDB table named `T` is stored under the CQL
/// keyspace `alternator_T`. Vector Store discovers indexes by scanning
/// `system_schema.indexes`, so the keyspace name is what VS uses to identify
/// an Alternator-backed index.
fn alternator_keyspace(table_name: &str) -> KeyspaceName {
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
struct JsonBodyInjectInterceptor {
    fields: Map<String, Value>,
}

impl JsonBodyInjectInterceptor {
    /// Creates a new interceptor that will inject the given `fields` into every
    /// outgoing request body.
    fn new(fields: impl IntoIterator<Item = (impl Into<String>, Value)>) -> Self {
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
async fn wait_for_alternator(db_ip: Ipv4Addr) {
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

async fn make_clients(actors: &TestActors) -> (Client, Vec<HttpClient>) {
    let db_ip = actors.services_subnet.ip(common::DB_OCTET_1);
    let dynamodb_client = make_dynamodb_client(db_ip).await;
    let vs_clients = common::get_default_vs_ips(actors)
        .into_iter()
        .map(|ip| HttpClient::new((ip, common::VS_PORT).into()))
        .collect();
    (dynamodb_client, vs_clients)
}

fn dynamo_float_list(values: impl IntoIterator<Item = f32>) -> AttributeValue {
    AttributeValue::L(
        values
            .into_iter()
            .map(|value| AttributeValue::N(value.to_string()))
            .collect(),
    )
}

/// A single DynamoDB item for test operations - a flat map of attribute
/// name -> `AttributeValue`. Constructed via builder chain:
///
/// ```ignore
/// Item::new("pk", AttributeValue::S("pk-a".into()))
///     .vec("vec", [1.0, 2.0, 3.0])
///     .attr("ttl", AttributeValue::N("1234".into()))
/// ```
#[derive(Debug, Clone)]
struct Item(pub HashMap<String, AttributeValue>);

impl Item {
    fn new(pk_attr: &str, pk_val: AttributeValue) -> Self {
        let mut map = HashMap::new();
        map.insert(pk_attr.to_string(), pk_val);
        Self(map)
    }

    /// HASH-only -> `pk="{prefix}-{suffix}"`,
    /// HASH+RANGE -> `pk=prefix, sk=suffix`.
    fn key(pk_attr: &str, sk_attr: Option<&str>, pk_prefix: &str, suffix: &str) -> Self {
        if let Some(sk) = sk_attr {
            Self::new(pk_attr, AttributeValue::S(pk_prefix.to_string()))
                .sk(sk, AttributeValue::S(suffix.to_string()))
        } else {
            Self::new(pk_attr, AttributeValue::S(format!("{pk_prefix}-{suffix}")))
        }
    }

    fn sk(mut self, sk_attr: &str, sk_val: AttributeValue) -> Self {
        self.0.insert(sk_attr.to_string(), sk_val);
        self
    }

    fn maybe_sk(self, sk_attr: Option<&str>, sk_val: AttributeValue) -> Self {
        if let Some(sk) = sk_attr {
            self.sk(sk, sk_val)
        } else {
            self
        }
    }

    fn vec(mut self, vec_attr: &str, v: [f32; 3]) -> Self {
        self.0.insert(vec_attr.to_string(), dynamo_float_list(v));
        self
    }

    fn attr(mut self, name: &str, val: AttributeValue) -> Self {
        self.0.insert(name.to_string(), val);
        self
    }
}

async fn wait_for_index_count(client: &HttpClient, index: &IndexInfo, expected_count: usize) {
    common::wait_for(
        || async {
            client
                .index_status(&index.keyspace, &index.index)
                .await
                .is_ok_and(|resp| {
                    resp.status == httpapi::IndexStatus::Serving && resp.count == expected_count
                })
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

/// Polls ANN until the returned keys match `expected` position-by-position.
async fn wait_for_ann(
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

    let expected_keys: Vec<(String, Option<String>)> = expected
        .iter()
        .map(|item| {
            let pk = av_to_key_string(item.0.get(pk_name).expect("expected Item has no pk attr"));
            let sk = sk_name
                .map(|sk| av_to_key_string(item.0.get(sk).expect("expected Item has no sk attr")));
            (pk, sk)
        })
        .collect();

    common::wait_for(
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

            let Some(pk_values) = primary_keys.get(&pk_column) else {
                return false;
            };
            if pk_values.len() != expect_count {
                return false;
            }

            let sk_values: Option<&Vec<Value>> = match sk_column.as_ref() {
                None => None,
                Some(col) => primary_keys.get(col),
            };
            if sk_column.is_some() && sk_values.is_none() {
                return false;
            }

            for (i, (exp_pk, exp_sk)) in expected_keys.iter().enumerate() {
                let got_pk = json_value_to_key_string(&pk_values[i]);
                if got_pk != *exp_pk {
                    return false;
                }
                if let (Some(sk_vals), Some(exp_sk_str)) = (sk_values, exp_sk) {
                    let got_sk = json_value_to_key_string(&sk_vals[i]);
                    if got_sk != *exp_sk_str {
                        return false;
                    }
                }
            }

            true
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

/// Converts an `AttributeValue` to a string for key comparison.
fn av_to_key_string(av: &AttributeValue) -> String {
    match av {
        AttributeValue::S(s) => s.clone(),
        AttributeValue::N(n) => n.clone(),
        AttributeValue::B(b) => b.as_ref().iter().fold(String::new(), |mut s, byte| {
            let _ = write!(s, "{byte:02x}");
            s
        }),
        other => format!("{other:?}"),
    }
}

/// Converts a `serde_json::Value` (from VS ANN response) to a string for
/// key comparison. Strips `0x` prefix from blob hex strings.
fn json_value_to_key_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.strip_prefix("0x").unwrap_or(s).to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        other => panic!("unexpected ANN key JSON value: {other:?}"),
    }
}

/// Creates an Alternator table with the given key schema and optional vector
/// indexes. Pass `&[]` for `vector_indexes` to create a plain table.
async fn create_alternator_table(
    client: &Client,
    table_name: &str,
    partition_key_name: &str,
    pk_type: ScalarAttributeType,
    sort_key_name: Option<&str>,
    vector_indexes: &[(&str, &str, usize)],
) -> Result<CreateTableOutput, SdkError<CreateTableError>> {
    let mut builder = client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(partition_key_name)
                .attribute_type(pk_type)
                .build()
                .expect("failed to build AttributeDefinition"),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(partition_key_name)
                .key_type(KeyType::Hash)
                .build()
                .expect("failed to build KeySchemaElement"),
        )
        .billing_mode(BillingMode::PayPerRequest);

    if let Some(sk) = sort_key_name {
        builder = builder
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(sk)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .expect("failed to build AttributeDefinition"),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(sk)
                    .key_type(KeyType::Range)
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
async fn update_table_vector_indexes(
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

async fn delete_alternator_table(client: &Client, table_name: &str) {
    client
        .delete_table()
        .table_name(table_name)
        .send()
        .await
        .expect("DeleteTable should succeed");
}

/// Asserts that an SDK result is a service error containing `expected_err`.
fn assert_service_error<O, E>(result: Result<O, SdkError<E>>, expected_err: &str)
where
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
            // NOTE: --alternator-ttl-period-in-seconds is already set in
            // DEFAULT_SCYLLA_ARGS (0.5s). ScyllaDB rejects duplicate flags.
        ]);
    }

    // Capture db_ip before actors is moved into init_with_config.
    let db_ip = actors.services_subnet.ip(common::DB_OCTET_1);
    let vs_configs = common::get_default_vs_node_configs(&actors).await;
    common::init_with_config(actors, scylla_configs, vs_configs).await;

    wait_for_alternator(db_ip).await;
    info!("finished");
}

/// Describes the key schema, attribute names, and name prefixes for a test
/// table. Controls what [`TableContext::create`] builds. See
/// [`name_patterns`] for the standard 2x2 test matrix.
#[derive(Debug, Clone)]
struct TableShape {
    /// When `Some`, the table name base is padded and composed with a unique
    /// suffix so that the total equals [`MAX_ALTERNATOR_TABLE_NAME_LEN`].
    /// When `None`, a plain unique name is used.
    pub table_prefix: Option<&'static str>,
    /// Same as `table_prefix`, but for the index name.
    pub index_prefix: Option<&'static str>,
    pub pk_name: String,
    pub sk_name: Option<String>,
    pub vec_name: Option<String>,
    /// Scalar type of the partition key attribute.  Use `ScalarAttributeType::S`
    /// for the default string key; `N` or `B` for numeric / binary key tests.
    pub pk_type: ScalarAttributeType,
}

impl TableShape {
    pub fn pk(&self) -> &str {
        &self.pk_name
    }
    pub fn sk(&self) -> Option<&str> {
        self.sk_name.as_deref()
    }
    pub fn vec(&self) -> Option<&str> {
        self.vec_name.as_deref()
    }
}

//
// Base strings contain the "interesting" characters we want to test:
// ASCII special chars, mixed case, hyphens, dots, and multi-byte UTF-8
// (Cyrillic 2-byte, CJK 3-byte, emoji 4-byte).  The `pad_to_len` helper
// extends them to the exact maximum length with ASCII 'X' padding.

/// Pads `base` with `pad` bytes to exactly `len` bytes.
///
/// # Panics
/// Panics if `base` is already longer than `len`.
fn pad_to_len(base: &str, len: usize, pad: char) -> String {
    assert!(
        base.len() <= len,
        "base ({} bytes) exceeds target length ({len})",
        base.len()
    );
    let mut s = String::with_capacity(len);
    s.push_str(base);
    for _ in 0..(len - base.len()) {
        s.push(pad);
    }
    debug_assert_eq!(s.len(), len);
    s
}

/// Base for special table-name prefixes - tests hyphens, dots, mixed case.
const SPECIAL_TABLE_BASE: &str = "123-With.Hyphens_UPPER.MixedCase-";

/// Base for special index-name prefixes - tests hyphens, dots, mixed case.
const SPECIAL_INDEX_BASE: &str = "123-idx.With.Hyphens_UPPER-MixedCase-";

/// Base for special attribute names - tests ASCII punctuation, Cyrillic
/// (2-byte UTF-8), CJK (3-byte UTF-8), and emoji (4-byte UTF-8).
const SPECIAL_ATTR_BASE_PK: &str = "1:pk'.\".\\@#$ кириллица 中文 αβ 🦀 ";
const SPECIAL_ATTR_BASE_SK: &str = "1:sk'.\".\\@#$ кириллица 中文 αβ 🦀 ";
const SPECIAL_ATTR_BASE_VEC: &str = "1:vec'.\".\\@#$ кириллица 中文 αβ 🦀 ";

/// The 2x2 matrix of table shapes exercised by every `_with_names` test.
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
fn name_patterns() -> Vec<TableShape> {
    vec![
        // 0: plain, HASH-only
        TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "pk".into(),
            sk_name: None,
            vec_name: Some("vec".into()),
            pk_type: ScalarAttributeType::S,
        },
        // 1: plain, HASH+RANGE
        TableShape {
            table_prefix: None,
            index_prefix: None,
            pk_name: "pk".into(),
            sk_name: Some("sk".into()),
            vec_name: Some("vec".into()),
            pk_type: ScalarAttributeType::S,
        },
        // 2: special, HASH-only
        TableShape {
            table_prefix: Some(SPECIAL_TABLE_BASE),
            index_prefix: Some(SPECIAL_INDEX_BASE),
            pk_name: pad_to_len(SPECIAL_ATTR_BASE_PK, MAX_ALTERNATOR_ATTRIBUTE_NAME_LEN, 'X'),
            sk_name: None,
            vec_name: Some(pad_to_len(
                SPECIAL_ATTR_BASE_VEC,
                MAX_ALTERNATOR_ATTRIBUTE_NAME_LEN,
                'X',
            )),
            pk_type: ScalarAttributeType::S,
        },
        // 3: special, HASH+RANGE
        TableShape {
            table_prefix: Some(SPECIAL_TABLE_BASE),
            index_prefix: Some(SPECIAL_INDEX_BASE),
            pk_name: pad_to_len(SPECIAL_ATTR_BASE_PK, MAX_ALTERNATOR_ATTRIBUTE_NAME_LEN, 'X'),
            sk_name: Some(pad_to_len(
                SPECIAL_ATTR_BASE_SK,
                MAX_ALTERNATOR_ATTRIBUTE_NAME_LEN,
                'X',
            )),
            vec_name: Some(pad_to_len(
                SPECIAL_ATTR_BASE_VEC,
                MAX_ALTERNATOR_ATTRIBUTE_NAME_LEN,
                'X',
            )),
            pk_type: ScalarAttributeType::S,
        },
    ]
}

/// Resolves the final table name, index name, and [`IndexInfo`] from a
/// [`TableShape`].  When a prefix is `Some`, the unique name is appended
/// with a `"_"` separator and the prefix is padded so the total hits the
/// maximum length. When `None`, a plain unique name is used.
///
/// This is the single source of truth for name construction - used by both
/// [`TableContext::create`] and tests that manage the table lifecycle directly.
fn resolve_table_names(shape: &TableShape) -> (String, String, IndexInfo) {
    let table_name = match shape.table_prefix {
        None => unique_alternator_table_name(),
        Some(base) => {
            let raw = format!("{base}_{}", unique_alternator_table_name());
            pad_to_len(&raw, MAX_ALTERNATOR_TABLE_NAME_LEN, 'X')
        }
    };
    let index_name = match shape.index_prefix {
        None => unique_alternator_index_name().to_string(),
        Some(base) => {
            let raw = format!("{base}_{}", unique_alternator_index_name());
            pad_to_len(&raw, MAX_ALTERNATOR_INDEX_NAME_LEN, 'X')
        }
    };
    let index = IndexInfo::new(alternator_keyspace(&table_name).as_ref(), &index_name);
    (table_name, index_name, index)
}

// ---------------------------------------------------------------------------

/// Test fixture that encapsulates the create-table -> wait -> operate -> cleanup
/// cycle. `done()` is idempotent (swallows `ResourceNotFoundException`).
struct TableContext {
    pub client: Client,
    pub vs_client: HttpClient,
    pub table_name: String,
    pub index: IndexInfo,
    pub shape: TableShape,
}

impl TableContext {
    /// Creates a new Alternator table and (optionally) a vector index.
    async fn create(actors: &TestActors, shape: &TableShape) -> Self {
        let (client, vs_clients) = make_clients(actors).await;
        let vs_client = vs_clients
            .into_iter()
            .next()
            .expect("need at least one VS client");

        let (table_name, _, index) = resolve_table_names(shape);

        let indexes: Vec<(&str, &str, usize)> = match shape.vec() {
            Some(va) => vec![(index.index.as_ref(), va, 3)],
            None => vec![],
        };
        create_alternator_table(
            &client,
            &table_name,
            shape.pk(),
            shape.pk_type.clone(),
            shape.sk(),
            &indexes,
        )
        .await
        .expect("CreateTable should succeed");
        if shape.vec().is_some() {
            common::wait_for_index(&vs_client, &index).await;
        }

        Self {
            client,
            vs_client,
            table_name,
            index,
            shape: shape.clone(),
        }
    }

    /// Inserts an item into the table.
    async fn put(&self, item: &Item) {
        let mut req = self.client.put_item().table_name(&self.table_name);
        for (attr_name, attr_val) in &item.0 {
            req = req.item(attr_name, attr_val.clone());
        }
        req.send().await.expect("PutItem should succeed");
    }

    /// Inserts an item, asserting that Scylla rejects it with `expected_err`.
    async fn put_expecting_error(&self, item: &Item, expected_err: &str) {
        let mut req = self.client.put_item().table_name(&self.table_name);
        for (attr_name, attr_val) in &item.0 {
            req = req.item(attr_name, attr_val.clone());
        }
        assert_service_error(req.send().await, expected_err);
    }

    /// Creates a table, inserts items, and adds a vector index via
    /// `UpdateTable`. Waits for VS to serve the index with the correct count.
    /// All `items` must carry a valid vector. Use
    /// [`Self::create_with_invalid_data`] when the dataset includes items VS
    /// should skip.
    async fn create_with_data(actors: &TestActors, shape: &TableShape, items: &[Item]) -> Self {
        Self::create_with_invalid_data(actors, shape, items, &[]).await
    }

    /// Like [`Self::create_with_data`] but also pre-inserts `invalid_items`
    /// (wrong type, missing vector, wrong dimensions) that VS should skip.
    /// Only `items` count toward the expected index count.
    async fn create_with_invalid_data(
        actors: &TestActors,
        shape: &TableShape,
        items: &[Item],
        invalid_items: &[Item],
    ) -> Self {
        let no_vec_shape = TableShape {
            vec_name: None,
            pk_type: shape.pk_type.clone(),
            ..shape.clone()
        };
        let ctx = Self::create(actors, &no_vec_shape).await;

        for item in items.iter().chain(invalid_items.iter()) {
            ctx.put(item).await;
        }

        let vec_attr = match &shape.vec_name {
            None => return ctx,
            Some(va) => va,
        };

        // Add the vector index via UpdateTable (initial-scan path).
        update_table_vector_indexes(
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

        ctx.wait_for_count(items.len()).await;

        Self {
            shape: TableShape {
                vec_name: Some(vec_attr.clone()),
                ..ctx.shape
            },
            ..ctx
        }
    }

    async fn wait_for_count(&self, n: usize) {
        wait_for_index_count(&self.vs_client, &self.index, n).await;
    }

    /// Waits until ANN returns exactly the expected items in the expected order.
    async fn wait_for_ann(&self, qvec: [f32; 3], expected: &[Item]) {
        wait_for_ann(
            &self.vs_client,
            &self.index,
            self.shape.pk(),
            self.shape.sk(),
            qvec,
            expected,
        )
        .await
    }

    /// Deletes the Alternator table. Idempotent.
    async fn done(&self) {
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
                    // Already deleted - nothing to do.
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
