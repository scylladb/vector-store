/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

//! Integration test: Alternator with authorization enabled (`--alternator-enforce-authorization=true`).
//!
//! Verifies that:
//! 1. A DynamoDB client with wrong credentials is rejected with `UnrecognizedClientException`.
//! 2. A role with only `CREATE ON ALL KEYSPACES` can create a table with a vector index and
//!    write items (which are auto-granted `MODIFY` at `CreateTable` time).
//! 3. The same limited role **cannot** delete a vector index (needs `ALTER`, which was not
//!    granted), and Alternator returns `AccessDeniedException`.
//! 4. Vector Store successfully indexes data written by the limited-permission role.

use async_backtrace::framed;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::Region;
use scylla::client::session::Session;
use std::net::Ipv4Addr;
use std::sync::LazyLock;
use tracing::info;
use uuid::Uuid;
use vector_search_validator_tests::ScyllaClusterExt;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::VectorStoreClusterExt;
use vector_search_validator_tests::common;

use super::ALTERNATOR_PORT;
use super::JsonBodyInjectInterceptor;
use super::alternator_keyspace;
use super::assert_service_error;
use super::create_alternator_table;
use super::dynamo_float_list;
use super::unique_alternator_index_name;
use super::unique_alternator_table_name;
use super::wait_for_index_count;

// ---------------------------------------------------------------------------
// Superuser credentials (generated once per process)
// ---------------------------------------------------------------------------

static SUPERUSER_NAME: LazyLock<String> = LazyLock::new(|| Uuid::new_v4().simple().to_string());
static SUPERUSER_PASSWORD: LazyLock<String> = LazyLock::new(|| Uuid::new_v4().simple().to_string());
static SUPERUSER_SALTED_PASSWORD: LazyLock<String> = LazyLock::new(|| {
    bcrypt::hash(&*SUPERUSER_PASSWORD, bcrypt::DEFAULT_COST)
        .expect("failed to hash superuser password")
});

/// Builds the ScyllaDB extra-config YAML that enables password authentication,
/// CQL authorization, and sets the superuser credentials.
fn scylla_auth_config() -> Vec<u8> {
    let name = &*SUPERUSER_NAME;
    let salted = &*SUPERUSER_SALTED_PASSWORD;
    format!(
        "authenticator: PasswordAuthenticator\n\
         authorizer: CassandraAuthorizer\n\
         auth_superuser_name: '{name}'\n\
         auth_superuser_salted_password: '{salted}'"
    )
    .into_bytes()
}

// ---------------------------------------------------------------------------
// DynamoDB client helpers
// ---------------------------------------------------------------------------

/// Builds a DynamoDB client that authenticates with the given SigV4 credentials.
async fn make_dynamodb_client_with_creds(
    db_ip: Ipv4Addr,
    access_key_id: &str,
    secret_access_key: &str,
) -> Client {
    let creds = Credentials::new(access_key_id, secret_access_key, None, None, "test");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(creds)
        .endpoint_url(format!("http://{db_ip}:{ALTERNATOR_PORT}"))
        .region(Region::new("us-east-1"))
        .load()
        .await;
    Client::new(&config)
}

/// Polls the Alternator endpoint with the given credentials until it responds
/// successfully.  With `--alternator-enforce-authorization=true`, the standard
/// `wait_for_alternator` (which uses dummy `"any"/"any"` creds) would loop
/// forever because every request returns `UnrecognizedClientException`.
async fn wait_for_alternator_with_creds(
    db_ip: Ipv4Addr,
    access_key_id: &str,
    secret_access_key: &str,
) {
    let client = make_dynamodb_client_with_creds(db_ip, access_key_id, secret_access_key).await;
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

/// Queries ScyllaDB for the `salted_hash` of the given role.
///
/// Newer ScyllaDB versions store roles in `system.roles`; older releases used
/// `system_auth_v2.roles` or `system_auth.roles`.  We try all three in order.
async fn get_salted_hash(session: &Session, role_name: &str) -> String {
    for keyspace in ["system", "system_auth_v2", "system_auth"] {
        let query = format!("SELECT salted_hash FROM {keyspace}.roles WHERE role = ?");
        if let Ok(result) = session.query_unpaged(query, (role_name,)).await {
            if let Ok(rows) = result.into_rows_result() {
                if let Ok((Some(hash),)) = rows.first_row::<(Option<String>,)>() {
                    return hash;
                }
            }
        }
    }
    panic!("Could not find salted_hash for role '{role_name}' in any known keyspace");
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Verifies correct authorization behaviour for Alternator with enforcement enabled.
///
/// See the module-level doc for the full scenario description.
#[framed]
async fn alternator_with_auth_enabled(actors: TestActors) {
    info!("started");

    // -----------------------------------------------------------------------
    // 1. Configure Scylla with auth + Alternator enforcement.
    // -----------------------------------------------------------------------
    let mut scylla_configs = common::get_default_scylla_node_configs(&actors).await;
    let auth_config = scylla_auth_config();
    for config in &mut scylla_configs {
        let node_ip = config.db_ip;
        config.args.extend([
            format!("--alternator-port={ALTERNATOR_PORT}"),
            format!("--alternator-address={node_ip}"),
            "--alternator-write-isolation=only_rmw_uses_lwt".to_string(),
            "--alternator-enforce-authorization=true".to_string(),
        ]);
        config.extra_config = Some(auth_config.clone());
    }

    // -----------------------------------------------------------------------
    // 2. Configure VS to connect as the superuser (VS talks CQL, not HTTP).
    // -----------------------------------------------------------------------
    let mut vs_configs = common::get_default_vs_node_configs(&actors).await;
    for config in &mut vs_configs {
        config.user = Some(SUPERUSER_NAME.clone());
        config.password = Some(SUPERUSER_PASSWORD.clone());
    }

    let db_ip = actors.services_subnet.ip(common::DB_OCTET_1);

    info!("Initializing cluster with Alternator auth enforcement enabled");
    common::init_dns(&actors).await;
    actors.db.start(scylla_configs).await;
    assert!(actors.db.wait_for_ready().await);
    actors.vs.start(vs_configs).await;
    assert!(actors.vs.wait_for_ready().await);

    // -----------------------------------------------------------------------
    // 3. Connect CQL as superuser; create a limited role with only CREATE.
    // -----------------------------------------------------------------------
    info!("Connecting to ScyllaDB as superuser");
    let (session, vs_clients) =
        common::prepare_connection_with_auth(&actors, &SUPERUSER_NAME, &SUPERUSER_PASSWORD).await;
    let vs_client = vs_clients
        .into_iter()
        .next()
        .expect("need at least one VS client");

    let role_name = Uuid::new_v4().simple().to_string();
    let role_password = Uuid::new_v4().simple().to_string();

    info!("Creating limited role '{role_name}' with only CREATE ON ALL KEYSPACES");
    session
        .query_unpaged(
            format!(
                "CREATE ROLE \"{role_name}\" WITH PASSWORD = '{role_password}' AND LOGIN = true"
            ),
            (),
        )
        .await
        .expect("failed to create limited role");

    session
        .query_unpaged(
            format!("GRANT CREATE ON ALL KEYSPACES TO \"{role_name}\""),
            (),
        )
        .await
        .expect("failed to grant CREATE to limited role");

    // -----------------------------------------------------------------------
    // 4. Derive Alternator credentials:
    //    access_key_id  = role name
    //    secret_access_key = salted_hash from system.roles
    // -----------------------------------------------------------------------
    info!("Fetching salted_hash for limited role '{role_name}'");
    let limited_salted_hash = get_salted_hash(&session, &role_name).await;

    let superuser_salted_hash = get_salted_hash(&session, &SUPERUSER_NAME).await;

    // Wait for Alternator to be ready using superuser credentials (dummy creds
    // would loop forever with enforcement on).
    wait_for_alternator_with_creds(db_ip, &SUPERUSER_NAME, &superuser_salted_hash).await;

    let limited_client =
        make_dynamodb_client_with_creds(db_ip, &role_name, &limited_salted_hash).await;

    // -----------------------------------------------------------------------
    // 5. Assert wrong credentials → UnrecognizedClientException.
    // -----------------------------------------------------------------------
    info!("Asserting wrong credentials yield UnrecognizedClientException");
    let bad_client =
        make_dynamodb_client_with_creds(db_ip, "nonexistent-role", "wrong-secret").await;
    assert_service_error(
        bad_client.list_tables().limit(1).send().await,
        "UnrecognizedClientException",
    );

    // -----------------------------------------------------------------------
    // 6. CreateTable with VectorIndexes as limited role → should succeed
    //    (only CREATE ON ALL KEYSPACES is required).
    // -----------------------------------------------------------------------
    let table_name = unique_alternator_table_name();
    let index_name = unique_alternator_index_name();
    let vec_attr = "vec";

    info!(
        "Asserting CreateTable with VectorIndexes succeeds for limited role (table '{table_name}')"
    );
    create_alternator_table(
        &limited_client,
        &table_name,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(index_name.as_ref(), vec_attr, 3)],
    )
    .await
    .expect("CreateTable with VectorIndexes should succeed for role with CREATE permission");

    let index = vector_store::IndexInfo::new(
        alternator_keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );

    // -----------------------------------------------------------------------
    // 7. Write two items as the limited role (auto-granted MODIFY at create time).
    // -----------------------------------------------------------------------
    info!("Writing items as limited role");
    for (pk_val, vec_vals) in [("item-a", [1.0_f32, 1.0, 1.0]), ("item-b", [1.0, 2.0, 4.0])] {
        limited_client
            .put_item()
            .table_name(&table_name)
            .item(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(pk_val.into()),
            )
            .item(vec_attr, dynamo_float_list(vec_vals))
            .send()
            .await
            .expect("PutItem should succeed for role with auto-granted MODIFY");
    }

    // -----------------------------------------------------------------------
    // Wait for VS to index both items.
    // Proves VS successfully indexes data written by a limited-permission role.
    // -----------------------------------------------------------------------
    info!("Waiting for VS to index 2 items written by limited role");
    common::wait_for_index(&vs_client, &index).await;
    wait_for_index_count(&vs_client, &index, 2).await;
    info!("VS successfully indexed 2 items");

    // -----------------------------------------------------------------------
    // 8. Revoke ALTER on the table from the creator role.
    //
    // CreateTable auto-grants all applicable table permissions (ALTER, DROP,
    // SELECT, MODIFY, AUTHORIZE) to the role that created it.  To test that
    // UpdateTable is gated on ALTER, we explicitly revoke it via the
    // superuser CQL session.
    //
    // The CQL resource for an Alternator table named T is:
    //   "alternator_T"."T"
    // -----------------------------------------------------------------------
    info!("Revoking ALTER on table '{table_name}' from limited role");
    session
        .query_unpaged(
            format!(
                "REVOKE ALTER ON TABLE \"alternator_{table_name}\".\"{table_name}\" FROM \"{role_name}\""
            ),
            (),
        )
        .await
        .expect("failed to revoke ALTER from limited role");

    // -----------------------------------------------------------------------
    // 9. UpdateTable deleting the index → AccessDeniedException
    //    (ALTER was just revoked; permission cache may take a moment to update,
    //    so we poll until the rejection is observed).
    // -----------------------------------------------------------------------
    info!("Waiting for UpdateTable delete-index to yield AccessDeniedException for limited role");
    let delete_update = serde_json::json!([{
        "Delete": {
            "IndexName": index_name.as_ref()
        }
    }]);
    common::wait_for_value(
        || {
            let client = limited_client.clone();
            let table = table_name.clone();
            let update = delete_update.clone();
            let index_name_str = index_name.to_string();
            async move {
                let result = client
                    .update_table()
                    .table_name(&table)
                    .customize()
                    .interceptor(JsonBodyInjectInterceptor::new([(
                        "VectorIndexUpdates",
                        update,
                    )]))
                    .send()
                    .await;
                match result {
                    Err(ref e) => {
                        use aws_sdk_dynamodb::error::ProvideErrorMetadata as _;
                        let code = e.code().unwrap_or("");
                        let msg = e.message().unwrap_or("");
                        if code.contains("AccessDeniedException")
                            || msg.contains("AccessDeniedException")
                        {
                            return Some(());
                        }
                        // Any other error is unexpected — surface it.
                        panic!("UpdateTable returned unexpected error: code={code:?} msg={msg:?}");
                    }
                    Ok(_) => {
                        // Permission cache not yet invalidated — retry.
                        // Re-create the index first so it exists for the next attempt.
                        // (UpdateTable delete succeeded, so the index is gone; we
                        //  must re-add it before the next delete attempt.)
                        client
                            .update_table()
                            .table_name(&table)
                            .customize()
                            .interceptor(JsonBodyInjectInterceptor::new([(
                                "VectorIndexUpdates",
                                serde_json::json!([{
                                    "Create": {
                                        "IndexName": index_name_str,
                                        "VectorAttribute": {
                                            "AttributeName": "vec",
                                            "Dimensions": 3
                                        }
                                    }
                                }]),
                            )]))
                            .send()
                            .await
                            .expect("re-creating index for next retry should succeed");
                        None
                    }
                }
            }
        },
        "UpdateTable delete-index to be rejected with AccessDeniedException",
        common::DEFAULT_TEST_TIMEOUT,
    )
    .await;
    info!("AccessDeniedException confirmed for UpdateTable after ALTER revoked");

    // -----------------------------------------------------------------------
    // 10. Confirm the index is still serving.
    //     (The last delete attempt was rejected, so the index survived.)
    // -----------------------------------------------------------------------
    info!("Confirming index is still serving after rejected delete");
    common::wait_for_index(&vs_client, &index).await;
    info!("Permission boundary verified: index intact");

    info!("Cleaning up");
    // Drop the CQL session before shutting down Scylla nodes.  The scylla
    // driver's background topology-refresh task is cancelled when the last
    // Arc<Session> is dropped, but the cancellation is not processed until the
    // tokio runtime gets a scheduling turn.  yield_now() gives it that turn so
    // the task is gone before the nodes disappear and we avoid spurious
    // "Failed to fetch metadata" warnings.
    drop(session);
    tokio::task::yield_now().await;
    common::cleanup(actors).await;

    info!("finished");
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case.with_test(
        "alternator_with_auth_enabled",
        common::DEFAULT_TEST_TIMEOUT,
        alternator_with_auth_enabled,
    )
}
