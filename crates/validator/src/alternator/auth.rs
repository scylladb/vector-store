/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Integration test: Alternator with authorization enabled (`--alternator-enforce-authorization=true`).
//!
//! Verifies that:
//! 1. A DynamoDB client with wrong credentials is rejected with `UnrecognizedClientException`.
//! 2. A role with only `CREATE ON ALL KEYSPACES` can create a table with a vector index and
//!    write items (which are auto-granted `MODIFY` at `CreateTable` time).
//! 3. The same limited role **cannot** mutate a vector index after `ALTER` (auto-granted by
//!    `CreateTable`) is explicitly revoked; Alternator returns `AccessDeniedException`.
//! 4. Vector Store successfully indexes data written by the limited-permission role.

use crate::TestActors;
use crate::common;
use scylla::client::session::Session;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

use crate::alternator;
use crate::alternator::ALTERNATOR_PORT;
use crate::alternator::JsonBodyInjectInterceptor;
use aws_sdk_dynamodb::error::ProvideErrorMetadata as _;

/// Polls the Alternator endpoint with the given credentials until it responds
/// successfully. With `--alternator-enforce-authorization=true`, the standard
/// `wait_for_alternator` (which uses dummy `"any"/"any"` creds) would loop
/// forever because every request returns `UnrecognizedClientException`.
async fn wait_for_alternator_with_creds(
    db_ip: Ipv4Addr,
    access_key_id: &str,
    secret_access_key: &str,
) {
    let client =
        alternator::make_dynamodb_client_with_creds(db_ip, access_key_id, secret_access_key).await;
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
        if let Ok(result) = session.query_unpaged(query, (role_name,)).await
            && let Ok(rows) = result.into_rows_result()
            && let Ok((Some(hash),)) = rows.first_row::<(Option<String>,)>()
        {
            return hash;
        }
    }
    panic!("Could not find salted_hash for role '{role_name}' in any known keyspace");
}

/// Verifies correct authorization behaviour for Alternator with enforcement enabled.
///
/// See the module-level doc for the full scenario description.
#[e2etest::test(group = alternator_auth)]
async fn alternator_with_auth_enabled(fixture: Arc<Fixture>) {
    let actors = &fixture.actors;
    info!("started");

    let db_ip = common::get_default_db_ip(actors);

    info!("Connecting to ScyllaDB as superuser");
    let (session, vs_clients) = common::prepare_connection_with_auth(
        actors,
        &common::SUPERUSER_NAME,
        &common::SUPERUSER_PASSWORD,
    )
    .await;

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

    // Alternator credentials: access_key_id = role name,
    // secret_access_key = salted_hash from system.roles.
    info!("Fetching salted_hash for limited role '{role_name}'");
    let limited_salted_hash = get_salted_hash(&session, &role_name).await;

    let superuser_salted_hash = get_salted_hash(&session, &common::SUPERUSER_NAME).await;

    wait_for_alternator_with_creds(db_ip, &common::SUPERUSER_NAME, &superuser_salted_hash).await;

    let limited_client =
        alternator::make_dynamodb_client_with_creds(db_ip, &role_name, &limited_salted_hash).await;
    info!("Asserting wrong credentials yield UnrecognizedClientException");
    let bad_client =
        alternator::make_dynamodb_client_with_creds(db_ip, "nonexistent-role", "wrong-secret")
            .await;
    alternator::assert_service_error(
        bad_client.list_tables().limit(1).send().await,
        "UnrecognizedClientException",
    );
    let table_name = alternator::unique_table_name();
    let index_name = alternator::unique_index_name();
    let vec_attr = "vec";

    info!(
        "Asserting CreateTable with VectorIndexes succeeds for limited role (table '{table_name}')"
    );
    alternator::create_table(
        &limited_client,
        &table_name,
        "pk",
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(index_name.as_ref(), vec_attr, 3)],
    )
    .await
    .expect("CreateTable with VectorIndexes should succeed for role with CREATE permission");

    let index = httpapi::IndexInfo::new(
        alternator::keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );
    info!("Writing items as limited role");
    for (pk_val, vec_vals) in [("item-a", [1.0_f32, 1.0, 1.0]), ("item-b", [1.0, 2.0, 4.0])] {
        limited_client
            .put_item()
            .table_name(&table_name)
            .item(
                "pk",
                aws_sdk_dynamodb::types::AttributeValue::S(pk_val.into()),
            )
            .item(vec_attr, alternator::float_list(vec_vals))
            .send()
            .await
            .expect("PutItem should succeed for role with auto-granted MODIFY");
    }
    info!("Waiting for VS to index 2 items written by limited role");
    common::wait_for_index_count(&vs_clients, &index, 2).await;
    info!("VS successfully indexed 2 items");

    // CreateTable auto-grants ALTER to the creator. Revoke it so we can
    // verify that UpdateTable (delete-index) is gated on ALTER.
    info!("Revoking ALTER on table '{table_name}' from limited role");
    session
        .query_unpaged(
            format!(
                "REVOKE ALTER ON TABLE \"{keyspace}\".\"{table_name}\" FROM \"{role_name}\"",
                keyspace = alternator::keyspace(&table_name),
            ),
            (),
        )
        .await
        .expect("failed to revoke ALTER from limited role");
    info!("Waiting for UpdateTable create/delete to yield AccessDeniedException for limited role");
    // Alternate between Delete and Create for the same index. Both require
    // ALTER. While the permission cache has not yet propagated the revocation
    // each operation may still succeed; we flip unconditionally after each
    // non-terminal attempt. Once auth kicks in either operation returns
    // AccessDeniedException and the loop exits successfully. Any other error
    // is unexpected and panics.
    tokio::time::timeout(common::DEFAULT_TEST_TIMEOUT, async {
        let mut try_delete = true;
        loop {
            let update = if try_delete {
                serde_json::json!([{"Delete": {"IndexName": index_name.as_ref()}}])
            } else {
                serde_json::json!([{
                    "Create": {
                        "IndexName": index_name.as_ref(),
                        "VectorAttribute": {
                                "AttributeName": vec_attr,
                            "Dimensions": 3
                        }
                    }
                }])
            };
            try_delete = !try_delete;

            let result = limited_client
                .update_table()
                .table_name(&table_name)
                .customize()
                .interceptor(JsonBodyInjectInterceptor::new([(
                    "VectorIndexUpdates",
                    update,
                )]))
                .send()
                .await;

            match result {
                Err(ref e) => {
                    let code = e.code().unwrap_or("");
                    let msg = e.message().unwrap_or("");
                    if code.contains("AccessDeniedException")
                        || msg.contains("AccessDeniedException")
                    {
                        // Auth denial confirmed - test passes.
                        return;
                    }
                    // Index not yet in the expected state - retry.
                    if code.contains("ResourceInUseException")
                        || msg.contains("already exists")
                        || code.contains("ResourceNotFoundException")
                        || msg.contains("not found")
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                    panic!("UpdateTable returned unexpected error: code={code:?} msg={msg:?}");
                }
                Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Timeout: UpdateTable was not rejected with AccessDeniedException after ALTER revoked"
        )
    });
    info!("AccessDeniedException confirmed for UpdateTable after ALTER revoked");
    info!("finished");
}

// Dedicated cluster: enforce-authorization changes the whole-cluster auth
// posture, so this is a top-level group with its own cluster rather than a
// member of the shared standard Alternator namespace.
e2etest::group!(
    name = alternator_auth,
    fixtures = (Fixture),
    parent = crate::validator
);

struct Fixture {
    actors: TestActors,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<crate::TestEnv>().await?.new_cluster().await;

        let scylla_configs = alternator::get_scylla_configs(
            &actors,
            [("--alternator-enforce-authorization", "true")],
            Some(common::scylla_auth_config()),
        )
        .await;

        let mut vs_configs = common::get_default_vs_node_configs(&actors).await;
        for config in &mut vs_configs {
            config.user = Some(common::SUPERUSER_NAME.clone());
            config.password = Some(common::SUPERUSER_PASSWORD.clone());
        }

        common::init_with_config(&actors, scylla_configs, vs_configs, false).await;

        Some(Self { actors })
    }

    async fn teardown(self) {
        common::cleanup(&self.actors).await;
    }

    // This cluster is independent of every other one, so its groups may run
    // alongside groups owning a different cluster.
    fn group_can_run_concurrently() -> bool {
        true
    }
}
