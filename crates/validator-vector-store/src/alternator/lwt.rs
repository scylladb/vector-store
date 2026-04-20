/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

//! Integration test: Alternator with `--alternator-write-isolation=always_use_lwt`.
//!
//! Under `always_use_lwt` every write — including plain `PutItem` and
//! `DeleteItem` (which are non-RMW operations) — is routed through the
//! LWT/Paxos path in ScyllaDB.  This exercises `learn_decision` in
//! `storage_proxy.cc`, which is where the CDC-enabled check was patched.
//!
//! The test verifies that Vector Store correctly indexes items written through
//! the LWT path by exercising all mutating operations:
//!
//! 1. `PutItem`  (non-RMW, uses LWT under `always_use_lwt`) — writes two items.
//! 2. `DeleteItem` (non-RMW, uses LWT under `always_use_lwt`) — removes one item.
//! 3. `UpdateItem SET #vec = :vec` (RMW, uses LWT in all modes) — updates the
//!    surviving item's vector; verified via ANN ordering.
//! 4. `BatchWriteItem` put ×2 (LWT path) — two items added; count rises to 3.
//! 5. `BatchWriteItem` mixed put+delete (LWT path) — one added, one removed;
//!    ANN order verified.
//! 6. `BatchWriteItem` delete-only (LWT path) — two items removed; count=1.

use async_backtrace::framed;
use aws_sdk_dynamodb::types::AttributeValue;
use tracing::info;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::common;
use vector_store::IndexInfo;

use super::ALTERNATOR_PORT;
use super::alternator_keyspace;
use super::create_alternator_table;
use super::dynamo_float_list;
use super::make_clients;
use super::unique_alternator_index_name;
use super::unique_alternator_table_name;
use super::wait_for_alternator;
use super::wait_for_index_count;

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Verifies that VS correctly indexes writes made through the LWT path when
/// `--alternator-write-isolation=always_use_lwt` is active.
///
/// Exercises all mutating Alternator operations under the LWT path:
/// `PutItem`, `DeleteItem`, `UpdateItem`, and `BatchWriteItem` (put-only,
/// mixed put+delete, and delete-only).
#[framed]
async fn alternator_with_always_use_lwt(actors: TestActors) {
    info!("started");

    // -----------------------------------------------------------------------
    // 1. Configure Scylla with always_use_lwt write isolation.
    // -----------------------------------------------------------------------
    let mut scylla_configs = common::get_default_scylla_node_configs(&actors).await;
    for config in &mut scylla_configs {
        let node_ip = config.db_ip;
        config.args.extend([
            format!("--alternator-port={ALTERNATOR_PORT}"),
            format!("--alternator-address={node_ip}"),
            "--alternator-write-isolation=always_use_lwt".to_string(),
            "--alternator-enforce-authorization=false".to_string(),
        ]);
    }

    let db_ip = actors.services_subnet.ip(common::DB_OCTET_1);

    info!("Initializing cluster with always_use_lwt write isolation");
    let vs_configs = common::get_default_vs_node_configs(&actors).await;
    common::init_with_config(actors.clone(), scylla_configs, vs_configs).await;

    wait_for_alternator(db_ip).await;

    // -----------------------------------------------------------------------
    // 2. Connect clients.
    // -----------------------------------------------------------------------
    let (client, vs_clients) = make_clients(&actors).await;
    let vs_client = vs_clients
        .into_iter()
        .next()
        .expect("need at least one VS client");

    // -----------------------------------------------------------------------
    // 3. Create a table with a 3-dimension vector index.
    // -----------------------------------------------------------------------
    let table_name = unique_alternator_table_name();
    let index_name = unique_alternator_index_name();
    let pk_attr = "pk";
    let vec_attr = "vec";

    info!("Creating table '{table_name}' with index '{index_name}'");
    create_alternator_table(
        &client,
        &table_name,
        pk_attr,
        aws_sdk_dynamodb::types::ScalarAttributeType::S,
        None,
        &[(index_name.as_ref(), vec_attr, 3)],
    )
    .await
    .expect("CreateTable with vector index should succeed");

    let index = IndexInfo::new(
        alternator_keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );

    common::wait_for_index(&vs_client, &index).await;
    info!("VS discovered index '{index_name}'");

    // -----------------------------------------------------------------------
    // 4. PutItem × 2 — non-RMW, goes through LWT under always_use_lwt.
    //    Verify VS indexes both items (count = 2).
    // -----------------------------------------------------------------------
    info!("Writing item 'item-a' via PutItem (LWT path)");
    client
        .put_item()
        .table_name(&table_name)
        .item(pk_attr, AttributeValue::S("item-a".into()))
        .item(vec_attr, dynamo_float_list([1.0_f32, 2.0, 4.0]))
        .send()
        .await
        .expect("PutItem item-a should succeed");

    info!("Writing item 'item-b' via PutItem (LWT path)");
    client
        .put_item()
        .table_name(&table_name)
        .item(pk_attr, AttributeValue::S("item-b".into()))
        .item(vec_attr, dynamo_float_list([4.0_f32, 2.0, 1.0]))
        .send()
        .await
        .expect("PutItem item-b should succeed");

    info!("Waiting for VS to index 2 items written via PutItem");
    wait_for_index_count(&vs_client, &index, 2).await;
    info!("VS indexed 2 items via PutItem LWT path");

    // -----------------------------------------------------------------------
    // 5. DeleteItem — non-RMW, goes through LWT under always_use_lwt.
    //    Remove item-b; verify count drops to 1.
    // -----------------------------------------------------------------------
    info!("Deleting item 'item-b' via DeleteItem (LWT path)");
    client
        .delete_item()
        .table_name(&table_name)
        .key(pk_attr, AttributeValue::S("item-b".into()))
        .send()
        .await
        .expect("DeleteItem item-b should succeed");

    info!("Waiting for VS to reflect DeleteItem (count = 1)");
    wait_for_index_count(&vs_client, &index, 1).await;
    info!("VS correctly removed deleted item from index");

    // -----------------------------------------------------------------------
    // 6. UpdateItem SET #vec = :vec — RMW, uses LWT in all modes.
    //    Update item-a from [1,2,4] to [1,1,1]; verify VS reflects the change
    //    by confirming that an ANN query for [1,1,1] returns item-a.
    // -----------------------------------------------------------------------
    info!("Updating item 'item-a' vector via UpdateItem SET (LWT path)");
    client
        .update_item()
        .table_name(&table_name)
        .key(pk_attr, AttributeValue::S("item-a".into()))
        .update_expression("SET #vec = :vec")
        .expression_attribute_names("#vec", vec_attr)
        .expression_attribute_values(":vec", dynamo_float_list([1.0_f32, 1.0, 1.0]))
        .send()
        .await
        .expect("UpdateItem item-a should succeed");

    // Verify the updated vector is reflected: ANN([1,1,1], limit=1) should
    // return item-a.  We poll until VS returns the expected result.
    info!("Waiting for VS to reflect UpdateItem vector change");
    use std::num::NonZeroUsize;
    use vector_store::Limit;
    use vector_store::Vector;

    let pk_col: vector_store::ColumnName = pk_attr.into();
    common::wait_for_value(
        || async {
            let (primary_keys, _, _) = vs_client
                .ann(
                    &index.keyspace,
                    &index.index,
                    Vector::from(vec![1.0_f32, 1.0, 1.0]),
                    None,
                    Limit::from(NonZeroUsize::new(1).unwrap()),
                )
                .await;
            let pks = primary_keys.get(&pk_col)?;
            if pks.len() == 1 {
                if let serde_json::Value::String(s) = &pks[0] {
                    if s == "item-a" {
                        return Some(());
                    }
                }
            }
            None
        },
        "ANN([1,1,1]) to return item-a after UpdateItem",
        common::DEFAULT_TEST_TIMEOUT,
    )
    .await;
    info!("VS correctly reflects UpdateItem vector change via LWT path");

    // -----------------------------------------------------------------------
    // 7. BatchWriteItem put ×2 — LWT path.
    //    Under always_use_lwt, do_batch_write takes the cas_write path.
    //    Put batch-a=[1,2,4] and batch-b=[4,2,1]; count rises from 1 to 3.
    // -----------------------------------------------------------------------
    info!("Writing batch-a and batch-b via BatchWriteItem (LWT path)");
    let batch_a_vec = dynamo_float_list([1.0_f32, 2.0, 4.0]);
    let batch_b_vec = dynamo_float_list([4.0_f32, 2.0, 1.0]);
    client
        .batch_write_item()
        .request_items(
            &table_name,
            vec![
                aws_sdk_dynamodb::types::WriteRequest::builder()
                    .put_request(
                        aws_sdk_dynamodb::types::PutRequest::builder()
                            .item(pk_attr, AttributeValue::S("batch-a".into()))
                            .item(vec_attr, batch_a_vec)
                            .build()
                            .expect("failed to build PutRequest"),
                    )
                    .build(),
                aws_sdk_dynamodb::types::WriteRequest::builder()
                    .put_request(
                        aws_sdk_dynamodb::types::PutRequest::builder()
                            .item(pk_attr, AttributeValue::S("batch-b".into()))
                            .item(vec_attr, batch_b_vec)
                            .build()
                            .expect("failed to build PutRequest"),
                    )
                    .build(),
            ],
        )
        .send()
        .await
        .expect("BatchWriteItem (put batch-a, batch-b) should succeed");

    info!("Waiting for VS to index batch-a and batch-b (count = 3)");
    wait_for_index_count(&vs_client, &index, 3).await;
    info!("VS indexed BatchWriteItem puts via LWT path");

    // -----------------------------------------------------------------------
    // 8. BatchWriteItem mixed put+delete — LWT path.
    //    Put batch-c=[-1,-1,-1], delete batch-a; count stays at 3.
    //    ANN([-1,-1,-1], limit=3):
    //      batch-c=[-1,-1,-1]: sim=1.0   (first)
    //      batch-b=[4,2,1]:    sim≈-0.882 (second)
    //      item-a=[1,1,1]:     sim=-1.0  (last)
    // -----------------------------------------------------------------------
    info!("Mixed BatchWriteItem (put batch-c, delete batch-a) via LWT path");
    let batch_c_vec = dynamo_float_list([-1.0_f32, -1.0, -1.0]);
    client
        .batch_write_item()
        .request_items(
            &table_name,
            vec![
                aws_sdk_dynamodb::types::WriteRequest::builder()
                    .put_request(
                        aws_sdk_dynamodb::types::PutRequest::builder()
                            .item(pk_attr, AttributeValue::S("batch-c".into()))
                            .item(vec_attr, batch_c_vec)
                            .build()
                            .expect("failed to build PutRequest"),
                    )
                    .build(),
                aws_sdk_dynamodb::types::WriteRequest::builder()
                    .delete_request(
                        aws_sdk_dynamodb::types::DeleteRequest::builder()
                            .key(pk_attr, AttributeValue::S("batch-a".into()))
                            .build()
                            .expect("failed to build DeleteRequest"),
                    )
                    .build(),
            ],
        )
        .send()
        .await
        .expect("BatchWriteItem (put batch-c, delete batch-a) should succeed");

    info!("Waiting for VS to reflect mixed BatchWriteItem (count = 3)");
    wait_for_index_count(&vs_client, &index, 3).await;

    // Verify ANN order: batch-c first, batch-b second, item-a last.
    common::wait_for_value(
        || async {
            let (primary_keys, _, _) = vs_client
                .ann(
                    &index.keyspace,
                    &index.index,
                    Vector::from(vec![-1.0_f32, -1.0, -1.0]),
                    None,
                    Limit::from(NonZeroUsize::new(3).unwrap()),
                )
                .await;
            let pks = primary_keys.get(&pk_col)?;
            if pks.len() != 3 {
                return None;
            }
            let expected = ["batch-c", "batch-b", "item-a"];
            for (i, exp) in expected.iter().enumerate() {
                if let serde_json::Value::String(s) = &pks[i] {
                    if s != exp {
                        return None;
                    }
                } else {
                    return None;
                }
            }
            Some(())
        },
        "ANN([-1,-1,-1]) to return [batch-c, batch-b, item-a] after mixed BatchWriteItem",
        common::DEFAULT_TEST_TIMEOUT,
    )
    .await;
    info!("VS correctly reflects mixed BatchWriteItem via LWT path");

    // -----------------------------------------------------------------------
    // 9. BatchWriteItem delete-only — LWT path.
    //    Delete batch-b and batch-c; count drops to 1 (only item-a remains).
    // -----------------------------------------------------------------------
    info!("Delete-only BatchWriteItem (delete batch-b, batch-c) via LWT path");
    client
        .batch_write_item()
        .request_items(
            &table_name,
            vec![
                aws_sdk_dynamodb::types::WriteRequest::builder()
                    .delete_request(
                        aws_sdk_dynamodb::types::DeleteRequest::builder()
                            .key(pk_attr, AttributeValue::S("batch-b".into()))
                            .build()
                            .expect("failed to build DeleteRequest"),
                    )
                    .build(),
                aws_sdk_dynamodb::types::WriteRequest::builder()
                    .delete_request(
                        aws_sdk_dynamodb::types::DeleteRequest::builder()
                            .key(pk_attr, AttributeValue::S("batch-c".into()))
                            .build()
                            .expect("failed to build DeleteRequest"),
                    )
                    .build(),
            ],
        )
        .send()
        .await
        .expect("BatchWriteItem (delete batch-b, batch-c) should succeed");

    info!("Waiting for VS to reflect delete-only BatchWriteItem (count = 1)");
    wait_for_index_count(&vs_client, &index, 1).await;
    info!("VS correctly reflects delete-only BatchWriteItem via LWT path");

    // -----------------------------------------------------------------------
    // 10. Cleanup.
    // -----------------------------------------------------------------------
    info!("Cleaning up");
    common::cleanup(actors).await;
    info!("finished");
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(super) fn register(test_case: TestCase) -> TestCase {
    test_case.with_test(
        "alternator_with_always_use_lwt",
        common::DEFAULT_TEST_TIMEOUT,
        alternator_with_always_use_lwt,
    )
}
