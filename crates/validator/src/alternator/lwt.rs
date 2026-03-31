/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

//! Integration test: Alternator with `--alternator-write-isolation=always_use_lwt`.
//!
//! Under `always_use_lwt` every write is routed through the LWT/Paxos path.
//! The test verifies that Vector Store correctly indexes items written through
//! this path by exercising `PutItem`, `DeleteItem`, `UpdateItem`, and
//! `BatchWriteItem` (put-only, mixed put+delete, delete-only).

use crate::TestActors;
use crate::common;
use async_backtrace::framed;
use aws_sdk_dynamodb::types::AttributeValue;
use e2etest::TestCase;
use httpapi::IndexInfo;
use tracing::info;

use super::ALTERNATOR_PORT;
use super::Item;
use super::alternator_keyspace;
use super::create_alternator_table;
use super::dynamo_float_list;
use super::make_clients;
use super::unique_alternator_index_name;
use super::unique_alternator_table_name;
use super::wait_for_alternator;
use super::wait_for_ann;
use super::wait_for_index_count;

/// Helper: build a PutItem `WriteRequest` from an `Item`.
fn put_write_request(item: &Item) -> aws_sdk_dynamodb::types::WriteRequest {
    let mut builder = aws_sdk_dynamodb::types::PutRequest::builder();
    for (attr_name, attr_val) in &item.0 {
        builder = builder.item(attr_name, attr_val.clone());
    }
    aws_sdk_dynamodb::types::WriteRequest::builder()
        .put_request(builder.build().expect("failed to build PutRequest"))
        .build()
}

/// Helper: build a DeleteItem `WriteRequest` from a pk attribute.
fn delete_write_request(
    pk_attr: &str,
    pk_val: AttributeValue,
) -> aws_sdk_dynamodb::types::WriteRequest {
    aws_sdk_dynamodb::types::WriteRequest::builder()
        .delete_request(
            aws_sdk_dynamodb::types::DeleteRequest::builder()
                .key(pk_attr, pk_val)
                .build()
                .expect("failed to build DeleteRequest"),
        )
        .build()
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Verifies that VS correctly indexes writes made through the LWT path when
/// `--alternator-write-isolation=always_use_lwt` is active.
#[framed]
async fn alternator_with_always_use_lwt(actors: TestActors) {
    info!("started");

    // 1. Configure Scylla with always_use_lwt write isolation.
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

    // 2. Connect clients.
    let (client, vs_clients) = make_clients(&actors).await;
    let vs_client = vs_clients
        .into_iter()
        .next()
        .expect("need at least one VS client");

    // 3. Create a table with a 3-dimension vector index.
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

    // 4. PutItem × 2 — verify VS indexes both items (count = 2).
    info!("Writing item-a and item-b via PutItem (LWT path)");
    client
        .put_item()
        .table_name(&table_name)
        .item(pk_attr, AttributeValue::S("item-a".into()))
        .item(vec_attr, dynamo_float_list([1.0_f32, 2.0, 4.0]))
        .send()
        .await
        .expect("PutItem item-a should succeed");

    client
        .put_item()
        .table_name(&table_name)
        .item(pk_attr, AttributeValue::S("item-b".into()))
        .item(vec_attr, dynamo_float_list([4.0_f32, 2.0, 1.0]))
        .send()
        .await
        .expect("PutItem item-b should succeed");

    wait_for_index_count(&vs_client, &index, 2).await;
    info!("VS indexed 2 items via PutItem LWT path");

    // 5. DeleteItem — remove item-b; verify count drops to 1.
    info!("Deleting item-b via DeleteItem (LWT path)");
    client
        .delete_item()
        .table_name(&table_name)
        .key(pk_attr, AttributeValue::S("item-b".into()))
        .send()
        .await
        .expect("DeleteItem item-b should succeed");

    wait_for_index_count(&vs_client, &index, 1).await;
    info!("VS correctly removed deleted item from index");

    // 6. UpdateItem SET — update item-a vector from [1,2,4] to [1,1,1].
    //    Count doesn't change, so we verify via ANN ordering.
    info!("Updating item-a vector via UpdateItem SET (LWT path)");
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

    wait_for_ann(
        &vs_client,
        &index,
        pk_attr,
        None,
        [1.0, 1.0, 1.0],
        &[Item::new(pk_attr, AttributeValue::S("item-a".into()))],
    )
    .await;
    info!("VS correctly reflects UpdateItem vector change via LWT path");

    // 7. BatchWriteItem put × 2 — put batch-a and batch-b; count rises to 3.
    let batch_a =
        Item::new(pk_attr, AttributeValue::S("batch-a".into())).vec(vec_attr, [1.0_f32, 2.0, 4.0]);
    let batch_b =
        Item::new(pk_attr, AttributeValue::S("batch-b".into())).vec(vec_attr, [4.0_f32, 2.0, 1.0]);

    info!("Writing batch-a and batch-b via BatchWriteItem (LWT path)");
    client
        .batch_write_item()
        .request_items(
            &table_name,
            vec![put_write_request(&batch_a), put_write_request(&batch_b)],
        )
        .send()
        .await
        .expect("BatchWriteItem (put batch-a, batch-b) should succeed");

    wait_for_index_count(&vs_client, &index, 3).await;
    info!("VS indexed BatchWriteItem puts via LWT path");

    // 8. BatchWriteItem mixed put+delete — put batch-c, delete batch-a; count stays at 3.
    //    ANN([-1,-1,-1], limit=3): batch-c first, batch-b second, item-a last.
    let batch_c = Item::new(pk_attr, AttributeValue::S("batch-c".into()))
        .vec(vec_attr, [-1.0_f32, -1.0, -1.0]);

    info!("Mixed BatchWriteItem (put batch-c, delete batch-a) via LWT path");
    client
        .batch_write_item()
        .request_items(
            &table_name,
            vec![
                put_write_request(&batch_c),
                delete_write_request(pk_attr, AttributeValue::S("batch-a".into())),
            ],
        )
        .send()
        .await
        .expect("BatchWriteItem (put batch-c, delete batch-a) should succeed");

    wait_for_index_count(&vs_client, &index, 3).await;

    wait_for_ann(
        &vs_client,
        &index,
        pk_attr,
        None,
        [-1.0, -1.0, -1.0],
        &[
            Item::new(pk_attr, AttributeValue::S("batch-c".into())),
            Item::new(pk_attr, AttributeValue::S("batch-b".into())),
            Item::new(pk_attr, AttributeValue::S("item-a".into())),
        ],
    )
    .await;
    info!("VS correctly reflects mixed BatchWriteItem via LWT path");

    // 9. BatchWriteItem delete-only — delete batch-b and batch-c; count drops to 1.
    info!("Delete-only BatchWriteItem (delete batch-b, batch-c) via LWT path");
    client
        .batch_write_item()
        .request_items(
            &table_name,
            vec![
                delete_write_request(pk_attr, AttributeValue::S("batch-b".into())),
                delete_write_request(pk_attr, AttributeValue::S("batch-c".into())),
            ],
        )
        .send()
        .await
        .expect("BatchWriteItem (delete batch-b, batch-c) should succeed");

    wait_for_index_count(&vs_client, &index, 1).await;
    info!("VS correctly reflects delete-only BatchWriteItem via LWT path");

    // 10. Cleanup.
    info!("Cleaning up");
    common::cleanup(actors).await;
    info!("finished");
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(super) fn register(test_case: TestCase<TestActors>) -> TestCase<TestActors> {
    test_case.with_test(
        "alternator_with_always_use_lwt",
        common::DEFAULT_TEST_TIMEOUT,
        alternator_with_always_use_lwt,
    )
}
