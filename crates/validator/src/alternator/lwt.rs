/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Integration test: Alternator with `--alternator-write-isolation=always_use_lwt`.
//!
//! Under `always_use_lwt` every write is routed through the LWT/Paxos path.
//! The test verifies that Vector Store correctly indexes items written through
//! this path by exercising `PutItem`, `DeleteItem`, `UpdateItem`, and
//! `BatchWriteItem` (put-only, mixed put+delete, delete-only).

use crate::TestActors;
use crate::alternator;
use crate::alternator::Item;
use crate::common;
use aws_sdk_dynamodb::types::AttributeValue;
use httpapi::IndexInfo;
use std::sync::Arc;
use tracing::info;

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

/// Verifies that VS correctly indexes writes made through the LWT path when
/// `--alternator-write-isolation=always_use_lwt` is active.
#[e2etest::test(group = alternator_lwt)]
async fn alternator_with_always_use_lwt(fixture: Arc<Fixture>) {
    let actors = &fixture.actors;
    info!("started");

    let (client, vs_clients) = alternator::make_clients(actors).await;

    let table_name = alternator::unique_table_name();
    let index_name = alternator::unique_index_name();
    let pk_attr = "pk";
    let vec_attr = "vec";

    info!("Creating table '{table_name}' with index '{index_name}'");
    alternator::create_table(
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
        alternator::keyspace(&table_name).as_ref(),
        index_name.as_ref(),
    );

    alternator::wait_for_index(&vs_clients, &index).await;
    info!("VS discovered index '{index_name}'");

    info!("Writing item-a and item-b via PutItem (LWT path)");
    client
        .put_item()
        .table_name(&table_name)
        .item(pk_attr, AttributeValue::S("item-a".into()))
        .item(vec_attr, alternator::float_list([1.0_f32, 2.0, 4.0]))
        .send()
        .await
        .expect("PutItem item-a should succeed");

    client
        .put_item()
        .table_name(&table_name)
        .item(pk_attr, AttributeValue::S("item-b".into()))
        .item(vec_attr, alternator::float_list([4.0_f32, 2.0, 1.0]))
        .send()
        .await
        .expect("PutItem item-b should succeed");

    common::wait_for_index_count(&vs_clients, &index, 2).await;
    info!("VS indexed 2 items via PutItem LWT path");

    info!("Deleting item-b via DeleteItem (LWT path)");
    client
        .delete_item()
        .table_name(&table_name)
        .key(pk_attr, AttributeValue::S("item-b".into()))
        .send()
        .await
        .expect("DeleteItem item-b should succeed");

    common::wait_for_index_count(&vs_clients, &index, 1).await;
    info!("VS correctly removed deleted item from index");

    // Count doesn't change, so we verify via ANN ordering.
    info!("Updating item-a vector via UpdateItem SET (LWT path)");
    client
        .update_item()
        .table_name(&table_name)
        .key(pk_attr, AttributeValue::S("item-a".into()))
        .update_expression("SET #vec = :vec")
        .expression_attribute_names("#vec", vec_attr)
        .expression_attribute_values(":vec", alternator::float_list([1.0_f32, 1.0, 1.0]))
        .send()
        .await
        .expect("UpdateItem item-a should succeed");

    alternator::wait_for_ann(
        &vs_clients,
        &index,
        pk_attr,
        None,
        [1.0, 1.0, 1.0],
        &[Item::new(pk_attr, AttributeValue::S("item-a".into()))],
    )
    .await;
    info!("VS correctly reflects UpdateItem vector change via LWT path");

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

    common::wait_for_index_count(&vs_clients, &index, 3).await;
    info!("VS indexed BatchWriteItem puts via LWT path");

    // ANN([-1,-1,-1], limit=3): batch-c first, batch-b second, item-a last.
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

    common::wait_for_index_count(&vs_clients, &index, 3).await;

    alternator::wait_for_ann(
        &vs_clients,
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

    common::wait_for_index_count(&vs_clients, &index, 1).await;
    info!("VS correctly reflects delete-only BatchWriteItem via LWT path");

    info!("finished");
}

// Dedicated cluster: `always_use_lwt` write isolation cannot coexist with the
// shared standard cluster, so this is a top-level group with its own cluster.
e2etest::group!(
    name = alternator_lwt,
    fixtures = (Fixture),
    parent = crate::validator
);

struct Fixture {
    actors: TestActors,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<crate::TestEnv>().await?.new_cluster().await;

        alternator::init_with_args(
            &actors,
            [("--alternator-write-isolation", "always_use_lwt")],
        )
        .await;

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
