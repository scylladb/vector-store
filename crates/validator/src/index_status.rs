/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::*;
use httpapi::IndexName;
use httpapi::IndexStatus;
use httpapi::KeyspaceName;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = index_status,
    fixtures = (TestContext),
    parent = crate::standard
);

#[e2etest::test(group = index_status)]
async fn status_returned_correctly(ctx: Arc<TestContext>) {
    info!("started");

    let table = ctx
        .create_table("pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None)
        .await;

    // Insert some vectors
    let embedding: Vec<f32> = vec![0.0, 0.0, 0.0];
    let stmt = ctx
        .session
        .prepare(format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"))
        .await
        .expect("failed to prepare insert statement");
    for i in 0..10000 {
        ctx.session
            .execute_unpaged(&stmt, (i, &embedding))
            .await
            .expect("failed to insert data");
    }

    let index = ctx.create_index(&table, "v").await;

    for client in &ctx.clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.status,
            IndexStatus::Serving,
            "Expected index status to be Serving after indexing is complete"
        );
        assert_eq!(
            index_status.count, 10000,
            "Expected 10000 vectors to be indexed"
        );
    }

    info!("finished");
}

#[e2etest::test(group = index_status)]
async fn status_returns_404_for_non_existent_index(ctx: Arc<TestContext>) {
    info!("started");

    // Assert that querying the status of a non-existent index returns an HTTP 404 error
    let keyspace_name = KeyspaceName::from("non_existent_keyspace".to_string());
    let index_name = IndexName::from("non_existent_index".to_string());
    for client in &ctx.clients {
        let index_status = client.index_status(&keyspace_name, &index_name).await;
        assert!(index_status.is_err());
        assert!(index_status.err().unwrap().to_string().contains("404"));
    }

    info!("finished");
}
