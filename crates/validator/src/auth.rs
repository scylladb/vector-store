/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::common::*;
use e2etest_scylla_cluster::ScyllaClusterExt;
use e2etest_vector_store_cluster::VectorStoreClusterExt;
use httpapi::IndexStatus;
use httpapi::NodeStatus;
use httpclient::HttpClient;
use scylla::client::session::Session;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

const WAITING_FOR_DB_DISCOVERY: Duration = Duration::from_secs(5);

/// The role Vector Store is pointed at. Each test decides what to grant it.
const ROLE: &str = "alice";
const ROLE_PASSWORD: &str = "alice_password";

e2etest::group!(name = auth, fixtures = (), parent = crate::owned);

/// Starts a cluster whose Vector Store nodes connect as [`ROLE`], which does
/// not exist yet, and returns a superuser session and a client per node.
async fn start_cluster_connecting_as_role(actors: &TestActors) -> (Arc<Session>, Vec<HttpClient>) {
    let mut scylla_configs = get_default_scylla_node_configs(actors).await;
    for config in scylla_configs.iter_mut() {
        config.extra_config = Some(scylla_auth_config());
    }

    let mut vs_configs = get_default_vs_node_configs(actors).await;
    for config in vs_configs.iter_mut() {
        config.user = Some(ROLE.to_string());
        config.password = Some(ROLE_PASSWORD.to_string());
    }

    info!("Initializing cluster");
    init_dns(actors).await;
    actors.db.start(scylla_configs).await;
    assert!(actors.db.wait_for_ready().await);
    actors.vs.start(vs_configs).await;

    info!("Waiting for DB discovery");
    sleep(WAITING_FOR_DB_DISCOVERY).await;

    info!("Connecting to scylladb as superuser over TLS");
    prepare_connection_with_auth(actors, &SUPERUSER_NAME, &SUPERUSER_PASSWORD).await
}

async fn create_role(session: &Session) {
    info!("Creating a role {ROLE} without permissions");
    session
        .query_unpaged(
            format!("CREATE ROLE {ROLE} WITH PASSWORD = '{ROLE_PASSWORD}' AND LOGIN = true"),
            (),
        )
        .await
        .expect("failed to create the role");
}

async fn grant_indexing(session: &Session) {
    info!("Granting VECTOR_SEARCH_INDEXING to {ROLE}");
    session
        .query_unpaged(
            format!("GRANT VECTOR_SEARCH_INDEXING ON ALL KEYSPACES TO {ROLE}"),
            (),
        )
        .await
        .expect("failed to grant permissions to the role");
}

async fn assert_all_connecting_to_db(clients: &[HttpClient]) {
    info!("Vector-store's should be in ConnectingToDb state");
    for client in clients {
        assert_eq!(client.status().await.unwrap(), NodeStatus::ConnectingToDb);
    }
}

#[e2etest::test(group = auth)]
async fn vs_doesnt_work_without_permission(cluster: Arc<CustomCluster>) {
    info!("started");

    let actors = cluster.actors();
    let (session, clients) = start_cluster_connecting_as_role(actors).await;

    assert_all_connecting_to_db(&clients).await;

    create_role(&session).await;

    info!("Waiting for DB discovery");
    sleep(WAITING_FOR_DB_DISCOVERY).await;

    // Logging in is not enough without VECTOR_SEARCH_INDEXING.
    assert_all_connecting_to_db(&clients).await;

    info!("finished");
}

#[e2etest::test(group = auth)]
async fn vs_works_when_permission_granted(cluster: Arc<CustomCluster>) {
    info!("started");

    let actors = cluster.actors();
    let (session, clients) = start_cluster_connecting_as_role(actors).await;

    assert_all_connecting_to_db(&clients).await;

    create_role(&session).await;
    grant_indexing(&session).await;

    info!("Waiting for vector-store ready state");
    assert!(actors.vs.wait_for_ready().await);

    info!("Creating keyspace, table and index");
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v1 VECTOR<FLOAT, 3>", None).await;
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v1")).await;

    info!("Waiting for index to be ready");
    for client in clients.iter() {
        wait_for_index(client, &index).await;
    }

    drop_keyspace(&session, &keyspace).await;

    info!("finished");
}

#[e2etest::test(group = auth)]
async fn cdc_works_with_auth(cluster: Arc<CustomCluster>) {
    info!("started");

    let actors = cluster.actors();
    let (session, clients) = start_cluster_connecting_as_role(actors).await;

    create_role(&session).await;
    grant_indexing(&session).await;

    info!("Waiting for vector-store ready state");
    assert!(actors.vs.wait_for_ready().await);

    info!("Creating keyspace, table and index");
    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v1 VECTOR<FLOAT, 3>", None).await;
    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v1")).await;

    info!("Waiting for index to be ready");
    for client in clients.iter() {
        wait_for_index(client, &index).await;
    }

    info!("Inserting data into CDC-enabled table");
    let stmt = session
        .prepare(format!("INSERT INTO {table} (pk, v1) VALUES (?, ?)"))
        .await
        .expect("failed to prepare insert statement");
    const ROWS: usize = 10;
    for i in 0..ROWS as i32 {
        let vector = vec![i as f32, (i + 1) as f32, (i + 2) as f32];
        session
            .execute_unpaged(&stmt, (i, vector))
            .await
            .expect("failed to insert row");
    }

    info!("Waiting for CDC to propagate data to index");
    wait_for(
        || async {
            for client in clients.iter() {
                match client.index_status(&index.keyspace, &index.index).await {
                    Ok(resp) if resp.status == IndexStatus::Serving && resp.count == ROWS => {}
                    _ => return false,
                }
            }
            true
        },
        format!("all {ROWS} rows to be indexed"),
        DEFAULT_OPERATION_TIMEOUT,
    )
    .await;

    drop_keyspace(&session, &keyspace).await;

    info!("finished");
}
