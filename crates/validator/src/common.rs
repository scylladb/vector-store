/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::dns::DnsExt;
use crate::scylla_cluster::ScyllaClusterExt;
use crate::tests::*;
use crate::vector_store_cluster::VectorStoreClusterExt;
use httpclient::HttpClient;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::response::query_result::QueryRowsResult;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::info;
use uuid::Uuid;
use vector_store::IndexInfo;

const VS_NAME: &str = "vs";

pub(crate) const VS_PORT: u16 = 6080;
pub(crate) const DB_PORT: u16 = 9042;

pub(crate) const VS_OCTET: u8 = 1;
pub(crate) const DB_OCTET: u8 = 2;

pub(crate) async fn get_default_vs_url(actors: &TestActors) -> String {
    format!(
        "http://{}.{}:{}",
        VS_NAME,
        actors.dns.domain().await,
        VS_PORT
    )
}

pub(crate) fn get_default_db_ip(actors: &TestActors) -> Ipv4Addr {
    actors.services_subnet.ip(DB_OCTET)
}

pub(crate) async fn init(actors: TestActors) {
    info!("started");

    let vs_ip = actors.services_subnet.ip(VS_OCTET);

    actors.dns.upsert(VS_NAME.to_string(), vs_ip).await;

    let vs_url = get_default_vs_url(&actors).await;

    let db_ip = get_default_db_ip(&actors);

    actors.db.start(vs_url, db_ip, None).await;
    assert!(actors.db.wait_for_ready().await);

    actors
        .vs
        .start((vs_ip, VS_PORT).into(), (db_ip, DB_PORT).into())
        .await;
    assert!(actors.vs.wait_for_ready().await);

    info!("finished");
}

pub(crate) async fn cleanup(actors: TestActors) {
    info!("started");
    actors.dns.remove(VS_NAME.to_string()).await;
    actors.vs.stop().await;
    actors.db.stop().await;
    info!("finished");
}

pub(crate) async fn prepare_connection(actors: &TestActors) -> (Arc<Session>, HttpClient) {
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(actors.services_subnet.ip(DB_OCTET).to_string())
            .build()
            .await
            .expect("failed to create session"),
    );
    let client = HttpClient::new((actors.services_subnet.ip(VS_OCTET), VS_PORT).into());
    (session, client)
}

pub(crate) async fn wait_for<F, Fut>(mut condition: F, msg: &str, timeout: Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    time::timeout(timeout, async {
        while !condition().await {
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Timeout on: {msg}"))
}

pub(crate) async fn wait_for_value<F, Fut, T>(mut poll_fn: F, msg: &str, timeout: Duration) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    time::timeout(timeout, async {
        loop {
            if let Some(value) = poll_fn().await {
                return value;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Timeout on: {msg}"))
}

pub(crate) async fn get_query_results(query: String, session: &Session) -> QueryRowsResult {
    session
        .query_unpaged(query, ())
        .await
        .expect("failed to run query")
        .into_rows_result()
        .expect("failed to get rows")
}

pub(crate) async fn get_opt_query_results(
    query: String,
    session: &Session,
) -> Option<QueryRowsResult> {
    session
        .query_unpaged(query, ())
        .await
        .ok()?
        .into_rows_result()
        .ok()
}

pub(crate) async fn create_keyspace(session: &Session) -> String {
    let keyspace = format!("ks_{}", Uuid::new_v4().simple());

    // Create keyspace
    session.query_unpaged(
        format!("CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"),
        (),
    ).await.expect("failed to create a keyspace");

    // Use keyspace
    session
        .use_keyspace(&keyspace, false)
        .await
        .expect("failed to use a keyspace");

    keyspace
}

pub(crate) async fn create_table(
    session: &Session,
    columns: &str,
    options: Option<&str>,
) -> String {
    let table = format!("tbl_{}", Uuid::new_v4().simple());

    let extra = if let Some(options) = options {
        format!("WITH {options}")
    } else {
        String::new()
    };

    // Create table
    session
        .query_unpaged(format!("CREATE TABLE {table} ({columns}) {extra}"), ())
        .await
        .expect("failed to create a table");

    table
}

pub(crate) async fn create_index(
    session: &Session,
    client: &HttpClient,
    table: &str,
    column: &str,
) -> IndexInfo {
    let index = format!("idx_{}", Uuid::new_v4().simple());

    // Create index
    session
        .query_unpaged(
            format!("CREATE INDEX {index} ON {table}({column}) USING 'vector_index'"),
            (),
        )
        .await
        .expect("failed to create an index");

    // Wait for the index to be created
    wait_for(
        || async {
            client
                .indexes()
                .await
                .iter()
                .any(|idx| idx.index.to_string() == index)
        },
        "Waiting for the first index to be created",
        Duration::from_secs(5),
    )
    .await;

    client
        .indexes()
        .await
        .into_iter()
        .find(|idx| idx.index.to_string() == index)
        .expect("index not found")
}
