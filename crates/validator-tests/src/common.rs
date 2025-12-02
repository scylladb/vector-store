/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::DnsExt;
use crate::ScyllaClusterExt;
use crate::ScyllaNodeConfig;
use crate::TestActors;
use crate::VectorStoreClusterExt;
use httpclient::HttpClient;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::response::query_result::QueryRowsResult;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::info;
use uuid::Uuid;
use vector_store::IndexInfo;
pub use vector_store::IndexName;
pub use vector_store::KeyspaceName;
pub use vector_store::httproutes::IndexStatus;

pub const DEFAULT_TEST_TIMEOUT: Duration = Duration::from_secs(120);

pub const VS_NAMES: [&str; 3] = ["vs1", "vs2", "vs3"];

pub const VS_PORT: u16 = 6080;
pub const DB_PORT: u16 = 9042;

pub const DB_OCTET_1: u8 = 1;
pub const DB_OCTET_2: u8 = 2;
pub const DB_OCTET_3: u8 = 3;
pub const VS_OCTET_1: u8 = 128;
pub const VS_OCTET_2: u8 = 129;
pub const VS_OCTET_3: u8 = 130;

pub async fn get_default_vs_urls(actors: &TestActors) -> Vec<String> {
    let domain = actors.dns.domain().await;
    VS_NAMES
        .iter()
        .map(|name| format!("http://{name}.{domain}:{VS_PORT}"))
        .collect()
}

pub fn get_default_vs_ips(actors: &TestActors) -> Vec<Ipv4Addr> {
    vec![
        actors.services_subnet.ip(VS_OCTET_1),
        actors.services_subnet.ip(VS_OCTET_2),
        actors.services_subnet.ip(VS_OCTET_3),
    ]
}

pub fn get_default_db_ips(actors: &TestActors) -> Vec<Ipv4Addr> {
    vec![
        actors.services_subnet.ip(DB_OCTET_1),
        actors.services_subnet.ip(DB_OCTET_2),
        actors.services_subnet.ip(DB_OCTET_3),
    ]
}

pub async fn get_default_scylla_node_configs(actors: &TestActors) -> Vec<ScyllaNodeConfig> {
    let default_vs_urls = get_default_vs_urls(actors).await;
    get_default_db_ips(actors)
        .iter()
        .enumerate()
        .map(|(i, &ip)| {
            let mut vs_urls = default_vs_urls.clone();
            ScyllaNodeConfig {
                db_ip: ip,
                primary_vs_uris: vec![vs_urls.remove(i)],
                secondary_vs_uris: vs_urls,
            }
        })
        .collect()
}

pub async fn init(actors: TestActors) {
    info!("started");

    let node_configs = get_default_scylla_node_configs(&actors).await;
    init_with_config(actors, node_configs).await;

    info!("finished");
}

pub async fn init_with_config(actors: TestActors, node_configs: Vec<ScyllaNodeConfig>) {
    let vs_ips = get_default_vs_ips(&actors);
    for (name, ip) in VS_NAMES.iter().zip(vs_ips.iter()) {
        actors.dns.upsert(name.to_string(), *ip).await;
    }

    let db_ip = node_configs.first().unwrap().db_ip; // Use the first DB node for vector store connection

    actors.db.start(node_configs, None).await;
    assert!(actors.db.wait_for_ready().await);
    actors
        .vs
        .start(
            (vs_ips[0], VS_PORT).into(),
            (db_ip, DB_PORT).into(),
            HashMap::new(),
        )
        .await;
    assert!(actors.vs.wait_for_ready().await);
}

pub async fn cleanup(actors: TestActors) {
    info!("started");
    for name in VS_NAMES.iter() {
        actors.dns.remove(name.to_string()).await;
    }
    actors.vs.stop().await;
    actors.db.stop().await;
    info!("finished");
}

pub async fn prepare_connection(actors: &TestActors) -> (Arc<Session>, HttpClient) {
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(actors.services_subnet.ip(DB_OCTET_1).to_string())
            .build()
            .await
            .expect("failed to create session"),
    );
    let client = HttpClient::new((actors.services_subnet.ip(VS_OCTET_1), VS_PORT).into());
    (session, client)
}

pub async fn wait_for<F, Fut>(mut condition: F, msg: &str, timeout: Duration)
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

pub async fn wait_for_value<F, Fut, T>(mut poll_fn: F, msg: &str, timeout: Duration) -> T
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

pub async fn wait_for_index(
    client: &HttpClient,
    index: &IndexInfo,
) -> vector_store::httproutes::IndexStatusResponse {
    wait_for_value(
        || async {
            match client.index_status(&index.keyspace, &index.index).await {
                Ok(resp) if resp.status == IndexStatus::Serving => Some(resp),
                _ => None,
            }
        },
        "Waiting for index to be SERVING",
        Duration::from_secs(20),
    )
    .await
}

pub async fn get_query_results(query: String, session: &Session) -> QueryRowsResult {
    session
        .query_unpaged(query, ())
        .await
        .expect("failed to run query")
        .into_rows_result()
        .expect("failed to get rows")
}

pub async fn get_opt_query_results(query: String, session: &Session) -> Option<QueryRowsResult> {
    session
        .query_unpaged(query, ())
        .await
        .ok()?
        .into_rows_result()
        .ok()
}

pub async fn create_keyspace(session: &Session) -> String {
    let keyspace = format!("ks_{}", Uuid::new_v4().simple());

    // Create keyspace with replication factor of 3 for the 3-node cluster
    session.query_unpaged(
        format!("CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}}"),
        (),
    ).await.expect("failed to create a keyspace");

    // Use keyspace
    session
        .use_keyspace(&keyspace, false)
        .await
        .expect("failed to use a keyspace");

    keyspace
}

pub async fn create_table(session: &Session, columns: &str, options: Option<&str>) -> String {
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

pub async fn create_index(
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
