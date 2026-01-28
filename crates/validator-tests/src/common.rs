/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::DnsExt;
use crate::ScyllaClusterExt;
use crate::ScyllaNodeConfig;
use crate::ScyllaProxyClusterExt;
use crate::ScyllaProxyNodeConfig;
use crate::TestActors;
use crate::VectorStoreClusterExt;
use crate::VectorStoreNodeConfig;
use async_backtrace::framed;
use httpclient::HttpClient;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::response::query_result::QueryRowsResult;
use scylla::statement::Statement;
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

pub const DEFAULT_TEST_TIMEOUT: Duration = Duration::from_secs(10 * 60); // 10 minutes
pub const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(20);

pub const VS_NAMES: [&str; 3] = ["vs1", "vs2", "vs3"];

pub const VS_PORT: u16 = 6080;
pub const DB_PORT: u16 = 9042;

pub const DB_OCTET_1: u8 = 1;
pub const DB_OCTET_2: u8 = 2;
pub const DB_OCTET_3: u8 = 3;
pub const DB_PROXY_OCTET_1: u8 = 11;
pub const DB_PROXY_OCTET_2: u8 = 12;
pub const DB_PROXY_OCTET_3: u8 = 13;
pub const VS_OCTET_1: u8 = 21;
pub const VS_OCTET_2: u8 = 22;
pub const VS_OCTET_3: u8 = 23;

#[framed]
pub async fn get_default_vs_urls(actors: &TestActors) -> Vec<String> {
    let domain = actors.dns.domain().await;
    VS_NAMES
        .iter()
        .map(|name| format!("http://{name}.{domain}:{VS_PORT}"))
        .collect()
}

#[framed]
pub fn get_default_vs_ips(actors: &TestActors) -> Vec<Ipv4Addr> {
    vec![
        actors.services_subnet.ip(VS_OCTET_1),
        actors.services_subnet.ip(VS_OCTET_2),
        actors.services_subnet.ip(VS_OCTET_3),
    ]
}

#[framed]
pub fn get_default_db_ips(actors: &TestActors) -> Vec<Ipv4Addr> {
    vec![
        actors.services_subnet.ip(DB_OCTET_1),
        actors.services_subnet.ip(DB_OCTET_2),
        actors.services_subnet.ip(DB_OCTET_3),
    ]
}

pub fn get_default_db_proxy_ips(actors: &TestActors) -> Vec<Ipv4Addr> {
    vec![
        actors.services_subnet.ip(DB_PROXY_OCTET_1),
        actors.services_subnet.ip(DB_PROXY_OCTET_2),
        actors.services_subnet.ip(DB_PROXY_OCTET_3),
    ]
}

#[framed]
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
                args: crate::default_scylla_args(),
                config: None,
            }
        })
        .collect()
}

#[framed]
pub async fn get_default_scylla_proxy_node_configs(
    actors: &TestActors,
) -> Vec<ScyllaProxyNodeConfig> {
    let db_ips = get_default_db_ips(actors);
    get_default_db_proxy_ips(actors)
        .into_iter()
        .zip(db_ips.into_iter())
        .map(|(proxy_addr, real_addr)| ScyllaProxyNodeConfig {
            real_addr,
            proxy_addr,
        })
        .collect()
}

#[framed]
pub fn get_default_vs_node_configs(actors: &TestActors) -> Vec<VectorStoreNodeConfig> {
    let db_ips = get_default_db_ips(actors);
    get_default_vs_ips(actors)
        .iter()
        .zip(db_ips.iter())
        .map(|(&vs_ip, &db_ip)| VectorStoreNodeConfig {
            vs_ip,
            db_ip,
            user: None,
            password: None,
            envs: HashMap::new(),
        })
        .collect()
}

#[framed]
pub fn get_proxy_vs_node_configs(actors: &TestActors) -> Vec<VectorStoreNodeConfig> {
    let db_proxy_ips = get_default_db_proxy_ips(actors);
    get_default_vs_ips(actors)
        .iter()
        .zip(db_proxy_ips.iter())
        .map(|(&vs_ip, &db_ip)| VectorStoreNodeConfig {
            vs_ip,
            db_ip,
            user: None,
            password: None,
            envs: HashMap::new(),
        })
        .collect()
}

#[framed]
pub async fn init(actors: TestActors) {
    info!("started");

    let scylla_configs = get_default_scylla_node_configs(&actors).await;
    let vs_configs = get_default_vs_node_configs(&actors);
    init_with_config(actors, scylla_configs, vs_configs).await;

    info!("finished");
}

#[framed]
pub async fn init_with_proxy(actors: TestActors) {
    info!("started");

    init_dns(&actors).await;

    let scylla_configs = get_default_scylla_node_configs(&actors).await;
    let scylla_proxy_configs = get_default_scylla_proxy_node_configs(&actors).await;
    let mut vs_configs = get_proxy_vs_node_configs(&actors);

    actors.db.start(scylla_configs).await;
    assert!(actors.db.wait_for_ready().await);
    let translation_map = actors.db_proxy.start(scylla_proxy_configs).await;
    let envs: HashMap<_, _> = [(
        "VECTOR_STORE_CQL_URI_TRANSLATION_MAP".to_string(),
        serde_json::to_string(&translation_map).unwrap(),
    )]
    .into_iter()
    .collect();
    vs_configs.iter_mut().for_each(|cfg| {
        cfg.envs.extend(envs.clone());
    });
    actors.vs.start(vs_configs).await;
    assert!(actors.vs.wait_for_ready().await);

    info!("finished");
}

#[framed]
pub async fn init_with_proxy_single_vs(actors: TestActors) {
    info!("started");

    init_dns(&actors).await;

    let mut scylla_configs = get_default_scylla_node_configs(&actors).await;
    let first_vs_url = scylla_configs
        .first()
        .unwrap()
        .primary_vs_uris
        .first()
        .unwrap()
        .clone();
    info!("Using VS URL: {first_vs_url}");
    scylla_configs.iter_mut().for_each(|config| {
        config.primary_vs_uris = vec![first_vs_url.clone()];
        config.secondary_vs_uris = vec![];
    });
    let scylla_proxy_configs = get_default_scylla_proxy_node_configs(&actors).await;
    let mut vs_configs: Vec<_> = get_proxy_vs_node_configs(&actors)
        .into_iter()
        .take(1)
        .collect();

    actors.db.start(scylla_configs).await;
    assert!(actors.db.wait_for_ready().await);
    let translation_map = actors.db_proxy.start(scylla_proxy_configs).await;
    let envs: HashMap<_, _> = [(
        "VECTOR_STORE_CQL_URI_TRANSLATION_MAP".to_string(),
        serde_json::to_string(&translation_map).unwrap(),
    )]
    .into_iter()
    .collect();
    vs_configs.iter_mut().for_each(|cfg| {
        cfg.envs.extend(envs.clone());
    });
    actors.vs.start(vs_configs).await;
    assert!(actors.vs.wait_for_ready().await);

    info!("finished");
}

#[framed]
pub async fn init_dns(actors: &TestActors) {
    let vs_ips = get_default_vs_ips(actors);
    for (name, ip) in VS_NAMES.iter().zip(vs_ips.iter()) {
        actors.dns.upsert(name.to_string(), *ip).await;
    }
}

#[framed]
pub async fn init_with_config(
    actors: TestActors,
    scylla_configs: Vec<ScyllaNodeConfig>,
    vs_configs: Vec<VectorStoreNodeConfig>,
) {
    init_dns(&actors).await;

    actors.db.start(scylla_configs).await;
    assert!(actors.db.wait_for_ready().await);
    actors.vs.start(vs_configs).await;
    assert!(actors.vs.wait_for_ready().await);
}

#[framed]
pub async fn cleanup(actors: TestActors) {
    info!("started");
    for name in VS_NAMES.iter() {
        actors.dns.remove(name.to_string()).await;
    }
    actors.vs.stop().await;
    actors.db_proxy.stop().await;
    actors.db.stop().await;
    info!("finished");
}

#[framed]
pub async fn prepare_connection_with_custom_vs_ips(
    actors: &TestActors,
    vs_ips: Vec<Ipv4Addr>,
) -> (Arc<Session>, Vec<HttpClient>) {
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(actors.services_subnet.ip(DB_OCTET_1).to_string())
            .build()
            .await
            .expect("failed to create session"),
    );
    let clients = vs_ips
        .iter()
        .map(|&ip| HttpClient::new((ip, VS_PORT).into()))
        .collect();
    (session, clients)
}

#[framed]
pub async fn prepare_connection_with_auth(
    actors: &TestActors,
    user: &str,
    password: &str,
) -> (Arc<Session>, Vec<HttpClient>) {
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(actors.services_subnet.ip(DB_OCTET_1).to_string())
            .user(user, password)
            .build()
            .await
            .expect("failed to create session"),
    );
    let vs_ips = get_default_vs_ips(actors);
    let clients = vs_ips
        .iter()
        .map(|&ip| HttpClient::new((ip, VS_PORT).into()))
        .collect();
    (session, clients)
}

#[framed]
pub async fn prepare_connection(actors: &TestActors) -> (Arc<Session>, Vec<HttpClient>) {
    prepare_connection_with_custom_vs_ips(actors, get_default_vs_ips(actors)).await
}

#[framed]
pub async fn prepare_connection_single_vs(actors: &TestActors) -> (Arc<Session>, Vec<HttpClient>) {
    prepare_connection_with_custom_vs_ips(
        actors,
        get_default_vs_ips(actors).into_iter().take(1).collect(),
    )
    .await
}

#[framed]
pub async fn wait_for<F, Fut>(mut condition: F, msg: impl AsRef<str>, timeout: Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    info!("Waiting for: {msg}", msg = msg.as_ref());
    time::timeout(timeout, async {
        while !condition().await {
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Timeout on: {msg}", msg = msg.as_ref()))
}

#[framed]
pub async fn wait_for_value<F, Fut, T>(mut poll_fn: F, msg: impl AsRef<str>, timeout: Duration) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    info!("Waiting for: {msg}", msg = msg.as_ref());
    time::timeout(timeout, async {
        loop {
            if let Some(value) = poll_fn().await {
                return value;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Timeout on: {msg}", msg = msg.as_ref()))
}

#[framed]
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
        Duration::from_secs(60),
    )
    .await
}

#[framed]
pub async fn get_query_results(query: impl Into<String>, session: &Session) -> QueryRowsResult {
    let mut stmt = Statement::new(query);
    stmt.set_is_idempotent(true);
    session
        .query_unpaged(stmt, ())
        .await
        .expect("failed to run query")
        .into_rows_result()
        .expect("failed to get rows")
}

#[framed]
pub async fn get_opt_query_results(
    query: impl Into<String>,
    session: &Session,
) -> Option<QueryRowsResult> {
    let mut stmt = Statement::new(query);
    stmt.set_is_idempotent(true);
    session
        .query_unpaged(stmt, ())
        .await
        .ok()?
        .into_rows_result()
        .ok()
}

#[framed]
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

#[framed]
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

#[framed]
pub async fn create_index(
    session: &Session,
    clients: &[HttpClient],
    table: &str,
    column: &str,
) -> IndexInfo {
    create_index_with_options(session, clients, table, column, None).await
}

#[framed]
pub async fn create_index_with_options(
    session: &Session,
    clients: &[HttpClient],
    table: &str,
    column: &str,
    options: Option<&str>,
) -> IndexInfo {
    let index = format!("idx_{}", Uuid::new_v4().simple());

    let extra = if let Some(options) = options {
        format!("WITH OPTIONS = {options}")
    } else {
        String::new()
    };

    // Create index
    session
        .query_unpaged(
            format!("CREATE INDEX {index} ON {table}({column}) USING 'vector_index' {extra}"),
            (),
        )
        .await
        .expect("failed to create an index");

    // Wait for the index to be created
    wait_for(
        || async {
            for client in clients.iter() {
                if !client
                    .indexes()
                    .await
                    .iter()
                    .any(|idx| idx.index.to_string() == index)
                {
                    return false;
                }
            }
            true
        },
        "Waiting for the first index to be created",
        Duration::from_secs(60),
    )
    .await;

    clients
        .first()
        .expect("No vector store clients provided")
        .indexes()
        .await
        .into_iter()
        .find(|idx| idx.index.to_string() == index)
        .expect("index not found")
}
