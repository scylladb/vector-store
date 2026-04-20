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
use crate::TlsExt;
use crate::VectorStoreClusterExt;
use crate::VectorStoreNodeConfig;
use async_backtrace::framed;
use httpclient::HttpClient;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::client::session::TlsContext;
use scylla::client::session_builder::SessionBuilder;
use scylla::response::query_result::QueryRowsResult;
use scylla::statement::Statement;
use std::collections::HashMap;
use std::iter;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tap::Pipe;
use tokio::time;
use tracing::info;
use vector_store::IndexInfo;
use vector_store::IndexName;
use vector_store::KeyspaceName;
use vector_store::TableName;
use vector_store::httproutes::IndexStatus;

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

/// Returns the default DB IPs for a given subnet. This variant can be used
/// before `TestActors` is constructed (e.g. during TLS cert generation).
pub fn get_default_db_ips_for_subnet(subnet: &crate::ServicesSubnet) -> Vec<Ipv4Addr> {
    vec![
        subnet.ip(DB_OCTET_1),
        subnet.ip(DB_OCTET_2),
        subnet.ip(DB_OCTET_3),
    ]
}

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
    get_default_db_ips_for_subnet(&actors.services_subnet)
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
    let cert_path = actors.tls.cert_path().await;
    let key_path = actors.tls.key_path().await;
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
                cert_path: Some(cert_path.clone()),
                key_path: Some(key_path.clone()),
                extra_config: None,
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
pub async fn get_default_vs_node_configs(actors: &TestActors) -> Vec<VectorStoreNodeConfig> {
    let db_ips = get_default_db_ips(actors);
    let cert_path = actors
        .tls
        .cert_path()
        .await
        .to_str()
        .expect("cert path is not valid UTF-8")
        .to_string();
    get_default_vs_ips(actors)
        .iter()
        .zip(db_ips.iter())
        .map(|(&vs_ip, &db_ip)| VectorStoreNodeConfig {
            vs_ip,
            db_ip,
            user: None,
            password: None,
            envs: [(
                "VECTOR_STORE_SCYLLADB_CERTIFICATE_FILE".to_string(),
                cert_path.clone(),
            )]
            .into_iter()
            .collect(),
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
            envs: Default::default(),
        })
        .collect()
}

/// Computes the proxy translation map from the known DB and proxy IPs.
/// This is the same mapping that `ScyllaProxyClusterExt::start` returns,
/// and can be used to reconstruct VS configs for proxy-based tests without
/// needing the original return value.
#[framed]
pub fn get_proxy_translation_map(
    actors: &TestActors,
) -> HashMap<std::net::SocketAddr, std::net::SocketAddr> {
    get_default_db_ips(actors)
        .into_iter()
        .zip(get_default_db_proxy_ips(actors).into_iter())
        .map(|(real_ip, proxy_ip)| {
            (
                std::net::SocketAddr::from((real_ip, DB_PORT)),
                std::net::SocketAddr::from((proxy_ip, DB_PORT)),
            )
        })
        .collect()
}

#[framed]
pub async fn init(actors: TestActors) {
    info!("started");

    let scylla_configs = get_default_scylla_node_configs(&actors).await;
    let vs_configs = get_default_vs_node_configs(&actors).await;
    init_with_config(actors, scylla_configs, vs_configs).await;

    info!("finished");
}

#[framed]
pub async fn init_with_proxy(actors: TestActors) {
    info!("started");

    init_dns(&actors).await;

    // Proxy operates at CQL frame level and cannot handle TLS, so ScyllaDB
    // must be started without TLS when using the proxy.
    let scylla_configs: Vec<ScyllaNodeConfig> = get_default_scylla_node_configs(&actors)
        .await
        .into_iter()
        .map(|mut c| {
            c.cert_path = None;
            c.key_path = None;
            c
        })
        .collect();
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

    // Proxy operates at CQL frame level and cannot handle TLS, so ScyllaDB
    // must be started without TLS when using the proxy.
    let mut scylla_configs: Vec<ScyllaNodeConfig> = get_default_scylla_node_configs(&actors)
        .await
        .into_iter()
        .map(|mut c| {
            c.cert_path = None;
            c.key_path = None;
            c
        })
        .collect();
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
    let tls_config = actors.tls.client_tls_config().await;
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(actors.services_subnet.ip(DB_OCTET_1).to_string())
            .tls_context(Some(TlsContext::from(tls_config)))
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
    let tls_config = actors.tls.client_tls_config().await;
    let session = Arc::new(
        SessionBuilder::new()
            .known_node(actors.services_subnet.ip(DB_OCTET_1).to_string())
            .user(user, password)
            .tls_context(Some(TlsContext::from(tls_config)))
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

/// Creates a CQL session and VS HTTP clients without TLS.
/// Use this variant for tests that go through the scylla-proxy, which
/// operates at the CQL frame level and cannot handle TLS traffic.
#[framed]
pub async fn prepare_connection_no_tls(actors: &TestActors) -> (Arc<Session>, Vec<HttpClient>) {
    prepare_connection_with_custom_vs_ips_no_tls(actors, get_default_vs_ips(actors)).await
}

/// Creates a CQL session (without TLS) and VS HTTP clients for a single VS.
#[framed]
pub async fn prepare_connection_single_vs_no_tls(
    actors: &TestActors,
) -> (Arc<Session>, Vec<HttpClient>) {
    prepare_connection_with_custom_vs_ips_no_tls(
        actors,
        get_default_vs_ips(actors).into_iter().take(1).collect(),
    )
    .await
}

#[framed]
pub async fn prepare_connection_with_custom_vs_ips_no_tls(
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
        format!(
            "Waiting for index to be SERVING at {url}",
            url = client.url()
        ),
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

static KEYSPACE_COUNTER: AtomicUsize = AtomicUsize::new(0);
static TABLE_COUNTER: AtomicUsize = AtomicUsize::new(0);
static INDEX_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn unique_name(prefix: &str, counter: &AtomicUsize) -> String {
    format!(
        "{prefix}_{counter}",
        counter = counter.fetch_add(1, Ordering::Relaxed)
    )
}

pub fn unique_keyspace_name() -> KeyspaceName {
    unique_name("ksp", &KEYSPACE_COUNTER).into()
}

pub fn unique_table_name() -> TableName {
    unique_name("tbl", &TABLE_COUNTER).into()
}

pub fn unique_index_name() -> IndexName {
    unique_name("idx", &INDEX_COUNTER).into()
}

#[framed]
pub async fn create_keyspace(session: &Session) -> KeyspaceName {
    let keyspace = unique_keyspace_name();

    // Create keyspace with replication factor of 3 for the 3-node cluster
    session.query_unpaged(
        format!("CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}}"),
        (),
    ).await.expect("failed to create a keyspace");

    // Use keyspace
    session
        .use_keyspace(keyspace.as_ref(), false)
        .await
        .expect("failed to use a keyspace");

    keyspace
}

#[framed]
pub async fn create_table(session: &Session, columns: &str, options: Option<&str>) -> TableName {
    let table = unique_table_name();

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
pub async fn create_index(query: CreateIndexQuery<'_>) -> IndexInfo {
    let cql_query = format!(
        "CREATE CUSTOM INDEX {index} ON {table}({partition_columns}{vector_column}{filter_columns}) USING 'vector_index' WITH OPTIONS = {{{options}}}",
        index = query.index,
        table = query.table,
        partition_columns = query.partition_columns,
        vector_column = query.vector_column,
        filter_columns = query.filter_columns,
        options = query.options,
    );
    info!("Create index: '{cql_query}'");
    query
        .session
        .query_unpaged(cql_query, ())
        .await
        .expect("failed to create an index");

    for client in query.clients {
        wait_for(
            || async {
                client
                    .indexes()
                    .await
                    .iter()
                    .any(|idx| idx.index == query.index)
            },
            format!(
                "the index {index} at {url} must be created",
                index = query.index,
                url = client.url()
            ),
            Duration::from_secs(60),
        )
        .await;
    }

    query.clients[0]
        .indexes()
        .await
        .into_iter()
        .find(|idx| idx.index == query.index)
        .expect("index not found")
}

pub struct CreateIndexQuery<'a> {
    session: &'a Session,
    clients: &'a [HttpClient],
    table: String,
    index: IndexName,
    partition_columns: String,
    vector_column: String,
    filter_columns: String,
    options: String,
}

impl<'a> CreateIndexQuery<'a> {
    pub fn new(
        session: &'a Session,
        clients: &'a [HttpClient],
        table: impl AsRef<str>,
        vector_column: impl AsRef<str>,
    ) -> Self {
        assert!(!clients.is_empty(), "No vector store clients provided");
        Self {
            session,
            clients,
            table: table.as_ref().into(),
            index: unique_index_name(),
            partition_columns: String::new(),
            vector_column: vector_column.as_ref().into(),
            filter_columns: String::new(),
            options: String::new(),
        }
    }

    pub fn partition_columns(
        mut self,
        partition_columns: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Self {
        self.partition_columns = partition_columns
            .into_iter()
            .map(|column| column.as_ref().to_string())
            .join(", ")
            .pipe(|columns| {
                if !columns.is_empty() {
                    format!("({}), ", columns)
                } else {
                    columns
                }
            });
        self
    }

    pub fn filter_columns(
        mut self,
        filter_columns: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Self {
        self.filter_columns = iter::once(String::new())
            .chain(
                filter_columns
                    .into_iter()
                    .map(|column| column.as_ref().to_string()),
            )
            .join(", ");
        self
    }

    pub fn options(
        mut self,
        options: impl IntoIterator<Item = (impl AsRef<str>, impl AsRef<str>)>,
    ) -> Self {
        self.options = options
            .into_iter()
            .map(|(k, v)| format!("'{}': '{}'", k.as_ref(), v.as_ref()))
            .join(", ");
        self
    }
}
