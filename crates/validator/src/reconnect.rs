/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::common::*;
use e2etest_firewall::FirewallExt;
use e2etest_scylla_cluster::ScyllaClusterExt;
use e2etest_scylla_proxy_cluster::ScyllaProxyClusterExt;
use e2etest_vector_store_cluster::VectorStoreClusterExt;
use httpapi::IndexInfo;
use httpapi::IndexName;
use httpapi::IndexStatus;
use httpapi::KeyspaceName;
use httpclient::HttpClient;
use scylla::client::session::Session;
use scylla::errors::ExecutionError;
use scylla_proxy::Condition;
use scylla_proxy::Reaction;
use scylla_proxy::RequestReaction;
use scylla_proxy::RequestRule;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tap::Pipe;
use tracing::info;

const FRAME_DELAY: Duration = Duration::from_millis(100);
const DATASET_SIZE: i32 = 100;
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(12); // slightly more than default 10s

e2etest::group!(name = reconnect, fixtures = (), parent = crate::validator);

struct Fixture {
    actors: Arc<TestActors>,
    session: Arc<Session>,
    clients: Vec<HttpClient>,
    keyspace: KeyspaceName,
    table: TableName,
    index: Arc<Mutex<Option<IndexName>>>,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        init_with_proxy_single_vs(&actors).await;

        let (session, clients) = prepare_connection_single_vs_no_tls(&actors).await;

        let keyspace = create_keyspace(&session).await;
        let table = create_table(
            &session,
            "id INT PRIMARY KEY, embedding VECTOR<FLOAT, 3>",
            Some("CDC = {'enabled': true}"),
        )
        .await;

        let stmt = session
            .prepare(format!(
                "INSERT INTO {table} (id, embedding) VALUES (?, [1.0, 2.0, 3.0])"
            ))
            .await
            .expect("failed to prepare a statement");

        info!("Inserting data into the table");
        for id in 0..DATASET_SIZE {
            session
                .execute_unpaged(&stmt, (id,))
                .await
                .expect("failed to insert a row");
        }

        let results = get_query_results(format!("SELECT * FROM {table}"), &session).await;
        let rows = results
            .rows::<(i32, Vec<f32>)>()
            .expect("failed to get rows");
        assert_eq!(rows.rows_remaining(), DATASET_SIZE as usize);

        // Flush to disk to ensure data is persisted before restarting nodes
        actors.db.flush().await;

        Some(Self {
            actors,
            session,
            clients,
            keyspace,
            table,
            index: Arc::new(Mutex::new(None)),
        })
    }

    async fn teardown(self) {
        info!("Dropping cluster");
        self.actors.firewall.turn_off_rules().await;
        cleanup(&self.actors).await;
    }
}

impl Fixture {
    fn client(&self) -> &HttpClient {
        self.clients.first().unwrap()
    }

    fn index_name(&self) -> IndexName {
        self.index
            .lock()
            .unwrap()
            .clone()
            .expect("index not created")
    }

    async fn create_index(&self) {
        info!("Creating index");
        let index = create_index(CreateIndexQuery::new(
            &self.session,
            &self.clients,
            &self.table,
            "embedding",
        ))
        .await;
        self.index.lock().unwrap().replace(index.index);
    }

    async fn wait_for_index(&self) -> IndexInfo {
        info!("Waiting for all vectors to be indexed");
        wait_for_index(
            self.client(),
            &IndexInfo::new(self.keyspace.as_ref(), self.index_name().as_ref()),
        )
        .await
    }

    async fn index_info(&self) -> IndexInfo {
        self.client()
            .index_status(&self.keyspace, &self.index_name())
            .await
            .expect("failed to get index status")
    }

    async fn query_sample_vector(&self) -> Result<(), ExecutionError> {
        self.session
            .query_unpaged(
                format!(
                    "SELECT * FROM {table} ORDER BY embedding ANN OF [1.0, 2.0, 3.0] LIMIT 1",
                    table = self.table
                ),
                (),
            )
            .await
            .map(|_| ())
    }

    async fn slow_db_proxy(&self) {
        info!("Slow communication between vector-store and scylla using proxy");
        self.actors
            .db_proxy
            .change_request_rules(Some(vec![RequestRule(
                Condition::True,
                RequestReaction::delay(FRAME_DELAY),
            )]))
            .await;
    }

    async fn restore_db_proxy(&self) {
        info!("Disable rules on scylla-proxy");
        self.actors.db_proxy.turn_off_rules().await;
    }

    async fn disconnect_db_proxy(&self, ips: impl IntoIterator<Item = usize>) {
        let proxy_ips = get_default_db_proxy_ips(&self.actors);
        let ips: Vec<_> = ips
            .into_iter()
            .map(|i| *proxy_ips.get(i).unwrap())
            .collect();
        info!("Disconnect scylla-proxy: {ips:?}");
        self.actors.firewall.drop_traffic(ips).await;
    }

    async fn reconnect_db_proxy(&self) {
        info!("Reconnect scylla-proxy");
        self.actors.firewall.turn_off_rules().await;
    }

    async fn counters(&self) -> BTreeMap<String, u64> {
        let client = self.client();
        let mut map = client.internals_counters().await.unwrap();
        map.append(&mut client.internals_session_counters().await.unwrap());
        map
    }

    async fn counter(&self, name: &str) -> u64 {
        *self.counters().await.get(name).unwrap()
    }

    async fn stop_vs_cluster(&self) {
        info!("Stopping VS cluster");
        self.actors.vs.stop().await;
    }

    async fn restart_vs_cluster(&self) {
        self.actors
            .vs
            .start(get_proxy_vs_node_configs(&self.actors).pipe(|mut nodes| {
                let translation_map = get_proxy_translation_map(&self.actors);
                for node in nodes.iter_mut() {
                    node.envs.insert(
                        "VECTOR_STORE_CQL_URI_TRANSLATION_MAP".to_string(),
                        serde_json::to_string(&translation_map).unwrap(),
                    );
                }
                nodes.truncate(1);
                nodes
            }))
            .await;
    }
}

#[e2etest::test(group = reconnect)]
async fn reconnect_doesnt_break_fullscan(fixture: Arc<Fixture>) {
    info!("Restart internals counter for cdc handler errors");
    fixture.client().internals_clear_counters().await.unwrap();
    fixture
        .client()
        .internals_start_counter("session-create-success".to_string())
        .await
        .unwrap();
    fixture
        .client()
        .internals_start_counter("session-create-failure".to_string())
        .await
        .unwrap();

    fixture.slow_db_proxy().await;

    fixture.create_index().await;

    info!("Checking that full scan isn't completed");
    let result = fixture.query_sample_vector().await;
    match &result {
        Err(e) if format!("{e:?}").contains("503 Service Unavailable") => {}
        _ => panic!("Expected SERVICE_UNAVAILABLE error, got: {result:?}"),
    }

    fixture.disconnect_db_proxy([0, 1, 2]).await;

    info!(
        "counters: {counters:?}",
        counters = fixture.counters().await
    );
    wait_for(
        || async {
            let counters = fixture.counters().await;
            if let Some(counter) = counters.get("session-create-success")
                && *counter > 0
            {
                return true;
            }
            if let Some(counter) = counters.get("session-create-failure")
                && *counter > 0
            {
                return true;
            }
            if let Some(counter) = counters.get("total-connections")
                && *counter == 0
            {
                return true;
            }
            false
        },
        "Connection must be closed",
        KEEPALIVE_TIMEOUT,
    )
    .await;
    info!(
        "counters: {counters:?}",
        counters = fixture.counters().await
    );

    info!("Check index status is still BOOTSTRAPPING");
    assert!(
        fixture.index_info().await.status == IndexStatus::Bootstrapping,
        "Full scan should be interrupted by disconnect"
    );

    fixture.reconnect_db_proxy().await;
    fixture.restore_db_proxy().await;

    wait_for(
        || async {
            info!(
                "counters: {counters:?}",
                counters = fixture.counters().await
            );
            info!(
                "status: {status:?}",
                status = fixture.index_info().await.status
            );
            fixture.query_sample_vector().await.is_ok()
        },
        "index must be built",
        Duration::from_secs(60),
    )
    .await;
}

#[e2etest::test(group = reconnect)]
async fn restarting_one_node_doesnt_break_fullscan(fixture: Arc<Fixture>) {
    fixture.slow_db_proxy().await;

    fixture.create_index().await;

    info!("Checking that full scan isn't completed");
    wait_for(
        || async { fixture.index_info().await.status == IndexStatus::Bootstrapping },
        format!(
            "index {index:?} must be bootstrapping",
            index = fixture.index_name()
        ),
        Duration::from_secs(10),
    )
    .await;

    let total_connections = fixture.counter("total-connections").await;

    fixture.disconnect_db_proxy([0]).await;

    wait_for(
        || async {
            let counters = fixture.counters().await;
            info!("counters: {counters:?}");
            *counters.get("total-connections").unwrap() < total_connections
        },
        "connections to proxy 0 must be closed",
        KEEPALIVE_TIMEOUT,
    )
    .await;

    fixture.reconnect_db_proxy().await;
    fixture.restore_db_proxy().await;

    info!("Waiting for all vectors to be indexed");
    let index_status = fixture.wait_for_index().await;
    assert_eq!(
        index_status.count, DATASET_SIZE as usize,
        "Expected {DATASET_SIZE} vectors to be indexed"
    );

    fixture
        .query_sample_vector()
        .await
        .expect("failed to query ANN search");
}

#[e2etest::test(group = reconnect)]
async fn restarting_all_nodes_doesnt_break_fullscan(fixture: Arc<Fixture>) {
    fixture.slow_db_proxy().await;

    fixture.create_index().await;

    wait_for(
        || async { fixture.index_info().await.status == IndexStatus::Bootstrapping },
        format!(
            "index {index:?} must be bootstrapping",
            index = fixture.index_name()
        ),
        Duration::from_secs(10),
    )
    .await;

    let total_connections = fixture.counter("total-connections").await;
    info!("Base number of total connections: {total_connections}");

    info!("Restart each node one by one");
    for proxy_no in [0, 1, 2] {
        fixture.disconnect_db_proxy([proxy_no]).await;

        wait_for(
            || async {
                let counters = fixture.counters().await;
                info!("counters: {counters:?}");
                *counters.get("total-connections").unwrap() < total_connections
            },
            format!(
                "connections to {proxy_no} must be closed, \
                number of connections should be less than {total_connections}"
            ),
            KEEPALIVE_TIMEOUT,
        )
        .await;

        fixture.reconnect_db_proxy().await;

        wait_for(
            || async {
                let counters = fixture.counters().await;
                info!("counters: {counters:?}");
                *counters.get("total-connections").unwrap() == total_connections
            },
            format!(
                "connections to {proxy_no} must be opened, \
                number of connections should be {total_connections}"
            ),
            KEEPALIVE_TIMEOUT,
        )
        .await;
    }

    fixture.restore_db_proxy().await;

    info!("Waiting for all vectors to be indexed");
    let index_status = fixture.wait_for_index().await;
    assert_eq!(
        index_status.count, DATASET_SIZE as usize,
        "Expected {DATASET_SIZE} vectors to be indexed"
    );

    fixture
        .query_sample_vector()
        .await
        .expect("failed to query ANN search");
}

#[e2etest::test(group = reconnect)]
async fn test_restarting_vs_cluster_does_not_break_setup(fixture: Arc<Fixture>) {
    fixture.slow_db_proxy().await;

    fixture.create_index().await;

    fixture.stop_vs_cluster().await;

    fixture.restore_db_proxy().await;

    fixture.restart_vs_cluster().await;

    info!("Waiting for all vectors to be indexed");
    let index_status = fixture.wait_for_index().await;
    assert_eq!(
        index_status.count, DATASET_SIZE as usize,
        "Expected {DATASET_SIZE} vectors to be indexed"
    );

    fixture
        .query_sample_vector()
        .await
        .expect("failed to query ANN search");
}
