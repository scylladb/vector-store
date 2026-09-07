/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use std::collections::HashMap;

use crate::TestActors;
use crate::common::*;
use e2etest_scylla_cluster::ScyllaClusterExt;
use e2etest_scylla_cluster::ScyllaNodeConfig;
use e2etest_tls::TlsExt;
use e2etest_vector_store_cluster::VectorStoreNodeConfig;
use scylla::statement::Statement;
use std::sync::Arc;
use tracing::info;

e2etest::group!(
    name = high_availability,
    fixtures = (Fixture),
    parent = crate::validator
);

struct Fixture {
    actors: TestActors,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<crate::TestEnv>().await?.new_cluster().await;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }

    // This cluster is independent of every other one, so its groups may run
    // alongside groups owning a different cluster.
    fn group_can_run_concurrently() -> bool {
        true
    }

    // Its nodes claim loopback addresses, ports and routes, which every other
    // cluster claims too, so it runs in a network namespace of its own, where
    // it is alone in claiming them.
    fn group_needs_own_namespace() -> bool {
        true
    }
}

#[e2etest::test(group = high_availability)]
async fn test_secondary_uri_works_correctly(fixture: Arc<Fixture>) {
    let actors = &fixture.actors;
    info!("started");

    let vs_urls = get_default_vs_urls(actors).await;
    let vs_url = &vs_urls[0];

    let db_ips = get_default_db_ips(actors);
    let first_vs_ip = get_default_vs_ips(actors)[0];
    let cert_path = actors.tls.cert_path().await;
    let key_path = actors.tls.key_path().await;
    let scylla_configs: Vec<ScyllaNodeConfig> = vec![
        ScyllaNodeConfig {
            db_ip: db_ips[0],
            primary_vs_uris: vec![vs_url.clone()],
            secondary_vs_uris: vec![],
            args: e2etest_scylla_cluster::default_scylla_args(),
            cert_path: Some(cert_path.clone()),
            key_path: Some(key_path.clone()),
            extra_config: Some(scylla_auth_config()),
        },
        ScyllaNodeConfig {
            db_ip: db_ips[1],
            primary_vs_uris: vec![],
            secondary_vs_uris: vec![vs_url.clone()],
            args: e2etest_scylla_cluster::default_scylla_args(),
            cert_path: Some(cert_path.clone()),
            key_path: Some(key_path.clone()),
            extra_config: Some(scylla_auth_config()),
        },
        ScyllaNodeConfig {
            db_ip: db_ips[2],
            primary_vs_uris: vec![],
            secondary_vs_uris: vec![vs_url.clone()],
            args: e2etest_scylla_cluster::default_scylla_args(),
            cert_path: Some(cert_path.clone()),
            key_path: Some(key_path.clone()),
            extra_config: Some(scylla_auth_config()),
        },
    ];
    let cert_env = actors
        .tls
        .cert_path()
        .await
        .to_str()
        .expect("cert path must be valid UTF-8")
        .to_string();
    let vs_configs = vec![VectorStoreNodeConfig {
        vs_ip: first_vs_ip,
        db_ip: get_default_db_ip(actors),
        envs: HashMap::from([(
            "VECTOR_STORE_SCYLLADB_CERTIFICATE_FILE".to_string(),
            cert_env,
        )]),
        user: Some(DEFAULT_DB_USER.to_string()),
        password: Some(DEFAULT_DB_PASSWORD.to_string()),
    }];
    init_with_config(actors, scylla_configs, vs_configs, true).await;

    let vs_ips = vec![first_vs_ip];
    let (session, clients) = prepare_connection_with_custom_vs_ips(actors, vs_ips).await;

    let keyspace = create_keyspace(&session).await;
    let table = create_table(&session, "pk INT PRIMARY KEY, v VECTOR<FLOAT, 3>", None).await;

    // Insert vectors
    for i in 0..100 {
        let embedding = vec![i as f32, (i * 2) as f32, (i * 3) as f32];
        session
            .query_unpaged(
                format!("INSERT INTO {table} (pk, v) VALUES (?, ?)"),
                (i, &embedding),
            )
            .await
            .expect("failed to insert data");
    }

    let index = create_index(CreateIndexQuery::new(&session, &clients, &table, "v")).await;

    for client in &clients {
        let index_status = wait_for_index(client, &index).await;
        assert_eq!(
            index_status.count, 100,
            "Expected 100 vectors to be indexed"
        );
    }

    // Down the first node with primary URI
    let first_node_ip = get_default_db_ip(actors);
    info!("Bringing down node {first_node_ip}");
    actors.db.down_node(first_node_ip).await;

    // Should work via secondary URIs
    let results = get_query_results(
        format!("SELECT pk FROM {table} ORDER BY v ANN OF [0.0, 0.0, 0.0] LIMIT 10"),
        &session,
    )
    .await;

    let rows = results
        .rows::<(i32,)>()
        .expect("failed to get rows after node down");
    assert!(
        rows.rows_remaining() <= 10,
        "Expected at most 10 results from ANN query after node down"
    );

    // Drop keyspace
    session
        .query_unpaged(
            {
                let mut stmt = Statement::new(format!("DROP KEYSPACE IF EXISTS {keyspace}"));
                stmt.set_is_idempotent(true);
                stmt
            },
            (),
        )
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
