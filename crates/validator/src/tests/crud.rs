/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::dns::DnsExt;
use crate::scylla_cluster::ScyllaClusterExt;
use crate::tests::*;
use crate::vector_store_cluster::VectorStoreClusterExt;
use httpclient::HttpClient;
use scylla::client::session_builder::SessionBuilder;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test(
            "simple_create_drop_index",
            timeout,
            simple_create_drop_index,
        )
}

const VS_NAME: &str = "vs";

const VS_PORT: u16 = 6080;
const DB_PORT: u16 = 9042;

const VS_OCTET: u8 = 1;
const DB_OCTET: u8 = 2;

async fn init(actors: TestActors) {
    info!("started");

    let vs_ip = actors.services_subnet.ip(VS_OCTET);

    actors.dns.upsert(VS_NAME.to_string(), Some(vs_ip)).await;

    let vs_url = format!(
        "http://{}.{}:{}",
        VS_NAME,
        actors.dns.domain().await,
        VS_PORT
    );

    let db_ip = actors.services_subnet.ip(DB_OCTET);

    actors.db.start(vs_url, db_ip, None).await;
    assert!(actors.db.wait_for_ready().await);

    actors
        .vs
        .start((vs_ip, VS_PORT).into(), (db_ip, DB_PORT).into())
        .await;
    assert!(actors.vs.wait_for_ready().await);

    info!("finished");
}

async fn cleanup(actors: TestActors) {
    info!("started");
    actors.dns.upsert(VS_NAME.to_string(), None).await;
    actors.vs.stop().await;
    actors.db.stop().await;
    info!("finished");
}

async fn simple_create_drop_index(actors: TestActors) {
    info!("started");

    let session = Arc::new(
        SessionBuilder::new()
            .known_node(actors.services_subnet.ip(DB_OCTET).to_string())
            .build()
            .await
            .expect("failed to create session"),
    );
    let client = HttpClient::new((actors.services_subnet.ip(VS_OCTET), VS_PORT).into());

    session.query_unpaged(
        "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        (),
    ).await.expect("failed to create a keyspace");

    session
        .use_keyspace("ks", false)
        .await
        .expect("failed to use a keyspace");

    session
        .query_unpaged(
            "
            CREATE TABLE tbl (id BIGINT PRIMARY KEY, embedding VECTOR<FLOAT, 3>)
            WITH cdc = {'enabled': true }
            ",
            (),
        )
        .await
        .expect("failed to create a table");

    session
        .query_unpaged(
            "CREATE INDEX idx ON tbl(embedding) USING 'vector_index'",
            (),
        )
        .await
        .expect("failed to create an index");

    while client.indexes().await.is_empty() {}
    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].keyspace.as_ref(), "ks");
    assert_eq!(indexes[0].index.as_ref(), "idx");

    session
        .query_unpaged("DROP INDEX idx", ())
        .await
        .expect("failed to drop an index");

    while !client.indexes().await.is_empty() {}

    session
        .query_unpaged("DROP KEYSPACE ks", ())
        .await
        .expect("failed to drop a keyspace");

    info!("finished");
}
