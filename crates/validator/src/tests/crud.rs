/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::dns::DnsExt;
use crate::scylla_cluster::ScyllaClusterExt;
use crate::tests::*;
use crate::vector_store_cluster::VectorStoreClusterExt;
use std::time::Duration;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test("dummy", timeout, dummy)
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

async fn dummy(_actors: TestActors) {
    info!("started");
    info!("finished");
}
