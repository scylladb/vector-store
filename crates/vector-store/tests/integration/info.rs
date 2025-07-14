/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_basic;
use crate::httpclient::HttpClient;
use std::net::SocketAddr;

#[tokio::test]
async fn get_applicaiton_info() {
    let node_state = vector_store::new_node_state().await;
    let (db_actor, _) = db_basic::new(node_state.clone());
    let (_server_actor, addr) = vector_store::run(
        SocketAddr::from(([127, 0, 0, 1], 0)).into(),
        Some(1),
        node_state,
        db_actor,
        vector_store::new_index_factory_usearch().unwrap(),
    )
    .await
    .unwrap();
    let client = HttpClient::new(addr);

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
}
