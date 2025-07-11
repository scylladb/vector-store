/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_basic;
use crate::httpclient::HttpClient;
use std::net::SocketAddr;

#[tokio::test]
async fn get_applicaiton_info() {
    let (db_actor, _) = db_basic::new();
    let (server_actor, addr) =
        vector_store::run(SocketAddr::from(([127, 0, 0, 1], 0)).into(), Some(1))
            .await
            .unwrap();
    let client = HttpClient::new(addr);
    vector_store::set_engine(
        &server_actor,
        vector_store::new_index_factory_usearch().unwrap(),
        db_actor,
    )
    .await
    .unwrap();

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
}
