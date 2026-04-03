/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_basic;
use crate::tls_utils::generate_server_cert;
use crate::tls_utils::init;
use crate::tls_utils::read_cert;
use reqwest::StatusCode;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;
use vector_store::Config;
use vector_store::HttpServerExt;
use vector_store::httproutes::PostIndexAnnRequest;

async fn run_server(
    addr: core::net::SocketAddr,
    tls_cert_path: Option<PathBuf>,
    tls_key_path: Option<PathBuf>,
) -> (impl Sized, core::net::SocketAddr, impl Sized) {
    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();
    let (db_actor, _db) = db_basic::new(node_state.clone());
    let (_, rx) = watch::channel(Arc::new(Config::default()));
    let index_factory = vector_store::new_index_factory_usearch(rx).unwrap();

    let config = vector_store::Config {
        vector_store_addr: addr,
        tls_cert_path,
        tls_key_path,
        ..Default::default()
    };

    let (_config_tx, config_rx) = watch::channel(Arc::new(config));

    let server = vector_store::run(node_state, db_actor, internals, index_factory, config_rx)
        .await
        .unwrap();
    let addr = server.get_address().await.unwrap();

    (server, addr, _config_tx)
}

#[tokio::test]
async fn test_https_server_responds() {
    init();

    let addr = core::net::SocketAddr::from(([127, 0, 0, 1], 0));
    let (cert_file, key_file) = generate_server_cert(&addr);

    let (_server, addr, _config_tx) = run_server(
        addr,
        Some(cert_file.path().to_path_buf()),
        Some(key_file.path().to_path_buf()),
    )
    .await;

    let client = reqwest::Client::builder()
        .add_root_certificate(read_cert(&cert_file))
        .build()
        .unwrap();

    let response = client
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Request to HTTP server metrics failed with status: {}",
        response.status()
    );

    let response = client
        .get(format!("https://{addr}/api/v1/status"))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Request to HTTPS server failed with status: {}",
        response.status()
    );

    let response = client
        .get(format!("http://{addr}/api/v1/status"))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Request to HTTP server failed with status: {}",
        response.status()
    );

    let response = client
        .post(format!("http://{addr}/api/v1/indexes/table/index/ann"))
        .json(&PostIndexAnnRequest {
            vector: vec![1.0].into(),
            filter: None,
            limit: NonZeroUsize::new(1).unwrap().into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = client
        .post(format!("https://{addr}/api/v1/indexes/table/index/ann"))
        .json(&PostIndexAnnRequest {
            vector: vec![1.0].into(),
            filter: None,
            limit: NonZeroUsize::new(1).unwrap().into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
