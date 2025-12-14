/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_basic;
use rcgen::CertifiedKey;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::watch;
use vector_store::Config;

fn create_temp_file<C: AsRef<[u8]>>(content: C) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(content.as_ref()).unwrap();
    file
}

async fn run_server(
    addr: core::net::SocketAddr,
    tls_cert_path: Option<PathBuf>,
    tls_key_path: Option<PathBuf>,
) -> (impl Sized, core::net::SocketAddr, impl Sized) {
    let node_state = vector_store::new_node_state().await;
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

    let (server, addr) = vector_store::run(node_state, db_actor, index_factory, config_rx)
        .await
        .unwrap();

    (server, addr, _config_tx)
}

#[tokio::test]
async fn test_https_server_responds() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install ring crypto provider");

    crate::enable_tracing();

    let addr = core::net::SocketAddr::from(([127, 0, 0, 1], 0));
    let CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![addr.ip().to_string()]).unwrap();

    let cert_file = create_temp_file(cert.pem().as_bytes());
    let key_file = create_temp_file(signing_key.serialize_pem().as_bytes());

    let (_server, addr, _config_tx) = run_server(
        addr,
        Some(cert_file.path().to_path_buf()),
        Some(key_file.path().to_path_buf()),
    )
    .await;

    let response = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(cert.pem().as_bytes()).unwrap())
        .build()
        .unwrap()
        .get(format!("https://{addr}/api/v1/status"))
        .send()
        .await
        .unwrap();

    assert!(
        response.status().is_success(),
        "Request to HTTPS server failed with status: {}",
        response.status()
    );
}
