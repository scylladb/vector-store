/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_basic;
use rcgen::CertifiedKey;
use std::io::Write;
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
    tls_config: Option<vector_store::TlsConfig>,
) -> (impl Sized, core::net::SocketAddr) {
    let node_state = vector_store::new_node_state().await;
    let (db_actor, _db) = db_basic::new(node_state.clone());
    let (_, rx) = watch::channel(Arc::new(Config::default()));
    let index_factory = vector_store::new_index_factory_usearch(rx).unwrap();

    let server_config = vector_store::HttpServerConfig {
        addr,
        tls: tls_config,
    };

    vector_store::run(server_config, node_state, db_actor, index_factory)
        .await
        .unwrap()
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

    let (_server, addr) = run_server(
        addr,
        Some(vector_store::TlsConfig {
            cert_path: cert_file.path().to_path_buf(),
            key_path: key_file.path().to_path_buf(),
        }),
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
