/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_basic;
use crate::tls_utils::generate_ca_cert;
use crate::tls_utils::generate_client_identity;
use crate::tls_utils::generate_server_cert;
use crate::tls_utils::init;
use crate::tls_utils::read_cert;
use crate::wait_for;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::watch;
use vector_store::Config;
use vector_store::ConfigManager;
use vector_store::HttpServerExt;

struct MtlsTestServer<S> {
    _server: S,
    _addr: core::net::SocketAddr,
    mtls_addr: core::net::SocketAddr,
    config_manager: ConfigManager,
    cert_file: NamedTempFile,
    _key_file: NamedTempFile,
    ca_cert_file: NamedTempFile,
    ca_key_file: NamedTempFile,
    mtls: S,
}

impl<S: HttpServerExt> MtlsTestServer<S> {
    fn update_mtls_ca_cert(&self, ca_cert_path: Option<&std::path::Path>) {
        self.config_manager.update_config(|config| {
            config.mtls_ca_cert_path = ca_cert_path.map(|p| p.to_path_buf());
        });
    }
}

async fn run_server(enable_mtls: bool) -> MtlsTestServer<impl Sized + HttpServerExt> {
    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();
    let (db_actor, _db) = db_basic::new(node_state.clone());

    let mtls_addr = core::net::SocketAddr::from(([127, 0, 0, 1], 0));
    let (_, rx) = watch::channel(Arc::new(Config::default()));
    let index_factory = vector_store::new_index_factory_usearch(rx).unwrap();

    let (cert_file, key_file) = generate_server_cert(&mtls_addr);
    let (ca_cert_file, ca_key_file) = generate_ca_cert();

    let config = Config {
        vector_store_addr: core::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        tls_cert_path: Some(cert_file.path().to_path_buf()),
        tls_key_path: Some(key_file.path().to_path_buf()),
        mtls_addr,
        mtls_ca_cert_path: if enable_mtls {
            Some(ca_cert_file.path().to_path_buf())
        } else {
            None
        },
        ..Config::default()
    };

    let (config_manager, receivers) = ConfigManager::new(config);

    let (server, mtls) =
        vector_store::run(node_state, db_actor, internals, index_factory, receivers)
            .await
            .unwrap();
    let addr = (*server.address().await.borrow()).unwrap();

    let mtls_addr = if enable_mtls {
        wait_for_address(&mtls).await
    } else {
        assert!(
            mtls.address().await.borrow().is_none(),
            "mTLS server should not be running initially"
        );
        mtls_addr
    };

    MtlsTestServer {
        _server: server,
        _addr: addr,
        mtls_addr,
        config_manager,
        cert_file,
        _key_file: key_file,
        ca_cert_file,
        ca_key_file,
        mtls,
    }
}

async fn run_server_with_mtls() -> MtlsTestServer<impl Sized + HttpServerExt> {
    run_server(true).await
}

async fn run_server_without_mtls() -> MtlsTestServer<impl Sized + HttpServerExt> {
    run_server(false).await
}

async fn wait_for_address(server: &(impl HttpServerExt + Sized)) -> core::net::SocketAddr {
    let mut rx = server.address().await;
    if let Some(addr) = *rx.borrow() {
        return addr;
    }
    rx.changed().await.unwrap();
    (*rx.borrow()).expect("server should provide an address after change")
}

#[tokio::test]
async fn test_mtls_server_rejects_client_without_certificate() {
    init();

    let server = run_server_with_mtls().await;

    let client = reqwest::Client::builder()
        .add_root_certificate(read_cert(&server.cert_file))
        .build()
        .unwrap();

    let result = client
        .get(format!("https://{}/api/v1/status", server.mtls_addr))
        .send()
        .await;

    assert!(
        result.is_err(),
        "Request without client certificate should fail"
    );
}

#[tokio::test]
async fn test_mtls_server_accepts_client_with_valid_certificate() {
    init();

    let server = run_server_with_mtls().await;

    let identity = generate_client_identity(&server.ca_key_file);

    let client = reqwest::Client::builder()
        .add_root_certificate(read_cert(&server.cert_file))
        .identity(identity)
        .build()
        .unwrap();

    let response = client
        .get(format!("https://{}/api/v1/status", server.mtls_addr))
        .send()
        .await
        .unwrap();

    assert!(
        response.status().is_success(),
        "Request with valid client certificate should succeed"
    );
}

#[tokio::test]
async fn test_mtls_server_rejects_plaintext_http() {
    init();

    let server = run_server_with_mtls().await;

    let client = reqwest::Client::new();
    let result = client
        .get(format!("http://{}/api/v1/status", server.mtls_addr))
        .send()
        .await;

    assert!(
        result.is_err() || !result.unwrap().status().is_success(),
        "Plaintext HTTP request to mTLS port should fail"
    );
}

#[tokio::test]
async fn test_mtls_config_none_to_some_starts_server() {
    init();

    let server = run_server_without_mtls().await;

    let ca_cert_path = server.ca_cert_file.path().to_path_buf();
    server.update_mtls_ca_cert(Some(&ca_cert_path));

    let identity = generate_client_identity(&server.ca_key_file);
    let client = reqwest::Client::builder()
        .add_root_certificate(read_cert(&server.cert_file))
        .identity(identity)
        .build()
        .unwrap();

    let mtls_addr = wait_for_address(&server.mtls).await;

    let response = client
        .get(format!("https://{}/api/v1/status", mtls_addr))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
}

#[tokio::test]
async fn test_mtls_config_some_to_none_stops_server() {
    init();

    let server = run_server_with_mtls().await;

    server.update_mtls_ca_cert(None);

    let identity = generate_client_identity(&server.ca_key_file);
    let client = reqwest::Client::builder()
        .add_root_certificate(read_cert(&server.cert_file))
        .identity(identity)
        .build()
        .unwrap();

    wait_for(
        async || {
            client
                .get(format!("https://{}/api/v1/status", server.mtls_addr))
                .send()
                .await
                .is_err()
        },
        "Waiting for mTLS server to stop",
    )
    .await;
}

#[tokio::test]
async fn test_mtls_config_some_to_some_updates_ca_cert() {
    init();

    let server = run_server_with_mtls().await;

    let (ca_cert_file, ca_key_file) = generate_ca_cert();

    let ca_cert_path = ca_cert_file.path().to_path_buf();
    server.update_mtls_ca_cert(Some(&ca_cert_path));

    let identity = generate_client_identity(&ca_key_file);

    let client = reqwest::Client::builder()
        .add_root_certificate(read_cert(&server.cert_file))
        .identity(identity)
        .build()
        .unwrap();

    wait_for(
        async || {
            client
                .get(format!("https://{}/api/v1/status", server.mtls_addr))
                .send()
                .await
                .map(|resp| resp.status().is_success())
                .unwrap_or(false)
        },
        "Waiting for mTLS server to restart with new CA",
    )
    .await;
}
