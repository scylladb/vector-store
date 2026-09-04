/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::TestActors;
use crate::common::*;
use rcgen::CertificateParams;
use rcgen::KeyPair;
use reqwest::Certificate;
use reqwest::Client;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tracing::info;

const TLS_FILE_CHECK_INTERVAL: &str = "100ms";
const TLS_RELOAD_TIMEOUT: Duration = Duration::from_secs(30);

e2etest::group!(
    name = tls_reload,
    fixtures = (Fixture),
    parent = crate::validator
);

struct Fixture {
    actors: Arc<TestActors>,
}

impl e2etest::Fixture for Fixture {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let actors = setup.setup::<TestActors>().await?;
        Some(Self { actors })
    }

    async fn teardown(self) {
        cleanup(&self.actors).await;
    }
}

fn write_server_identity_pem(
    cert_file: &NamedTempFile,
    key_file: &NamedTempFile,
    vs_ips: &[Ipv4Addr],
) -> Vec<u8> {
    let params =
        CertificateParams::new(vs_ips.iter().map(ToString::to_string).collect::<Vec<_>>()).unwrap();
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();

    let cert_pem = cert.pem();
    std::fs::write(cert_file.path(), cert_pem.as_bytes()).unwrap();
    std::fs::write(key_file.path(), key_pair.serialize_pem()).unwrap();

    cert_pem.into_bytes()
}

async fn https_status_ok(vs_ips: &[Ipv4Addr], cert_pem: &[u8]) -> bool {
    let cert = Certificate::from_pem(cert_pem).unwrap();
    let client = Client::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();

    for ip in vs_ips {
        let Ok(response) = client
            .get(format!("https://{ip}:{VS_PORT}/api/v1/status"))
            .send()
            .await
        else {
            return false;
        };
        if !response.status().is_success() {
            return false;
        }
    }

    true
}

#[e2etest::test(group = tls_reload)]
async fn reloads_tls_identity_after_cert_file_rotation(actors: Arc<TestActors>) {
    info!("started");

    let cert_file = NamedTempFile::new().unwrap();
    let key_file = NamedTempFile::new().unwrap();

    let scylla_configs = get_default_scylla_node_configs(&actors).await;
    let mut vs_configs = get_default_vs_node_configs(&actors).await;
    let vs_ips = get_default_vs_ips(&actors);

    let cert_v1 = write_server_identity_pem(&cert_file, &key_file, &vs_ips);

    for config in vs_configs.iter_mut() {
        config.envs.insert(
            "VECTOR_STORE_TLS_CERT_PATH".to_string(),
            cert_file.path().to_str().unwrap().to_string(),
        );
        config.envs.insert(
            "VECTOR_STORE_TLS_KEY_PATH".to_string(),
            key_file.path().to_str().unwrap().to_string(),
        );
        config.envs.insert(
            "VECTOR_STORE_TLS_FILE_CHECK_INTERVAL".to_string(),
            TLS_FILE_CHECK_INTERVAL.to_string(),
        );
    }

    init_with_config(&actors, scylla_configs, vs_configs, true).await;

    wait_for(
        || async { https_status_ok(&vs_ips, &cert_v1).await },
        "all VS nodes should serve HTTPS with initial certificate",
        TLS_RELOAD_TIMEOUT,
    )
    .await;

    info!("Rotating cert and key in place");
    let cert_v2 = write_server_identity_pem(&cert_file, &key_file, &vs_ips);

    wait_for(
        || async { https_status_ok(&vs_ips, &cert_v2).await },
        "all VS nodes should serve HTTPS with rotated certificate",
        TLS_RELOAD_TIMEOUT,
    )
    .await;

    wait_for(
        || async { !https_status_ok(&vs_ips, &cert_v1).await },
        "old certificate should stop being accepted after reload",
        TLS_RELOAD_TIMEOUT,
    )
    .await;

    info!("finished");
}
