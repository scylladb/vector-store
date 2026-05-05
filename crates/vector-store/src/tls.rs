/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls_pki_types::pem::PemObject;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, PartialEq)]
pub struct ServerIdentity {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

impl ServerIdentity {
    pub async fn new(cert_path: &Path, key_path: &Path) -> anyhow::Result<Self> {
        let cert_pem = tokio::fs::read(cert_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read server cert at {cert_path:?}: {e}"))?;
        let key_pem = tokio::fs::read(key_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read server key at {key_path:?}: {e}"))?;
        Ok(Self { cert_pem, key_pem })
    }
}

#[derive(Clone)]
pub struct TlsServerConfig {
    server_config: Arc<rustls::ServerConfig>,
    identity: ServerIdentity,
}

impl TlsServerConfig {
    pub fn new(identity: &ServerIdentity) -> anyhow::Result<Self> {
        let server_config = build_server_config(identity)?;
        Ok(Self {
            server_config,
            identity: identity.clone(),
        })
    }

    pub fn server_config(&self) -> &Arc<rustls::ServerConfig> {
        &self.server_config
    }

    pub fn describe_changes(&self, other: &TlsServerConfig) -> Vec<String> {
        let mut changes = Vec::new();
        if self.identity != other.identity {
            changes.push("server identity changed".to_string());
        }
        changes
    }
}

impl PartialEq for TlsServerConfig {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

fn build_server_config(identity: &ServerIdentity) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let cert_chain = CertificateDer::pem_slice_iter(&identity.cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse server certificate PEM: {e}"))?;

    let private_key = PrivateKeyDer::from_pem_slice(&identity.key_pem)
        .map_err(|e| anyhow::anyhow!("Failed to parse server private key PEM: {e}"))?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)?;

    Ok(Arc::new(config))
}
