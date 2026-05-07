/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::server::WebPkiClientVerifier;
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

#[derive(Clone, PartialEq)]
pub struct CaBundle {
    ca_pem: Vec<u8>,
}

impl CaBundle {
    pub async fn new(ca_cert_path: &Path) -> anyhow::Result<Self> {
        let ca_pem = tokio::fs::read(ca_cert_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read mTLS CA cert at {ca_cert_path:?}: {e}"))?;
        Ok(Self { ca_pem })
    }
}

#[derive(Clone)]
pub struct TlsServerConfig {
    server_config: Arc<rustls::ServerConfig>,
    identity: ServerIdentity,
    ca_bundle: Option<CaBundle>,
}

impl TlsServerConfig {
    pub fn new(identity: &ServerIdentity) -> anyhow::Result<Self> {
        let server_config = build_server_config(identity)?;
        Ok(Self {
            server_config,
            identity: identity.clone(),
            ca_bundle: None,
        })
    }

    pub fn new_mtls(identity: &ServerIdentity, ca_bundle: &CaBundle) -> anyhow::Result<Self> {
        let server_config = build_mtls_server_config(identity, ca_bundle)?;
        tracing::info!("mTLS enabled with client certificate verification");
        Ok(Self {
            server_config,
            identity: identity.clone(),
            ca_bundle: Some(ca_bundle.clone()),
        })
    }

    pub fn is_mtls(&self) -> bool {
        self.ca_bundle.is_some()
    }

    pub fn server_config(&self) -> &Arc<rustls::ServerConfig> {
        &self.server_config
    }

    pub fn describe_changes(&self, other: &TlsServerConfig) -> Vec<String> {
        let mut changes = Vec::new();
        if self.identity != other.identity {
            changes.push("server identity changed".to_string());
        }
        match (&self.ca_bundle, &other.ca_bundle) {
            (None, Some(_)) => changes.push("mTLS enabled".to_string()),
            (Some(_), None) => changes.push("mTLS disabled".to_string()),
            (Some(a), Some(b)) if a != b => changes.push("mTLS CA bundle changed".to_string()),
            _ => {}
        }
        changes
    }
}

impl PartialEq for TlsServerConfig {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity && self.ca_bundle == other.ca_bundle
    }
}

fn build_server_config(identity: &ServerIdentity) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let (cert_chain, private_key) = parse_identity(identity)?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)?;

    Ok(Arc::new(config))
}

fn build_mtls_server_config(
    identity: &ServerIdentity,
    ca_bundle: &CaBundle,
) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let (cert_chain, private_key) = parse_identity(identity)?;

    let ca_certs = CertificateDer::pem_slice_iter(&ca_bundle.ca_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse mTLS CA certificate PEM: {e}"))?;

    let mut root_store = RootCertStore::empty();
    let (added, ignored) = root_store.add_parsable_certificates(ca_certs);
    if added == 0 {
        anyhow::bail!("No valid CA certificates found in mTLS CA cert file");
    }
    if ignored > 0 {
        tracing::warn!(
            "{ignored} CA certificate(s) in the mTLS CA bundle could not be parsed and were skipped"
        );
    }

    let verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build mTLS client verifier: {e}"))?;

    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, private_key)
        .map_err(|e| anyhow::anyhow!("Failed to build mTLS server config: {e}"))?;

    Ok(Arc::new(config))
}

fn parse_identity(
    identity: &ServerIdentity,
) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_chain = CertificateDer::pem_slice_iter(&identity.cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse server certificate PEM: {e}"))?;

    let private_key = PrivateKeyDer::from_pem_slice(&identity.key_pem)
        .map_err(|e| anyhow::anyhow!("Failed to parse server private key PEM: {e}"))?;

    Ok((cert_chain, private_key))
}
