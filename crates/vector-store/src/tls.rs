/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::bail;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::ClientCertVerifier;
use rustls_pki_types::pem::PemObject;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, PartialEq, Debug)]
pub struct ServerIdentity {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

impl ServerIdentity {
    pub fn new(cert_path: &Path, key_path: &Path) -> anyhow::Result<Self> {
        let cert_pem = std::fs::read(cert_path)
            .map_err(|e| anyhow::anyhow!("Failed to read server cert at {cert_path:?}: {e}"))?;
        let key_pem = std::fs::read(key_path)
            .map_err(|e| anyhow::anyhow!("Failed to read server key at {key_path:?}: {e}"))?;
        Ok(Self { cert_pem, key_pem })
    }
}

#[derive(Clone, Debug)]
pub struct MtlsVerifier {
    ca_cert_pem: Vec<u8>,
    verifier: Arc<dyn ClientCertVerifier>,
}

impl MtlsVerifier {
    pub fn new(ca_cert_path: &Path) -> anyhow::Result<Self> {
        let ca_cert_pem = std::fs::read(ca_cert_path)
            .map_err(|e| anyhow::anyhow!("Failed to read mTLS CA cert at {ca_cert_path:?}: {e}"))?;
        let verifier = build_client_cert_verifier(&ca_cert_pem)?;
        Ok(Self {
            ca_cert_pem,
            verifier,
        })
    }

    fn verifier(&self) -> Arc<dyn ClientCertVerifier> {
        self.verifier.clone()
    }
}

impl PartialEq for MtlsVerifier {
    fn eq(&self, other: &Self) -> bool {
        self.ca_cert_pem == other.ca_cert_pem
    }
}

impl Eq for MtlsVerifier {}

#[derive(Clone)]
pub(crate) struct TlsServerConfig {
    server_config: Arc<rustls::ServerConfig>,
    is_mtls: bool,
    identity: ServerIdentity,
    verifier: Option<MtlsVerifier>,
}

impl TlsServerConfig {
    pub fn new(identity: &ServerIdentity) -> anyhow::Result<Self> {
        let server_config = build_server_config(identity, None)?;
        Ok(Self {
            server_config,
            is_mtls: false,
            identity: identity.clone(),
            verifier: None,
        })
    }

    pub fn new_mtls(identity: &ServerIdentity, verifier: &MtlsVerifier) -> anyhow::Result<Self> {
        let server_config = build_server_config(identity, Some(verifier))?;
        Ok(Self {
            server_config,
            is_mtls: true,
            identity: identity.clone(),
            verifier: Some(verifier.clone()),
        })
    }

    pub fn server_config(&self) -> &Arc<rustls::ServerConfig> {
        &self.server_config
    }

    pub fn is_mtls(&self) -> bool {
        self.is_mtls
    }

    pub fn protocol_label(&self) -> &'static str {
        if self.is_mtls {
            "HTTPS (mTLS)"
        } else {
            "HTTPS"
        }
    }

    pub fn describe_changes(&self, other: &TlsServerConfig) -> Vec<String> {
        let mut changes = Vec::new();
        if self.is_mtls != other.is_mtls {
            changes.push(format!(
                "protocol {} -> {}",
                self.protocol_label(),
                other.protocol_label()
            ));
        }
        if self.identity != other.identity {
            changes.push("server certificate changed".to_string());
        }
        if self.verifier != other.verifier {
            changes.push("mTLS CA certificate changed".to_string());
        }
        changes
    }
}

impl PartialEq for TlsServerConfig {
    fn eq(&self, other: &Self) -> bool {
        self.is_mtls == other.is_mtls
            && self.identity == other.identity
            && self.verifier == other.verifier
    }
}

fn build_server_config(
    identity: &ServerIdentity,
    verifier: Option<&MtlsVerifier>,
) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let cert_chain = CertificateDer::pem_slice_iter(&identity.cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse server certificate PEM: {e}"))?;

    let private_key = PrivateKeyDer::from_pem_slice(&identity.key_pem)
        .map_err(|e| anyhow::anyhow!("Failed to parse server private key PEM: {e}"))?;

    let config = match verifier {
        Some(v) => rustls::ServerConfig::builder()
            .with_client_cert_verifier(v.verifier())
            .with_single_cert(cert_chain, private_key)?,
        None => rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)?,
    };

    Ok(Arc::new(config))
}

fn build_client_cert_verifier(ca_pem: &[u8]) -> anyhow::Result<Arc<dyn ClientCertVerifier>> {
    let ca_certs = CertificateDer::pem_slice_iter(ca_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse mTLS CA certificate PEM: {e}"))?;

    let mut root_store = RootCertStore::empty();
    let (added, ignored) = root_store.add_parsable_certificates(ca_certs);
    if added == 0 {
        bail!("No valid CA certificates found in mTLS CA cert file");
    }
    if ignored > 0 {
        tracing::warn!(
            "{ignored} CA certificate(s) in the mTLS CA bundle could not be parsed and were skipped"
        );
    }

    WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build mTLS client verifier: {e}"))
}
