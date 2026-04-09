/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Config;
use crate::engine::Engine;
use crate::httproutes;
use crate::internals::Internals;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
use anyhow::bail;
use axum_server::Handle;
use axum_server::accept::NoDelayAcceptor;
use axum_server::tls_rustls::RustlsConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::server::WebPkiClientVerifier;
use rustls_pki_types::pem::PemObject;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HttpServerConfig {
    pub addr: SocketAddr,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub mtls_ca_cert_path: Option<PathBuf>,
}

pub enum HttpServer {
    Address {
        tx: oneshot::Sender<watch::Receiver<Option<SocketAddr>>>,
    },
}

pub trait HttpServerExt {
    fn address(
        &self,
    ) -> impl std::future::Future<Output = watch::Receiver<Option<SocketAddr>>> + Send;
}

impl HttpServerExt for mpsc::Sender<HttpServer> {
    async fn address(&self) -> watch::Receiver<Option<SocketAddr>> {
        let (tx, rx) = oneshot::channel();
        self.send(HttpServer::Address { tx })
            .await
            .expect("HttpServerExt::address: internal actor should receive request");
        rx.await
            .expect("HttpServerExt::address: internal actor should send response")
    }
}

struct ServerDeps {
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    index_engine_version: String,
}

async fn load_mtls_config(
    cert_path: &PathBuf,
    key_path: &PathBuf,
    ca_cert_path: &PathBuf,
) -> anyhow::Result<RustlsConfig> {
    let ca_pem = tokio::fs::read(ca_cert_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read mTLS CA cert at {ca_cert_path:?}: {e}"))?;

    let ca_certs = CertificateDer::pem_slice_iter(&ca_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse mTLS CA certificate PEM: {e}"))?;

    let mut root_store = RootCertStore::empty();
    let (added, ignored) = root_store.add_parsable_certificates(ca_certs);
    if added == 0 {
        bail!("No valid CA certificates found in mTLS CA cert file {ca_cert_path:?}");
    }
    if ignored > 0 {
        tracing::warn!(
            "{ignored} CA certificate(s) in the mTLS CA bundle could not be parsed and were skipped"
        );
    }

    let verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build mTLS client verifier: {e}"))?;

    let cert_pem = tokio::fs::read(cert_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read server cert at {cert_path:?}: {e}"))?;
    let key_pem = tokio::fs::read(key_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read server key at {key_path:?}: {e}"))?;

    let cert_chain = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse server certificate PEM: {e}"))?;

    let private_key = PrivateKeyDer::from_pem_slice(&key_pem)
        .map_err(|e| anyhow::anyhow!("Failed to parse server private key PEM: {e}"))?;

    let server_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, private_key)
        .map_err(|e| anyhow::anyhow!("Failed to build mTLS server config: {e}"))?;

    tracing::info!("mTLS enabled with CA cert from {ca_cert_path:?}");

    Ok(RustlsConfig::from_config(Arc::new(server_config)))
}

async fn load_tls_config(config: &HttpServerConfig) -> anyhow::Result<Option<RustlsConfig>> {
    match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert_path), Some(key_path)) => match &config.mtls_ca_cert_path {
            Some(ca_path) => load_mtls_config(cert_path, key_path, ca_path)
                .await
                .map(Some),
            None => {
                let tls = RustlsConfig::from_pem_file(cert_path, key_path)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to load TLS config: {e}"))?;
                Ok(Some(tls))
            }
        },
        _ => {
            if config.mtls_ca_cert_path.is_some() {
                bail!(
                    "mTLS CA certificate path is configured, but TLS certificate or key path is missing"
                );
            } else {
                Ok(None)
            }
        }
    }
}

fn protocol(tls_config: &Option<RustlsConfig>) -> &'static str {
    if tls_config.is_some() {
        "HTTPS"
    } else {
        "HTTP"
    }
}

/// Retry spawning a server with exponential backoff
async fn spawn_server_with_retry(
    config: &HttpServerConfig,
    deps: &ServerDeps,
) -> anyhow::Result<(Handle, SocketAddr)> {
    let mut retry_delay = Duration::from_millis(50);
    let max_retries = 10;

    for attempt in 1..=max_retries {
        if attempt > 1 {
            time::sleep(retry_delay).await;
        }

        match spawn_server(config, deps).await {
            Ok(result) => return Ok(result),
            Err(e) => {
                if attempt < max_retries {
                    tracing::warn!(
                        "Failed to start HTTP server (attempt {}/{}): {e}, retrying in {:?}",
                        attempt,
                        max_retries,
                        retry_delay
                    );
                    // Exponential backoff: 50ms, 100ms, 200ms, 400ms, 800ms, 1600ms, ...
                    retry_delay =
                        Duration::from_millis((retry_delay.as_millis() * 2).min(2000) as u64);
                } else {
                    return Err(e);
                }
            }
        }
    }

    unreachable!()
}

async fn enable_server(
    config: &HttpServerConfig,
    deps: &ServerDeps,
    addr_tx: &watch::Sender<Option<SocketAddr>>,
) -> anyhow::Result<Handle> {
    tracing::info!("HTTP server being enabled");
    let (handle, addr) = spawn_server_with_retry(config, deps).await?;
    let protocol = if config.mtls_ca_cert_path.is_some() {
        "HTTPS (mTLS)"
    } else if config.tls_cert_path.is_some() {
        "HTTPS"
    } else {
        "HTTP"
    };
    tracing::info!("{} server started successfully on {}", protocol, addr);
    addr_tx.send(Some(addr)).ok();
    Ok(handle)
}

fn disable_server(handle: Option<Handle>, addr_tx: &watch::Sender<Option<SocketAddr>>) {
    tracing::info!("HTTP server being disabled");
    if let Some(handle) = handle {
        handle.graceful_shutdown(Some(Duration::from_secs(10)));
        tracing::info!("HTTP server shut down");
    }
    addr_tx.send(None).ok();
}

async fn reload_server(
    old_config: &HttpServerConfig,
    new_config: &HttpServerConfig,
    current_handle: Option<Handle>,
    deps: &ServerDeps,
    addr_tx: &watch::Sender<Option<SocketAddr>>,
) -> Option<Handle> {
    let changes = describe_config_changes(old_config, new_config);
    tracing::info!("HTTP server configuration changed ({changes}), reloading...");

    if let Some(handle) = current_handle {
        tracing::info!("Shutting down old HTTP server");
        handle.graceful_shutdown(Some(Duration::from_secs(10)));
    }

    match spawn_server_with_retry(new_config, deps).await {
        Ok((handle, addr)) => {
            let protocol = if new_config.mtls_ca_cert_path.is_some() {
                "HTTPS (mTLS)"
            } else if new_config.tls_cert_path.is_some() {
                "HTTPS"
            } else {
                "HTTP"
            };
            tracing::info!("{} server reloaded successfully on {}", protocol, addr);
            addr_tx.send(Some(addr)).ok();
            Some(handle)
        }
        Err(e) => {
            tracing::error!("Failed to reload HTTP server: {e}");
            tracing::error!(
                "HTTP server is now offline - previous server was shut down but new server failed to start"
            );
            addr_tx.send(None).ok();
            None
        }
    }
}

async fn handle_config_change(
    current_config: &Option<HttpServerConfig>,
    new_config: &Option<HttpServerConfig>,
    current_handle: Option<Handle>,
    deps: &ServerDeps,
    addr_tx: &watch::Sender<Option<SocketAddr>>,
) -> Option<Handle> {
    match (current_config, new_config) {
        (None, None) => current_handle,
        (None, Some(config)) => match enable_server(config, deps, addr_tx).await {
            Ok(handle) => Some(handle),
            Err(e) => {
                tracing::error!("Failed to start HTTP server: {e}");
                None
            }
        },
        (Some(_), None) => {
            disable_server(current_handle, addr_tx);
            None
        }
        (Some(old), Some(new)) => reload_server(old, new, current_handle, deps, addr_tx).await,
    }
}

pub(crate) async fn new<F>(
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    index_engine_version: String,
    mut config_rx: watch::Receiver<Arc<Config>>,
    get_server_config: F,
) -> anyhow::Result<Sender<HttpServer>>
where
    F: Fn(&Config) -> Option<HttpServerConfig> + Send + 'static,
{
    // minimal size as channel is used as a lifetime guard
    const CHANNEL_SIZE: usize = 1;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    let (addr_tx, addr_rx) = watch::channel::<Option<SocketAddr>>(None);

    let deps = ServerDeps {
        state: state.clone(),
        engine: engine.clone(),
        metrics: metrics.clone(),
        internals: internals.clone(),
        index_engine_version: index_engine_version.clone(),
    };

    let initial_config = get_server_config(&config_rx.borrow());

    // Start initial server if config is provided
    let mut current_handle = if let Some(ref config) = initial_config {
        let (handle, addr) = spawn_server_with_retry(config, &deps).await?;
        addr_tx.send(Some(addr)).ok();
        Some(handle)
    } else {
        tracing::info!("HTTP server disabled by configuration");
        None
    };

    // Spawn supervisor task that monitors config changes and manages server restarts
    tokio::spawn({
        async move {
            let mut current_config = initial_config;

            loop {
                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            None => break,
                            Some(HttpServer::Address { tx }) => {
                                tx.send(addr_rx.clone()).expect("failed to send response");
                            }
                        }
                    }
                    result = config_rx.changed() => {
                        if result.is_err() {
                            break;
                        }

                        let new_config = get_server_config(&config_rx.borrow());

                        if current_config != new_config {
                            current_handle =
                                handle_config_change(&current_config, &new_config, current_handle, &deps, &addr_tx)
                                    .await;
                            current_config = new_config;
                        }
                    }
                }
            }

            // Final shutdown
            if let Some(handle) = current_handle {
                tracing::info!("HTTP server shutting down");
                handle.graceful_shutdown(Some(Duration::from_secs(10)));
                // Brief delay to allow clean shutdown
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            addr_tx.send(None).ok();
        }
    });

    Ok(tx)
}

fn describe_config_changes(old: &HttpServerConfig, new: &HttpServerConfig) -> String {
    let mut changes = Vec::new();
    if old.addr != new.addr {
        changes.push(format!("address {} -> {}", old.addr, new.addr));
    }
    if old.tls_cert_path != new.tls_cert_path || old.tls_key_path != new.tls_key_path {
        let tls_label = |c: &HttpServerConfig| {
            if c.tls_cert_path.is_some() && c.tls_key_path.is_some() {
                "enabled"
            } else {
                "disabled"
            }
        };
        changes.push(format!("TLS {} -> {}", tls_label(old), tls_label(new)));
    }
    if old.mtls_ca_cert_path != new.mtls_ca_cert_path {
        let mtls_label = |c: &HttpServerConfig| {
            if c.mtls_ca_cert_path.is_some() {
                "enabled"
            } else {
                "disabled"
            }
        };
        changes.push(format!("mTLS {} -> {}", mtls_label(old), mtls_label(new)));
    }
    changes.join(", ")
}

/// Spawn a new HTTP server instance with the given configuration
/// Returns the handle and the actual bound address
async fn spawn_server(
    config: &HttpServerConfig,
    deps: &ServerDeps,
) -> anyhow::Result<(Handle, SocketAddr)> {
    let tls_config = load_tls_config(config).await?;
    let is_mtls = config.mtls_ca_cert_path.is_some();
    let protocol = if is_mtls {
        "HTTPS (mTLS)"
    } else {
        protocol(&tls_config)
    };
    let addr = config.addr;

    let handle = Handle::new();

    tokio::spawn({
        let handle = handle.clone();
        let state = deps.state.clone();
        let engine = deps.engine.clone();
        let metrics = deps.metrics.clone();
        let internals = deps.internals.clone();
        let index_engine_version = deps.index_engine_version.clone();

        async move {
            let result = match tls_config {
                Some(tls_config) if is_mtls => {
                    // mTLS requires HTTPS-only (no dual protocol)
                    axum_server::bind_rustls(addr, tls_config)
                        .handle(handle)
                        .serve(
                            httproutes::new(
                                engine,
                                metrics,
                                state,
                                internals,
                                index_engine_version,
                                true,
                            )
                            .into_make_service(),
                        )
                        .await
                }
                Some(tls_config) => {
                    // Regular TLS allows dual protocol (HTTP + HTTPS)
                    axum_server_dual_protocol::bind_dual_protocol(addr, tls_config)
                        .handle(handle)
                        .serve(
                            httproutes::new(
                                engine,
                                metrics,
                                state,
                                internals,
                                index_engine_version,
                                true,
                            )
                            .into_make_service(),
                        )
                        .await
                }
                _ => {
                    axum_server::bind(addr)
                        .handle(handle)
                        .acceptor(NoDelayAcceptor::new())
                        .serve(
                            httproutes::new(
                                engine,
                                metrics,
                                state,
                                internals,
                                index_engine_version,
                                false,
                            )
                            .into_make_service(),
                        )
                        .await
                }
            };
            result.unwrap_or_else(|e| panic!("failed to run {protocol} server: {e}"));
        }
    });

    // Wait for server to be listening and get actual bound address
    // Add timeout to prevent hanging forever if server fails to start
    let actual_addr = time::timeout(Duration::from_secs(5), handle.listening())
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for server to start"))?
        .ok_or(anyhow::anyhow!(
            "server failed to start - listening notification not received"
        ))?;

    Ok((handle, actual_addr))
}
