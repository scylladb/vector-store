/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Config;
use crate::engine::Engine;
use crate::httproutes;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
use axum_server::Handle;
use axum_server::accept::NoDelayAcceptor;
use axum_server::tls_rustls::RustlsAcceptor;
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;

pub(crate) enum HttpServer {}

async fn load_tls_config(config: &Config) -> anyhow::Result<Option<RustlsConfig>> {
    match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let config = RustlsConfig::from_pem_file(cert_path, key_path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to load TLS config: {e}"))?;
            Ok(Some(config))
        }
        _ => Ok(None),
    }
}

fn protocol(tls_config: &Option<RustlsConfig>) -> &'static str {
    if tls_config.is_some() {
        "HTTPS"
    } else {
        "HTTP"
    }
}

pub(crate) async fn new(
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    index_engine_version: String,
    mut config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<(Sender<HttpServer>, SocketAddr)> {
    // minimal size as channel is used as a lifetime guard
    const CHANNEL_SIZE: usize = 1;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    let initial_config = config_rx.borrow().clone();

    // Start initial server and get actual bound address
    let (initial_handle, actual_addr) = spawn_server(
        &initial_config,
        state.clone(),
        engine.clone(),
        metrics.clone(),
        index_engine_version.clone(),
    )
    .await?;

    // Spawn supervisor task that monitors config changes and manages server restarts
    tokio::spawn({
        let state = state.clone();
        let engine = engine.clone();
        let metrics = metrics.clone();
        let index_engine_version = index_engine_version.clone();

        async move {
            let mut current_handle = initial_handle;
            let mut current_addr = initial_config.vector_store_addr;
            let mut current_tls_cert = initial_config.tls_cert_path.clone();
            let mut current_tls_key = initial_config.tls_key_path.clone();

            loop {
                tokio::select! {
                    result = rx.recv() => {
                        if result.is_none() {
                            break;
                        }
                    }
                    result = config_rx.changed() => {
                        if result.is_err() {
                            break;
                        }

                        let new_config = config_rx.borrow().clone();

                        // Check if HTTP server config changed
                        let addr_changed = current_addr != new_config.vector_store_addr;
                        let tls_changed = current_tls_cert != new_config.tls_cert_path
                            || current_tls_key != new_config.tls_key_path;

                        if addr_changed || tls_changed {
                            let mut changes = Vec::new();
                            if addr_changed {
                                changes.push(format!("address {} -> {}", current_addr, new_config.vector_store_addr));
                            }
                            if tls_changed {
                                let old_tls = if current_tls_cert.is_some() && current_tls_key.is_some() {
                                    "enabled"
                                } else {
                                    "disabled"
                                };
                                let new_tls = if new_config.tls_cert_path.is_some() && new_config.tls_key_path.is_some() {
                                    "enabled"
                                } else {
                                    "disabled"
                                };
                                changes.push(format!("TLS {} -> {}", old_tls, new_tls));
                            }

                            tracing::info!("HTTP server configuration changed ({}), reloading...", changes.join(", "));

                            // Gracefully shutdown old server and wait for it to complete
                            tracing::info!("Shutting down old HTTP server");
                            current_handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));

                            // Wait for the port to become available (max 2 seconds = 40 attempts * 50ms)
                            wait_for_port_available(current_addr, 40).await;

                            // Validate and start new server
                            match spawn_server(
                                &new_config,
                                state.clone(),
                                engine.clone(),
                                metrics.clone(),
                                index_engine_version.clone(),
                            )
                            .await
                            {
                                Ok((handle, new_actual_addr)) => {
                                    current_handle = handle;
                                    current_addr = new_config.vector_store_addr;
                                    current_tls_cert = new_config.tls_cert_path.clone();
                                    current_tls_key = new_config.tls_key_path.clone();

                                    let protocol = if new_config.tls_cert_path.is_some() { "HTTPS" } else { "HTTP" };
                                    tracing::info!("{} server reloaded successfully on {}", protocol, new_actual_addr);
                                }
                                Err(e) => {
                                    tracing::error!("Failed to reload HTTP server: {e}");
                                    tracing::error!("HTTP server is now offline - previous server was shut down but new server failed to start");
                                }
                            }
                        }
                    }
                }
            }

            // Final shutdown
            tracing::info!("HTTP server shutting down");
            current_handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
            // Wait for the port to be released (max 2 seconds)
            wait_for_port_available(current_addr, 40).await;
        }
    });

    Ok((tx, actual_addr))
}

/// Wait for a port to become available by attempting to bind to it.
/// Returns when the port can be successfully bound, or after max_attempts.
async fn wait_for_port_available(addr: SocketAddr, max_attempts: u32) {
    for attempt in 1..=max_attempts {
        match TcpListener::bind(addr).await {
            Ok(_) => {
                tracing::debug!(
                    "Port {} is available after {} attempts",
                    addr.port(),
                    attempt
                );
                return;
            }
            Err(_) => {
                if attempt < max_attempts {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }
    tracing::warn!(
        "Port {} availability check completed after {} attempts - port may still be in use",
        addr.port(),
        max_attempts
    );
}

/// Spawn a new HTTP server instance with the given configuration
/// Returns the handle and the actual bound address
async fn spawn_server(
    config: &Config,
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    index_engine_version: String,
) -> anyhow::Result<(Handle, SocketAddr)> {
    let tls_config = load_tls_config(config).await?;
    let protocol = protocol(&tls_config);
    let addr = config.vector_store_addr;

    let handle = Handle::new();

    tokio::spawn({
        let handle = handle.clone();
        async move {
            let router = httproutes::new(engine, metrics, state, index_engine_version);
            let server = axum_server::bind(addr).handle(handle);
            let acceptor = NoDelayAcceptor::new();

            let result = match tls_config {
                Some(config) => {
                    server
                        .acceptor(RustlsAcceptor::new(config).acceptor(acceptor))
                        .serve(router.into_make_service())
                        .await
                }
                _ => {
                    server
                        .acceptor(acceptor)
                        .serve(router.into_make_service())
                        .await
                }
            };
            result.unwrap_or_else(|e| panic!("failed to run {protocol} server: {e}"));
        }
    });

    // Wait for server to be listening and get actual bound address
    // Add timeout to prevent hanging forever if server fails to start
    let actual_addr = tokio::time::timeout(std::time::Duration::from_secs(5), handle.listening())
        .await
        .map_err(|_| anyhow::anyhow!("timeout waiting for server to start"))?
        .ok_or(anyhow::anyhow!(
            "server failed to start - listening notification not received"
        ))?;

    Ok((handle, actual_addr))
}
