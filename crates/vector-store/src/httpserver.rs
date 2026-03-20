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
use axum_server::Handle;
use axum_server::accept::NoDelayAcceptor;
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::time;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HttpServerConfig {
    pub addr: SocketAddr,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
}

pub(crate) enum HttpServer {}

async fn load_tls_config(config: &HttpServerConfig) -> anyhow::Result<Option<RustlsConfig>> {
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

/// Retry spawning a server with exponential backoff
async fn spawn_server_with_retry(
    config: &HttpServerConfig,
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    index_engine_version: String,
) -> anyhow::Result<(Handle, SocketAddr)> {
    let mut retry_delay = Duration::from_millis(50);
    let max_retries = 10;

    for attempt in 1..=max_retries {
        if attempt > 1 {
            time::sleep(retry_delay).await;
        }

        match spawn_server(
            config,
            state.clone(),
            engine.clone(),
            metrics.clone(),
            internals.clone(),
            index_engine_version.clone(),
        )
        .await
        {
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

pub(crate) async fn new<F>(
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    index_engine_version: String,
    mut config_rx: watch::Receiver<Arc<Config>>,
    get_server_config: F,
) -> anyhow::Result<(Sender<HttpServer>, SocketAddr)>
where
    F: Fn(&Config) -> HttpServerConfig + Send + 'static,
{
    // minimal size as channel is used as a lifetime guard
    const CHANNEL_SIZE: usize = 1;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    let initial_config = get_server_config(&config_rx.borrow());

    // Start initial server and get actual bound address
    let (initial_handle, actual_addr) = spawn_server_with_retry(
        &initial_config,
        state.clone(),
        engine.clone(),
        metrics.clone(),
        internals.clone(),
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
            let mut current_config = initial_config;

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

                        let new_config = get_server_config(&config_rx.borrow());

                        if current_config != new_config {
                            let changes = describe_config_changes(
                                &current_config,
                                &new_config,
                            );

                            tracing::info!("HTTP server configuration changed ({changes}), reloading...");

                            // Gracefully shutdown old server and wait for it to complete
                            tracing::info!("Shutting down old HTTP server");
                            current_handle.graceful_shutdown(Some(Duration::from_secs(10)));

                            // Start new server with retry
                            match spawn_server_with_retry(
                                &new_config,
                                state.clone(),
                                engine.clone(),
                                metrics.clone(),
                                internals.clone(),
                                index_engine_version.clone(),
                            )
                            .await
                            {
                                Ok((handle, new_actual_addr)) => {
                                    let protocol = if new_config.tls_cert_path.is_some() { "HTTPS" } else { "HTTP" };
                                    tracing::info!("{} server reloaded successfully on {}", protocol, new_actual_addr);

                                    current_handle = handle;
                                    current_config = new_config;

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
            current_handle.graceful_shutdown(Some(Duration::from_secs(10)));
            // Brief delay to allow clean shutdown
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    Ok((tx, actual_addr))
}

fn describe_config_changes(old: &HttpServerConfig, new: &HttpServerConfig) -> String {
    let mut changes = Vec::new();
    if old.addr != new.addr {
        changes.push(format!("address {} -> {}", old.addr, new.addr));
    }
    if old.tls_cert_path != new.tls_cert_path || old.tls_key_path != new.tls_key_path {
        let label = |c: &HttpServerConfig| {
            if c.tls_cert_path.is_some() && c.tls_key_path.is_some() {
                "enabled"
            } else {
                "disabled"
            }
        };
        changes.push(format!("TLS {} -> {}", label(old), label(new)));
    }
    changes.join(", ")
}

/// Spawn a new HTTP server instance with the given configuration
/// Returns the handle and the actual bound address
async fn spawn_server(
    config: &HttpServerConfig,
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    index_engine_version: String,
) -> anyhow::Result<(Handle, SocketAddr)> {
    let tls_config = load_tls_config(config).await?;
    let protocol = protocol(&tls_config);
    let addr = config.addr;

    let handle = Handle::new();

    tokio::spawn({
        let handle = handle.clone();
        async move {
            let result = match tls_config {
                Some(tls_config) => {
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
