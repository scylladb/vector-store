/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::config_manager::HttpServerConfig;
use crate::engine::Engine;
use crate::httproutes;
use crate::internals::Internals;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
use axum::Router;
use axum_server::Handle;
use axum_server::accept::NoDelayAcceptor;
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time;

pub enum HttpServer {
    Router { tx: oneshot::Sender<Router> },
}

pub trait HttpServerExt {
    fn router(&self) -> impl Future<Output = Router>;
}

impl HttpServerExt for Sender<HttpServer> {
    async fn router(&self) -> Router {
        let (tx, rx) = oneshot::channel();
        self.send(HttpServer::Router { tx })
            .await
            .expect("HttpServerExt::router: internal actor should receive request");
        rx.await
            .expect("HttpServerExt::router: internal actor should send response")
    }
}

struct ServerDeps {
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    index_engine_version: String,
}

/// Retry spawning a server with exponential backoff
async fn spawn_server_with_retry(
    config: &HttpServerConfig,
    deps: &ServerDeps,
) -> anyhow::Result<(Handle<SocketAddr>, SocketAddr, Router)> {
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

pub(crate) async fn new(
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    index_engine_version: String,
    mut config_rx: watch::Receiver<Arc<HttpServerConfig>>,
) -> anyhow::Result<(Sender<HttpServer>, SocketAddr)> {
    // minimal size as channel is used as a lifetime guard
    const CHANNEL_SIZE: usize = 1;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    let deps = ServerDeps {
        state,
        engine,
        metrics,
        internals,
        index_engine_version,
    };

    let initial_config = config_rx.borrow().clone();

    // Start initial server and get actual bound address
    let (initial_handle, actual_addr, mut router) =
        spawn_server_with_retry(&initial_config, &deps).await?;

    // Spawn supervisor task that monitors config changes and manages server restarts
    tokio::spawn(async move {
        let mut current_handle = initial_handle;
        let mut current_config = initial_config;

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                         break;
                    };
                    match msg {
                        HttpServer::Router { tx } => {
                            _ = tx.send(router.clone());
                        }
                    }
                }

                result = config_rx.changed() => {
                    if result.is_err() {
                        break;
                    }

                    let new_config = config_rx.borrow().clone();

                    if *current_config != *new_config {
                        let changes = describe_config_changes(&current_config, &new_config);
                        tracing::info!("HTTP server configuration changed ({changes}), reloading...");

                        // Gracefully shutdown old server and wait for it to complete
                        tracing::info!("Shutting down old HTTP server");
                        current_handle.graceful_shutdown(Some(Duration::from_secs(10)));

                        // Start new server with retry
                        match spawn_server_with_retry(&new_config, &deps).await {
                            Ok((handle, new_actual_addr, new_router)) => {
                                current_handle = handle;
                                current_config = new_config;
                                router = new_router;
                                tracing::info!(
                                    "{} server reloaded successfully on {}",
                                    current_config.protocol_label(),
                                    new_actual_addr
                                );
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
    });

    Ok((tx, actual_addr))
}

/// Spawn a new HTTP server instance with the given configuration
/// Returns the handle and the actual bound address
async fn spawn_server(
    config: &HttpServerConfig,
    deps: &ServerDeps,
) -> anyhow::Result<(Handle<SocketAddr>, SocketAddr, Router)> {
    let protocol = config.protocol_label();
    let addr = config.addr;

    let handle = Handle::new();

    let tls_config = config
        .tls
        .as_ref()
        .map(|t| RustlsConfig::from_config(Arc::clone(t.server_config())));

    let router = httproutes::new(
        deps.engine.clone(),
        deps.metrics.clone(),
        deps.state.clone(),
        deps.internals.clone(),
        deps.index_engine_version.clone(),
        tls_config.is_some(),
    )
    .await;
    tokio::spawn({
        let handle = handle.clone();
        let router = router.clone();

        async move {
            let result = match tls_config {
                Some(tls_config) => {
                    axum_server_dual_protocol::bind_dual_protocol(addr, tls_config)
                        .handle(handle)
                        .serve(router.into_make_service())
                        .await
                }
                _ => {
                    axum_server::bind(addr)
                        .handle(handle)
                        .acceptor(NoDelayAcceptor::new())
                        .serve(router.into_make_service())
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

    Ok((handle, actual_addr, router))
}

fn describe_config_changes(old: &HttpServerConfig, new: &HttpServerConfig) -> String {
    let mut changes = Vec::new();
    if old.addr != new.addr {
        changes.push(format!("address {} -> {}", old.addr, new.addr));
    }
    match (&old.tls, &new.tls) {
        (Some(old_tls), Some(new_tls)) => changes.extend(old_tls.describe_changes(new_tls)),
        (None, Some(_)) => changes.push("TLS enabled".to_string()),
        (Some(_), None) => changes.push("TLS disabled".to_string()),
        (None, None) => {}
    }
    changes.join(", ")
}
