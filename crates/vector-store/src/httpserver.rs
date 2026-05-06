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
    Router {
        tx: oneshot::Sender<Option<Router>>,
    },
    Address {
        tx: oneshot::Sender<watch::Receiver<Option<SocketAddr>>>,
    },
}

pub trait HttpServerExt {
    fn router(&self) -> impl Future<Output = Option<Router>>;
    fn address(&self) -> impl Future<Output = watch::Receiver<Option<SocketAddr>>>;
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

    async fn router(&self) -> Option<Router> {
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

async fn enable_server(
    config: &HttpServerConfig,
    deps: &ServerDeps,
) -> anyhow::Result<(Handle<SocketAddr>, SocketAddr, Router)> {
    tracing::info!("HTTP server being enabled");
    let (handle, addr, router) = spawn_server_with_retry(config, deps).await?;
    tracing::info!(
        "{} server started successfully on {}",
        config.protocol_label(),
        addr
    );
    Ok((handle, addr, router))
}

fn disable_server(handle: Option<Handle<SocketAddr>>) {
    tracing::info!("HTTP server being disabled");
    if let Some(handle) = handle {
        handle.graceful_shutdown(Some(Duration::from_secs(10)));
        tracing::info!("HTTP server shut down");
    }
}

async fn reload_server(
    old_config: &HttpServerConfig,
    new_config: &HttpServerConfig,
    current_handle: Option<Handle<SocketAddr>>,
    deps: &ServerDeps,
) -> (
    Option<Handle<SocketAddr>>,
    Option<SocketAddr>,
    Option<Router>,
) {
    let changes = describe_config_changes(old_config, new_config);
    tracing::info!("HTTP server configuration changed ({changes}), reloading...");

    if let Some(handle) = current_handle {
        tracing::info!("Shutting down old HTTP server");
        handle.graceful_shutdown(Some(Duration::from_secs(10)));
    }

    match spawn_server_with_retry(new_config, deps).await {
        Ok((handle, addr, router)) => {
            tracing::info!(
                "{} server reloaded successfully on {}",
                new_config.protocol_label(),
                addr
            );
            (Some(handle), Some(addr), Some(router))
        }
        Err(e) => {
            tracing::error!("Failed to reload HTTP server: {e}");
            tracing::error!(
                "HTTP server is now offline - previous server was shut down but new server failed to start"
            );
            (None, None, None)
        }
    }
}

async fn handle_config_change(
    current_config: &Option<Arc<HttpServerConfig>>,
    new_config: &Option<Arc<HttpServerConfig>>,
    current_handle: Option<Handle<SocketAddr>>,
    deps: &ServerDeps,
    addr_tx: &watch::Sender<Option<SocketAddr>>,
    router: &mut Option<Router>,
) -> Option<Handle<SocketAddr>> {
    match (current_config, new_config) {
        (None, None) => current_handle,
        (None, Some(config)) => match enable_server(config, deps).await {
            Ok((handle, addr, new_router)) => {
                addr_tx.send(Some(addr)).ok();
                *router = Some(new_router);
                Some(handle)
            }
            Err(e) => {
                tracing::error!("Failed to start HTTP server: {e}");
                addr_tx.send(None).ok();
                None
            }
        },
        (Some(_), None) => {
            disable_server(current_handle);
            addr_tx.send(None).ok();
            *router = None;
            None
        }
        (Some(old), Some(new)) => {
            if **old != **new {
                let (handle, addr, new_router) =
                    reload_server(old, new, current_handle, deps).await;
                addr_tx.send(addr).ok();
                if let Some(r) = new_router {
                    *router = Some(r);
                }
                handle
            } else {
                current_handle
            }
        }
    }
}

pub(crate) async fn new(
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    index_engine_version: String,
    mut config_rx: watch::Receiver<Option<Arc<HttpServerConfig>>>,
) -> anyhow::Result<Sender<HttpServer>> {
    // minimal size as channel is used as a lifetime guard
    const CHANNEL_SIZE: usize = 1;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    let (addr_tx, addr_rx) = watch::channel::<Option<SocketAddr>>(None);

    let deps = ServerDeps {
        state,
        engine,
        metrics,
        internals,
        index_engine_version,
    };

    let initial_config = config_rx.borrow().clone();

    // Start initial server if config is provided
    let (mut current_handle, mut router) = if let Some(ref config) = initial_config {
        let (handle, actual_addr, router) = spawn_server_with_retry(config, &deps).await?;
        addr_tx.send(Some(actual_addr)).ok();
        (Some(handle), Some(router))
    } else {
        tracing::info!("HTTP server disabled by configuration");
        (None, None)
    };

    // Spawn supervisor task that monitors config changes and manages server restarts
    tokio::spawn(async move {
        let mut current_config = initial_config;

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                         break;
                    };
                    match msg {
                        HttpServer::Address { tx } => {
                            tx.send(addr_rx.clone()).expect("failed to send response");
                        }
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

                    if current_config != new_config {
                        current_handle =
                            handle_config_change(&current_config, &new_config, current_handle, &deps, &addr_tx, &mut router)
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
    });

    Ok(tx)
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

    let router = httproutes::new(
        deps.engine.clone(),
        deps.metrics.clone(),
        deps.state.clone(),
        deps.internals.clone(),
        deps.index_engine_version.clone(),
        config.tls.is_some(),
    );
    tokio::spawn({
        let handle = handle.clone();
        let router = router.clone();
        let tls = config.tls.clone();

        async move {
            let result = match tls {
                Some(ref tls_config) if tls_config.is_mtls() => {
                    let rustls_config =
                        RustlsConfig::from_config(Arc::clone(tls_config.server_config()));
                    axum_server::bind_rustls(addr, rustls_config)
                        .handle(handle)
                        .serve(router.into_make_service())
                        .await
                }
                Some(ref tls_config) => {
                    let rustls_config =
                        RustlsConfig::from_config(Arc::clone(tls_config.server_config()));
                    axum_server_dual_protocol::bind_dual_protocol(addr, rustls_config)
                        .handle(handle)
                        .serve(router.into_make_service())
                        .await
                }
                None => {
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
