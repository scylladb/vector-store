/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::config_manager::HttpServerConfig;
use crate::engine::Engine;
use crate::httproutes;
use crate::indexes::Indexes;
use crate::internals::Internals;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
use anyhow::bail;
use axum::Router;
use axum_server::Handle;
use axum_server::accept::NoDelayAcceptor;
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;

const SPAWN_TIMEOUT: Duration = Duration::from_secs(5);

type ServerTask = JoinHandle<std::io::Result<()>>;

struct RunningServer {
    handle: Handle<SocketAddr>,
    task: ServerTask,
}

impl RunningServer {
    async fn shutdown(self) {
        const GRACEFUL_SHUTDOWN_DURATION: Duration = Duration::from_secs(10);
        const AWAIT_TASK_TIMEOUT: Duration =
            Duration::from_secs(GRACEFUL_SHUTDOWN_DURATION.as_secs() + 5);
        self.handle
            .graceful_shutdown(Some(GRACEFUL_SHUTDOWN_DURATION));
        let mut task = self.task;
        match time::timeout(AWAIT_TASK_TIMEOUT, &mut task).await {
            Ok(Ok(Ok(()))) => tracing::info!("HTTP server task completed"),
            Ok(Ok(Err(e))) => tracing::warn!("HTTP server task completed with error: {e}"),
            Ok(Err(join_err)) => tracing::warn!("HTTP server task panicked: {join_err}"),
            Err(_) => {
                tracing::warn!(
                    "Timed out waiting for HTTP server task to complete after {AWAIT_TASK_TIMEOUT:?}, aborting"
                );
                task.abort();
                let _ = task.await;
            }
        }
    }
}

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
    indexes: Arc<RwLock<Indexes>>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    index_engine_version: String,
}

/// Retry spawning a server with exponential backoff
async fn spawn_server_with_retry(
    config: &HttpServerConfig,
    deps: &ServerDeps,
) -> anyhow::Result<(RunningServer, SocketAddr, Router)> {
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
) -> anyhow::Result<(RunningServer, SocketAddr, Router)> {
    tracing::info!("HTTP server being enabled");
    let (server, addr, router) = spawn_server_with_retry(config, deps).await?;
    tracing::info!(
        "{} server started successfully on {}",
        config.protocol_label(),
        addr
    );
    Ok((server, addr, router))
}

async fn disable_server(server: Option<RunningServer>) {
    tracing::info!("HTTP server being disabled");
    if let Some(server) = server {
        server.shutdown().await;
    }
}

async fn reload_server(
    old_config: &HttpServerConfig,
    new_config: &HttpServerConfig,
    current_server: Option<RunningServer>,
    deps: &ServerDeps,
) -> (Option<RunningServer>, Option<SocketAddr>, Option<Router>) {
    let changes = describe_config_changes(old_config, new_config);
    tracing::info!("HTTP server configuration changed ({changes}), reloading...");

    if let Some(server) = current_server {
        tracing::info!("Shutting down old HTTP server");
        server.shutdown().await;
    }

    match spawn_server_with_retry(new_config, deps).await {
        Ok((server, addr, router)) => {
            tracing::info!(
                "{} server reloaded successfully on {}",
                new_config.protocol_label(),
                addr
            );
            (Some(server), Some(addr), Some(router))
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
    current_server: Option<RunningServer>,
    deps: &ServerDeps,
    addr_tx: &watch::Sender<Option<SocketAddr>>,
    router: &mut Option<Router>,
) -> Option<RunningServer> {
    match (current_config, new_config) {
        // No change: do nothing
        (None, None) => current_server,
        // New server enabled: start it
        (None, Some(config)) => match enable_server(config, deps).await {
            Ok((server, addr, new_router)) => {
                addr_tx.send(Some(addr)).ok();
                *router = Some(new_router);
                Some(server)
            }
            Err(e) => {
                tracing::error!("Failed to start HTTP server: {e}");
                addr_tx.send(None).ok();
                None
            }
        },
        // Server disabled: stop it
        (Some(_), None) => {
            disable_server(current_server).await;
            addr_tx.send(None).ok();
            *router = None;
            None
        }
        // Config changed: reload server
        (Some(old), Some(new)) => {
            if **old != **new {
                let (server, addr, new_router) =
                    reload_server(old, new, current_server, deps).await;
                addr_tx.send(addr).ok();
                if let Some(r) = new_router {
                    *router = Some(r);
                }
                server
            } else {
                current_server
            }
        }
    }
}

pub(crate) async fn new(
    state: Sender<NodeState>,
    indexes: Arc<RwLock<Indexes>>,
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
        indexes,
        engine,
        metrics,
        internals,
        index_engine_version,
    };

    let initial_config = config_rx.borrow().clone();

    // Start initial server if config is provided
    let (mut current_server, mut router) = if let Some(ref config) = initial_config {
        let (server, actual_addr, router) = spawn_server_with_retry(config, &deps).await?;
        addr_tx.send(Some(actual_addr)).ok();
        (Some(server), Some(router))
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
                        current_server =
                            handle_config_change(&current_config, &new_config, current_server, &deps, &addr_tx, &mut router)
                                .await;
                        current_config = new_config;
                    }
                }
            }
        }

        // Final shutdown
        if let Some(server) = current_server {
            tracing::info!("HTTP server shutting down");
            server.shutdown().await;
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
) -> anyhow::Result<(RunningServer, SocketAddr, Router)> {
    let protocol = config.protocol_label();
    let addr = config.addr;

    let handle = Handle::new();

    let router = httproutes::new(
        Arc::clone(&deps.indexes),
        deps.engine.clone(),
        deps.metrics.clone(),
        deps.state.clone(),
        deps.internals.clone(),
        deps.index_engine_version.clone(),
        config.tls.is_some(),
    )
    .await;
    let mut server_task = tokio::spawn({
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
            if let Err(ref e) = result {
                tracing::error!("failed to run {protocol} server: {e}");
            }
            result
        }
    });

    let actual_addr = tokio::select! {
        addr = handle.listening() => {
            addr.ok_or_else(|| anyhow::anyhow!(
                "{protocol} server failed to start - listening notification not received"
            ))?
        }
        result = &mut server_task => {
            match result {
                Ok(Err(e)) => bail!(e),
                Ok(Ok(())) => bail!("{protocol} server exited unexpectedly"),
                Err(join_err) => bail!("{protocol} server task panicked: {join_err}"),
            }
        }
        _ = time::sleep(SPAWN_TIMEOUT) => {
            server_task.abort();
            let _ = server_task.await;
            bail!("timeout waiting for {protocol} server to start");
        }
    };

    Ok((
        RunningServer {
            handle,
            task: server_task,
        },
        actual_addr,
        router,
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexes::Indexes;

    fn test_deps() -> ServerDeps {
        let (state_tx, _state_rx) = mpsc::channel(1);
        let (engine_tx, _engine_rx) = mpsc::channel(1);
        let (internals_tx, _internals_rx) = mpsc::channel(1);
        ServerDeps {
            state: state_tx,
            indexes: Arc::new(RwLock::new(Indexes::new())),
            engine: engine_tx,
            metrics: Arc::new(Metrics::new()),
            internals: internals_tx,
            index_engine_version: "test".to_string(),
        }
    }

    #[tokio::test]
    async fn spawn_server_returns_error_on_occupied_port() {
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let occupied_addr = occupied.local_addr().unwrap();
        let config = HttpServerConfig {
            addr: occupied_addr,
            tls: None,
        };
        let deps = test_deps();

        let result = spawn_server(&config, &deps).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn spawn_server_binds_to_available_port() {
        let config = HttpServerConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            tls: None,
        };
        let deps = test_deps();

        let (server, addr, _router) = spawn_server(&config, &deps).await.unwrap();

        assert_ne!(addr.port(), 0);

        server.shutdown().await;
    }

    #[tokio::test]
    async fn server_reload_rebinds_same_port() {
        let deps = test_deps();
        let config = HttpServerConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            tls: None,
        };
        let (server, addr, _router) = spawn_server(&config, &deps).await.unwrap();

        let new_config = HttpServerConfig { addr, tls: None };

        let (new_server, new_addr, _new_router) =
            reload_server(&config, &new_config, Some(server), &deps).await;

        assert!(
            new_server.is_some(),
            "server should successfully reload on the same port"
        );
        assert_eq!(new_addr, Some(addr));

        new_server.unwrap().shutdown().await;
    }
}
