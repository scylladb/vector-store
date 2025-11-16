/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Config;
use crate::HttpServerConfig;
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
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;

pub(crate) enum HttpServer {}

async fn load_tls_config(config: &HttpServerConfig) -> anyhow::Result<Option<RustlsConfig>> {
    match &config.tls {
        Some(tls) => {
            let config = RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
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
    config: HttpServerConfig,
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    index_engine_version: String,
    mut config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<(Sender<HttpServer>, SocketAddr)> {
    // minimal size as channel is used as a lifetime guard
    const CHANNEL_SIZE: usize = 1;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    let handle = Handle::new();
    let tls_config = load_tls_config(&config).await?;
    let protocol = protocol(&tls_config);

    let initial_addr = config.addr;

    tokio::spawn({
        let handle = handle.clone();
        async move {
            loop {
                tokio::select! {
                    result = rx.recv() => {
                        if result.is_none() {
                            break;
                        }
                        // If we received a message (is_some), continue the loop
                    }
                    result = config_rx.changed() => {
                        if result.is_err() {
                            break;
                        }
                        let new_config = config_rx.borrow();
                        if initial_addr != new_config.vector_store_addr {
                            tracing::warn!(
                                "Configuration change detected that requires server restart:\n  \
                                Vector store address: {} -> {}",
                                initial_addr,
                                new_config.vector_store_addr
                            );
                            tracing::warn!(
                                "This change has been stored but will not take effect until the server is restarted."
                            );
                        }
                    }
                }
            }
            tracing::info!("{protocol} server shutting down");
            // 10 secs is how long docker will wait to force shutdown
            handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
        }
    });

    tokio::spawn({
        let handle = handle.clone();
        async move {
            let router = httproutes::new(engine, metrics, state, index_engine_version);
            let server = axum_server::bind(config.addr).handle(handle);
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
    let addr = handle
        .listening()
        .await
        .ok_or(anyhow::anyhow!("failed to get listening address"))?;
    Ok((tx, addr))
}
