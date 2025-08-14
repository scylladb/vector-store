/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::HttpServerAddr;
use crate::engine::Engine;
use crate::httproutes;
use crate::metrics::Metrics;
use crate::node_state::NodeState;
use axum::serve::ListenerExt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tracing::error;

pub(crate) enum HttpServer {}

pub(crate) async fn new(
    addr: HttpServerAddr,
    state: Sender<NodeState>,
    engine: Sender<Engine>,
    metrics: Arc<Metrics>,
    index_engine_version: String,
) -> anyhow::Result<(Sender<HttpServer>, SocketAddr)> {
    let listener = TcpListener::bind(addr.0).await?;
    let addr = listener.local_addr()?;
    let listener = listener.tap_io(|socket| {
        if let Err(err) = socket.set_nodelay(true) {
            error!("Failed to set TCP_NODELAY on HTTP server socket: {err}");
        }
    });

    // minimal size as channel is used as a lifetime guard
    const CHANNEL_SIZE: usize = 1;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    let notify = Arc::new(Notify::new());

    tokio::spawn({
        let notify = Arc::clone(&notify);
        async move {
            while rx.recv().await.is_some() {}
            notify.notify_one();
        }
    });
    tokio::spawn(async move {
        axum::serve(
            listener,
            httproutes::new(engine, metrics, state, index_engine_version),
        )
        .with_graceful_shutdown(async move {
            notify.notified().await;
            tracing::info!("HTTP server shutting down");
        })
        .await
        .expect("failed to run web server");
    });

    Ok((tx, addr))
}
