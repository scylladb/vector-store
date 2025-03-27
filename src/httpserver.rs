/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Proprietary
 */

use crate::engine::Engine;
use crate::httproutes;
use crate::HttpServerAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;

pub(crate) enum HttpServer {}

pub(crate) async fn new(
    addr: HttpServerAddr,
    engine: Sender<Engine>,
) -> anyhow::Result<Sender<HttpServer>> {
    let listener = TcpListener::bind(addr.0).await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    let (tx, mut rx) = mpsc::channel(10);
    let notify = Arc::new(Notify::new());
    tokio::spawn({
        let notify = Arc::clone(&notify);
        async move {
            while rx.recv().await.is_some() {}
            notify.notify_one();
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, httproutes::new(engine))
            .with_graceful_shutdown(async move {
                notify.notified().await;
            })
            .await
            .expect("failed to run web server");
    });
    Ok(tx)
}
