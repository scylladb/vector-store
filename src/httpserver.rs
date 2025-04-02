/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::HttpServerAddr;
use crate::engine::Engine;
use crate::httproutes;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

pub(crate) enum HttpServer {}

pub(crate) async fn new(
    addr: HttpServerAddr,
    engine: Sender<Engine>,
) -> anyhow::Result<(Sender<HttpServer>, SocketAddr)> {
    let listener = TcpListener::bind(addr.0).await?;
    let addr = listener.local_addr()?;

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
        axum::serve(listener, httproutes::new(engine))
            .with_graceful_shutdown(async move {
                notify.notified().await;
            })
            .await
            .expect("failed to run web server");
    });

    Ok((tx, addr))
}
