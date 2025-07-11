/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::HttpServerAddr;
use crate::engine::Engine;
use crate::httproutes;
use crate::metrics::Metrics;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

pub(crate) type WrapEngine = Arc<RwLock<Option<Sender<Engine>>>>;

pub enum ServerMsg {
    SetEngine(Sender<Engine>),
    ChangeState(httproutes::Status),
}

pub(crate) async fn new(
    addr: HttpServerAddr,
    metrics: Arc<Metrics>,
) -> anyhow::Result<(Sender<ServerMsg>, SocketAddr)> {
    let listener = TcpListener::bind(addr.0).await?;
    let addr = listener.local_addr()?;

    // minimal size as channel is used as a lifetime guard
    const CHANNEL_SIZE: usize = 1;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    let notify = Arc::new(Notify::new());

    let engine = Arc::new(RwLock::new(None));
    let node_state = Arc::new(Mutex::new(httproutes::Status::Initializing));
    tokio::spawn({
        let engine = engine.clone();
        let node_state = node_state.clone();
        let notify = Arc::clone(&notify);
        async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ServerMsg::SetEngine(engine_sender) => {
                        let mut engine_lock = engine.write().await;
                        *engine_lock = Some(engine_sender);
                    }
                    ServerMsg::ChangeState(state) => {
                        let mut state_lock = node_state.lock().await;
                        match *state_lock {
                            httproutes::Status::Initializing => {
                                if state == httproutes::Status::ConnectingToDb {
                                    *state_lock = state;
                                }
                            }
                            httproutes::Status::ConnectingToDb => {
                                if state == httproutes::Status::DiscoveringIndexes {
                                    *state_lock = state;
                                }
                            }
                            httproutes::Status::DiscoveringIndexes => {
                                if state == httproutes::Status::IndexingEmbeddings {
                                    *state_lock = state;
                                }
                            }
                            httproutes::Status::IndexingEmbeddings => {
                                if state == httproutes::Status::Serving {
                                    *state_lock = state;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            notify.notify_one();
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, httproutes::new(engine, metrics, node_state))
            .with_graceful_shutdown(async move {
                notify.notified().await;
            })
            .await
            .expect("failed to run web server");
    });

    Ok((tx, addr))
}
