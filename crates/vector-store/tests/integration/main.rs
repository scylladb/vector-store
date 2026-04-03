/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod db_basic;
mod https;
mod info;
mod memory_limit;
mod mock_opensearch;
mod openapi;
mod opensearch;
mod quantization;
mod routing;
mod status;
mod tls_utils;
mod usearch;

use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task;
use tokio::time;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use vector_store::Config;
use vector_store::ConfigReceivers;
use vector_store::HttpServerConfig;
use vector_store::tls;

static INIT_TRACING: Once = Once::new();

fn enable_tracing() {
    INIT_TRACING.call_once(|| {
        tracing_subscriber::registry()
            .with(EnvFilter::try_new("info").unwrap())
            .with(fmt::layer().with_target(false))
            .init();
    });
}

async fn wait_for<F, Fut>(mut condition: F, msg: &str)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    time::timeout(Duration::from_secs(5), async {
        while !condition().await {
            task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Timeout on: {msg}"))
}

async fn wait_for_value<T>(mut producer: impl AsyncFnMut() -> Option<T>, msg: &str) -> T {
    time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(value) = producer().await {
                break value;
            }
            task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Timeout on: {msg}"))
}

pub(crate) struct ConfigSenders {
    #[allow(dead_code)]
    pub(crate) config: watch::Sender<Arc<Config>>,
    #[allow(dead_code)]
    pub(crate) http: watch::Sender<Arc<HttpServerConfig>>,
}

pub(crate) async fn create_config_channels(config: Config) -> (ConfigReceivers, ConfigSenders) {
    let tls = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert), Some(key)) => {
            let identity = tls::ServerIdentity::new(cert, key).await.unwrap();
            Some(tls::TlsServerConfig::new(&identity).unwrap())
        }
        _ => None,
    };
    let http = HttpServerConfig {
        addr: config.vector_store_addr,
        tls,
    };
    let (config_tx, config_rx) = watch::channel(Arc::new(config));
    let (http_tx, http_rx) = watch::channel(Arc::new(http));
    let receivers = ConfigReceivers {
        config: config_rx,
        http: http_rx,
    };
    let senders = ConfigSenders {
        config: config_tx,
        http: http_tx,
    };
    (receivers, senders)
}
