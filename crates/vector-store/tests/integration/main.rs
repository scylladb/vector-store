/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod db_basic;
mod https;
mod info;
mod memory_limit;
mod mock_opensearch;
mod mtls;
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
    pub(crate) http: watch::Sender<Option<Arc<HttpServerConfig>>>,
    #[allow(dead_code)]
    pub(crate) mtls_http: watch::Sender<Option<Arc<HttpServerConfig>>>,
}

impl ConfigSenders {
    pub(crate) async fn send_config(&self, config: Config) {
        let (http, mtls_http) = derive_http_configs(&config).await;
        self.config.send(Arc::new(config)).unwrap();
        self.http.send(Some(Arc::new(http))).unwrap();
        self.mtls_http.send(mtls_http).unwrap();
    }
}

async fn derive_http_configs(config: &Config) -> (HttpServerConfig, Option<Arc<HttpServerConfig>>) {
    let identity = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert), Some(key)) => Some(tls::ServerIdentity::new(cert, key).await.unwrap()),
        _ => None,
    };
    let http_tls = identity
        .as_ref()
        .map(|id| tls::TlsServerConfig::new(id).unwrap());
    let http = HttpServerConfig {
        addr: config.vector_store_addr,
        tls: http_tls,
    };
    let mtls_http = match (&identity, &config.mtls_ca_cert_path) {
        (Some(id), Some(ca_path)) => {
            let ca_bundle = tls::CaBundle::new(ca_path).await.unwrap();
            let mtls_tls = tls::TlsServerConfig::new_mtls(id, &ca_bundle).unwrap();
            Some(Arc::new(HttpServerConfig {
                addr: config.mtls_addr,
                tls: Some(mtls_tls),
            }))
        }
        _ => None,
    };
    (http, mtls_http)
}

pub(crate) async fn create_config_channels(config: Config) -> (ConfigReceivers, ConfigSenders) {
    let (http, mtls_http) = derive_http_configs(&config).await;
    let (config_tx, config_rx) = watch::channel(Arc::new(config));
    let (http_tx, http_rx) = watch::channel(Some(Arc::new(http)));
    let (mtls_http_tx, mtls_http_rx) = watch::channel(mtls_http);
    let receivers = ConfigReceivers {
        config: config_rx,
        http: http_rx,
        mtls_http: mtls_http_rx,
    };
    let senders = ConfigSenders {
        config: config_tx,
        http: http_tx,
        mtls_http: mtls_http_tx,
    };
    (receivers, senders)
}
