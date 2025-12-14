/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::{db_basic, mock_opensearch};
use httpclient::HttpClient;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::watch;
use vector_store::Config;

async fn run_vs(
    index_factory: Box<dyn vector_store::IndexFactory + Send + Sync>,
) -> (HttpClient, impl Sized, impl Sized) {
    let node_state = vector_store::new_node_state().await;
    let (db_actor, _) = db_basic::new(node_state.clone());

    let config = vector_store::Config {
        vector_store_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (_config_tx, config_rx) = watch::channel(Arc::new(config));

    let (server, addr) = vector_store::run(node_state, db_actor, index_factory, config_rx)
        .await
        .unwrap();
    (HttpClient::new(addr), server, _config_tx)
}

#[tokio::test]
async fn get_application_info_usearch() {
    let (_, rx) = watch::channel(Arc::new(Config::default()));
    let (client, _server, _config_tx) =
        run_vs(vector_store::new_index_factory_usearch(rx).unwrap()).await;

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
    assert_eq!(info.engine, format!("usearch-{}", usearch::version()));
}

#[tokio::test]
async fn get_application_info_opensearch() {
    let server = mock_opensearch::TestOpenSearchServer::start().await;
    let (_, config_rx) = watch::channel(Arc::new(vector_store::Config::default()));
    let index_factory =
        vector_store::new_index_factory_opensearch(server.base_url(), config_rx).unwrap();
    let (client, _server, _config_tx) = run_vs(index_factory).await;

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
    assert_eq!(info.engine, "opensearch");
}
