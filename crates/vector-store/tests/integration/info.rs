/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::create_config_channels;
use crate::usearch::test_config;
use crate::{db_basic, mock_opensearch};
use httpclient::HttpClient;
use std::sync::Arc;
use tokio::sync::watch;
use vector_store::Config;
use vector_store::HttpServerExt;

async fn run_vs(
    index_factory: Box<dyn vector_store::IndexFactory + Send + Sync>,
) -> (HttpClient, impl Sized, impl Sized) {
    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();
    let (db_actor, _) = db_basic::new(node_state.clone());

    let (receivers, senders) = create_config_channels(test_config()).await;
    let server = vector_store::run(node_state, db_actor, internals, index_factory, receivers)
        .await
        .unwrap();
    let addr = (*server.address().await.borrow()).unwrap();
    (HttpClient::new(addr), server, senders)
}

#[tokio::test]
async fn get_application_info_usearch() {
    let (_, rx) = watch::channel(Arc::new(Config::default()));
    let (client, _server, _config_senders) =
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
    let (client, _server, _config_senders) = run_vs(index_factory).await;

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
    assert_eq!(info.engine, "opensearch");
}
