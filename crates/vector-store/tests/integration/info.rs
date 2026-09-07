/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::create_config_channels;
use crate::vs_index::usearch_test_config;
use crate::{db_basic, mock_opensearch};
use httpclient::HttpClient;
use vector_store::Config;
use vector_store::DiskannBackendKind;
use vector_store::HttpServerExt;

async fn run_vs(config: Config) -> (HttpClient, impl Sized, impl Sized) {
    let node_state = vector_store::new_node_state().await;
    let (db_actor, _) = db_basic::new(node_state.clone());

    let (receivers, senders) = create_config_channels(config).await;
    let (server, _mtls) = vector_store::run(Some(node_state), Some(db_actor), receivers)
        .await
        .unwrap();
    let addr = (*server.address().await.borrow()).unwrap();
    (HttpClient::new(addr), server, senders)
}

#[tokio::test]
async fn get_application_info_usearch() {
    let (client, _server, _config_senders) = run_vs(usearch_test_config()).await;

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
    assert_eq!(info.engine, format!("usearch-{}", usearch::version()));
}

#[tokio::test]
async fn get_application_info_opensearch() {
    let server = mock_opensearch::TestOpenSearchServer::start().await;
    let (client, _server, _config_senders) = run_vs(Config {
        opensearch_addr: Some(server.base_url()),
        ..usearch_test_config()
    })
    .await;

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
    assert_eq!(info.engine, "opensearch");
}

#[tokio::test]
async fn get_application_info_diskann() {
    let (client, _server, _config_senders) = run_vs(Config {
        diskann_backend: Some(DiskannBackendKind::Inmem),
        ..usearch_test_config()
    })
    .await;

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
    assert_eq!(info.engine, format!("diskann-{}", diskann::version()));
}
