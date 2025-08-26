/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::{db_basic, mock_opensearch};
use httpclient::HttpClient;
use std::net::SocketAddr;

async fn run_vs(
    index_factory: Box<dyn vector_store::IndexFactory + Send + Sync>,
) -> (HttpClient, impl Sized) {
    let node_state = vector_store::new_node_state().await;
    let (db_actor, _) = db_basic::new(node_state.clone());
    let (server, addr) = vector_store::run(
        SocketAddr::from(([127, 0, 0, 1], 0)).into(),
        Some(1),
        node_state,
        db_actor,
        index_factory,
    )
    .await
    .unwrap();
    (HttpClient::new(addr), server)
}

#[tokio::test]
async fn get_application_info_usearch() {
    let (client, _server) = run_vs(vector_store::new_index_factory_usearch().unwrap()).await;

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
    assert_eq!(info.engine, format!("usearch-{}", usearch::version()));
}

#[tokio::test]
async fn get_application_info_opensearch() {
    let server = mock_opensearch::TestOpenSearchServer::start().await;
    let index_factory = vector_store::new_index_factory_opensearch(server.base_url()).unwrap();
    let (client, _server) = run_vs(index_factory).await;

    let info = client.info().await;

    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(info.service, env!("CARGO_PKG_NAME"));
    assert_eq!(info.engine, "opensearch");
}
