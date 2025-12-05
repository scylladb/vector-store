/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use httpclient::HttpClient;
use httpclient::IndexStatus;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time;
use tracing::info;

pub(crate) fn new_http_clients(addrs: Vec<SocketAddr>) -> Vec<HttpClient> {
    addrs.into_iter().map(HttpClient::new).collect()
}

pub(crate) async fn wait_for_indexes_ready(keyspace: &str, index: &str, clients: &[HttpClient]) {
    let keyspace = keyspace.to_string().into();
    let index = index.to_string().into();
    for client in clients {
        let url = client.url();
        loop {
            if let Ok(status) = client.index_status(&keyspace, &index).await {
                let status = status.status;
                if status == IndexStatus::Serving {
                    break;
                } else {
                    info!(
                        "Waiting for index {index} at {url} to be ready (current status: {status:?})"
                    );
                }
            } else {
                info!("Waiting for index {index} at {url} to be available (index not found yet)");
            };
            time::sleep(Duration::from_secs(1)).await;
        }
        info!("Index {index} at {url} is ready");
    }
}
