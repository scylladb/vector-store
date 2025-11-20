/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::INDEX;
use crate::KEYSPACE;
use httpclient::HttpClient;
use httpclient::IndexStatus;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time;
use tracing::info;

pub(crate) fn new_http_clients(addrs: Vec<SocketAddr>) -> Vec<HttpClient> {
    addrs.into_iter().map(HttpClient::new).collect()
}

pub(crate) async fn wait_for_indexes_ready(clients: &[HttpClient]) {
    for client in clients {
        let url = client.url();
        loop {
            if let Ok(status) = client
                .index_status(&KEYSPACE.to_string().into(), &INDEX.to_string().into())
                .await
            {
                let status = status.status;
                if status == IndexStatus::Serving {
                    break;
                } else {
                    info!(
                        "Waiting for index {INDEX} at {url} to be ready (current status: {status:?})"
                    );
                }
            } else {
                info!("Waiting for index {INDEX} at {url} to be available (index not found yet)");
            };
            time::sleep(Duration::from_secs(1)).await;
        }
        info!("Index {INDEX} at {url} is ready");
    }
}
