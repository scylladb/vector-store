/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use vector_store::ColumnName;
use vector_store::Distance;
use vector_store::Embedding;
use vector_store::IndexId;
use vector_store::IndexMetadata;
use vector_store::Limit;
use vector_store::httproutes::PostIndexAnnRequest;
use vector_store::httproutes::PostIndexAnnResponse;

pub(crate) struct HttpClient {
    client: Client,
    url_api: String,
}

impl HttpClient {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self {
            url_api: format!("http://{addr}/api/v1"),
            client: Client::new(),
        }
    }

    pub(crate) async fn indexes(&self) -> Vec<IndexId> {
        self.client
            .get(format!("{}/indexes", self.url_api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    pub(crate) async fn ann(
        &self,
        index: &IndexMetadata,
        embedding: Embedding,
        limit: Limit,
    ) -> (HashMap<ColumnName, Vec<Value>>, Vec<Distance>) {
        let resp = self
            .client
            .post(format!(
                "{}/indexes/{}/{}/ann",
                self.url_api, index.keyspace_name, index.index_name
            ))
            .json(&PostIndexAnnRequest { embedding, limit })
            .send()
            .await
            .unwrap()
            .json::<PostIndexAnnResponse>()
            .await
            .unwrap();
        (resp.primary_keys, resp.distances)
    }

    pub(crate) async fn count(&self, index: &IndexMetadata) -> Option<usize> {
        self.client
            .get(format!(
                "{}/indexes/{}/{}/count",
                self.url_api, index.keyspace_name, index.index_name
            ))
            .send()
            .await
            .unwrap()
            .json::<usize>()
            .await
            .ok()
    }
}
