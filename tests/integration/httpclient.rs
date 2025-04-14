/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use reqwest::Client;
use std::collections::HashMap;
use std::net::SocketAddr;
use vector_store::ColumnName;
use vector_store::Distance;
use vector_store::Embeddings;
use vector_store::IndexId;
use vector_store::IndexMetadata;
use vector_store::Key;
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
        embeddings: Embeddings,
        limit: Limit,
    ) -> (HashMap<ColumnName, Vec<Key>>, Vec<Distance>) {
        let resp = self
            .client
            .post(format!(
                "{}/indexes/{}/{}/ann",
                self.url_api, index.keyspace_name, index.index_name
            ))
            .json(&PostIndexAnnRequest { embeddings, limit })
            .send()
            .await
            .unwrap()
            .json::<PostIndexAnnResponse>()
            .await
            .unwrap();
        (resp.primary_keys, resp.distances)
    }
}
