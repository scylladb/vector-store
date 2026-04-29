/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use httpapi::ColumnName;
use httpapi::Distance;
use httpapi::IndexInfo;
use httpapi::IndexName;
use httpapi::IndexStatusResponse;
use httpapi::InfoResponse;
use httpapi::KeyspaceName;
use httpapi::Limit;
use httpapi::NodeStatus;
use httpapi::PostIndexAnnFilter;
use httpapi::PostIndexAnnRequest;
use httpapi::PostIndexAnnResponse;
use httpapi::SimilarityScore;
use httpapi::Vector;
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client,
    url_api: String,
    url_internals_api: String,
}

impl HttpClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            url_api: format!("http://{addr}/api/v1"),
            url_internals_api: format!("http://{addr}/api/internals"),
            client: Client::new(),
        }
    }

    pub fn url(&self) -> &str {
        self.url_api.as_str()
    }

    pub async fn indexes(&self) -> Vec<IndexInfo> {
        self.client
            .get(format!("{}/indexes", self.url_api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    pub async fn ann(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
        vector: Vector,
        filter: Option<PostIndexAnnFilter>,
        limit: Limit,
    ) -> (
        HashMap<ColumnName, Vec<Value>>,
        Vec<Distance>,
        Vec<SimilarityScore>,
    ) {
        let resp = self
            .post_ann(keyspace_name, index_name, vector, filter, limit)
            .await
            .json::<PostIndexAnnResponse>()
            .await
            .unwrap();
        (resp.primary_keys, resp.distances, resp.similarity_scores)
    }

    pub async fn post_ann(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
        vector: Vector,
        filter: Option<PostIndexAnnFilter>,
        limit: Limit,
    ) -> reqwest::Response {
        let request = PostIndexAnnRequest {
            vector,
            filter,
            limit,
        };
        self.post_ann_data(keyspace_name, index_name, &request)
            .await
    }

    pub async fn post_ann_data<T: Serialize>(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
        data: &T,
    ) -> reqwest::Response {
        self.client
            .post(format!(
                "{}/indexes/{}/{}/ann",
                self.url_api, keyspace_name, index_name
            ))
            .json(data)
            .send()
            .await
            .unwrap()
    }

    pub async fn index_status(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
    ) -> anyhow::Result<IndexStatusResponse> {
        let response = self
            .client
            .get(format!(
                "{}/indexes/{}/{}/status",
                self.url_api, keyspace_name, index_name
            ))
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.json::<IndexStatusResponse>().await?)
        } else {
            let status = response.status();
            let error_text = response.text().await?;
            Err(anyhow::anyhow!("HTTP {status}: {error_text}"))
        }
    }

    pub async fn info(&self) -> InfoResponse {
        self.client
            .get(format!("{}/info", self.url_api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    pub async fn status(&self) -> anyhow::Result<NodeStatus> {
        Ok(self
            .client
            .get(format!("{}/status", self.url_api))
            .send()
            .await?
            .json()
            .await?)
    }

    pub async fn internals_counters(&self) -> anyhow::Result<BTreeMap<String, u64>> {
        Ok(self
            .client
            .get(format!("{}/counters", self.url_internals_api))
            .send()
            .await?
            .json()
            .await?)
    }

    pub async fn internals_clear_counters(&self) -> anyhow::Result<()> {
        self.client
            .delete(format!("{}/counters", self.url_internals_api))
            .send()
            .await?;
        Ok(())
    }

    pub async fn internals_start_counter(&self, id: String) -> anyhow::Result<()> {
        self.client
            .put(format!("{}/counters/{id}", self.url_internals_api))
            .send()
            .await?;
        Ok(())
    }

    pub async fn internals_session_counters(&self) -> anyhow::Result<BTreeMap<String, u64>> {
        Ok(self
            .client
            .get(format!("{}/session-counters", self.url_internals_api))
            .send()
            .await?
            .json()
            .await?)
    }
}
