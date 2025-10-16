/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use vector_store::ColumnName;
use vector_store::Distance;
use vector_store::IndexInfo;
use vector_store::IndexName;
use vector_store::KeyspaceName;
use vector_store::Limit;
use vector_store::Vector;
use vector_store::httproutes::InfoResponse;
use vector_store::httproutes::PostIndexAnnRequest;
use vector_store::httproutes::PostIndexAnnResponse;
use vector_store::httproutes::Status;

pub struct HttpClient {
    client: Client,
    url_api: String,
}

impl HttpClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            url_api: format!("http://{addr}/api/v1"),
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
        limit: Limit,
    ) -> (HashMap<ColumnName, Vec<Value>>, Vec<Distance>) {
        let resp = self
            .post_ann(keyspace_name, index_name, vector, limit)
            .await
            .json::<PostIndexAnnResponse>()
            .await
            .unwrap();
        (resp.primary_keys, resp.distances)
    }

    pub async fn post_ann(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
        vector: Vector,
        limit: Limit,
    ) -> reqwest::Response {
        let request = PostIndexAnnRequest { vector, limit };
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

    pub async fn count(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
    ) -> Option<usize> {
        self.client
            .get(format!(
                "{}/indexes/{}/{}/count",
                self.url_api, keyspace_name, index_name
            ))
            .send()
            .await
            .unwrap()
            .json::<usize>()
            .await
            .ok()
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

    pub async fn status(&self) -> anyhow::Result<Status> {
        Ok(self
            .client
            .get(format!("{}/status", self.url_api))
            .send()
            .await?
            .json()
            .await?)
    }
}
