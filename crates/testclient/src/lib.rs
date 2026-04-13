/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use axum::Router;
use axum_test::TestResponse;
use axum_test::TestServer;
use httpapi::IndexName;
use httpapi::KeyspaceName;
use httpapi::Limit;
use httpapi::PostIndexAnnFilter;
use httpapi::PostIndexAnnRequest;
use httpapi::Vector;

#[derive(Debug)]
pub struct TestClient {
    test_server: TestServer,
    url_api: String,
}

impl TestClient {
    pub fn new(router: Router) -> Self {
        Self {
            url_api: "/api/v1".to_string(),
            test_server: TestServer::new(router),
        }
    }

    #[hotpath::measure]
    pub async fn index_status(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
    ) -> TestResponse {
        self.test_server
            .get(&format!(
                "{}/indexes/{}/{}/status",
                self.url_api, keyspace_name, index_name
            ))
            .await
    }

    #[hotpath::measure]
    pub async fn ann(
        &self,
        keyspace_name: &KeyspaceName,
        index_name: &IndexName,
        vector: Vector,
        filter: Option<PostIndexAnnFilter>,
        limit: Limit,
    ) -> TestResponse {
        self.test_server
            .post(&format!(
                "{}/indexes/{}/{}/ann",
                self.url_api, keyspace_name, index_name
            ))
            .json(&PostIndexAnnRequest {
                vector,
                filter,
                limit,
            })
            .await
    }
}
