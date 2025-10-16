/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::INDEX;
use crate::KEYSPACE;
use crate::MetricType;
use crate::Query;
use crate::TABLE;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;

const ID: &str = "id";
const VECTOR_ID: &str = "vector_id";
const VECTOR: &str = "vector";

#[derive(Clone)]
pub(crate) struct Scylla(Arc<State>);

struct State {
    session: Session,
    st_search: Option<PreparedStatement>,
}

impl Scylla {
    pub(crate) async fn new(uri: SocketAddr) -> Self {
        let session = SessionBuilder::new()
            .known_node(uri.to_string())
            .default_execution_profile_handle(
                ExecutionProfile::builder()
                    .consistency(Consistency::One)
                    .build()
                    .into_handle(),
            )
            .build()
            .await
            .unwrap();

        let st_search = session
            .prepare(format!(
                "SELECT {VECTOR_ID} FROM {KEYSPACE}.{TABLE} ORDER BY {VECTOR} ANN OF ? LIMIT ?"
            ))
            .await
            .ok();

        Self(Arc::new(State { session, st_search }))
    }

    pub(crate) async fn create_table(&self, dimension: usize) {
        self.0.session
        .query_unpaged(
            format!(
                "
                CREATE KEYSPACE {KEYSPACE}
                WITH replication = {{'class': 'NetworkTopologyStrategy' , 'replication_factor': '3'}}
                AND tablets = {{'enabled': 'false'}}
                "
            ),
            &[],
        )
        .await
        .unwrap();

        self.0
            .session
            .query_unpaged(
                format!(
                    "
                CREATE TABLE {KEYSPACE}.{TABLE} (
                    {ID} uuid PRIMARY KEY,
                    {VECTOR_ID} bigint,
                    {VECTOR} vector<float, {dimension}>,
                )
                ",
                ),
                &[],
            )
            .await
            .unwrap();
    }

    pub(crate) async fn drop_table(&self) {
        self.0
            .session
            .query_unpaged(format!("DROP KEYSPACE IF EXISTS {KEYSPACE}"), &[])
            .await
            .unwrap();
    }

    pub(crate) async fn create_index(
        &self,
        metric_type: MetricType,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) {
        let metric_type = match metric_type {
            MetricType::Euclidean => "EUCLIDEAN",
            MetricType::Cosine => "COSINE",
            MetricType::DotProduct => "DOT_PRODUCT",
        };
        self.0
            .session
            .query_unpaged(
                format!(
                    "
                CREATE CUSTOM INDEX {INDEX} ON {KEYSPACE}.{TABLE} ({VECTOR})
                USING 'vector_index' WITH OPTIONS = {{
                    'similarity_function': '{metric_type}',
                    'maximum_node_connections': '{m}',
                    'construction_beam_width': '{ef_construction}',
                    'search_beam_width': '{ef_search}'
               }}
               "
                ),
                &[],
            )
            .await
            .unwrap();
    }

    pub(crate) async fn drop_index(&self) {
        self.0
            .session
            .query_unpaged(format!("DROP INDEX IF EXISTS {KEYSPACE}.{INDEX}"), &[])
            .await
            .unwrap();
    }

    pub(crate) async fn upload_vectors(
        &self,
        stream: impl Stream<Item = (i64, Vec<f32>)>,
        concurrency: usize,
    ) {
        let mut st_insert = self
            .0
            .session
            .prepare(format!(
                "INSERT INTO {KEYSPACE}.{TABLE} ({ID}, {VECTOR_ID}, {VECTOR}) VALUES (?, ?, ?)"
            ))
            .await
            .unwrap();
        st_insert.set_consistency(Consistency::Any);

        let semaphore = Arc::new(Semaphore::new(concurrency));

        let mut stream = pin!(stream);
        while let Some((vector_id, vector)) = stream.next().await {
            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
            let scylla = Arc::clone(&self.0);
            let st_insert = st_insert.clone();
            tokio::spawn(async move {
                scylla
                    .session
                    .execute_unpaged(&st_insert, (Uuid::new_v4(), vector_id, vector))
                    .await
                    .unwrap();
                drop(permit);
            });
        }
        _ = semaphore.acquire_many(concurrency as u32).await.unwrap();
    }

    pub(crate) async fn search(&self, query: &Query) -> f64 {
        let found = self
            .0
            .session
            .execute_iter(
                self.0.st_search.as_ref().unwrap().clone(),
                (&query.query, query.neighbors.len() as i32),
            )
            .await
            .unwrap()
            .rows_stream::<(i64,)>()
            .unwrap()
            .map_ok(|(vector_id,)| vector_id)
            .try_collect()
            .await
            .unwrap();
        query.neighbors.intersection(&found).count() as f64 / query.neighbors.len() as f64
    }
}
