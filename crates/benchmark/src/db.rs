/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexOption;
use crate::MetricType;
use crate::Query;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tap::Pipe;
use tokio::fs;
use tokio::sync::Semaphore;
use tokio::time;
use tracing::error;
use tracing::info;

const VECTOR_ID: &str = "vector_id";
const VECTOR: &str = "vector";
const LEVEL: &str = "level";

#[derive(Clone)]
pub(crate) struct Scylla(Arc<State>);

struct State {
    session: Session,
    st_search: Option<PreparedStatement>,
    st_search_local: Option<PreparedStatement>,
}

impl Scylla {
    pub(crate) async fn new(
        uri: SocketAddr,
        user: Option<String>,
        passwd_path: Option<PathBuf>,
        keyspace: &str,
        table: &str,
    ) -> Self {
        let passwd = if let Some(path) = passwd_path {
            fs::read_to_string(path)
                .await
                .expect("Failed to read password file")
                .trim()
                .to_string()
        } else {
            String::new()
        };
        let session = SessionBuilder::new()
            .known_node(uri.to_string())
            .default_execution_profile_handle(
                ExecutionProfile::builder()
                    .consistency(Consistency::One)
                    .build()
                    .into_handle(),
            )
            .pipe(|builder| {
                if let (Some(user), passwd) = (user, passwd) {
                    builder.user(user, passwd)
                } else {
                    builder
                }
            })
            .build()
            .await
            .unwrap();

        let st_search = session
            .prepare(format!(
                "SELECT {VECTOR_ID} FROM {keyspace}.{table} ORDER BY {VECTOR} ANN OF ? LIMIT ?"
            ))
            .await
            .ok();
        let st_search_local = session
            .prepare(format!(
                "SELECT {VECTOR_ID} FROM {keyspace}.{table} WHERE {LEVEL} = ? ORDER BY {VECTOR} ANN OF ? LIMIT ?"
            ))
            .await
            .ok();

        Self(Arc::new(State {
            session,
            st_search,
            st_search_local,
        }))
    }

    pub(crate) async fn create_table(
        &self,
        keyspace: &str,
        table: &str,
        dimension: usize,
        replication_factor: usize,
    ) {
        self.0.session
        .query_unpaged(
            format!(
                "
                CREATE KEYSPACE {keyspace}
                WITH replication = {{'class': 'NetworkTopologyStrategy' , 'replication_factor': '{replication_factor}'}}
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
                CREATE TABLE {keyspace}.{table} (
                    {LEVEL} bigint,
                    {VECTOR_ID} bigint,
                    {VECTOR} vector<float, {dimension}>,
                    PRIMARY KEY ({LEVEL}, {VECTOR_ID})
                )
                ",
                ),
                &[],
            )
            .await
            .unwrap();
    }

    pub(crate) async fn drop_table(&self, keyspace: &str) {
        self.0
            .session
            .query_unpaged(format!("DROP KEYSPACE IF EXISTS {keyspace}"), &[])
            .await
            .unwrap();
    }

    pub(crate) async fn create_index(
        &self,
        keyspace: &str,
        table: &str,
        index: &str,
        config: IndexOption,
    ) {
        let metric_type = match config.metric_type {
            MetricType::Euclidean => "EUCLIDEAN",
            MetricType::Cosine => "COSINE",
            MetricType::DotProduct => "DOT_PRODUCT",
        };
        let local = if config.local {
            format!("({LEVEL}), ")
        } else {
            String::new()
        };
        self.0
            .session
            .query_unpaged(
                format!(
                    "
                    CREATE CUSTOM INDEX {index} ON {keyspace}.{table} ({local}{VECTOR})
                    USING 'vector_index' WITH OPTIONS = {{
                        'similarity_function': '{metric_type}',
                        'maximum_node_connections': '{m}',
                        'construction_beam_width': '{ef_construction}',
                        'search_beam_width': '{ef_search}'
                   }}
                   ",
                    m = config.m,
                    ef_construction = config.ef_construction,
                    ef_search = config.ef_search
                ),
                &[],
            )
            .await
            .unwrap();
    }

    pub(crate) async fn drop_index(&self, keyspace: &str, index: &str) {
        self.0
            .session
            .query_unpaged(format!("DROP INDEX IF EXISTS {keyspace}.{index}"), &[])
            .await
            .unwrap();
    }

    pub(crate) async fn upload_vectors(
        &self,
        keyspace: &str,
        table: &str,
        level_fn: impl Fn(i64) -> i64,
        mut stream: BoxStream<'static, (i64, Vec<f32>)>,
        concurrency: usize,
    ) {
        let mut st_insert = self
            .0
            .session
            .prepare(format!(
                "INSERT INTO {keyspace}.{table} ({LEVEL}, {VECTOR_ID}, {VECTOR}) VALUES (?, ?, ?)"
            ))
            .await
            .unwrap();
        st_insert.set_consistency(Consistency::Any);

        let semaphore = Arc::new(Semaphore::new(concurrency));

        let mut count = 0;
        while let Some((vector_id, vector)) = stream.next().await {
            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
            let scylla = Arc::clone(&self.0);
            let st_insert = st_insert.clone();

            count += 1;
            if count % 1_000_000 == 0 {
                info!("Uploading vector {}M", count / 1_000_000);
            }

            let level = level_fn(vector_id);
            tokio::spawn(async move {
                scylla
                    .session
                    .execute_unpaged(&st_insert, (level, vector_id, vector))
                    .await
                    .unwrap();
                drop(permit);
            });
        }
        _ = semaphore.acquire_many(concurrency as u32).await.unwrap();
    }

    pub(crate) async fn search(&self, level: Option<u8>, query: &Query) -> f64 {
        let found = if let Some(level) = level {
            time::timeout(Duration::from_secs(10), async move {
                self.0
                    .session
                    .execute_iter(
                        self.0.st_search_local.as_ref().unwrap().clone(),
                        (level as i64, &query.query, query.neighbors.len() as i32),
                    )
                    .await
                    .unwrap()
                    .rows_stream::<(i64,)>()
                    .unwrap()
                    .map_ok(|(vector_id,)| vector_id)
                    .try_collect()
                    .await
                    .unwrap()
            })
            .await
        } else {
            time::timeout(Duration::from_secs(10), async move {
                self.0
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
                    .unwrap()
            })
            .await
        };
        let Ok(found) = found else {
            error!("Search query timed out");
            return 0.0;
        };
        query.neighbors.intersection(&found).count() as f64 / query.neighbors.len() as f64
    }
}
