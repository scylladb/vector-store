/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Query;
use futures::TryStreamExt;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tap::Pipe;
use tokio::fs;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::time;
use tracing::error;
use tracing::info;

const VECTOR_ID: &str = "vector_id";
const VECTOR: &str = "vector";
const BUCKET: &str = "bucket";

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
                "SELECT {VECTOR_ID} FROM {keyspace}.{table} WHERE {BUCKET} = ? ORDER BY {VECTOR} ANN OF ? LIMIT ?"
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
                    {BUCKET} bigint,
                    {VECTOR_ID} bigint,
                    {VECTOR} vector<float, {dimension}>,
                    PRIMARY KEY (({BUCKET}, {VECTOR_ID}))
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
        local: bool,
        options: &str,
    ) {
        let local = if local {
            format!("({BUCKET}), ")
        } else {
            String::new()
        };
        self.0
            .session
            .query_unpaged(
                format!(
                    "
                    CREATE CUSTOM INDEX {index} ON {keyspace}.{table} ({local}{VECTOR})
                    USING 'vector_index' WITH OPTIONS = {options}
                   "
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
        buckets: Arc<BTreeMap<i64, u8>>,
        mut rx: mpsc::Receiver<(i64, Box<[f32]>)>,
        concurrency: usize,
    ) {
        let mut st_insert = self
            .0
            .session
            .prepare(format!(
                "INSERT INTO {keyspace}.{table} ({BUCKET}, {VECTOR_ID}, {VECTOR}) VALUES (?, ?, ?)"
            ))
            .await
            .unwrap();
        st_insert.set_consistency(Consistency::Any);

        let semaphore = Arc::new(Semaphore::new(concurrency));

        let mut count = 0;
        while let Some((vector_id, vector)) = rx.recv().await {
            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
            let scylla = Arc::clone(&self.0);
            let st_insert = st_insert.clone();

            count += 1;
            if count % 1_000_000 == 0 {
                info!("Uploading vector {}M", count / 1_000_000);
            }

            let bucket = *buckets.get(&vector_id).unwrap_or(&u8::MAX) as i64;
            tokio::spawn(async move {
                scylla
                    .session
                    .execute_unpaged(&st_insert, (bucket, vector_id, vector))
                    .await
                    .unwrap();
                drop(permit);
            });
        }
        _ = semaphore.acquire_many(concurrency as u32).await.unwrap();
    }

    pub(crate) async fn delete_rows(
        &self,
        keyspace: &str,
        table: &str,
        buckets: Arc<BTreeMap<i64, u8>>,
        mut rx: mpsc::Receiver<i64>,
        concurrency: usize,
    ) {
        let mut st_delete = self
            .0
            .session
            .prepare(format!(
                "DELETE FROM {keyspace}.{table} WHERE {BUCKET} = ? AND {VECTOR_ID} = ?"
            ))
            .await
            .unwrap();
        st_delete.set_consistency(Consistency::Any);

        let semaphore = Arc::new(Semaphore::new(concurrency));

        let mut count = 0;
        while let Some(vector_id) = rx.recv().await {
            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
            let scylla = Arc::clone(&self.0);
            let st_delete = st_delete.clone();

            count += 1;
            if count % 1_000_000 == 0 {
                info!("Deleting vector {}M", count / 1_000_000);
            }

            let bucket = *buckets.get(&vector_id).unwrap_or(&u8::MAX) as i64;
            tokio::spawn(async move {
                scylla
                    .session
                    .execute_unpaged(&st_delete, (bucket, vector_id))
                    .await
                    .unwrap();
                drop(permit);
            });
        }
        _ = semaphore.acquire_many(concurrency as u32).await.unwrap();
    }

    pub(crate) async fn search(&self, bucket: Option<u8>, query: &Query) -> f64 {
        let found = if let Some(bucket) = bucket {
            time::timeout(Duration::from_secs(10), async move {
                self.0
                    .session
                    .execute_iter(
                        self.0.st_search_local.as_ref().unwrap().clone(),
                        (bucket as i64, &query.query, query.neighbors.len() as i32),
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
