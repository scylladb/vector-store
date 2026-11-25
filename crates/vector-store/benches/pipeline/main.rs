/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod ann;
mod corpus;
#[path = "../../tests/integration/db_basic.rs"]
#[allow(dead_code)]
mod db_basic;
mod fts;

use axum::http::StatusCode;
use criterion::Criterion;
use criterion::criterion_group;
use db_basic::DbBasic;
use futures::StreamExt;
use futures::stream;
use httpapi::IndexInfo;
use httpapi::IndexStatus;
use itertools::Itertools;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tap::Pipe;
use testclient::TestClient;
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio::task;
use vector_store::Config;
use vector_store::ConfigReceivers;
use vector_store::HttpServerConfig;
use vector_store::HttpServerExt;

const ENV_CONCURRENCY: &str = "BENCHES_CONCURRENCY";
const CDC_SAFETY_INTERVAL: Duration = Duration::from_millis(1);
const CDC_SLEEP_INTERVAL: Duration = Duration::ZERO;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        _ = dotenvy::dotenv();

        #[cfg(feature = "console")]
        console_subscriber::init();

        #[cfg(not(feature = "console"))]
        {
            use tracing_subscriber::EnvFilter;
            use tracing_subscriber::fmt;
            use tracing_subscriber::prelude::*;

            tracing_subscriber::registry()
                .with(EnvFilter::try_new("none").unwrap())
                .with(fmt::layer().with_target(false))
                .init();
        }
    });
}

fn default_runtime() -> Runtime {
    let threads = dotenvy::var("VECTOR_STORE_THREADS")
        .ok()
        .and_then(|s| s.parse().ok());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .pipe(|mut b| {
            if let Some(threads) = threads {
                b.worker_threads(threads);
            }
            b
        })
        .enable_all()
        .build()
        .unwrap();
    hotpath::tokio_runtime!(runtime.handle());
    runtime
}

async fn default_config() -> Arc<Config> {
    fn env(key: &str) -> anyhow::Result<String> {
        Ok(match key {
            "VECTOR_STORE_USEARCH_SIMULATOR" => {
                dotenvy::var(key).unwrap_or("1us:1us:1us".to_string())
            }
            "VECTOR_STORE_CDC_SAFETY_INTERVAL" => format!("{CDC_SAFETY_INTERVAL:?}"),
            "VECTOR_STORE_CDC_SLEEP_INTERVAL" => format!("{CDC_SLEEP_INTERVAL:?}"),
            _ => dotenvy::var(key)?,
        })
    }
    Arc::new(vector_store::load_config(env).await.unwrap())
}

fn default_concurrency() -> usize {
    dotenvy::var(ENV_CONCURRENCY)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

async fn run_vector_store(
    config: watch::Receiver<Arc<Config>>,
    db: DbBasic,
) -> (impl Sized, Arc<TestClient>) {
    let addr = config.borrow().vector_store_addr;
    let (http_tx, http_rx) = watch::channel(Some(Arc::new(HttpServerConfig { addr, tls: None })));
    let (_mtls_tx, mtls_http_rx) = watch::channel(None);
    let receivers = ConfigReceivers {
        config,
        http: http_rx,
        mtls_http: mtls_http_rx,
    };

    let (server, _mtls) = vector_store::run(db, receivers).await.unwrap();

    let client = Arc::new(TestClient::new(server.router().await.unwrap()));
    ((http_tx, server), client)
}

async fn wait_until_index_is_created(
    client: &TestClient,
    keyspace_name: &httpapi::KeyspaceName,
    index_name: &httpapi::IndexName,
) {
    loop {
        let response = client.index_status(keyspace_name, index_name).await;
        if response.status_code() == StatusCode::OK {
            break;
        }
        task::yield_now().await;
    }
}

async fn wait_until_index_is_ready(
    client: &TestClient,
    keyspace_name: &httpapi::KeyspaceName,
    index_name: &httpapi::IndexName,
) {
    loop {
        let response = client.index_status(keyspace_name, index_name).await;
        if response.status_code() == StatusCode::OK
            && response.json::<IndexInfo>().status == IndexStatus::Serving
        {
            break;
        }
        task::yield_now().await;
    }
}

async fn wait_until_index_is_removed(
    client: &TestClient,
    keyspace_name: &httpapi::KeyspaceName,
    index_name: &httpapi::IndexName,
) {
    loop {
        let response = client.index_status(keyspace_name, index_name).await;
        if response.status_code() != StatusCode::OK {
            break;
        }
        task::yield_now().await;
    }
}

fn wait_until_all_tasks_finished(runtime: &Runtime) {
    while runtime.metrics().num_alive_tasks() > 1 {
        std::thread::yield_now();
    }
}

#[hotpath::measure]
async fn run_with_concurrency<F, Fut>(concurrency: usize, iters: u64, f: F) -> Duration
where
    F: Fn(usize, u64) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Duration> + Send + 'static,
{
    let counter = Arc::new(AtomicU64::new(0));
    let tasks = (0..concurrency)
        .map(|concurrency| {
            let counter = Arc::clone(&counter);
            let f = f.clone();
            tokio::spawn(async move {
                let mut durations = Vec::new();
                loop {
                    let it = counter.fetch_add(1, Ordering::Relaxed);
                    if it >= iters {
                        break;
                    }
                    let duration = f(concurrency, it).await;
                    durations.push(duration);
                }
                durations.into_iter().fold(Duration::ZERO, |acc, x| acc + x)
            })
        })
        .collect_vec();
    stream::iter(tasks)
        .then(|task| async move { task.await.unwrap() })
        .fold(Duration::ZERO, |acc, x| async move { acc + x })
        .await
}

criterion_group!(
    benches,
    ann::fullscan_insert,
    ann::search,
    ann::cdc_insert,
    ann::cdc_update,
    ann::cdc_delete,
    ann::search_while_updating,
    ann::search_while_inserting,
    ann::search_while_deleting,
    fts::fts_fullscan_build,
    fts::fts_search,
    fts::fts_search_while_inserting,
);

#[hotpath::main]
fn main() {
    benches();

    Criterion::default().configure_from_args().final_summary();
}
