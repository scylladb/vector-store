/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use std::net::ToSocketAddrs;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
mod info;

// Index creating/querying is CPU bound task, so that vector-store uses rayon ThreadPool for them.
// From the start there was no need (network traffic seems to be not so high) to support more than
// one thread per network IO bound tasks.
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    _ = dotenvy::dotenv();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info"))?)
        .with(fmt::layer().with_target(false))
        .init();

    tracing::info!(
        "Starting {} version {}",
        info::Info::name(),
        info::Info::version()
    );

    let vector_store_addr = dotenvy::var("VECTOR_STORE_URI")
        .unwrap_or("127.0.0.1:6080".to_string())
        .to_socket_addrs()?
        .next()
        .ok_or(anyhow!("Unable to parse VECTOR_STORE_URI env (host:port)"))?
        .into();

    let scylladb_uri = dotenvy::var("VECTOR_STORE_SCYLLADB_URI")
        .unwrap_or("127.0.0.1:9042".to_string())
        .into();

    let background_threads = dotenvy::var("VECTOR_STORE_THREADS")
        .ok()
        .and_then(|v| v.parse().ok());

    let node_state = vector_store::new_node_state().await;

    let opensearch_addr = dotenvy::var("VECTOR_STORE_OPENSEARCH_URI").ok();

    let index_factory = if let Some(addr) = opensearch_addr {
        tracing::info!("Using OpenSearch index factory at {addr}");
        vector_store::new_index_factory_opensearch(addr)?
    } else {
        tracing::info!("Using Usearch index factory");
        vector_store::new_index_factory_usearch()?
    };

    let db_actor = vector_store::new_db(scylladb_uri, node_state.clone()).await?;

    let (_server_actor, addr) = vector_store::run(
        vector_store_addr,
        background_threads,
        node_state,
        db_actor,
        index_factory,
    )
    .await?;
    tracing::info!("listening on {addr}");

    vector_store::wait_for_shutdown().await;

    Ok(())
}
