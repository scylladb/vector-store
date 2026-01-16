/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
mod config_manager;
mod info;

use config_manager::ConfigManager;

#[derive(Parser)]
#[clap(version)]
struct Args {}

fn dotenvy_to_std_var(key: &'static str) -> Result<String, std::env::VarError> {
    dotenvy::var(key).map_err(|_| std::env::VarError::NotPresent)
}

// Index creating/querying is CPU bound task, so that vector-store uses rayon ThreadPool for them.
// From the start there was no need (network traffic seems to be not so high) to support more than
// one thread per network IO bound tasks.
fn main() -> anyhow::Result<()> {
    // Initialize logging early, before loading configuration, disable colors will be read twice
    let disable_colors: bool = dotenvy::var("VECTOR_STORE_DISABLE_COLORS")
        .unwrap_or("false".to_string())
        .trim()
        .parse()
        .or(Err(anyhow!(
            "Unable to parse VECTOR_STORE_DISABLE_COLORS env (true/false)"
        )))?;

    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info"))?)
        .with(fmt::layer().with_target(false).with_ansi(!disable_colors))
        .init();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install aws-lc-rs crypto provider");

    _ = dotenvy::dotenv();

    // Load configuration early to get disable_colors for logging setup
    let config_future = config_manager::load_config(dotenvy_to_std_var);
    let loaded_config = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(config_future)?;

    _ = Args::parse();

    tracing::info!(
        "Starting {} version {}",
        info::Info::name(),
        info::Info::version()
    );

    // Create ConfigManager with initial configuration
    let (config_manager, config_rx) = ConfigManager::new(loaded_config);
    let config = config_rx.borrow().clone();

    let threads = config.threads;

    vector_store::block_on(threads, async move || {
        // Start SIGHUP handler now that we're in the Tokio runtime
        config_manager.start(dotenvy_to_std_var);

        let node_state = vector_store::new_node_state().await;

        let opensearch_addr = config.opensearch_addr.clone();

        let index_factory = if let Some(addr) = opensearch_addr {
            tracing::info!("Using OpenSearch index factory at {addr}");
            vector_store::new_index_factory_opensearch(addr, config_rx.clone())?
        } else {
            tracing::info!("Using Usearch index factory");
            vector_store::new_index_factory_usearch(config_rx.clone())?
        };

        let internals = vector_store::new_internals();
        let db_actor =
            vector_store::new_db(node_state.clone(), internals.clone(), config_rx.clone()).await?;

        let (_server_actor, addr) = vector_store::run(
            node_state,
            db_actor,
            internals,
            index_factory,
            config_rx.clone(),
        )
        .await?;
        tracing::info!("listening on {addr}");

        vector_store::wait_for_shutdown().await;

        anyhow::Ok(())
    })?;

    Ok(())
}
