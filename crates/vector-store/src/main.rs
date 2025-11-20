/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use anyhow::bail;
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

fn tls_config() -> anyhow::Result<Option<vector_store::TlsConfig>> {
    let cert_path = dotenvy::var("VECTOR_STORE_TLS_CERT_PATH").ok();
    let key_path = dotenvy::var("VECTOR_STORE_TLS_KEY_PATH").ok();

    match (cert_path, key_path) {
        (Some(cert_path), Some(key_path)) => Ok(Some(vector_store::TlsConfig {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        })),
        (None, None) => Ok(None),
        _ => {
            bail!(
                "Both VECTOR_STORE_TLS_CERT_PATH and VECTOR_STORE_TLS_KEY_PATH must be set together"
            )
        }
    }
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

    let vector_store_addr = config.vector_store_addr;

    let http_server_config = vector_store::HttpServerConfig {
        addr: vector_store_addr,
        tls: tls_config()?,
    };
    let scylladb_uri = config.scylladb_uri.clone().into();

    let threads = config.threads;

    vector_store::block_on(threads, async move || {
        // Start SIGHUP handler now that we're in the Tokio runtime
        config_manager.start(dotenvy_to_std_var);

        let node_state = vector_store::new_node_state().await;

        let opensearch_addr = config.opensearch_addr.clone();

        let index_factory = if let Some(addr) = opensearch_addr {
            tracing::info!("Using OpenSearch index factory at {addr}");
            vector_store::new_index_factory_opensearch(addr)?
        } else {
            tracing::info!("Using Usearch index factory");
            vector_store::new_index_factory_usearch(config_rx)?
        };

        let credentials = config.credentials.clone();
        let db_actor = vector_store::new_db(scylladb_uri, node_state.clone(), credentials).await?;

        let (_server_actor, addr) =
            vector_store::run(http_server_config, node_state, db_actor, index_factory).await?;
        tracing::info!("listening on {addr}");

        vector_store::wait_for_shutdown().await;

        anyhow::Ok(())
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_config_none_when_no_env_vars() {
        temp_env::with_vars_unset(
            ["VECTOR_STORE_TLS_CERT_PATH", "VECTOR_STORE_TLS_KEY_PATH"],
            || {
                let config = tls_config();
                assert!(config.is_ok());
                assert!(config.unwrap().is_none());
            },
        );
    }

    #[test]
    fn tls_config_success_when_both_set() {
        temp_env::with_vars(
            [
                ("VECTOR_STORE_TLS_CERT_PATH", Some("/path/to/cert.pem")),
                ("VECTOR_STORE_TLS_KEY_PATH", Some("/path/to/key.pem")),
            ],
            || {
                let config = tls_config().unwrap().unwrap();
                assert_eq!(config.cert_path.to_str(), Some("/path/to/cert.pem"));
                assert_eq!(config.key_path.to_str(), Some("/path/to/key.pem"));
            },
        );
    }

    #[test]
    fn tls_config_error_when_only_cert_set() {
        temp_env::with_vars(
            [
                ("VECTOR_STORE_TLS_CERT_PATH", Some("/path/to/cert.pem")),
                ("VECTOR_STORE_TLS_KEY_PATH", None),
            ],
            || {
                let result = tls_config();
                assert!(result.is_err());
            },
        );
    }

    #[test]
    fn tls_config_error_when_only_key_set() {
        temp_env::with_vars(
            [
                ("VECTOR_STORE_TLS_CERT_PATH", None),
                ("VECTOR_STORE_TLS_KEY_PATH", Some("/path/to/key.pem")),
            ],
            || {
                let result = tls_config();
                assert!(result.is_err());
            },
        );
    }
}
