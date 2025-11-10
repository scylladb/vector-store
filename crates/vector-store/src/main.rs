/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use anyhow::bail;
use clap::Parser;
use secrecy::ExposeSecret;
use std::net::ToSocketAddrs;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use vector_store::Config;
mod info;

#[derive(Parser)]
#[clap(version)]
struct Args {}

fn dotenvy_to_std_var(key: &'static str) -> Result<String, std::env::VarError> {
    dotenvy::var(key).map_err(|_| std::env::VarError::NotPresent)
}

async fn credentials<F>(env: F) -> anyhow::Result<Option<vector_store::Credentials>>
where
    F: Fn(&'static str) -> Result<String, std::env::VarError>,
{
    const USERNAME_ENV: &str = "VECTOR_STORE_SCYLLADB_USERNAME";
    const PASS_FILE_ENV: &str = "VECTOR_STORE_SCYLLADB_PASSWORD_FILE";
    const CERT_FILE_ENV: &str = "VECTOR_STORE_SCYLLADB_CERTIFICATE_FILE";
    // Check for certificate file
    let certificate_path = match env(CERT_FILE_ENV) {
        Ok(val) => {
            tracing::debug!("{} = {:?}", CERT_FILE_ENV, val);
            Some(std::path::PathBuf::from(val))
        }
        Err(_) => None,
    };

    // Check for username/password authentication
    let username = match env(USERNAME_ENV) {
        Ok(val) => {
            tracing::debug!("{} = {}", USERNAME_ENV, val);
            Some(val)
        }
        Err(_) => None,
    };

    // If neither certificate nor username is provided, return None
    if certificate_path.is_none() && username.is_none() {
        tracing::debug!(
            "No credentials or certificate configured, connecting without authentication"
        );
        return Ok(None);
    }

    // Handle username/password if username is provided
    let (username, password) = if let Some(username) = username {
        if username.is_empty() {
            bail!("credentials: {USERNAME_ENV} must not be empty");
        }

        let Ok(password_file) = env(PASS_FILE_ENV) else {
            bail!("credentials: {PASS_FILE_ENV} env required when {USERNAME_ENV} is set");
        };

        let password = secrecy::SecretString::new(
            tokio::fs::read_to_string(&password_file)
                .await
                .map_err(|e| anyhow!("credentials: failed to read password file: {e}"))?
                .into(),
        );

        (
            Some(username),
            Some(secrecy::SecretString::new(
                password.expose_secret().trim().into(),
            )),
        )
    } else {
        tracing::info!("No username/password configured, using certificate-only authentication");
        (None, None)
    };
    Ok(Some(vector_store::Credentials {
        username,
        password,
        certificate_path,
    }))
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
async fn load_config<F>(env: F) -> anyhow::Result<Config>
where
    F: Fn(&'static str) -> Result<String, std::env::VarError>,
{
    let mut config = Config::default();

    if let Some(disable_colors) = env("VECTOR_STORE_DISABLE_COLORS")
        .ok()
        .map(|v| {
            v.trim().parse().or(Err(anyhow!(
                "Unable to parse VECTOR_STORE_DISABLE_COLORS env (true/false)"
            )))
        })
        .transpose()?
    {
        config.disable_colors = disable_colors;
    }

    if let Some(vector_store_addr) = env("VECTOR_STORE_URI")
        .ok()
        .map(|v| {
            v.to_socket_addrs()
                .map_err(|_| anyhow!("Unable to parse VECTOR_STORE_URI env (host:port)"))?
                .next()
                .ok_or(anyhow!("Unable to parse VECTOR_STORE_URI env (host:port)"))
        })
        .transpose()?
    {
        config.vector_store_addr = vector_store_addr;
    }

    if let Ok(scylladb_uri) = env("VECTOR_STORE_SCYLLADB_URI") {
        config.scylladb_uri = scylladb_uri;
    }

    if let Some(threads) = env("VECTOR_STORE_THREADS")
        .ok()
        .map(|v| v.parse())
        .transpose()?
    {
        config.threads = Some(threads);
    }

    if let Ok(opensearch_addr) = env("VECTOR_STORE_OPENSEARCH_URI") {
        config.opensearch_addr = Some(opensearch_addr);
    }

    config.credentials = credentials(env).await?;

    Ok(config)
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

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install ring crypto provider");

    _ = dotenvy::dotenv();

    // Load configuration early to get disable_colors for logging setup
    let config_future = load_config(dotenvy_to_std_var);
    let config = tokio::runtime::Builder::new_current_thread()
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

    let vector_store_addr = config.vector_store_addr;

    let http_server_config = vector_store::HttpServerConfig {
        addr: vector_store_addr,
        tls: tls_config()?,
    };
    let scylladb_uri = config.scylladb_uri.into();

    let threads = config.threads;

    vector_store::block_on(threads, async move || {
        let node_state = vector_store::new_node_state().await;

        let opensearch_addr = config.opensearch_addr;

        let index_factory = if let Some(addr) = opensearch_addr {
            tracing::info!("Using OpenSearch index factory at {addr}");
            vector_store::new_index_factory_opensearch(addr)?
        } else {
            tracing::info!("Using Usearch index factory");
            vector_store::new_index_factory_usearch()?
        };

        let db_actor =
            vector_store::new_db(scylladb_uri, node_state.clone(), config.credentials).await?;

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
    use std::collections::HashMap;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const USERNAME: &str = "test_user";
    const PASSWORD: &str = "test_pass";

    fn mock_env(
        vars: HashMap<&'static str, String>,
    ) -> impl Fn(&'static str) -> Result<String, std::env::VarError> {
        move |key| vars.get(key).cloned().ok_or(std::env::VarError::NotPresent)
    }

    fn pass_file(pass: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{pass}").unwrap();
        file
    }

    fn path(file: &NamedTempFile) -> String {
        file.path().to_str().unwrap().into()
    }

    #[tokio::test]
    async fn credentials_none_when_no_username() {
        let env = mock_env(HashMap::new());

        let creds = credentials(env).await.unwrap();

        assert!(creds.is_none());
    }

    #[tokio::test]
    async fn credentials_error_when_username_empty() {
        let file = pass_file(PASSWORD);
        let env = mock_env(HashMap::from([
            ("VECTOR_STORE_SCYLLADB_USERNAME", "".into()),
            ("VECTOR_STORE_SCYLLADB_PASSWORD_FILE", path(&file)),
        ]));

        let result = credentials(env).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn credentials_error_when_no_password_file_env() {
        let env = mock_env(HashMap::from([(
            "VECTOR_STORE_SCYLLADB_USERNAME",
            USERNAME.into(),
        )]));

        let result = credentials(env).await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "credentials: VECTOR_STORE_SCYLLADB_PASSWORD_FILE env required when VECTOR_STORE_SCYLLADB_USERNAME is set"
        );
    }

    #[tokio::test]
    async fn credentials_error_when_password_file_not_found() {
        let env = mock_env(HashMap::from([
            ("VECTOR_STORE_SCYLLADB_USERNAME", USERNAME.into()),
            (
                "VECTOR_STORE_SCYLLADB_PASSWORD_FILE",
                "/no/such/file/exists".into(),
            ),
        ]));

        let result = credentials(env).await;

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            !err.contains("/no/such/file/exists"),
            "error message should not leak filename: {err}"
        );
    }

    #[tokio::test]
    async fn credentials_success() {
        let file = pass_file(PASSWORD);
        let env = mock_env(HashMap::from([
            ("VECTOR_STORE_SCYLLADB_USERNAME", USERNAME.into()),
            ("VECTOR_STORE_SCYLLADB_PASSWORD_FILE", path(&file)),
        ]));

        let creds = credentials(env).await.unwrap().unwrap();

        assert_eq!(creds.username, Some(USERNAME.to_string()));
        assert_eq!(creds.password.as_ref().unwrap().expose_secret(), PASSWORD);
        assert_eq!(creds.certificate_path, None);
    }

    #[tokio::test]
    async fn credentials_success_with_trimmed_password() {
        let file = pass_file("  \n my_trimmed_pass \t\n");
        let env = mock_env(HashMap::from([
            ("VECTOR_STORE_SCYLLADB_USERNAME", USERNAME.into()),
            ("VECTOR_STORE_SCYLLADB_PASSWORD_FILE", path(&file)),
        ]));

        let creds = credentials(env).await.unwrap().unwrap();

        assert_eq!(creds.username, Some(USERNAME.to_string()));
        assert_eq!(
            creds.password.as_ref().unwrap().expose_secret(),
            "my_trimmed_pass"
        );
    }

    #[tokio::test]
    async fn credentials_with_certificate_only() {
        let env = mock_env(HashMap::from([(
            "VECTOR_STORE_SCYLLADB_CERTIFICATE_FILE",
            "/path/to/cert.pem".into(),
        )]));

        let creds = credentials(env).await.unwrap().unwrap();

        assert_eq!(creds.username, None);
        assert!(creds.password.is_none());
        assert_eq!(
            creds.certificate_path,
            Some(std::path::PathBuf::from("/path/to/cert.pem"))
        );
    }

    #[tokio::test]
    async fn credentials_with_both_username_and_certificate() {
        let file = pass_file(PASSWORD);
        let env = mock_env(HashMap::from([
            ("VECTOR_STORE_SCYLLADB_USERNAME", USERNAME.into()),
            ("VECTOR_STORE_SCYLLADB_PASSWORD_FILE", path(&file)),
            (
                "VECTOR_STORE_SCYLLADB_CERTIFICATE_FILE",
                "/path/to/cert.pem".into(),
            ),
        ]));

        let creds = credentials(env).await.unwrap().unwrap();

        assert_eq!(creds.username, Some(USERNAME.to_string()));
        assert_eq!(creds.password.as_ref().unwrap().expose_secret(), PASSWORD);
        assert_eq!(
            creds.certificate_path,
            Some(std::path::PathBuf::from("/path/to/cert.pem"))
        );
    }

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
