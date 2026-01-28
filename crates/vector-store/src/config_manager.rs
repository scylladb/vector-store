/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use anyhow::bail;
use itertools::Itertools;
use secrecy::ExposeSecret;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use vector_store::Config;

pub struct ConfigManager {
    config_tx: watch::Sender<Arc<Config>>,
}

impl ConfigManager {
    /// Create a new ConfigManager and return both the manager and a receiver for configuration
    /// change notifications. The receiver can be cloned to share with multiple consumers.
    ///
    /// After creating the ConfigManager, call `start()` from within a Tokio runtime context
    /// to begin listening for SIGHUP signals.
    ///
    /// # Arguments
    /// * `config` - Initial configuration
    ///
    /// # Returns
    /// A tuple of (ConfigManager, receiver for configuration changes)
    pub fn new(config: Config) -> (Self, watch::Receiver<Arc<Config>>) {
        let (config_tx, config_rx) = watch::channel(Arc::new(config));
        (Self { config_tx }, config_rx)
    }

    /// Start listening for SIGHUP signals in a background task.
    /// Must be called from within a Tokio runtime context (e.g., inside vector_store::block_on).
    ///
    /// # Arguments
    /// * `env` - Function to read environment variables
    pub fn start<F>(self, env: F)
    where
        F: Fn(&'static str) -> Result<String, std::env::VarError> + Send + Sync + 'static,
    {
        tokio::spawn(async move {
            self.handle_sighup(env).await;
        });
    }

    /// Reload configuration from environment variables.
    /// This will notify all watchers of the configuration change.
    /// Also checks for changes that require a server restart and logs warnings.
    pub async fn reload_config<F>(&self, env: F) -> anyhow::Result<()>
    where
        F: Fn(&'static str) -> Result<String, std::env::VarError>,
    {
        // Get old config before reload
        let old_config = self.config_tx.borrow().clone();

        // Load new configuration
        let new_config = Arc::new(load_config(env).await?);

        // Check for restart-required changes
        self.check_restart_required_changes(&old_config, &new_config);

        // Update the config atomically - watch will notify all receivers
        self.config_tx.send(new_config)?;

        tracing::info!("Configuration reloaded successfully");
        Ok(())
    }

    /// Check for configuration changes that require a server restart and log warnings.
    fn check_restart_required_changes(&self, old_config: &Config, new_config: &Config) {
        let mut changes = Vec::new();

        // Check disable_colors
        if old_config.disable_colors != new_config.disable_colors {
            changes.push(format!(
                "Log coloring disabled: {} -> {}",
                old_config.disable_colors, new_config.disable_colors
            ));
        }

        // Check threads
        if old_config.threads != new_config.threads {
            changes.push(format!(
                "Thread count: {:?} -> {:?}",
                old_config.threads, new_config.threads
            ));
        }

        // Log all changes if any were detected
        if !changes.is_empty() {
            tracing::warn!(
                "Configuration changes detected that require server restart:\n  {}",
                changes.join("\n  ")
            );
            tracing::warn!(
                "These changes have been stored but will not take effect until the server is restarted."
            );
        }
    }

    /// Start listening for SIGHUP signals and reload configuration when received.
    /// This function runs in a loop and should be spawned in a separate task.
    /// The loop will exit when there are no more configuration receivers.
    ///
    /// Note: This is called automatically by ConfigManager::start(), so you typically
    /// don't need to call this manually.
    ///
    /// # Arguments
    /// * `env` - Function to read environment variables
    pub async fn handle_sighup<F>(self, env: F)
    where
        F: Fn(&'static str) -> Result<String, std::env::VarError>,
    {
        let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .expect("failed to install SIGHUP handler");

        // Check receiver count periodically to allow loop exit even without SIGHUP
        let mut check_interval = tokio::time::interval(Duration::from_secs(1));
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = sighup.recv() => {
                    tracing::info!("Received SIGHUP signal, reloading configuration...");

                    // Re-read the .env file to pick up any changes
                    if let Err(e) = dotenvy::from_filename_override(".env") {
                        tracing::debug!("No .env file found or error reading it: {}", e);
                    }

                    match self.reload_config(&env).await {
                        Ok(()) => {
                            tracing::info!("Configuration reloaded successfully");
                        }
                        Err(e) => {
                            tracing::error!("Failed to reload configuration: {}", e);
                        }
                    }
                }
                _ = check_interval.tick() => {
                    // Periodically check if there are any receivers left
                    if self.config_tx.receiver_count() == 0 {
                        tracing::debug!("No more configuration receivers, stopping SIGHUP handler");
                        break;
                    }
                }
            }
        }
    }
}

async fn credentials<F>(env: &F) -> anyhow::Result<Option<vector_store::Credentials>>
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

pub async fn load_config<F>(env: F) -> anyhow::Result<Config>
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

    if let Some(memory_limit) = env("VECTOR_STORE_MEMORY_LIMIT")
        .ok()
        .map(|v| v.parse())
        .transpose()?
    {
        config.memory_limit = Some(memory_limit);
    }

    if let Some(memory_usage_check_interval) = env("VECTOR_STORE_MEMORY_USAGE_CHECK_INTERVAL")
        .ok()
        .map(|v| v.parse::<humantime::Duration>())
        .transpose()?
    {
        config.memory_usage_check_interval = Some(memory_usage_check_interval.into());
    }

    if let Ok(opensearch_addr) = env("VECTOR_STORE_OPENSEARCH_URI") {
        config.opensearch_addr = Some(opensearch_addr);
    }

    config.usearch_simulator = env("VECTOR_STORE_USEARCH_SIMULATOR")
        .ok()
        .map(|v| v.split(':').map(|s| s.parse::<humantime::Duration>()).map_ok(|v| v.into()).collect::<Result<Vec<_>, _>>().map_err(|err| {
            anyhow!("Unable to parse VECTOR_STORE_USEARCH_SIMULATOR env (search_us:add_us:delete_us:...): {err}")
        })).transpose()?;

    config.credentials = credentials(&env).await?;

    // Load TLS configuration
    let tls_cert_path = env("VECTOR_STORE_TLS_CERT_PATH")
        .ok()
        .map(std::path::PathBuf::from);
    let tls_key_path = env("VECTOR_STORE_TLS_KEY_PATH")
        .ok()
        .map(std::path::PathBuf::from);

    config.cql_keepalive_interval = env("VECTOR_STORE_CQL_KEEPALIVE_INTERVAL")
        .ok()
        .map(|v| v.parse::<humantime::Duration>())
        .transpose()?
        .map(|v| v.into());

    config.cql_keepalive_timeout = env("VECTOR_STORE_CQL_KEEPALIVE_TIMEOUT")
        .ok()
        .map(|v| v.parse::<humantime::Duration>())
        .transpose()?
        .map(|v| v.into());

    config.cql_tcp_keepalive_interval = env("VECTOR_STORE_CQL_TCP_KEEPALIVE_INTERVAL")
        .ok()
        .map(|v| v.parse::<humantime::Duration>())
        .transpose()?
        .map(|v| v.into());

    config.cdc_safety_interval = env("VECTOR_STORE_CDC_SAFETY_INTERVAL")
        .ok()
        .map(|v| v.parse::<humantime::Duration>())
        .transpose()?
        .map(|v| v.into());

    config.cdc_sleep_interval = env("VECTOR_STORE_CDC_SLEEP_INTERVAL")
        .ok()
        .map(|v| v.parse::<humantime::Duration>())
        .transpose()?
        .map(|v| v.into());

    config.cql_uri_translation_map = env("VECTOR_STORE_CQL_URI_TRANSLATION_MAP")
        .ok()
        .map(|v| serde_json::from_str(&v))
        .transpose()?;

    // Validate that both cert and key are provided together, or neither
    match (&tls_cert_path, &tls_key_path) {
        (Some(_), Some(_)) | (None, None) => {
            config.tls_cert_path = tls_cert_path;
            config.tls_key_path = tls_key_path;
        }
        _ => {
            bail!(
                "Both VECTOR_STORE_TLS_CERT_PATH and VECTOR_STORE_TLS_KEY_PATH must be set together"
            )
        }
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;
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

        let creds = credentials(&env).await.unwrap();

        assert!(creds.is_none());
    }

    #[tokio::test]
    async fn credentials_error_when_username_empty() {
        let file = pass_file(PASSWORD);
        let env = mock_env(HashMap::from([
            ("VECTOR_STORE_SCYLLADB_USERNAME", "".into()),
            ("VECTOR_STORE_SCYLLADB_PASSWORD_FILE", path(&file)),
        ]));

        let result = credentials(&env).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn credentials_error_when_no_password_file_env() {
        let env = mock_env(HashMap::from([(
            "VECTOR_STORE_SCYLLADB_USERNAME",
            USERNAME.into(),
        )]));

        let result = credentials(&env).await;

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

        let result = credentials(&env).await;

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

        let creds = credentials(&env).await.unwrap().unwrap();

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

        let creds = credentials(&env).await.unwrap().unwrap();

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

        let creds = credentials(&env).await.unwrap().unwrap();

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

        let creds = credentials(&env).await.unwrap().unwrap();

        assert_eq!(creds.username, Some(USERNAME.to_string()));
        assert_eq!(creds.password.as_ref().unwrap().expose_secret(), PASSWORD);
        assert_eq!(
            creds.certificate_path,
            Some(std::path::PathBuf::from("/path/to/cert.pem"))
        );
    }

    #[tokio::test]
    async fn config_manager_reload_notifies_watchers() {
        let initial_config = Config {
            vector_store_addr: "127.0.0.1:6080".parse().unwrap(),
            scylladb_uri: "127.0.0.1:9042".to_string(),
            threads: None,
            memory_limit: None,
            memory_usage_check_interval: None,
            opensearch_addr: None,
            credentials: None,
            usearch_simulator: None,
            disable_colors: false,
            tls_cert_path: None,
            tls_key_path: None,
            cql_keepalive_interval: None,
            cql_keepalive_timeout: None,
            cql_tcp_keepalive_interval: None,
            cql_uri_translation_map: None,
            cdc_safety_interval: None,
            cdc_sleep_interval: None,
        };

        let (config_manager, mut config_rx) = ConfigManager::new(initial_config);

        // Get initial config
        let initial = config_rx.borrow().clone();
        assert_eq!(initial.vector_store_addr.to_string(), "127.0.0.1:6080");
        assert_eq!(initial.scylladb_uri, "127.0.0.1:9042");

        // Reload config with different environment values
        let env = mock_env(HashMap::from([
            ("VECTOR_STORE_URI", "192.168.1.100:7070".into()),
            ("VECTOR_STORE_SCYLLADB_URI", "192.168.1.200:9043".into()),
        ]));
        config_manager.reload_config(env).await.unwrap();

        // Wait for change notification
        config_rx.changed().await.unwrap();

        // Verify new config values
        let updated = config_rx.borrow();
        assert_eq!(updated.vector_store_addr.to_string(), "192.168.1.100:7070");
        assert_eq!(updated.scylladb_uri, "192.168.1.200:9043");
    }

    #[tokio::test]
    async fn config_manager_multiple_watchers() {
        let initial_config = Config::default();
        let (config_manager, config_rx) = ConfigManager::new(initial_config);

        // Clone receivers for multiple watchers
        let mut rx1 = config_rx.clone();
        let mut rx2 = config_rx.clone();
        let mut rx3 = config_rx;

        // Spawn tasks to wait for changes
        let task1 = tokio::spawn(async move {
            rx1.changed().await.unwrap();
            rx1.borrow().vector_store_addr.to_string()
        });
        let task2 = tokio::spawn(async move {
            rx2.changed().await.unwrap();
            rx2.borrow().vector_store_addr.to_string()
        });
        let task3 = tokio::spawn(async move {
            rx3.changed().await.unwrap();
            rx3.borrow().vector_store_addr.to_string()
        });

        // Give tasks time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Reload config
        let env = mock_env(HashMap::from([(
            "VECTOR_STORE_URI",
            "192.168.1.100:8080".into(),
        )]));
        config_manager.reload_config(env).await.unwrap();

        // Verify all watchers received the update
        let addr1 = task1.await.unwrap();
        let addr2 = task2.await.unwrap();
        let addr3 = task3.await.unwrap();

        assert_eq!(addr1, "192.168.1.100:8080");
        assert_eq!(addr2, "192.168.1.100:8080");
        assert_eq!(addr3, "192.168.1.100:8080");
    }

    #[tokio::test]
    async fn load_config_memory_limit() {
        let env = mock_env(HashMap::new());
        let config = load_config(env).await.unwrap();
        assert_eq!(config.memory_limit, None);

        let env = mock_env(HashMap::from([(
            "VECTOR_STORE_MEMORY_LIMIT",
            "104857600".into(),
        )]));
        let config = load_config(env).await.unwrap();
        assert_eq!(config.memory_limit, Some(104857600));
    }

    #[tokio::test]
    async fn load_config_memory_usage_check_interval() {
        let env = mock_env(HashMap::new());
        let config = load_config(env).await.unwrap();
        assert_eq!(config.memory_usage_check_interval, None);

        let env = mock_env(HashMap::from([(
            "VECTOR_STORE_MEMORY_USAGE_CHECK_INTERVAL",
            "100ms".into(),
        )]));
        let config = load_config(env).await.unwrap();
        assert_eq!(
            config.memory_usage_check_interval,
            Some(Duration::from_millis(100))
        );
    }
}
