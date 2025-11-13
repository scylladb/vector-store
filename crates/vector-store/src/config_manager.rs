/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use anyhow::bail;
use secrecy::ExposeSecret;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::RwLock;
use vector_store::Config;

pub struct ConfigManager {
    config: Arc<RwLock<Arc<Config>>>,
}

impl ConfigManager {
    pub fn new(config: Config) -> Self {
        Self {
            config: Arc::new(RwLock::new(Arc::new(config))),
        }
    }

    /// Get a read-only reference to the current configuration.
    /// Returns an Arc<Config> which can be freely cloned and shared,
    /// but cannot be used to modify the configuration.
    pub fn config(&self) -> Arc<Config> {
        Arc::clone(&self.config.read().unwrap())
    }
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

    if let Ok(opensearch_addr) = env("VECTOR_STORE_OPENSEARCH_URI") {
        config.opensearch_addr = Some(opensearch_addr);
    }

    config.credentials = credentials(env).await?;

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
}
