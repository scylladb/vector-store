/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Config;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::Interval;
use tokio::time::MissedTickBehavior;

/// Monitors one file and detects content changes with a stable in-memory hash.
pub(crate) struct FileMonitor {
    path: Option<PathBuf>,
    last_content_hash: Option<u64>,
}

impl FileMonitor {
    pub(crate) async fn new(path: Option<PathBuf>) -> Self {
        let last_content_hash = Self::read_content_hash(path.as_deref()).await;
        Self {
            path,
            last_content_hash,
        }
    }

    pub(crate) async fn update(&mut self, path: Option<PathBuf>) {
        self.path = path;
        self.last_content_hash = Self::read_content_hash(self.path.as_deref()).await;
    }

    pub(crate) async fn has_changes(&mut self) -> bool {
        let current_hash = Self::read_content_hash(self.path.as_deref()).await;
        let changed = current_hash != self.last_content_hash;
        self.last_content_hash = current_hash;
        changed
    }

    async fn read_content_hash(path: Option<&Path>) -> Option<u64> {
        let path = path?;
        let content = tokio::fs::read(path).await.ok()?;
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        Some(hasher.finish())
    }
}

/// Tracks TLS files to detect in-place content changes on a fixed interval.
pub(crate) struct TlsFilesMonitor {
    cert: FileMonitor,
    key: FileMonitor,
    ca_cert: FileMonitor,
    check_interval: Interval,
}

impl TlsFilesMonitor {
    pub(crate) async fn new(config: &Config, check_interval: Duration) -> Self {
        let mut check_interval = tokio::time::interval(check_interval);
        check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Skip the first immediate tick so the first check happens after the interval.
        check_interval.tick().await;
        Self {
            cert: FileMonitor::new(config.tls_cert_path.clone()).await,
            key: FileMonitor::new(config.tls_key_path.clone()).await,
            ca_cert: FileMonitor::new(config.mtls_ca_cert_path.clone()).await,
            check_interval,
        }
    }

    /// Update tracked paths and content hashes from a new config.
    pub(crate) async fn update(&mut self, config: &Config) {
        self.cert.update(config.tls_cert_path.clone()).await;
        self.key.update(config.tls_key_path.clone()).await;
        self.ca_cert.update(config.mtls_ca_cert_path.clone()).await;
    }

    /// Wait for the next scheduled check tick. Cancel-safe for use in `tokio::select!`.
    pub(crate) async fn tick(&mut self) {
        self.check_interval.tick().await;
    }

    /// Check if any TLS file has changed content since the last check.
    pub(crate) async fn has_changes(&mut self) -> bool {
        // Check every file, so that a cert and key change found by one check is reported once.
        let cert_changed = self.cert.has_changes().await;
        let key_changed = self.key.has_changes().await;
        let ca_cert_changed = self.ca_cert.has_changes().await;
        cert_changed || key_changed || ca_cert_changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn tls_file_monitor_no_paths_no_changes() {
        let config = Config::default();
        let mut monitor = TlsFilesMonitor::new(&config, Duration::from_secs(30)).await;
        assert!(!monitor.has_changes().await);
    }

    #[tokio::test]
    async fn tls_file_monitor_detects_content_change() {
        let mut cert_file = NamedTempFile::new().unwrap();
        writeln!(cert_file, "old cert content").unwrap();

        let config = Config {
            tls_cert_path: Some(cert_file.path().to_path_buf()),
            ..Default::default()
        };

        let mut monitor = TlsFilesMonitor::new(&config, Duration::from_secs(30)).await;
        assert!(!monitor.has_changes().await);

        tokio::fs::write(cert_file.path(), "new cert content")
            .await
            .unwrap();

        assert!(monitor.has_changes().await);
    }

    #[tokio::test]
    async fn tls_file_monitor_update_resets_state() {
        let mut cert_file = NamedTempFile::new().unwrap();
        writeln!(cert_file, "cert content").unwrap();

        let config = Config {
            tls_cert_path: Some(cert_file.path().to_path_buf()),
            ..Default::default()
        };

        let mut monitor = TlsFilesMonitor::new(&config, Duration::from_secs(30)).await;

        tokio::fs::write(cert_file.path(), "new cert content")
            .await
            .unwrap();

        // Update should reset tracking
        monitor.update(&config).await;
        assert!(!monitor.has_changes().await);
    }

    #[tokio::test]
    async fn tls_file_monitor_reports_detected_change_once() {
        let mut cert_file = NamedTempFile::new().unwrap();
        writeln!(cert_file, "cert content").unwrap();
        let mut key_file = NamedTempFile::new().unwrap();
        writeln!(key_file, "key content").unwrap();
        let config = Config {
            tls_cert_path: Some(cert_file.path().to_path_buf()),
            tls_key_path: Some(key_file.path().to_path_buf()),
            ..Default::default()
        };
        let mut monitor = TlsFilesMonitor::new(&config, Duration::from_secs(30)).await;
        tokio::fs::write(cert_file.path(), "new cert content")
            .await
            .unwrap();
        tokio::fs::write(key_file.path(), "new key content")
            .await
            .unwrap();

        let first_check = monitor.has_changes().await;
        let second_check = monitor.has_changes().await;

        // The cert and key change is reported by the first check only.
        assert!(first_check);
        assert!(!second_check);
    }
}
