/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use std::net::Ipv4Addr;
use std::sync::LazyLock;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

static DEFAULT_SCYLLA_ARGS: LazyLock<RwLock<Vec<String>>> = LazyLock::new(|| {
    RwLock::new(
        [
            "--overprovisioned",
            "--developer-mode=true",
            "--smp=2",
            "--memory=1G",
            "--unsafe-bypass-fsync=on",
            "--kernel-page-cache=on",
            "--reactor-backend=io_uring",
            "--rf-rack-valid-keyspaces=true",
            "--collectd=0",
            "--max-networking-io-control-blocks=1000",
            "--commitlog-use-o-dsync=0",
            "--flush-schema-tables-after-modification=false",
            "--auto-snapshot=0",
            "--logger-log-level=compaction=warn",
            "--logger-log-level=migration_manager=warn",
            "--shutdown-announce-in-ms=0",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
    )
});

/// Returns the default ScyllaDB arguments used when starting a scylla instance
pub fn default_scylla_args() -> Vec<String> {
    DEFAULT_SCYLLA_ARGS
        .read()
        .expect("failed to acquire read lock on DEFAULT_SCYLLA_ARGS")
        .clone()
}

/// Sets the default ScyllaDB arguments used when starting a scylla instance. It changes
/// the default arguments for all validator's tests. It is needed to be able to customize
/// default arguments for all tests from scylladb.git repository without touching code
/// in the vector-store.git.
pub fn set_default_scylla_args(args: Vec<String>) {
    *DEFAULT_SCYLLA_ARGS
        .write()
        .expect("failed to acquire write lock on DEFAULT_SCYLLA_ARGS") = args;
}

/// Configuration for a single ScyllaDB node in the test cluster.
#[derive(Clone)]
pub struct ScyllaNodeConfig {
    /// The IP address of this ScyllaDB node.
    pub db_ip: Ipv4Addr,
    /// Primary Vector Store URIs (--vector-store-primary-uri).
    pub primary_vs_uris: Vec<String>,
    /// Secondary Vector Store URIs (--vector-store-secondary-uri).
    pub secondary_vs_uris: Vec<String>,
    /// Additional args to pass to the ScyllaDB process.
    pub args: Vec<String>,
}

pub enum ScyllaCluster {
    Version {
        tx: oneshot::Sender<String>,
    },
    Start {
        node_configs: Vec<ScyllaNodeConfig>,
        conf: Option<Vec<u8>>,
    },
    WaitForReady {
        tx: oneshot::Sender<bool>,
    },
    Stop {
        tx: oneshot::Sender<()>,
    },
    Up {
        node_configs: Vec<ScyllaNodeConfig>,
        conf: Option<Vec<u8>>,
    },
    UpNode {
        node_config: ScyllaNodeConfig,
        conf: Option<Vec<u8>>,
    },
    Down {
        tx: oneshot::Sender<()>,
    },
    DownNode {
        db_ip: Ipv4Addr,
        tx: oneshot::Sender<()>,
    },
    Flush {
        tx: oneshot::Sender<()>,
    },
}

pub trait ScyllaClusterExt {
    /// Returns the version of the ScyllaDB executable.
    fn version(&self) -> impl Future<Output = String>;

    /// Starts the ScyllaDB cluster with the given node configurations.
    fn start(
        &self,
        node_configs: Vec<ScyllaNodeConfig>,
        conf: Option<Vec<u8>>,
    ) -> impl Future<Output = ()>;

    /// Starts the ScyllaDB cluster with the given vector store URI and database IPs and authorization/authentication enabled.
    fn start_with_auth(
        &self,
        node_configs: Vec<ScyllaNodeConfig>,
        conf: Option<Vec<u8>>,
    ) -> impl Future<Output = ()>;

    /// Stops the ScyllaDB cluster.
    fn stop(&self) -> impl Future<Output = ()>;

    /// Waits for the ScyllaDB cluster to be ready.
    fn wait_for_ready(&self) -> impl Future<Output = bool>;

    /// Starts a paused cluster back again.
    fn up(
        &self,
        node_configs: Vec<ScyllaNodeConfig>,
        conf: Option<Vec<u8>>,
    ) -> impl Future<Output = ()>;

    /// Starts a paused cluster back again with authorization/authentication enabled.
    fn up_with_auth(
        &self,
        node_configs: Vec<ScyllaNodeConfig>,
        conf: Option<Vec<u8>>,
    ) -> impl Future<Output = ()>;

    /// Pauses a cluster.
    fn down(&self) -> impl Future<Output = ()>;

    /// Starts a single paused ScyllaDB instance back again.
    fn up_node(
        &self,
        node_config: ScyllaNodeConfig,
        conf: Option<Vec<u8>>,
    ) -> impl Future<Output = ()>;

    /// Starts a single paused ScyllaDB instance back again with authorization/authentication enabled.
    fn up_node_with_auth(
        &self,
        node_config: ScyllaNodeConfig,
        conf: Option<Vec<u8>>,
    ) -> impl Future<Output = ()>;

    /// Pauses a single ScyllaDB instance.
    fn down_node(&self, db_ip: Ipv4Addr) -> impl Future<Output = ()>;

    /// Restarts a single ScyllaDB instance.
    fn restart(&self, node_config: &ScyllaNodeConfig) -> impl Future<Output = ()>;

    /// Flushes all memtables to disk on all nodes.
    fn flush(&self) -> impl Future<Output = ()>;
}

impl ScyllaClusterExt for mpsc::Sender<ScyllaCluster> {
    #[framed]
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Version { tx })
            .await
            .expect("ScyllaClusterExt::version: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::version: internal actor should send response")
    }

    #[framed]
    async fn start(&self, node_configs: Vec<ScyllaNodeConfig>, conf: Option<Vec<u8>>) {
        self.send(ScyllaCluster::Start { node_configs, conf })
            .await
            .expect("ScyllaClusterExt::start: internal actor should receive request");
    }

    #[framed]
    async fn start_with_auth(&self, node_configs: Vec<ScyllaNodeConfig>, conf: Option<Vec<u8>>) {
        let mut actual_conf =
            "authenticator: PasswordAuthenticator\nauthorizer: CassandraAuthorizer\ntablets_mode_for_new_keyspaces: enabled"
                .as_bytes()
                .to_vec();
        if let Some(c) = conf {
            actual_conf.extend_from_slice(&c);
        }
        self.start(node_configs, Some(actual_conf)).await;
    }

    #[framed]
    async fn stop(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Stop { tx })
            .await
            .expect("ScyllaClusterExt::stop: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::stop: internal actor should send response");
    }

    #[framed]
    async fn wait_for_ready(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::WaitForReady { tx })
            .await
            .expect("ScyllaClusterExt::wait_for_ready: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::wait_for_ready: internal actor should send response")
    }

    #[framed]
    async fn up(&self, node_configs: Vec<ScyllaNodeConfig>, conf: Option<Vec<u8>>) {
        self.send(ScyllaCluster::Up { node_configs, conf })
            .await
            .expect("ScyllaClusterExt::up: internal actor should receive request")
    }

    #[framed]
    async fn up_with_auth(&self, node_configs: Vec<ScyllaNodeConfig>, conf: Option<Vec<u8>>) {
        let mut actual_conf =
            "authenticator: PasswordAuthenticator\nauthorizer: CassandraAuthorizer\ntablets_mode_for_new_keyspaces: enabled"
                .as_bytes()
                .to_vec();
        if let Some(c) = conf {
            actual_conf.extend_from_slice(&c);
        }
        self.up(node_configs, Some(actual_conf)).await;
    }

    #[framed]
    async fn up_node(&self, node_config: ScyllaNodeConfig, conf: Option<Vec<u8>>) {
        self.send(ScyllaCluster::UpNode { node_config, conf })
            .await
            .expect("ScyllaClusterExt::up_node: internal actor should receive request")
    }

    #[framed]
    async fn up_node_with_auth(&self, node_config: ScyllaNodeConfig, conf: Option<Vec<u8>>) {
        let mut actual_conf =
            "authenticator: PasswordAuthenticator\nauthorizer: CassandraAuthorizer\ntablets_mode_for_new_keyspaces: enabled"
                .as_bytes()
                .to_vec();
        if let Some(c) = conf {
            actual_conf.extend_from_slice(&c);
        }
        self.up_node(node_config, Some(actual_conf)).await;
    }

    #[framed]
    async fn down(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Down { tx })
            .await
            .expect("ScyllaClusterExt::down: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::down: internal actor should send response");
    }

    #[framed]
    async fn down_node(&self, db_ip: Ipv4Addr) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::DownNode { db_ip, tx })
            .await
            .expect("ScyllaClusterExt::down_node: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::down_node: internal actor should send response");
    }

    #[framed]
    async fn restart(&self, node_config: &ScyllaNodeConfig) {
        self.down_node(node_config.db_ip).await;
        self.up_node(node_config.clone(), None).await;
        assert!(self.wait_for_ready().await);
    }

    #[framed]
    async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Flush { tx })
            .await
            .expect("ScyllaClusterExt::flush: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::flush: internal actor should send response");
    }
}
