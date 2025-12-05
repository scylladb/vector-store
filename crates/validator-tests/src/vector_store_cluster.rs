/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Configuration for a single Vector Store node in the test cluster.
#[derive(Clone)]
pub struct VectorStoreNodeConfig {
    /// The IP address of this Vector Store node.
    pub vs_ip: Ipv4Addr,
    /// The IP address of the ScyllaDB node to connect to.
    pub db_ip: Ipv4Addr,
    /// Additional environment variables to pass to the Vector Store process.
    pub envs: HashMap<String, String>,
}

impl VectorStoreNodeConfig {
    pub fn vs_addr(&self) -> SocketAddr {
        (self.vs_ip, crate::common::VS_PORT).into()
    }

    pub fn db_addr(&self) -> SocketAddr {
        (self.db_ip, crate::common::DB_PORT).into()
    }
}

pub enum VectorStoreCluster {
    Version {
        tx: oneshot::Sender<String>,
    },
    Start {
        node_configs: Vec<VectorStoreNodeConfig>,
    },
    Stop {
        tx: oneshot::Sender<()>,
    },
    WaitForReady {
        tx: oneshot::Sender<bool>,
    },
    Up {
        node_configs: Vec<VectorStoreNodeConfig>,
    },
    UpNode {
        node_config: VectorStoreNodeConfig,
    },
    Down {
        tx: oneshot::Sender<()>,
    },
    DownNode {
        vs_ip: Ipv4Addr,
        tx: oneshot::Sender<()>,
    },
}

pub trait VectorStoreClusterExt {
    /// Returns the version of the vector-store binary.
    fn version(&self) -> impl Future<Output = String>;

    /// Starts the vector-store cluster with the given node configurations.
    fn start(&self, node_configs: Vec<VectorStoreNodeConfig>) -> impl Future<Output = ()>;

    /// Stops the vector-store cluster.
    fn stop(&self) -> impl Future<Output = ()>;

    /// Waits for the vector-store cluster to be ready.
    fn wait_for_ready(&self) -> impl Future<Output = bool>;

    /// Starts a paused cluster back again.
    fn up(&self, node_configs: Vec<VectorStoreNodeConfig>) -> impl Future<Output = ()>;

    /// Pauses a cluster.
    fn down(&self) -> impl Future<Output = ()>;

    /// Starts a single paused Vector Store instance back again.
    fn up_node(&self, node_config: VectorStoreNodeConfig) -> impl Future<Output = ()>;

    /// Pauses a single Vector Store instance.
    fn down_node(&self, vs_ip: Ipv4Addr) -> impl Future<Output = ()>;

    /// Restarts a single Vector Store instance.
    fn restart(&self, node_config: &VectorStoreNodeConfig) -> impl Future<Output = ()>;
}

impl VectorStoreClusterExt for mpsc::Sender<VectorStoreCluster> {
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::Version { tx })
            .await
            .expect("VectorStoreClusterExt::version: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::version: internal actor should send response")
    }

    async fn start(&self, node_configs: Vec<VectorStoreNodeConfig>) {
        self.send(VectorStoreCluster::Start { node_configs })
            .await
            .expect("VectorStoreClusterExt::start: internal actor should receive request");
    }

    async fn stop(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::Stop { tx })
            .await
            .expect("VectorStoreClusterExt::stop: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::stop: internal actor should send response");
    }

    async fn wait_for_ready(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::WaitForReady { tx })
            .await
            .expect("VectorStoreClusterExt::wait_for_ready: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::wait_for_ready: internal actor should send response")
    }

    async fn up(&self, node_configs: Vec<VectorStoreNodeConfig>) {
        self.send(VectorStoreCluster::Up { node_configs })
            .await
            .expect("VectorStoreClusterExt::up: internal actor should receive request");
    }

    async fn up_node(&self, node_config: VectorStoreNodeConfig) {
        self.send(VectorStoreCluster::UpNode { node_config })
            .await
            .expect("VectorStoreClusterExt::up_node: internal actor should receive request");
    }

    async fn down(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::Down { tx })
            .await
            .expect("VectorStoreClusterExt::down: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::down: internal actor should send response");
    }

    async fn down_node(&self, vs_ip: Ipv4Addr) {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::DownNode { vs_ip, tx })
            .await
            .expect("VectorStoreClusterExt::down_node: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::down_node: internal actor should send response");
    }

    async fn restart(&self, node_config: &VectorStoreNodeConfig) {
        self.down_node(node_config.vs_ip).await;
        self.up_node(node_config.clone()).await;
        assert!(self.wait_for_ready().await);
    }
}
