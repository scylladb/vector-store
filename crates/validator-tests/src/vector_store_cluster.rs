/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
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
    StartWithAuth {
        node_configs: Vec<VectorStoreNodeConfig>,
        user: String,
        password: String,
    },
    StartNode {
        node_config: VectorStoreNodeConfig,
    },
    StartNodeWithAuth {
        node_config: VectorStoreNodeConfig,
        user: String,
        password: String,
    },
    Stop {
        tx: oneshot::Sender<()>,
    },
    StopNode {
        vs_ip: Ipv4Addr,
        tx: oneshot::Sender<()>,
    },
    WaitForReady {
        tx: oneshot::Sender<bool>,
    },
}

pub trait VectorStoreClusterExt {
    /// Returns the version of the vector-store binary.
    fn version(&self) -> impl Future<Output = String>;

    /// Starts the vector-store cluster with the given node configurations.
    fn start(&self, node_configs: Vec<VectorStoreNodeConfig>) -> impl Future<Output = ()>;

    /// Starts the vector-store cluster with the given node configurations and authentication.
    fn start_with_auth(
        &self,
        node_configs: Vec<VectorStoreNodeConfig>,
        user: String,
        password: String,
    ) -> impl Future<Output = ()>;

    /// Starts a single vector-store instance.
    fn start_node(&self, node_config: VectorStoreNodeConfig) -> impl Future<Output = ()>;

    /// Starts a single vector-store instance with authentication.
    fn start_node_with_auth(
        &self,
        node_config: VectorStoreNodeConfig,
        user: String,
        password: String,
    ) -> impl Future<Output = ()>;

    /// Stops the vector-store cluster.
    fn stop(&self) -> impl Future<Output = ()>;

    /// Stops a single vector-store instance.
    fn stop_node(&self, vs_ip: Ipv4Addr) -> impl Future<Output = ()>;

    /// Waits for the vector-store cluster to be ready.
    fn wait_for_ready(&self) -> impl Future<Output = bool>;

    /// Restarts a single vector-store instance.
    fn restart(&self, node_config: &VectorStoreNodeConfig) -> impl Future<Output = ()>;
}

impl VectorStoreClusterExt for mpsc::Sender<VectorStoreCluster> {
    #[framed]
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::Version { tx })
            .await
            .expect("VectorStoreClusterExt::version: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::version: internal actor should send response")
    }

    #[framed]
    async fn start(&self, node_configs: Vec<VectorStoreNodeConfig>) {
        self.send(VectorStoreCluster::Start { node_configs })
            .await
            .expect("VectorStoreClusterExt::start: internal actor should receive request");
    }

    #[framed]
    async fn start_with_auth(
        &self,
        node_configs: Vec<VectorStoreNodeConfig>,
        user: String,
        password: String,
    ) {
        self.send(VectorStoreCluster::StartWithAuth {
            node_configs,
            user,
            password,
        })
        .await
        .expect("VectorStoreClusterExt::start_with_auth: internal actor should receive request");
    }

    #[framed]
    async fn start_node(&self, node_config: VectorStoreNodeConfig) {
        self.send(VectorStoreCluster::StartNode { node_config })
            .await
            .expect("VectorStoreClusterExt::start_node: internal actor should receive request");
    }

    #[framed]
    async fn start_node_with_auth(
        &self,
        node_config: VectorStoreNodeConfig,
        user: String,
        password: String,
    ) {
        self.send(VectorStoreCluster::StartNodeWithAuth {
            node_config,
            user,
            password,
        })
        .await
        .expect(
            "VectorStoreClusterExt::start_node_with_auth: internal actor should receive request",
        );
    }

    #[framed]
    async fn stop(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::Stop { tx })
            .await
            .expect("VectorStoreClusterExt::stop: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::stop: internal actor should send response");
    }

    #[framed]
    async fn stop_node(&self, vs_ip: Ipv4Addr) {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::StopNode { vs_ip, tx })
            .await
            .expect("VectorStoreClusterExt::stop_node: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::stop_node: internal actor should send response");
    }

    #[framed]
    async fn wait_for_ready(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.send(VectorStoreCluster::WaitForReady { tx })
            .await
            .expect("VectorStoreClusterExt::wait_for_ready: internal actor should receive request");
        rx.await
            .expect("VectorStoreClusterExt::wait_for_ready: internal actor should send response")
    }

    #[framed]
    async fn restart(&self, node_config: &VectorStoreNodeConfig) {
        self.stop_node(node_config.vs_ip).await;
        self.start_node(node_config.clone()).await;
        assert!(self.wait_for_ready().await);
    }
}
