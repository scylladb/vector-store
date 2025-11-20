/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::net::Ipv4Addr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub enum ScyllaCluster {
    Version {
        tx: oneshot::Sender<String>,
    },
    Start {
        vs_uri: String,
        db_ips: Vec<Ipv4Addr>,
        conf: Option<Vec<u8>>,
    },
    WaitForReady {
        tx: oneshot::Sender<bool>,
    },
    Stop,
    Up {
        vs_uri: String,
        conf: Option<Vec<u8>>,
    },
    Down,
}

pub trait ScyllaClusterExt {
    /// Returns the version of the ScyllaDB executable.
    fn version(&self) -> impl Future<Output = String>;

    /// Starts the ScyllaDB cluster with the given vector store URI and database IPs.
    fn start(
        &self,
        vs_uri: String,
        db_ips: Vec<Ipv4Addr>,
        conf: Option<Vec<u8>>,
    ) -> impl Future<Output = ()>;

    /// Stops the ScyllaDB instance.
    fn stop(&self) -> impl Future<Output = ()>;

    /// Waits for the ScyllaDB cluster to be ready.
    fn wait_for_ready(&self) -> impl Future<Output = bool>;

    /// Starts a paused instance back again.
    fn up(&self, vs_uri: String, conf: Option<Vec<u8>>) -> impl Future<Output = ()>;

    /// Pauses an instance
    fn down(&self) -> impl Future<Output = ()>;
}

impl ScyllaClusterExt for mpsc::Sender<ScyllaCluster> {
    async fn version(&self) -> String {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::Version { tx })
            .await
            .expect("ScyllaClusterExt::version: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::version: internal actor should send response")
    }

    async fn start(&self, vs_uri: String, db_ips: Vec<Ipv4Addr>, conf: Option<Vec<u8>>) {
        self.send(ScyllaCluster::Start {
            vs_uri,
            db_ips,
            conf,
        })
        .await
        .expect("ScyllaClusterExt::start: internal actor should receive request");
    }

    async fn stop(&self) {
        self.send(ScyllaCluster::Stop)
            .await
            .expect("ScyllaClusterExt::stop: internal actor should receive request");
    }

    async fn wait_for_ready(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaCluster::WaitForReady { tx })
            .await
            .expect("ScyllaClusterExt::wait_for_ready: internal actor should receive request");
        rx.await
            .expect("ScyllaClusterExt::wait_for_ready: internal actor should send response")
    }

    async fn up(&self, vs_uri: String, conf: Option<Vec<u8>>) {
        self.send(ScyllaCluster::Up { vs_uri, conf })
            .await
            .expect("ScyllaClusterExt::up: internal actor should receive request")
    }

    async fn down(&self) {
        self.send(ScyllaCluster::Down)
            .await
            .expect("ScyllaClusterExt::down: internal actor should receive request")
    }
}
