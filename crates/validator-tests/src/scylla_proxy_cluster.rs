/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::framed;
use scylla_proxy::RequestRule;
use scylla_proxy::ResponseRule;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct ScyllaProxyNodeConfig {
    pub real_addr: Ipv4Addr,
    pub proxy_addr: Ipv4Addr,
}

pub enum ScyllaProxyCluster {
    Start {
        node_configs: Vec<ScyllaProxyNodeConfig>,
        tx: oneshot::Sender<HashMap<SocketAddr, SocketAddr>>,
    },
    Stop {
        tx: oneshot::Sender<()>,
    },
    ChangeRequestRules {
        rules: Option<Vec<RequestRule>>,
        tx: oneshot::Sender<()>,
    },
    ChangeResponseRules {
        rules: Option<Vec<ResponseRule>>,
        tx: oneshot::Sender<()>,
    },
    TurnOffRules {
        tx: oneshot::Sender<()>,
    },
}

pub trait ScyllaProxyClusterExt {
    /// Starts the ScyllaDB proxy cluster with the given node configurations.
    fn start(
        &self,
        node_configs: Vec<ScyllaProxyNodeConfig>,
    ) -> impl Future<Output = HashMap<SocketAddr, SocketAddr>>;

    /// Stops the ScyllaDB proxy cluster.
    fn stop(&self) -> impl Future<Output = ()>;

    /// Change request rules within the cluster using given node configurations.
    fn change_request_rules(&self, rules: Option<Vec<RequestRule>>) -> impl Future<Output = ()>;

    /// Change response rules within the cluster using given node configurations.
    fn change_response_rules(&self, rules: Option<Vec<ResponseRule>>) -> impl Future<Output = ()>;

    /// Turn off all rules within the cluster.
    fn turn_off_rules(&self) -> impl Future<Output = ()>;
}

impl ScyllaProxyClusterExt for mpsc::Sender<ScyllaProxyCluster> {
    #[framed]
    async fn start(
        &self,
        node_configs: Vec<ScyllaProxyNodeConfig>,
    ) -> HashMap<SocketAddr, SocketAddr> {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaProxyCluster::Start { node_configs, tx })
            .await
            .expect("ScyllaProxyClusterExt::start: internal actor should receive request");
        rx.await
            .expect("ScyllaProxyClusterExt::start: internal actor should send response")
    }

    #[framed]
    async fn stop(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaProxyCluster::Stop { tx })
            .await
            .expect("ScyllaProxyClusterExt::stop: internal actor should receive request");
        rx.await
            .expect("ScyllaProxyClusterExt::stop: internal actor should send response");
    }

    #[framed]
    async fn change_request_rules(&self, rules: Option<Vec<RequestRule>>) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaProxyCluster::ChangeRequestRules { rules, tx })
            .await
            .expect("ScyllaProxyClusterExt::change_request_rules: internal actor should receive request");
        rx.await.expect(
            "ScyllaProxyClusterExt::change_request_rules: internal actor should send response",
        )
    }

    #[framed]
    async fn change_response_rules(&self, rules: Option<Vec<ResponseRule>>) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaProxyCluster::ChangeResponseRules { rules, tx })
            .await
            .expect("ScyllaProxyClusterExt::change_response_rules: internal actor should receive request");
        rx.await.expect(
            "ScyllaProxyClusterExt::change_response_rules: internal actor should send response",
        )
    }

    #[framed]
    async fn turn_off_rules(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(ScyllaProxyCluster::TurnOffRules { tx })
            .await
            .expect("ScyllaProxyClusterExt::turn_off_rules: internal actor should receive request");
        rx.await
            .expect("ScyllaProxyClusterExt::turn_off_rules: internal actor should send response")
    }
}
