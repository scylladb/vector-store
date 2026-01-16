/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::frame;
use async_backtrace::framed;
use scylla_proxy::Node;
use scylla_proxy::Proxy;
use scylla_proxy::RequestRule;
use scylla_proxy::ResponseRule;
use scylla_proxy::RunningProxy;
use tokio::sync::mpsc;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::info;
use vector_search_validator_tests::ScyllaProxyCluster;
use vector_search_validator_tests::ScyllaProxyNodeConfig;

const DEFAULT_SCYLLA_CQL_PORT: u16 = 9042;

#[framed]
pub(crate) async fn new() -> mpsc::Sender<ScyllaProxyCluster> {
    let (tx, mut rx) = mpsc::channel(10);

    tokio::spawn(
        frame!(async move {
            debug!("starting");

            let mut proxy = None;

            while let Some(msg) = rx.recv().await {
                process(msg, &mut proxy).await;
            }

            info!("Final shutting down the scylla proxy cluster...");
            stop(&mut proxy).await;

            debug!("finished");
        })
        .instrument(debug_span!("db")),
    );

    tx
}

#[framed]
async fn process(msg: ScyllaProxyCluster, proxy: &mut Option<RunningProxy>) {
    match msg {
        ScyllaProxyCluster::Start { node_configs, tx } => {
            start(node_configs, proxy).await;
            tx.send(())
                .expect("process ScyllaProxyCluster::Start: failed to send a response");
        }

        ScyllaProxyCluster::Stop { tx } => {
            stop(proxy).await;
            tx.send(())
                .expect("process ScyllaProxyCluster::Stop: failed to send a response");
        }

        ScyllaProxyCluster::ChangeRequestRules { rules, tx } => {
            change_request_rules(rules, proxy);
            tx.send(()).expect(
                "process ScyllaProxyCluster::ChangeRequestRules: failed to send a response",
            );
        }

        ScyllaProxyCluster::ChangeResponseRules { rules, tx } => {
            change_response_rules(rules, proxy);
            tx.send(()).expect(
                "process ScyllaProxyCluster::ChangeResponseRules: failed to send a response",
            );
        }

        ScyllaProxyCluster::TurnOffRules { tx } => {
            turn_off_rules(proxy);
            tx.send(())
                .expect("process ScyllaProxyCluster::TurnOffRules: failed to send a response");
        }
    }
}

#[framed]
async fn start(node_configs: Vec<ScyllaProxyNodeConfig>, proxy: &mut Option<RunningProxy>) {
    if node_configs.is_empty() {
        return;
    }
    stop(proxy).await;

    *proxy = Proxy::new(node_configs.into_iter().map(|cfg| {
        Node::new(
            (cfg.real_addr, DEFAULT_SCYLLA_CQL_PORT).into(),
            (cfg.proxy_addr, DEFAULT_SCYLLA_CQL_PORT).into(),
            scylla_proxy::ShardAwareness::QueryNode,
            None,
            None,
        )
    }))
    .run()
    .await
    .inspect_err(|err| {
        error!("Failed to start scylla proxy cluster: {err}");
    })
    .ok();
}

#[framed]
async fn stop(proxy: &mut Option<RunningProxy>) {
    let Some(proxy) = proxy.take() else {
        return;
    };
    if let Err(err) = proxy.finish().await {
        error!("Error while shutting down scylla proxy cluster: {err}");
    }
}

#[framed]
fn change_request_rules(rules: Option<Vec<RequestRule>>, proxy: &mut Option<RunningProxy>) {
    let Some(proxy) = proxy else {
        return;
    };
    proxy
        .running_nodes
        .iter_mut()
        .for_each(|node| node.change_request_rules(rules.clone()));
}

#[framed]
fn change_response_rules(rules: Option<Vec<ResponseRule>>, proxy: &mut Option<RunningProxy>) {
    let Some(proxy) = proxy else {
        return;
    };
    proxy
        .running_nodes
        .iter_mut()
        .for_each(|node| node.change_response_rules(rules.clone()));
}

#[framed]
fn turn_off_rules(proxy: &mut Option<RunningProxy>) {
    let Some(proxy) = proxy else {
        return;
    };
    proxy.turn_off_rules();
}
