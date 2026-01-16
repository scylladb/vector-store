/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::frame;
use async_backtrace::framed;
use httpclient::HttpClient;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;
use tempfile::tempdir;
use tempfile::tempdir_in;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;
use vector_search_validator_tests::VectorStoreCluster;
use vector_search_validator_tests::VectorStoreNodeConfig;
use vector_store::httproutes::NodeStatus;

#[framed]
pub(crate) async fn new(
    path: PathBuf,
    verbose: bool,
    disable_colors: bool,
    tmpdir: PathBuf,
) -> mpsc::Sender<VectorStoreCluster> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        crate::executable_exists(&path).await,
        "vector-store executable '{path:?}' does not exist"
    );

    let mut state = State::new(path, verbose, disable_colors, tmpdir).await;

    tokio::spawn(
        frame!(async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &mut state).await;
            }

            info!("Final shutting down the vector-store cluster...");
            stop(&mut state).await;

            debug!("finished");
        })
        .instrument(debug_span!("vs")),
    );

    tx
}

struct NodeState {
    vs_ip: Ipv4Addr,
    child: Option<Child>,
    client: Option<HttpClient>,
    _workdir: Option<TempDir>,
}

struct State {
    path: PathBuf,
    nodes: Vec<NodeState>,
    version: String,
    verbose: bool,
    disable_colors: bool,
    tmpdir: PathBuf,
}

impl State {
    #[framed]
    async fn new(path: PathBuf, verbose: bool, disable_colors: bool, tmpdir: PathBuf) -> Self {
        let version = String::from_utf8_lossy(
            &Command::new(&path)
                .arg("--version")
                .output()
                .await
                .expect("vs: State::new: failed to execute vector-store")
                .stdout,
        )
        .trim()
        .to_string();

        Self {
            path,
            version,
            nodes: Vec::new(),
            verbose,
            disable_colors,
            tmpdir,
        }
    }
}

#[framed]
async fn process(msg: VectorStoreCluster, state: &mut State) {
    match msg {
        VectorStoreCluster::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process VectorStoreCluster::Version: failed to send a response");
        }

        VectorStoreCluster::Start { node_configs } => {
            start(node_configs, state).await;
        }

        VectorStoreCluster::StartNode { node_config } => {
            start_node(node_config, state).await;
        }

        VectorStoreCluster::Stop { tx } => {
            stop(state).await;
            tx.send(())
                .expect("process VectorStoreCluster::Stop: failed to send a response");
        }

        VectorStoreCluster::StopNode { vs_ip, tx } => {
            stop_node(state, vs_ip).await;
            tx.send(())
                .expect("process VectorStoreCluster::StopNode: failed to send a response");
        }

        VectorStoreCluster::WaitForReady { tx } => {
            tx.send(wait_for_ready(state).await)
                .expect("process VectorStoreCluster::WaitForReady: failed to send a response");
        }
    }
}

#[framed]
async fn run_node(node_config: &VectorStoreNodeConfig, state: &State, workdir: &TempDir) -> Child {
    let mut cmd = Command::new(&state.path);
    if !state.verbose {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }

    cmd.env("VECTOR_STORE_URI", node_config.vs_addr().to_string())
        .env(
            "VECTOR_STORE_SCYLLADB_URI",
            node_config.db_addr().to_string(),
        )
        .env("VECTOR_STORE_THREADS", "2")
        .env(
            "VECTOR_STORE_DISABLE_COLORS",
            state.disable_colors.to_string(),
        )
        .env("VECTOR_STORE_CQL_KEEPALIVE_INTERVAL", "2s")
        .env("VECTOR_STORE_CQL_KEEPALIVE_TIMEOUT", "10s")
        .env("VECTOR_STORE_CQL_TCP_KEEPALIVE_INTERVAL", "1s")
        .env("VECTOR_STORE_CDC_SAFETY_INTERVAL", "3s")
        .env("VECTOR_STORE_CDC_SLEEP_INTERVAL", "1s");

    if let Some(u) = node_config.user.as_deref()
        && let Some(p) = node_config.password.as_deref()
    {
        File::create_new(workdir.path().join("password"))
            .await
            .expect("run_node: failed to create auth_credentials file")
            .write_all(p.as_bytes())
            .await
            .expect("run_node: failed to write password to auth_credentials file");

        cmd.env("VECTOR_STORE_SCYLLADB_USERNAME", u).env(
            "VECTOR_STORE_SCYLLADB_PASSWORD_FILE",
            workdir.path().join("password").to_str().unwrap(),
        );
    }

    for (k, v) in &node_config.envs {
        cmd.env(k, v);
    }

    cmd.spawn().expect("start: failed to spawn vector-store")
}

#[framed]
async fn start(node_configs: Vec<VectorStoreNodeConfig>, state: &mut State) {
    if node_configs.is_empty() {
        return;
    }

    let vs_ips: Vec<Ipv4Addr> = node_configs.iter().map(|c| c.vs_ip).collect();
    debug!("vector_store_cluster: using VS IPs: {:?}", vs_ips);

    for (i, node_config) in node_configs.iter().enumerate() {
        info!(
            "Starting Vector Store node {} on IP {} (connecting to DB {})",
            i + 1,
            node_config.vs_ip,
            node_config.db_ip
        );

        let workdir = tempdir_in(&state.tmpdir).expect("start: failed to create temporary workdir");
        let child = run_node(node_config, state, &workdir).await;
        let client = HttpClient::new(node_config.vs_addr());

        state.nodes.push(NodeState {
            vs_ip: node_config.vs_ip,
            child: Some(child),
            client: Some(client),
            _workdir: Some(workdir),
        });
    }

    debug!(
        "Started {} Vector Store nodes, waiting for initialization...",
        state.nodes.len()
    );
}

#[framed]
async fn start_node(node_config: VectorStoreNodeConfig, state: &mut State) {
    if state.nodes.is_empty() {
        return;
    }

    let node_index = state
        .nodes
        .iter()
        .position(|n| n.vs_ip == node_config.vs_ip);

    if let Some(idx) = node_index {
        let workdir = tempdir().expect("start_node: failed to create temporary workdir");
        let child = run_node(&node_config, state, &workdir).await;
        let client = HttpClient::new(node_config.vs_addr());

        state.nodes[idx] = NodeState {
            vs_ip: node_config.vs_ip,
            child: Some(child),
            client: Some(client),
            _workdir: Some(workdir),
        };
    }
}

#[framed]
async fn stop(state: &mut State) {
    for node in &mut state.nodes {
        if let Some(mut child) = node.child.take() {
            child
                .start_kill()
                .expect("stop: failed to send SIGTERM to vector-store process");
            child
                .wait()
                .await
                .expect("stop: failed to wait for vector-store process to exit");
        }
        node.client = None;
    }
    state.nodes.clear();
}

#[framed]
async fn stop_node(state: &mut State, vs_ip: Ipv4Addr) {
    if let Some(node) = state.nodes.iter_mut().find(|n| n.vs_ip == vs_ip) {
        if let Some(mut child) = node.child.take() {
            child
                .start_kill()
                .expect("stop_node: failed to send SIGTERM to vector-store process");
            child
                .wait()
                .await
                .expect("stop_node: failed to wait for vector-store process to exit");
        }
        node.client = None;
    }
}

#[framed]
async fn wait_for_node(client: &HttpClient) -> bool {
    time::timeout(Duration::from_secs(30), async {
        loop {
            let status = client.status().await;
            if matches!(status, Ok(NodeStatus::Serving)) {
                return true;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false)
}

#[framed]
/// Waits for all Vector Store nodes to be ready by checking the status of each node.
async fn wait_for_ready(state: &State) -> bool {
    if state.nodes.is_empty() {
        tracing::error!("No Vector Store nodes to wait for - nodes list is empty");
        return false;
    }

    for node in &state.nodes {
        if let Some(ref client) = node.client {
            info!(
                "Waiting for Vector Store node at IP {} to be ready...",
                &node.vs_ip
            );
            assert!(wait_for_node(client).await);
            info!("Vector Store node at IP {} is ready.", &node.vs_ip);
        } else {
            tracing::error!("Client for node at IP {} is not initialized.", &node.vs_ip);
            return false;
        }
    }

    true
}
