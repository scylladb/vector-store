/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::net::Ipv4Addr;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;
use tokio::fs;
use tokio::process::Child;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;
use vector_search_validator_tests::ScyllaCluster;

pub(crate) async fn new(
    path: PathBuf,
    default_conf: PathBuf,
    tempdir: PathBuf,
    verbose: bool,
) -> mpsc::Sender<ScyllaCluster> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        crate::executable_exists(&path).await,
        "scylla executable '{path:?}' does not exist"
    );
    assert!(
        crate::file_exists(&default_conf).await,
        "scylla config '{default_conf:?}' does not exist"
    );

    let mut state = State::new(path, default_conf, tempdir, verbose).await;

    tokio::spawn(
        async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &mut state).await;
            }

            debug!("finished");
        }
        .instrument(debug_span!("db")),
    );

    tx
}

struct NodeState {
    db_ip: Ipv4Addr,
    child: Option<Child>,
    workdir: Option<TempDir>,
}

struct State {
    path: PathBuf,
    default_conf: PathBuf,
    tempdir: PathBuf,
    nodes: Vec<NodeState>,
    version: String,
    verbose: bool,
}

impl State {
    async fn new(path: PathBuf, default_conf: PathBuf, tempdir: PathBuf, verbose: bool) -> Self {
        let version = String::from_utf8_lossy(
            &Command::new(&path)
                .arg("--version")
                .output()
                .await
                .expect("db: State::new: failed to execute scylla")
                .stdout,
        )
        .trim()
        .to_string();

        Self {
            path,
            default_conf,
            version,
            tempdir,
            nodes: Vec::new(),
            verbose,
        }
    }
}

async fn process(msg: ScyllaCluster, state: &mut State) {
    match msg {
        ScyllaCluster::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process ScyllaCluster::Version: failed to send a response");
        }

        ScyllaCluster::Start {
            vs_uri,
            db_ips,
            conf,
        } => {
            start(vs_uri, db_ips, conf, state).await;
        }

        ScyllaCluster::Stop => {
            stop(state).await;
        }

        ScyllaCluster::WaitForReady { tx } => {
            tx.send(wait_for_ready(state).await)
                .expect("process ScyllaCluster::WaitForReady: failed to send a response");
        }

        ScyllaCluster::Up { vs_uri, conf } => {
            up(vs_uri, conf, state).await;
        }

        ScyllaCluster::Down => {
            down(state).await;
        }
    }
}

async fn run_node(
    vs_uri: &String,
    db_ip: &Ipv4Addr,
    seeds: &str,
    conf: &Option<Vec<u8>>,
    path: &Path,
    rack: &str,
    state: &State,
) -> Child {
    let conf = if let Some(conf) = conf {
        let conf_path = path.join("scylla.conf");
        fs::write(&conf_path, conf)
            .await
            .expect("start: failed to write scylla config");
        conf_path
    } else {
        state.default_conf.clone()
    };

    let conf_dir = path.join("conf");
    fs::create_dir_all(&conf_dir)
        .await
        .expect("start: failed to create conf directory");

    let rack_dc_properties = format!(
        "
    dc=datacenter1
    rack={rack}
    prefer_local=true
    "
    );
    let properties_path = conf_dir.join("cassandra-rackdc.properties");
    debug!(
        "Creating cassandra-rackdc.properties at {:?}",
        properties_path
    );

    fs::write(&properties_path, &rack_dc_properties)
        .await
        .expect("start: failed to write cassandra-rackdc.properties");

    let mut cmd = Command::new(&state.path);
    if !state.verbose {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }

    cmd.env("SCYLLA_CONF", &conf_dir);

    cmd.arg("--overprovisioned")
        .arg("--options-file")
        .arg(&conf)
        .arg("--workdir")
        .arg(path)
        .arg("--listen-address")
        .arg(db_ip.to_string())
        .arg("--rpc-address")
        .arg(db_ip.to_string())
        .arg("--api-address")
        .arg(db_ip.to_string())
        .arg("--seed-provider-parameters")
        .arg(format!("seeds={seeds}"))
        .arg("--vector-store-primary-uri")
        .arg(vs_uri)
        .arg("--developer-mode")
        .arg("true")
        .arg("--smp")
        .arg("2")
        .arg("--log-to-stdout")
        .arg("true")
        .arg("--logger-ostream-type")
        .arg("stdout")
        .arg("--rf-rack-valid-keyspaces")
        .arg("true")
        .arg("--endpoint-snitch")
        .arg("GossipingPropertyFileSnitch")
        .spawn()
        .expect("run_node: failed to spawn scylladb")
}

async fn start(vs_uri: String, db_ips: Vec<Ipv4Addr>, conf: Option<Vec<u8>>, state: &mut State) {
    debug!("scylla_cluster: using DB IPs: {:?}", db_ips);

    // The first node IP will be used as seed for whole cluster
    let seed_ip = db_ips[0];

    // Start each node sequentially with proper seed configuration
    for (i, db_ip) in db_ips.iter().enumerate() {
        let workdir = TempDir::new_in(&state.tempdir)
            .expect("start: failed to create temporary directory for scylladb");
        let rack = format!("rack{}", i + 1);
        let seeds = seed_ip.to_string();

        info!("Starting Scylla node {} on IP {} in {}", i + 1, db_ip, rack);

        let child = run_node(&vs_uri, db_ip, &seeds, &conf, workdir.path(), &rack, state).await;

        state.nodes.push(NodeState {
            db_ip: *db_ip,
            child: Some(child),
            workdir: Some(workdir),
        });
    }

    debug!(
        "Started {} Scylla nodes in {}-rack cluster configuration, waiting for initialization...",
        state.nodes.len(),
        state.nodes.len()
    );
}

async fn stop(state: &mut State) {
    for node in &mut state.nodes {
        if let Some(mut child) = node.child.take() {
            child
                .start_kill()
                .expect("stop: failed to send SIGTERM to scylladb process");
            child
                .wait()
                .await
                .expect("stop: failed to wait for scylladb process to exit");
        }
        node.workdir = None;
    }
    state.nodes.clear();
}

async fn down(state: &mut State) {
    for node in &mut state.nodes {
        if let Some(mut child) = node.child.take() {
            child
                .start_kill()
                .expect("down: failed to send SIGTERM to scylladb process");
            child
                .wait()
                .await
                .expect("down: failed to wait for scylladb process to exit");
        }
    }
}

async fn wait_for_node(state: &State, ip: Ipv4Addr) -> bool {
    let mut cmd = Command::new(&state.path);
    cmd.arg("nodetool")
        .arg("-h")
        .arg(ip.to_string())
        .arg("status");

    loop {
        if String::from_utf8_lossy(
            &cmd.output()
                .await
                .expect("start: failed to run nodetool")
                .stdout,
        )
        .lines()
        .any(|line| line.starts_with(&format!("UN {ip}")))
        {
            return true;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

/// Waits for ScyllaDB to be ready by checking the nodetool status.
async fn wait_for_ready(state: &State) -> bool {
    if state.nodes.is_empty() {
        tracing::error!("No nodes to wait for - nodes list is empty");
        return false;
    }

    for node in &state.nodes {
        tracing::info!("Waiting for node at IP {} to be ready...", node.db_ip);
        wait_for_node(state, node.db_ip).await;
        tracing::info!("Node at IP {} is ready.", node.db_ip);
    }

    true
}

async fn up(vs_uri: String, conf: Option<Vec<u8>>, state: &mut State) {
    if state.nodes.is_empty() {
        return;
    }

    // Use the first node as seed for whole cluster
    let seed_ip = state.nodes[0].db_ip.to_string();

    let nodes = state
        .nodes
        .iter()
        .enumerate()
        .map(|(i, node)| {
            (
                i,
                node.db_ip,
                node.workdir.as_ref().unwrap().path().to_path_buf(),
            )
        })
        .collect::<Vec<_>>();

    for (i, db_ip, path) in nodes.into_iter() {
        let rack = format!("rack{}", i + 1);
        let child = run_node(&vs_uri, &db_ip, &seed_ip, &conf, &path, &rack, state).await;
        state.nodes[i].child = Some(child);
    }
}
