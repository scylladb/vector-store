/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use async_backtrace::frame;
use async_backtrace::framed;
use std::net::Ipv4Addr;
use std::os::unix;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use sysinfo::Gid;
use sysinfo::Uid;
use sysinfo::Users;
use tap::Pipe;
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
use vector_search_validator_tests::ScyllaNodeConfig;

const DEFAULT_SCYLLA_CQL_PORT: u16 = 9042;

#[framed]
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
        frame!(async move {
            debug!("starting");

            while let Some(msg) = rx.recv().await {
                process(msg, &mut state).await;
            }

            info!("Final shutting down the scylla cluster...");
            stop(&mut state).await;

            debug!("finished");
        })
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
    #[framed]
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

#[framed]
async fn process(msg: ScyllaCluster, state: &mut State) {
    match msg {
        ScyllaCluster::Version { tx } => {
            tx.send(state.version.clone())
                .expect("process ScyllaCluster::Version: failed to send a response");
        }

        ScyllaCluster::Start { node_configs } => {
            start(node_configs, state).await;
        }

        ScyllaCluster::Stop { tx } => {
            stop(state).await;
            tx.send(())
                .expect("process ScyllaCluster::Stop: failed to send a response");
        }

        ScyllaCluster::WaitForReady { tx } => {
            tx.send(wait_for_ready(state).await)
                .expect("process ScyllaCluster::WaitForReady: failed to send a response");
        }

        ScyllaCluster::Up { node_configs } => {
            up(node_configs, state).await;
        }

        ScyllaCluster::UpNode { node_config } => {
            up_node(node_config, state).await;
        }

        ScyllaCluster::Down { tx } => {
            down(state).await;
            tx.send(())
                .expect("process ScyllaCluster::Down: failed to send a response");
        }

        ScyllaCluster::DownNode { db_ip, tx } => {
            down_node(state, db_ip).await;
            tx.send(())
                .expect("process ScyllaCluster::DownNode: failed to send a response");
        }

        ScyllaCluster::Flush { tx } => {
            flush(state).await;
            tx.send(())
                .expect("process ScyllaCluster::Flush: failed to send a response");
        }
    }
}

#[framed]
async fn run_node(
    node_config: &ScyllaNodeConfig,
    seeds: &str,
    path: &Path,
    rack: &str,
    state: &State,
) -> Child {
    let conf = if let Some(conf) = node_config.config.as_ref() {
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

    let rack_dc_properties = format!("dc=datacenter1\nrack={rack}\nprefer_local=true\n");
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

    if !seeds.is_empty() {
        cmd.arg("--seed-provider-parameters")
            .arg(format!("seeds={seeds}"));
    }

    if !node_config.primary_vs_uris.is_empty() {
        cmd.arg("--vector-store-primary-uri")
            .arg(node_config.primary_vs_uris.join(","));
    }

    if !node_config.secondary_vs_uris.is_empty() {
        cmd.arg("--vector-store-secondary-uri")
            .arg(node_config.secondary_vs_uris.join(","));
    }

    let db_ip = node_config.db_ip;

    cmd.arg("--options-file")
        .arg(&conf)
        .arg("--workdir")
        .arg(path)
        .arg("--listen-address")
        .arg(db_ip.to_string())
        .arg("--rpc-address")
        .arg(db_ip.to_string())
        .arg("--api-address")
        .arg(db_ip.to_string())
        .arg("--log-to-stdout")
        .arg("true")
        .arg("--logger-ostream-type")
        .arg("stdout")
        .arg("--endpoint-snitch")
        .arg("GossipingPropertyFileSnitch")
        .args(node_config.args.clone())
        .pipe(|cmd| {
            if let Some((uid, gid)) = get_scylla_user() {
                info!("Starting ScyllaDB process as user 'scylla' ({uid:?}, {gid:?})");
                cmd.uid(*uid).gid(*gid);
            }
            cmd
        })
        .spawn()
        .expect("start: failed to spawn scylladb")
}

#[framed]
async fn start(node_configs: Vec<ScyllaNodeConfig>, state: &mut State) {
    if node_configs.is_empty() {
        return;
    }

    let db_ips: Vec<Ipv4Addr> = node_configs.iter().map(|c| c.db_ip).collect();
    debug!("scylla_cluster: using DB IPs: {:?}", db_ips);

    // The first node IP will be used as seed for whole cluster
    let seed_ip = db_ips.first().unwrap();

    // Start each node sequentially with proper seed configuration
    for (i, node_config) in node_configs.iter().enumerate() {
        let workdir = TempDir::new_in(&state.tempdir)
            .expect("start: failed to create temporary directory for scylladb");
        if let Some((uid, gid)) = get_scylla_user() {
            info!("Change owner of a ScyllaDB workdir to the 'scylla' user ({uid:?}, {gid:?})");
            unix::fs::chown(workdir.path(), Some(*uid), Some(*gid))
                .expect("start: failed to change ownership of scylladb workdir");
        }
        let rack = format!("rack{}", i + 1);
        let seeds = seed_ip.to_string();

        info!(
            "Starting Scylla node {} on IP {} in {} (primary: {:?}, secondary: {:?})",
            i + 1,
            node_config.db_ip,
            rack,
            node_config.primary_vs_uris,
            node_config.secondary_vs_uris
        );

        let child = run_node(node_config, &seeds, workdir.path(), &rack, state).await;

        state.nodes.push(NodeState {
            db_ip: node_config.db_ip,
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

#[framed]
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

#[framed]
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

#[framed]
async fn down_node(state: &mut State, db_ip: Ipv4Addr) {
    if let Some(node) = state.nodes.iter_mut().find(|n| n.db_ip == db_ip)
        && let Some(mut child) = node.child.take()
    {
        child
            .start_kill()
            .expect("down_node: failed to send SIGTERM to scylladb process");
        child
            .wait()
            .await
            .expect("down_node: failed to wait for scylladb process to exit");
    }
}

#[framed]
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
            loop {
                if is_cql_port_ready(ip).await {
                    return true;
                }
                time::sleep(Duration::from_millis(100)).await;
            }
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

#[framed]
async fn is_cql_port_ready(ip: Ipv4Addr) -> bool {
    use std::net::SocketAddr;
    use tokio::net::TcpStream;

    let addr = SocketAddr::from((ip, DEFAULT_SCYLLA_CQL_PORT));

    match tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr)).await {
        Ok(Ok(_)) => {
            debug!("CQL port {} is open on {}", DEFAULT_SCYLLA_CQL_PORT, ip);
            true
        }
        _ => {
            debug!(
                "CQL port {} is not yet ready on {}",
                DEFAULT_SCYLLA_CQL_PORT, ip
            );
            false
        }
    }
}

#[framed]
/// Waits for ScyllaDB to be ready by checking the nodetool status.
async fn wait_for_ready(state: &State) -> bool {
    if state.nodes.is_empty() {
        tracing::error!("No ScyllaDB nodes to wait for - nodes list is empty");
        return false;
    }

    for node in &state.nodes {
        tracing::info!(
            "Waiting for ScyllaDB node at IP {} to be ready...",
            &node.db_ip
        );
        wait_for_node(state, node.db_ip).await;
        tracing::info!("ScyllaDB node at IP {} is ready.", &node.db_ip);
    }

    true
}

#[framed]
async fn up(node_configs: Vec<ScyllaNodeConfig>, state: &mut State) {
    if state.nodes.is_empty() {
        return;
    }

    // Use the first node as seed for whole cluster
    let seed_ip = state.nodes.first().unwrap().db_ip.to_string();

    let nodes = state
        .nodes
        .iter()
        .enumerate()
        .map(|(i, node)| (i, node.workdir.as_ref().unwrap().path().to_path_buf()))
        .collect::<Vec<_>>();

    for (i, path) in nodes.into_iter() {
        let rack = format!("rack{}", i + 1);
        let child = run_node(&node_configs[i], &seed_ip, &path, &rack, state).await;
        state.nodes[i].child = Some(child);
    }
}

#[framed]
async fn up_node(node_config: ScyllaNodeConfig, state: &mut State) {
    if state.nodes.is_empty() {
        return;
    }

    // Use the first node as seed
    let seed_ip = state.nodes.first().unwrap().db_ip.to_string();

    if let Some((i, node)) = state
        .nodes
        .iter()
        .enumerate()
        .find(|(_, n)| n.db_ip == node_config.db_ip)
    {
        let rack = format!("rack{}", i + 1);
        let path = node.workdir.as_ref().unwrap().path().to_path_buf();
        let child = run_node(&node_config, &seed_ip, &path, &rack, state).await;
        state.nodes[i].child = Some(child);
    }
}

#[framed]
async fn flush(state: &State) {
    for node in &state.nodes {
        info!("Flushing node {}", node.db_ip);
        let mut cmd = Command::new(&state.path);
        cmd.arg("nodetool")
            .arg("-h")
            .arg(node.db_ip.to_string())
            .arg("flush");

        let output = cmd
            .output()
            .await
            .expect("flush: failed to run nodetool flush");

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::error!("nodetool flush failed on {}: {}", node.db_ip, stderr);
        } else {
            info!("Flush completed on node {}", node.db_ip);
        }
    }
}

fn get_scylla_user() -> Option<(Uid, Gid)> {
    Users::new_with_refreshed_list()
        .list()
        .iter()
        .find(|u| u.name() == "scylla")
        .map(|user| (user.id().clone(), user.group_id()))
}
