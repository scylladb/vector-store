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
use std::sync::LazyLock;
use std::sync::RwLock;
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
use tokio::sync::oneshot;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;

const DEFAULT_SCYLLA_CQL_PORT: u16 = 9042;

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
            "--tablets-mode-for-new-keyspaces=enabled",
            "--alternator-ttl-period-in-seconds=0.5",
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
    /// Optional path to the TLS certificate file for client encryption.
    pub cert_path: Option<PathBuf>,
    /// Optional path to the TLS key file for client encryption.
    pub key_path: Option<PathBuf>,
    /// Optional extra YAML config to append to the ScyllaDB options file
    /// (e.g. authentication settings).
    pub extra_config: Option<Vec<u8>>,
}

pub enum ScyllaCluster {
    Version {
        tx: oneshot::Sender<String>,
    },
    Start {
        node_configs: Vec<ScyllaNodeConfig>,
    },
    WaitForReady {
        tx: oneshot::Sender<bool>,
    },
    Stop {
        tx: oneshot::Sender<()>,
    },
    Up {
        node_configs: Vec<ScyllaNodeConfig>,
    },
    UpNode {
        node_config: ScyllaNodeConfig,
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
    fn start(&self, node_configs: Vec<ScyllaNodeConfig>) -> impl Future<Output = ()>;

    /// Stops the ScyllaDB cluster.
    fn stop(&self) -> impl Future<Output = ()>;

    /// Waits for the ScyllaDB cluster to be ready.
    fn wait_for_ready(&self) -> impl Future<Output = bool>;

    /// Starts a paused cluster back again.
    fn up(&self, node_configs: Vec<ScyllaNodeConfig>) -> impl Future<Output = ()>;

    /// Pauses a cluster.
    fn down(&self) -> impl Future<Output = ()>;

    /// Starts a single paused ScyllaDB instance back again.
    fn up_node(&self, node_config: ScyllaNodeConfig) -> impl Future<Output = ()>;

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
    async fn start(&self, node_configs: Vec<ScyllaNodeConfig>) {
        self.send(ScyllaCluster::Start { node_configs })
            .await
            .expect("ScyllaClusterExt::start: internal actor should receive request");
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
    async fn up(&self, node_configs: Vec<ScyllaNodeConfig>) {
        self.send(ScyllaCluster::Up { node_configs })
            .await
            .expect("ScyllaClusterExt::up: internal actor should receive request")
    }

    #[framed]
    async fn up_node(&self, node_config: ScyllaNodeConfig) {
        self.send(ScyllaCluster::UpNode { node_config })
            .await
            .expect("ScyllaClusterExt::up_node: internal actor should receive request")
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
        self.up_node(node_config.clone()).await;
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

#[framed]
pub async fn new(
    path: PathBuf,
    default_conf: PathBuf,
    tempdir: PathBuf,
    verbose: bool,
) -> mpsc::Sender<ScyllaCluster> {
    let (tx, mut rx) = mpsc::channel(10);

    assert!(
        e2etest::executable_exists(&path).await,
        "scylla executable '{path:?}' does not exist"
    );
    assert!(
        e2etest::file_exists(&default_conf).await,
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
    let has_tls = node_config.cert_path.is_some() && node_config.key_path.is_some();
    let has_extra = node_config.extra_config.is_some();

    let conf = if has_tls || has_extra {
        let mut config = Vec::new();
        if let (Some(cert), Some(key)) = (&node_config.cert_path, &node_config.key_path) {
            config.extend_from_slice(
                format!(
                    r#"client_encryption_options:
  enabled: true
  certificate: {cert}
  keyfile: {key}"#,
                    cert = cert.display(),
                    key = key.display()
                )
                .as_bytes(),
            );
        }
        if let Some(extra) = &node_config.extra_config {
            if !config.is_empty() {
                config.push(b'\n');
            }
            config.extend_from_slice(extra);
        }
        let conf_path = path.join("scylla.conf");
        fs::write(&conf_path, config)
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
