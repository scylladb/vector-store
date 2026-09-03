/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod alternator;
mod ann;
mod auth;
mod cdc;
mod coexisting_indexes;
mod common;
mod connection_timeout;
mod crud;
mod db_timeout;
mod filtering;
mod fts;
mod full_scan;
mod high_availability;
mod index_create;
mod index_modify;
mod index_status;
mod quantization_and_rescoring;
mod reconnect;
mod routing;
mod serde;
mod similarity_functions;
mod tls_reload;

use clap::Parser;
use clap::Subcommand;
use common::ProxyCluster;
use common::SharedCluster;
use e2etest::Config;
use e2etest::Progress;
use e2etest_dns::Dns;
use e2etest_dns::DnsExt;
use e2etest_firewall::Firewall;
use e2etest_firewall::FirewallExt;
use e2etest_firewall::Owner as FirewallOwner;
use e2etest_scylla_cluster::ScyllaCluster;
use e2etest_scylla_cluster::ScyllaClusterExt;
use e2etest_scylla_proxy_cluster::ScyllaProxyCluster;
use e2etest_tls::Tls;
use e2etest_vector_store_cluster::VectorStoreCluster;
use e2etest_vector_store_cluster::VectorStoreClusterExt;
use std::env;
use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::time;
use tracing::error;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[derive(Parser)]
#[clap(version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the list of available tests and exit.
    List,

    /// Run the E2E tests.
    Run(RunArgs),
}

#[derive(clap::Args)]
struct RunArgs {
    /// IP address for the DNS server to bind to. Must be a loopback address.
    #[arg(short, long, default_value = "127.0.1.1", value_name = "IP")]
    dns_ip: Ipv4Addr,

    /// IP address for the base services to bind to. Must be a loopback address.
    #[arg(short, long, default_value = "127.0.2.1", value_name = "IP")]
    base_ip: Ipv4Addr,

    /// Path to the ScyllaDB configuration file.
    #[arg(short, long, default_value = "conf/scylla.yaml", value_name = "PATH")]
    scylla_default_conf: PathBuf,

    /// Path to the base tmp directory.
    #[arg(short, long, default_value = "/tmp", value_name = "PATH")]
    tmpdir: PathBuf,

    /// Enable verbose logging for Scylla and vector-store.
    #[arg(short, long, default_value = "false")]
    verbose: bool,

    /// Disable ansi colors in the log output.
    #[arg(long, default_value = "false")]
    disable_colors: bool,

    /// Enable duplicating errors information into the stderr stream.
    #[arg(long, default_value = "false")]
    duplicate_errors: bool,

    /// Maximum number of tests to run concurrently within a group.
    /// Defaults to the number of available CPUs.
    ///
    /// Only groups whose fixtures allow it (those sharing a cluster and a
    /// keyspace, with per-test tables and indexes) run concurrently; groups
    /// that mutate cluster-wide state always run serially regardless of this
    /// value. Set to 1 to force fully sequential execution.
    #[arg(long, default_value_t = default_concurrency(), value_name = "N")]
    concurrency: usize,

    /// Directory to write the per-test log files to. Each test gets its own
    /// file, so that a concurrent run's output stays readable.
    #[arg(long, value_name = "PATH")]
    log_dir: Option<PathBuf>,

    /// Maximum number of test groups to run concurrently.
    ///
    /// Only groups sharing a cluster with their siblings may overlap; a group
    /// owning a cluster that mutates node, network or authorization state
    /// always runs on its own. The work in flight is bounded by this times
    /// `--concurrency`, so raising both loads the cluster quickly.
    #[arg(long, default_value = "4", value_name = "N")]
    group_concurrency: usize,

    /// Path to the ScyllaDB executable.
    #[arg(value_name = "PATH")]
    scylla: PathBuf,

    /// Path to the Vector Store executable.
    #[arg(value_name = "PATH")]
    vector_store: PathBuf,

    /// Filters to select specific tests to run.
    /// The syntax is as follows:
    ///     `<partially_matching_test_group_name>::<partially_matching_test_case_name>`
    /// Wrap either side in double quotes to require an exact match, for example:
    ///     `"crud"::`
    ///     `::"simple_create"`
    /// Without specifying `::`, the filter will try to match both the group and test names.
    #[arg(value_name = "FILTER")]
    filters: Vec<String>,
}

fn init(args: RunArgs) -> Config {
    let ansi = !args.disable_colors;
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install aws-lc-rs crypto provider");

    let log_dir = args
        .log_dir
        .clone()
        .unwrap_or_else(|| args.tmpdir.join("vector-search-validator-logs"));

    // The log always goes to a file per test. It also goes to the console, but
    // only under --verbose: otherwise the console is left to the results, which
    // is the whole point of reporting them.
    //
    // Every layer shares one filter rather than carrying its own. A per-layer
    // filter is decided and applied in two steps, and an `info!` whose
    // arguments await something logs in between, which loses the decision and
    // prints a line the filter had rejected.
    let console = args.verbose.then(|| {
        fmt::layer()
            .with_target(false)
            .with_ansi(ansi)
            .with_writer(std::io::stdout)
    });

    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info,hickory_server=warn"))
                .expect("Failed to create EnvFilter"),
        )
        .with(
            args.duplicate_errors.then_some(
                fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_target(false)
                    .with_ansi(ansi)
                    .with_filter(LevelFilter::ERROR)
                    .with_filter(filter::filter_fn(|metadata| {
                        metadata.target().starts_with("e2etest")
                    })),
            ),
        )
        .with(e2etest::logging::per_test_files(&log_dir))
        .with(console)
        .init();

    // Printed rather than logged, so that it shows without --verbose: a result
    // on the console is only useful next to the log that explains it.
    println!("Per-test logs: {}", log_dir.display());

    // Without --verbose the console shows a line per group, repainted as the
    // run goes: a spinner while its cluster comes up, then a mark per test as
    // each one ends. With it, a line per test naming it with its result.
    let progress = if args.verbose {
        Progress::Lines
    } else {
        Progress::Ticks
    };
    let concurrency = args.concurrency;
    let group_concurrency = args.group_concurrency;
    args.filters
        .iter()
        .fold(Config::default(), |acc, filter| acc.with_filter(filter))
        .with_permanent_fixture(args)
        .with_default_timeout(common::DEFAULT_TEST_TIMEOUT)
        .with_concurrency(concurrency)
        .with_group_concurrency(group_concurrency)
        .with_progress(progress)
        .with_colors(ansi)
}

/// The number of CPUs available to the process, used as the default number of
/// tests to run concurrently. Falls back to a single test when the count
/// cannot be determined.
fn default_concurrency() -> usize {
    thread::available_parallelism().map_or(1, NonZeroUsize::get)
}

fn validate_different_subnet(dns_ip: Ipv4Addr, base_ip: Ipv4Addr) {
    let dns_octets = dns_ip.octets();
    let base_octets = base_ip.octets();
    assert!(
        dns_octets[1] != base_octets[1] || dns_octets[2] != base_octets[2],
        "DNS server should serve addresses from a different subnet than its own"
    );
}

e2etest::group!(name = validator, fixtures = (TestEnv));

// Umbrella group owning the shared standard cluster. Groups whose tests only
// need the default cluster (and do not mutate cluster-wide state) are
// declared with `parent = crate::standard`, so a full run boots this cluster
// once instead of once per group.
e2etest::group!(
    name = standard,
    fixtures = (SharedCluster),
    parent = validator
);

// Umbrella group owning the shared proxy cluster (scylla-proxy in front of the
// database, single Vector Store node). Groups whose tests only manipulate
// proxy rules — never topology or Vector Store configuration — are declared
// with `parent = crate::proxy` and access the cluster through a per-test
// ProxyTestContext that resets the rules around every test.
e2etest::group!(name = proxy, fixtures = (ProxyCluster), parent = validator);

pub async fn run() -> ExitCode {
    let args = Args::parse();
    let root = validator();
    let stats = match args.command {
        Command::List => {
            root.test_names().into_iter().for_each(|name| {
                println!("{name}");
            });
            return ExitCode::SUCCESS;
        }
        Command::Run(args) => e2etest::run(init(args), root).await,
    };

    info!("Waiting for all tasks to finish...");
    if time::timeout(common::DEFAULT_TEST_TIMEOUT, async {
        while Handle::current().metrics().num_alive_tasks() > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_err()
    {
        error!("Timed out waiting for tasks to finish");
    } else {
        info!("All tasks finished");
    }

    if !stats.is_success() {
        error!(
            "Tests skipped by fixture errors:\n{list}",
            list = format_test_names(&stats.tests_skipped_by_fixture_err_names())
        );
        error!(
            "Tests failed:\n{list}",
            list = format_test_names(&stats.failed_names())
        );
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

pub(crate) fn format_test_names(names: &[String]) -> String {
    names
        .iter()
        .map(|name| format!("- {name}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Represents a subnet for services, derived from a base IP address.
pub struct ServicesSubnet([u8; 3]);

impl ServicesSubnet {
    pub fn new(ip: Ipv4Addr) -> Self {
        assert!(
            ip.is_loopback(),
            "Base IP for services must be a loopback address"
        );

        let octets = ip.octets();
        assert!(
            octets[3] == 1,
            "Base IP for services must have the last octet set to 1"
        );

        Self([octets[0], octets[1], octets[2]])
    }

    /// Returns an IP address in the subnet with the specified last octet.
    pub fn ip(&self, octet: u8) -> Ipv4Addr {
        [self.0[0], self.0[1], self.0[2], octet].into()
    }
}

/// How many last-octet addresses each cluster reserves in the services subnet.
/// A cluster needs nine (three ScyllaDB, three proxy and three Vector Store
/// nodes); the rest of the block leaves room to add more.
pub const CLUSTER_OCTET_STRIDE: u8 = 32;

/// How many clusters can exist side by side, limited by the last octet the
/// stride leaves available. Groups owning a cluster may run concurrently only
/// up to this many at a time.
pub const MAX_CLUSTERS: usize = 8;

/// The shared, process-wide services: the loopback subnet the clusters are
/// carved out of, the certificate they all present, the DNS zone naming their
/// Vector Store nodes, and the host firewall.
///
/// Clusters are allocated from here so that several can run at the same time,
/// each with its own nodes on its own addresses.
struct TestEnv {
    args: Arc<RunArgs>,
    services_subnet: Arc<ServicesSubnet>,
    tls: mpsc::Sender<Tls>,
    dns: mpsc::Sender<Dns>,
    firewall: mpsc::Sender<Firewall>,
    /// Address blocks not currently taken by a cluster.
    free_clusters: Arc<Mutex<Vec<usize>>>,
}

impl e2etest::Fixture for TestEnv {
    async fn setup(setup: &mut impl e2etest::Setup) -> Option<Self> {
        let args = setup.get::<RunArgs>().await.unwrap();

        validate_different_subnet(args.dns_ip, args.base_ip);

        let services_subnet = Arc::new(ServicesSubnet::new(args.base_ip));
        // Every cluster presents the same certificate, so it has to name the
        // ScyllaDB addresses of all of them.
        let tls = e2etest_tls::new(&common::all_cluster_db_ips(&services_subnet)).await;
        let dns = e2etest_dns::new(args.dns_ip).await;
        let firewall = e2etest_firewall::new().await;

        info!(
            "{} version: {}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        );
        info!("dns version: {}", dns.version().await);

        Some(Self {
            args,
            services_subnet,
            tls,
            dns,
            firewall,
            free_clusters: Arc::new(Mutex::new((0..MAX_CLUSTERS).rev().collect())),
        })
    }

    async fn teardown(self) {}

    fn test_can_run_concurrently() -> bool {
        true
    }

    fn group_can_run_concurrently() -> bool {
        true
    }
}

impl TestEnv {
    /// Reserves a block of addresses and spawns the cluster actors that drive
    /// the nodes in it. The returned actors are independent of every other
    /// cluster's, so their groups can run at the same time.
    async fn new_cluster(&self) -> TestActors {
        let index = self
            .free_clusters
            .lock()
            .expect("the cluster pool lock is never held across a panic")
            .pop()
            .unwrap_or_else(|| {
                panic!(
                    "no free address block: at most {MAX_CLUSTERS} clusters can exist at once, \
                     so --group-concurrency must stay below that"
                )
            });
        let args = &self.args;

        let db = e2etest_scylla_cluster::new(
            args.scylla.clone(),
            args.scylla_default_conf.clone(),
            args.tmpdir.clone(),
            args.verbose,
        )
        .await;
        let vs = e2etest_vector_store_cluster::new(
            args.vector_store.clone(),
            args.verbose,
            args.disable_colors,
            args.tmpdir.clone(),
        )
        .await;
        let db_proxy = e2etest_scylla_proxy_cluster::new().await;

        if index == 0 {
            info!("scylla version: {}", db.version().await);
            info!("vector-store version: {}", vs.version().await);
        }

        TestActors {
            slot: Arc::new(ClusterSlot {
                index,
                pool: Arc::clone(&self.free_clusters),
            }),
            services_subnet: Arc::clone(&self.services_subnet),
            tls: self.tls.clone(),
            dns: self.dns.clone(),
            firewall: self.firewall.clone(),
            db,
            vs,
            db_proxy,
        }
    }
}

/// One cluster's actors, together with the slice of the services subnet its
/// nodes live on. Created by [`TestEnv::new_cluster`], never shared between
/// groups that own different clusters.
/// A reserved address block, returned to the pool once the cluster's actors are
/// dropped, so that a later group can reuse it. Held behind an `Arc` because
/// `TestActors` is cloned, and released only when the last clone goes away.
struct ClusterSlot {
    index: usize,
    pool: Arc<Mutex<Vec<usize>>>,
}

impl Drop for ClusterSlot {
    fn drop(&mut self) {
        if let Ok(mut pool) = self.pool.lock() {
            pool.push(self.index);
        }
    }
}

#[derive(Clone)]
struct TestActors {
    /// This cluster's address block, which also names its Vector Store nodes.
    slot: Arc<ClusterSlot>,
    pub(crate) services_subnet: Arc<ServicesSubnet>,
    pub(crate) tls: mpsc::Sender<Tls>,
    pub(crate) dns: mpsc::Sender<Dns>,
    pub(crate) firewall: mpsc::Sender<Firewall>,
    pub(crate) db: mpsc::Sender<ScyllaCluster>,
    pub(crate) vs: mpsc::Sender<VectorStoreCluster>,
    pub(crate) db_proxy: mpsc::Sender<ScyllaProxyCluster>,
}

impl TestActors {
    /// Index of this cluster, which distinguishes its nodes' DNS names.
    pub(crate) fn cluster(&self) -> usize {
        self.slot.index
    }

    /// The first last-octet address of this cluster's block.
    pub(crate) fn octet_base(&self) -> u8 {
        u8::try_from(self.slot.index).expect("cluster index fits in an octet")
            * CLUSTER_OCTET_STRIDE
    }

    /// Blocks traffic to the given addresses, replacing whatever this cluster
    /// blocked before. Other clusters' rules are left alone, so a group that
    /// cuts its own nodes off can run alongside one doing the same to its own.
    pub(crate) async fn drop_traffic(&self, ips: Vec<Ipv4Addr>) {
        self.firewall.drop_traffic(self.firewall_owner(), ips).await;
    }

    /// Unblocks everything this cluster blocked.
    pub(crate) async fn turn_off_firewall_rules(&self) {
        self.firewall.turn_off_rules(self.firewall_owner()).await;
    }

    /// The firewall rules of this cluster, kept apart from every other
    /// cluster's by its address block.
    fn firewall_owner(&self) -> FirewallOwner {
        FirewallOwner(self.slot.index as u64)
    }
}
