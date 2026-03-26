/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod alternator;
mod ann;
mod auth;
mod cdc;
mod common;
mod connection_timeout;
mod crud;
mod db_timeout;
mod filtering;
mod full_scan;
mod high_availability;
mod index_create;
mod index_status;
mod quantization_and_rescoring;
mod reconnect;
mod serde;
mod similarity_functions;

use async_backtrace::framed;
use e2etest::TestCase;
use e2etest_dns::Dns;
use e2etest_dns::DnsExt;
use e2etest_firewall::Firewall;
use e2etest_scylla_cluster::ScyllaCluster;
use e2etest_scylla_cluster::ScyllaClusterExt;
use e2etest_scylla_proxy_cluster::ScyllaProxyCluster;
use e2etest_tls::Tls;
use e2etest_vector_store_cluster::VectorStoreCluster;
use e2etest_vector_store_cluster::VectorStoreClusterExt;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[derive(clap::Args)]
struct Args {
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

    /// Path to the ScyllaDB executable.
    #[arg(value_name = "PATH")]
    scylla: PathBuf,

    /// Path to the Vector Store executable.
    #[arg(value_name = "PATH")]
    vector_store: PathBuf,
}

fn init(args: &Args) {
    let ansi = !args.disable_colors;
    let rust_log = if args.verbose {
        "info"
    } else {
        "info,hickory_server=warn"
    };
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install aws-lc-rs crypto provider");

    tracing_subscriber::registry()
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
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new(rust_log))
                .expect("Failed to create EnvFilter"),
        )
        .with(
            fmt::layer()
                .with_target(false)
                .with_ansi(ansi)
                .with_writer(std::io::stdout),
        )
        .init();
}

#[framed]
/// Returns a vector of all known test cases to be run. Each test case is registered with a name
async fn register() -> Vec<(String, TestCase<TestActors>)> {
    test_cases().await.collect()
}

fn validate_different_subnet(dns_ip: Ipv4Addr, base_ip: Ipv4Addr) {
    let dns_octets = dns_ip.octets();
    let base_octets = base_ip.octets();
    assert!(
        dns_octets[1] != base_octets[1] || dns_octets[2] != base_octets[2],
        "DNS server should serve addresses from a different subnet than its own"
    );
}

async fn fixture(args: &Args) -> TestActors {
    validate_different_subnet(args.dns_ip, args.base_ip);

    let services_subnet = Arc::new(ServicesSubnet::new(args.base_ip));
    let tls = e2etest_tls::new(&common::get_default_db_ips_for_subnet(&services_subnet)).await;
    let dns = e2etest_dns::new(args.dns_ip).await;
    let firewall = e2etest_firewall::new().await;
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

    info!(
        "{} version: {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    let version = db.version().await;
    info!("scylla version: {}", version);
    info!("dns version: {}", dns.version().await);
    info!("vector-store version: {}", vs.version().await);

    TestActors {
        services_subnet,
        tls,
        dns,
        firewall,
        db,
        vs,
        db_proxy,
    }
}

pub fn run() -> Result<(), &'static str> {
    e2etest::run(init, register, fixture)
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

#[derive(Clone)]
struct TestActors {
    pub(crate) services_subnet: Arc<ServicesSubnet>,
    pub(crate) tls: mpsc::Sender<Tls>,
    pub(crate) dns: mpsc::Sender<Dns>,
    pub(crate) firewall: mpsc::Sender<Firewall>,
    pub(crate) db: mpsc::Sender<ScyllaCluster>,
    pub(crate) vs: mpsc::Sender<VectorStoreCluster>,
    pub(crate) db_proxy: mpsc::Sender<ScyllaProxyCluster>,
}

#[framed]
pub async fn test_cases() -> impl Iterator<Item = (String, TestCase<TestActors>)> {
    vec![
        ("alternator", alternator::new().await),
        ("ann", ann::new().await),
        ("auth", auth::new().await),
        ("cdc", cdc::new().await),
        ("connection_timeout", connection_timeout::new().await),
        ("crud", crud::new().await),
        ("db_timeout", db_timeout::new().await),
        ("filtering", filtering::new().await),
        ("full_scan", full_scan::new().await),
        ("high_availability", high_availability::new().await),
        ("index_status", index_status::new().await),
        ("index_create", index_create::new().await),
        ("reconnect", reconnect::new().await),
        ("serde", serde::new().await),
        ("similarity_function", similarity_functions::new().await),
        (
            "quantization_and_rescoring",
            quantization_and_rescoring::new().await,
        ),
    ]
    .into_iter()
    .map(|(name, test_case)| (name.to_string(), test_case))
}
