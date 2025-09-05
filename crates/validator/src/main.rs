/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod common;
mod dns;
mod scylla_cluster;
mod tests;
mod vector_store_cluster;

use clap::Parser;
use dns::DnsExt;
use scylla_cluster::ScyllaClusterExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tests::TestActors;
use tests::TestCase;
use tokio::fs;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use vector_store_cluster::VectorStoreClusterExt;

#[derive(Debug, Parser)]
#[clap(version)]
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

    /// Enable verbose logging for Scylla and vector-store.
    #[arg(short, long, default_value = "false")]
    verbose: bool,

    /// Path to the ScyllaDB executable.
    #[arg(value_name = "PATH")]
    scylla: PathBuf,

    /// Path to the Vector Store executable.
    #[arg(value_name = "PATH")]
    vector_store: PathBuf,

    /// Filters to select specific tests to run.
    /// The syntax is as follows:
    ///     `<partially_matching_test_file_name>::<partially_matching_test_case_name>`
    /// Without specifying `::`, the filter will try to match both the file and test names.
    #[arg(value_name = "FILTER")]
    filters: Vec<String>,
}

async fn file_exists(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path).await else {
        return false;
    };
    metadata.is_file()
}

async fn executable_exists(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path).await else {
        return false;
    };
    metadata.is_file() && (metadata.permissions().mode() & 0o111 != 0)
}

/// Represents a subnet for services, derived from a base IP address.
struct ServicesSubnet([u8; 3]);

impl ServicesSubnet {
    fn new(ip: Ipv4Addr) -> Self {
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
    fn ip(&self, octet: u8) -> Ipv4Addr {
        [self.0[0], self.0[1], self.0[2], octet].into()
    }
}

fn validate_different_subnet(dns_ip: Ipv4Addr, base_ip: Ipv4Addr) {
    let dns_octets = dns_ip.octets();
    let base_octets = base_ip.octets();
    assert!(
        dns_octets[1] != base_octets[1] || dns_octets[2] != base_octets[2],
        "DNS server should serve addresses from a different subnet than its own"
    );
}

fn fetch_matching_tests(filter: &str, test_case: &TestCase) -> HashSet<String> {
    let matching_tests: HashSet<String> = test_case
        .tests()
        .iter()
        .filter_map(|(test_name, _, _)| {
            if !filter.is_empty() && test_name.contains(filter) {
                Some(test_name.clone())
            } else {
                None
            }
        })
        .collect();
    matching_tests
}

fn update_filter_map(
    filter_map: &mut HashMap<String, HashSet<String>>,
    file_name: &str,
    matching_tests: HashSet<String>,
) {
    // If this file already has some tests selected, merge them
    filter_map
        .entry(file_name.to_string())
        .and_modify(|existing| {
            if !existing.is_empty() {
                existing.extend(matching_tests.iter().cloned());
            }
        })
        .or_insert(matching_tests);
}

/// Parse command line filters into the expected filter format for test execution.
/// Returns a HashMap where:
/// - Key: test file name (e.g., "crud", "full_scan")
/// - Value: HashSet of specific test names within that file (empty means run all tests in file)
fn parse_test_filters(
    filters: &[String],
    test_cases: &[(String, TestCase)],
) -> HashMap<String, HashSet<String>> {
    if filters.is_empty() {
        return HashMap::new(); // Run all tests
    }

    let mut filter_map: HashMap<String, HashSet<String>> = HashMap::new();

    for filter in filters {
        // Check for <file>::<test> syntax
        let parts: Vec<&str> = filter.splitn(2, "::").collect();
        if parts.len() == 2 {
            // Found `::`, split into file and test parts
            let file_part = parts[0];
            let test_part = parts[1];

            for (file_name, test_case) in test_cases {
                if file_part.is_empty() || file_name.contains(file_part) {
                    let matching_tests = fetch_matching_tests(test_part, test_case);
                    // If test_part is empty, run all tests in file
                    if !matching_tests.is_empty() || test_part.is_empty() {
                        update_filter_map(&mut filter_map, file_name, matching_tests);
                    }
                }
            }
        } else {
            // Not found `::`, check for matching both file and test case name
            let filter = parts[0];

            for (file_name, test_case) in test_cases {
                if file_name.contains(filter) {
                    filter_map.entry(file_name.to_string()).or_default();
                }
                let matching_tests = fetch_matching_tests(filter, test_case);
                if !matching_tests.is_empty() {
                    update_filter_map(&mut filter_map, file_name, matching_tests);
                }
            }
        }
    }

    filter_map
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .expect("Failed to create EnvFilter"),
        )
        .with(fmt::layer().with_target(false))
        .init();

    let args = Args::parse();

    validate_different_subnet(args.dns_ip, args.base_ip);

    let services_subnet = Arc::new(ServicesSubnet::new(args.base_ip));
    let dns = dns::new(args.dns_ip).await;
    let db = scylla_cluster::new(args.scylla, args.scylla_default_conf, args.verbose).await;
    let vs = vector_store_cluster::new(args.vector_store, args.verbose).await;

    info!(
        "{} version: {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    info!("dns version: {}", dns.version().await);
    info!("scylla version: {}", db.version().await);
    info!("vector-store version: {}", vs.version().await);

    let test_cases = tests::register().await;
    let filter_map = parse_test_filters(&args.filters, &test_cases);

    assert!(
        tests::run(
            TestActors {
                services_subnet,
                dns,
                db,
                vs,
            },
            test_cases,
            Arc::new(filter_map)
        )
        .await
    );
}

#[cfg(test)]
pub(crate) mod validator_tests {
    use super::*;

    fn make_test_cases() -> Vec<(String, TestCase)> {
        vec![
            (
                "crud".to_string(),
                TestCase::make_dummy_test_cases(&["simple_create", "drop_index"]),
            ),
            (
                "full_scan".to_string(),
                TestCase::make_dummy_test_cases(&["scan_index", "scan_all"]),
            ),
            (
                "other".to_string(),
                TestCase::make_dummy_test_cases(&["misc", "simple_misc"]),
            ),
        ]
    }

    #[test]
    fn test_empty_filters_runs_all() {
        let test_cases = make_test_cases();
        let filters: Vec<String> = vec![];
        let result = parse_test_filters(&filters, &test_cases);
        assert!(result.is_empty());
    }

    #[test]
    fn test_file_partial_match() {
        let test_cases = make_test_cases();
        let filters = vec!["crud".to_string()];
        let result = parse_test_filters(&filters, &test_cases);
        assert!(result.contains_key("crud"));
        assert!(result["crud"].is_empty());
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_test_case_partial_match() {
        let test_cases = make_test_cases();
        let filters = vec!["simple".to_string()];
        let result = parse_test_filters(&filters, &test_cases);
        assert!(result["crud"].contains("simple_create"));
        assert!(result["other"].contains("simple_misc"));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_file_and_test_case_syntax() {
        let test_cases = make_test_cases();
        let filters = vec!["crud::simple".to_string()];
        let result = parse_test_filters(&filters, &test_cases);
        assert!(result["crud"].contains("simple_create"));
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_file_and_empty_test_case_syntax() {
        let test_cases = make_test_cases();
        let filters = vec!["crud::".to_string()];
        let result = parse_test_filters(&filters, &test_cases);
        assert!(result.contains_key("crud"));
        assert!(result["crud"].is_empty());
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_empty_file_and_test_case_syntax() {
        let test_cases = make_test_cases();
        let filters = vec!["::simple".to_string()];
        let result = parse_test_filters(&filters, &test_cases);
        assert!(result["crud"].contains("simple_create"));
        assert!(result["other"].contains("simple_misc"));
        assert_eq!(result.len(), 2);
    }
}
