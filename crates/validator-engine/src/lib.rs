/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod dns;
mod scylla_cluster;
mod scylla_proxy_cluster;
mod vector_store_cluster;

use async_backtrace::frame;
use async_backtrace::framed;
use clap::Parser;
use clap::Subcommand;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::panic;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::task;
use tokio::time;
use tracing::error;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use vector_search_validator_tests::DnsExt;
use vector_search_validator_tests::ScyllaClusterExt;
use vector_search_validator_tests::ServicesSubnet;
use vector_search_validator_tests::TestActors;
use vector_search_validator_tests::TestCase;
use vector_search_validator_tests::VectorStoreClusterExt;

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

    /// Run the vector-search-validator tests.
    Run {
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

        /// Filters to select specific tests to run.
        /// The syntax is as follows:
        ///     `<partially_matching_test_file_name>::<partially_matching_test_case_name>`
        /// Without specifying `::`, the filter will try to match both the file and test names.
        #[arg(value_name = "FILTER")]
        filters: Vec<String>,
    },
}

#[framed]
async fn file_exists(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path).await else {
        return false;
    };
    metadata.is_file()
}

#[framed]
async fn executable_exists(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path).await else {
        return false;
    };
    metadata.is_file() && (metadata.permissions().mode() & 0o111 != 0)
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
    test_case
        .tests()
        .iter()
        .filter_map(|(test_name, _, _)| {
            if !filter.is_empty() && test_name.contains(filter) {
                Some(test_name.clone())
            } else {
                None
            }
        })
        .collect()
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
        if let Some((file_part, test_part)) = filter.split_once("::") {
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

#[framed]
/// Returns a vector of all known test cases to be run. Each test case is registered with a name
async fn register() -> Vec<(String, TestCase)> {
    vector_search_validator_vector_store::test_cases()
        .await
        .chain(vector_search_validator_scylla::test_cases().await)
        .collect()
}

#[framed]
pub fn run() -> Result<(), &'static str> {
    let args = Args::parse();

    let (ansi, rust_log) = match &args.command {
        Command::Run {
            disable_colors,
            verbose,
            ..
        } => (
            !disable_colors,
            if *verbose {
                "info"
            } else {
                "info,hickory_server=warn"
            },
        ),
        _ => (true, "info,hickory_server=warn"),
    };
    tracing_subscriber::registry()
        .with({
            if let Command::Run {
                duplicate_errors, ..
            } = &args.command
            {
                duplicate_errors.then_some(
                    fmt::layer()
                        .with_writer(std::io::stderr)
                        .with_target(false)
                        .with_ansi(ansi)
                        .with_filter(LevelFilter::ERROR)
                        .with_filter(filter::filter_fn(|metadata| {
                            metadata.target() == "vector_search_validator_tests"
                                || metadata.target() == "vector_search_validator_engine"
                        })),
                )
            } else {
                None
            }
        })
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

    panic::set_hook(Box::new(|info| {
        error!("{info}");
    }));

    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(frame!(async move {
            let test_cases = register().await;
            if let Command::List = &args.command {
                test_cases
                    .into_iter()
                    .flat_map(|(test_case_name, test_case)| {
                        let tests: Vec<_> = test_case
                            .tests()
                            .iter()
                            .map(move |(test_name, _, _)| test_name.clone())
                            .collect();
                        tests
                            .into_iter()
                            .map(move |test_name| (test_case_name.clone(), test_name))
                    })
                    .for_each(|(test_case_name, test_name)| {
                        println!("{test_case_name}::{test_name}");
                    });
                return Ok(());
            }

            let Command::Run {
                dns_ip,
                base_ip,
                scylla,
                vector_store,
                scylla_default_conf,
                tmpdir,
                verbose,
                filters,
                disable_colors,
                ..
            } = args.command
            else {
                unreachable!();
            };

            validate_different_subnet(dns_ip, base_ip);

            let services_subnet = Arc::new(ServicesSubnet::new(base_ip));
            let dns = dns::new(dns_ip).await;
            let db =
                scylla_cluster::new(scylla, scylla_default_conf, tmpdir.clone(), verbose).await;
            let vs = vector_store_cluster::new(vector_store, verbose, disable_colors, tmpdir).await;
            let db_proxy = scylla_proxy_cluster::new().await;

            info!(
                "{} version: {}",
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION")
            );
            let version = db.version().await;
            info!("scylla version: {}", version);
            info!("dns version: {}", dns.version().await);
            info!("vector-store version: {}", vs.version().await);

            let filter_map = parse_test_filters(&filters, &test_cases);

            let result = vector_search_validator_tests::run(
                TestActors {
                    services_subnet,
                    dns,
                    db,
                    vs,
                    db_proxy,
                },
                test_cases,
                Arc::new(filter_map),
            )
            .await
            .then_some(())
            .ok_or("Some vector-search-validator tests failed");

            info!("Waiting for all tasks to finish...");
            const FINISH_TASKS_TIMEOUT: Duration = Duration::from_secs(10);
            if time::timeout(FINISH_TASKS_TIMEOUT, async {
                while Handle::current().metrics().num_alive_tasks() > 0 {
                    task::yield_now().await;
                }
            })
            .await
            .is_err()
            {
                error!("Timed out waiting for tasks to finish");
            } else {
                info!("All tasks finished");
            }

            result
        }))
}

#[cfg(test)]
pub(crate) mod validator_tests {
    use super::*;

    fn make_dummy_test_cases(test_names: &[&str]) -> TestCase {
        let mut tc = TestCase::empty();
        for &name in test_names {
            tc = tc.with_test(
                name.to_string(),
                std::time::Duration::ZERO,
                |_actors| async {},
            );
        }
        tc
    }

    fn make_test_cases() -> Vec<(String, TestCase)> {
        vec![
            (
                "crud".to_string(),
                make_dummy_test_cases(&["simple_create", "drop_index"]),
            ),
            (
                "full_scan".to_string(),
                make_dummy_test_cases(&["scan_index", "scan_all"]),
            ),
            (
                "other".to_string(),
                make_dummy_test_cases(&["misc", "simple_misc"]),
            ),
        ]
    }

    #[test]
    fn test_no_filters_runs_all() {
        let test_cases = make_test_cases();
        let filters: Vec<String> = vec![];
        let result = parse_test_filters(&filters, &test_cases);
        assert!(result.is_empty());
    }

    #[test]
    fn test_empty_filters_runs_all() {
        let test_cases = make_test_cases();
        let filters: Vec<String> = vec!["::".to_string()];
        let result = parse_test_filters(&filters, &test_cases);
        // It should contain all available test files with empty test cases (running all)
        assert_eq!(result.len(), 3);
        assert!(result["crud"].is_empty());
        assert!(result["full_scan"].is_empty());
        assert!(result["other"].is_empty());
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
