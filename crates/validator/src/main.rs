/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod tests;

use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use tests::TestActors;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

#[derive(Debug, Parser)]
#[clap(version)]
struct Args {}

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

    let _args = Args::parse();

    info!(
        "{} version: {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    let test_cases = tests::register().await;

    // TODO: implement a filter using cmdline arguments
    assert!(tests::run(TestActors {}, test_cases, Arc::new(HashMap::new())).await);
}
