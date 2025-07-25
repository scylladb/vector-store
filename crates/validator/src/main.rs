/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use clap::Parser;
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
}
