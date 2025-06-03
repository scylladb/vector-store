/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod db_basic;
mod httpclient;

#[cfg(not(feature = "opensearch"))]
mod usearch;

#[cfg(feature = "opensearch")]
mod mock_opensearch;
#[cfg(feature = "opensearch")]
mod opensearch;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

fn enable_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_new("info").unwrap())
        .with(fmt::layer().with_target(false))
        .init();
}
