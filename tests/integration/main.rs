/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// TODO: This should be removed when the opensearch tests are implemented.
#![cfg_attr(feature = "opensearch", allow(dead_code))]

mod db_basic;
mod httpclient;

#[cfg(not(feature = "opensearch"))]
mod usearch;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

fn enable_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_new("info").unwrap())
        .with(fmt::layer().with_target(false))
        .init();
}
