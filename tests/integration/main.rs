/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod assert;
mod db_basic;
mod httpclient;

mod usearch;

mod mock_opensearch;
mod opensearch;

mod info;

mod openapi;

use std::sync::Once;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

static INIT_TRACING: Once = Once::new();

fn enable_tracing() {
    INIT_TRACING.call_once(|| {
        tracing_subscriber::registry()
            .with(EnvFilter::try_new("info").unwrap())
            .with(fmt::layer().with_target(false))
            .init();
    });
}
