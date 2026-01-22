/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod db_basic;
mod https;
mod info;
mod memory_limit;
mod mock_opensearch;
mod openapi;
mod opensearch;
mod quantization;
mod status;
mod usearch;

use std::sync::Once;
use std::time::Duration;
use tokio::task;
use tokio::time;
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

async fn wait_for<F, Fut>(mut condition: F, msg: &str)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    time::timeout(Duration::from_secs(5), async {
        while !condition().await {
            task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Timeout on: {msg}"))
}

async fn wait_for_value<T>(mut producer: impl AsyncFnMut() -> Option<T>, msg: &str) -> T {
    time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(value) = producer().await {
                break value;
            }
            task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Timeout on: {msg}"))
}
