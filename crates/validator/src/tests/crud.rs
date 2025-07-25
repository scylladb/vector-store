/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::tests::*;
use std::time::Duration;
use tracing::info;

pub(crate) async fn new() -> TestCase {
    let timeout = Duration::from_secs(30);
    TestCase::empty()
        .with_init(timeout, init)
        .with_cleanup(timeout, cleanup)
        .with_test("dummy", timeout, dummy)
}

async fn init(_actors: TestActors) {
    info!("started");
    info!("finished");
}

async fn cleanup(_actors: TestActors) {
    info!("started");
    info!("finished");
}

async fn dummy(_actors: TestActors) {
    info!("started");
    info!("finished");
}
