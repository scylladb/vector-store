/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use tokio::runtime::Handle;
use tokio::task::Unconstrained;
use tokio::task::coop;

pub(crate) fn hotpath_async<F>(inner: F) -> Unconstrained<F> {
    coop::unconstrained(inner)
}

pub(crate) fn num_workers() -> usize {
    Handle::current().metrics().num_workers()
}

pub(crate) fn channel_size() -> usize {
    const CHANNEL_SIZE_PER_WORKER: usize = 3;
    num_workers() * CHANNEL_SIZE_PER_WORKER
}
