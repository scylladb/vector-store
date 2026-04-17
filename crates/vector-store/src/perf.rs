/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use std::num::NonZeroUsize;
use tokio::runtime::Handle;
use tokio::task::Unconstrained;
use tokio::task::coop;

pub(crate) fn hotpath_async<F>(inner: F) -> Unconstrained<F> {
    coop::unconstrained(inner)
}

pub(crate) fn num_workers() -> NonZeroUsize {
    NonZeroUsize::new(Handle::current().metrics().num_workers())
        .unwrap_or_else(|| NonZeroUsize::new(1).unwrap())
}

pub(crate) fn channel_size() -> NonZeroUsize {
    const CHANNEL_SIZE_PER_WORKER: NonZeroUsize = NonZeroUsize::new(3).unwrap();
    num_workers()
        .checked_mul(CHANNEL_SIZE_PER_WORKER)
        .unwrap_or_else(|| NonZeroUsize::new(1).unwrap())
}
