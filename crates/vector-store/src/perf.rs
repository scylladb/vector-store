/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use tokio::task::Unconstrained;
use tokio::task::coop;

pub(crate) fn hotpath_async<F>(inner: F) -> Unconstrained<F> {
    coop::unconstrained(inner)
}
