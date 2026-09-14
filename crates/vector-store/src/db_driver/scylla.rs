/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::db_driver::DbDriver;

#[derive(Clone, Debug)]
struct ScyllaDriver;

impl DbDriver for ScyllaDriver {}

pub(super) fn new() -> impl DbDriver {
    ScyllaDriver
}
