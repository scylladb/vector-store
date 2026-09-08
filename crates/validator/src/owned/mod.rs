/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Groups that cannot share a cluster and start one of their own.

mod auth;
mod connection_timeout;
mod db_timeout;
mod high_availability;
mod reconnect;
mod tls_reload;

e2etest::group!(name = owned, fixtures = (), parent = crate::validator);
