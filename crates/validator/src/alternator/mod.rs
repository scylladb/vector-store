/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod auth;
mod batch_write_item;
mod create_table;
mod delete_item;
mod lwt;
mod put_item;
mod query;
mod ttl;
mod types;
mod update_item;
mod update_table;

e2etest::group!(name = alternator, fixtures = (), parent = crate::validator);
