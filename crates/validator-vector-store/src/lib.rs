/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod ann;
mod auth;
mod crud;
mod db_timeout;
mod full_scan;
mod high_availability;
mod index_status;
mod reconnect;
mod serde;

use async_backtrace::framed;
use vector_search_validator_tests::TestCase;

#[framed]
pub async fn test_cases() -> impl Iterator<Item = (String, TestCase)> {
    vec![
        ("ann", ann::new().await),
        ("auth", auth::new().await),
        ("crud", crud::new().await),
        ("db_timeout", db_timeout::new().await),
        ("full_scan", full_scan::new().await),
        ("high_availability", high_availability::new().await),
        ("index_status", index_status::new().await),
        ("reconnect", reconnect::new().await),
        ("serde", serde::new().await),
    ]
    .into_iter()
    .map(|(name, test_case)| (name.to_string(), test_case))
}
