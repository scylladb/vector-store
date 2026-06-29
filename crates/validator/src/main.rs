/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    vector_search_validator::run().await
}
