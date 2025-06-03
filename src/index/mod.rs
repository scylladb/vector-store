/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

pub mod actor;
pub mod factory;

pub(crate) use actor::Index;
pub(crate) use actor::IndexExt;

pub(crate) mod opensearch;
pub(crate) mod usearch;
