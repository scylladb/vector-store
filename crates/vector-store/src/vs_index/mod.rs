/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

pub mod actor;
pub mod factory;
pub mod validator;

pub(crate) use actor::VsIndex;
pub(crate) use actor::VsIndexExt;
pub(crate) use validator::Error;

#[allow(dead_code)]
pub(crate) mod diskann;
pub(crate) mod opensearch;
pub(crate) mod usearch;
