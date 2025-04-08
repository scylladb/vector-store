/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

pub mod actor;

pub(crate) use actor::Index;
pub(crate) use actor::IndexExt;
#[cfg(feature = "opensearch")]
pub(crate) mod opensearch;
#[cfg(not(feature = "opensearch"))]
pub(crate) mod usearch;

#[cfg(feature = "opensearch")]
pub(crate) use opensearch::new;
#[cfg(not(feature = "opensearch"))]
pub(crate) use usearch::new;
