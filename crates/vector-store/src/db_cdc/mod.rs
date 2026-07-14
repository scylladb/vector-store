/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

pub(crate) mod actor;
mod consumer;

pub(crate) const READER_WIDE: &str = "wide";
pub(crate) const READER_FINE: &str = "fine";

pub(crate) use actor::CdcReaderConfig;
pub(crate) use actor::new;
