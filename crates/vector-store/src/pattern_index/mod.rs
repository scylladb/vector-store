/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod actor;
mod factory;
mod tantivy;

use crate::memory::Memory;
use crate::worker::Worker;
pub(crate) use actor::PatternIndex;
pub(crate) use actor::PatternIndexExt;
pub(crate) use factory::PatternIndexConfiguration;
pub(crate) use factory::PatternIndexFactory;
use tokio::sync::mpsc;

pub(crate) fn new_factory_tantivy(
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> Box<dyn PatternIndexFactory + Send + Sync> {
    Box::new(tantivy::new_factory(worker, memory))
}
