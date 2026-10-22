/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod actor;
mod factory;
mod tantivy;

use crate::memory::Memory;
use crate::metrics::Metrics;
use crate::worker::Worker;
pub(crate) use actor::FtsIndex;
pub(crate) use actor::FtsIndexExt;
pub(crate) use factory::FtsIndexConfiguration;
pub(crate) use factory::FtsIndexFactory;
use std::sync::Arc;
use tantivy::TantivyIndexFactory;
use tokio::sync::mpsc;

pub(crate) fn new_fts_index_factory_tantivy(
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
    metrics: Arc<Metrics>,
) -> Box<dyn FtsIndexFactory + Send + Sync> {
    Box::new(TantivyIndexFactory::new(worker, memory, metrics))
}
