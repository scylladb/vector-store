/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod actor;
mod factory;
mod tantivy;

use crate::memory::Memory;
use crate::worker::Worker;
pub(crate) use actor::FtsIndex;
pub(crate) use actor::FtsIndexExt;
pub(crate) use factory::FtsIndexFactory;
use tantivy::TantivyIndexFactory;
use tokio::sync::mpsc;

pub(crate) fn new_fts_index_factory_tantivy(
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> Box<dyn FtsIndexFactory + Send + Sync> {
    Box::new(TantivyIndexFactory::new(worker, memory))
}
