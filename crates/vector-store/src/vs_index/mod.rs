/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod actor;
mod diskann;
mod factory;
mod opensearch;
mod usearch;
mod validator;

use crate::Config;
use crate::memory::Memory;
use crate::worker::Worker;
pub(crate) use actor::CountR;
pub(crate) use actor::VsIndex;
pub(crate) use actor::VsIndexExt;
pub(crate) use factory::VsIndexConfiguration;
pub(crate) use factory::VsIndexFactory;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
pub(crate) use validator::Error;

pub(crate) fn new_index_factory_usearch(
    config_tx: watch::Receiver<Arc<Config>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> anyhow::Result<Box<dyn VsIndexFactory + Send + Sync>> {
    Ok(Box::new(usearch::new_usearch(config_tx, worker, memory)?))
}

pub(crate) fn new_index_factory_opensearch(
    addr: String,
    config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<Box<dyn VsIndexFactory + Send + Sync>> {
    Ok(Box::new(opensearch::new_opensearch(&addr, config_rx)?))
}

pub(crate) fn new_index_factory_diskann(
    config_rx: watch::Receiver<Arc<Config>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> anyhow::Result<Box<dyn VsIndexFactory + Send + Sync>> {
    Ok(Box::new(diskann::new_diskann(config_rx, worker, memory)?))
}
