/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod actor;
#[cfg(feature = "gpu")]
mod cuvs;
mod diskann;
mod factory;
mod opensearch;
mod usearch;
mod validator;

use crate::Config;
use crate::memory::Memory;
use crate::worker::Worker;
use actor::AnnR;
pub(crate) use actor::CountR;
use actor::Message;
pub(crate) use actor::VsIndexModify;
pub(crate) use actor::VsIndexModifyExt;
pub(crate) use actor::VsIndexSearch;
pub(crate) use actor::VsIndexSearchExt;
pub(crate) use factory::VsIndexConfiguration;
pub(crate) use factory::VsIndexFactory;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
pub(crate) use validator::Error;

async fn recv(
    rx_search: &mut mpsc::Receiver<VsIndexSearch>,
    rx_modify: &mut mpsc::Receiver<VsIndexModify>,
) -> Option<Message> {
    tokio::select! {
        // The order of the select branches is important. We want to prioritize search messages
        // over modify messages. We shouldn't starve modify messages since tokio runtime uses a
        // fair scheduler. From observations, it is visible that providing new search requests is
        // working in waves: first buffered then fully consumed.
        biased;

        Some(msg) = rx_search.recv() => Some(Message::Search(msg)),
        Some(msg) = rx_modify.recv() => Some(Message::Modify(msg)),
        else => None,
    }
}

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

#[cfg(feature = "gpu")]
pub(crate) fn new_index_factory_cuvs(
    config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<Box<dyn VsIndexFactory + Send + Sync>> {
    Ok(Box::new(cuvs::new_cuvs(config_rx)?))
}

#[cfg(not(feature = "gpu"))]
pub(crate) fn new_index_factory_cuvs(
    _config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<Box<dyn VsIndexFactory + Send + Sync>> {
    Err(anyhow::anyhow!(
        "VECTOR_STORE_USE_GPU is set but this vector-store binary was built without GPU support \
         (missing the `gpu` Cargo feature).
         Install libcuvs (see scripts/setup-gpu) and rebuild with `--features gpu`, \
         or unset VECTOR_STORE_USE_GPU to use the default USearch backend."
    ))
}

#[cfg(all(test, not(feature = "gpu")))]
mod tests {
    use super::*;

    #[test]
    fn new_index_factory_cuvs_fails_without_gpu_feature() {
        let (_, config_rx) = watch::channel(Arc::new(Config::default()));
        let err = match new_index_factory_cuvs(config_rx) {
            Ok(_) => panic!("expected an error when the `gpu` feature is disabled"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("built without GPU support"));
        assert!(err.contains("--features gpu"));
    }
}
