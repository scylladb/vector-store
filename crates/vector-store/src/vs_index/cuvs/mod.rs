/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! GPU-accelerated vector index backed by NVIDIA cuVS (CAGRA).
//!
//! # Threading
//!
//! cuVS handles are raw pointers, so they are neither `Send` nor `Sync`, and
//! every cuVS call blocks the calling thread. Both facts rule out the shared
//! [`crate::worker`] pool, which requires `Send` closures and runs them inline
//! on a runtime worker.
//!
//! So each index owns a dedicated thread. The async actor keeps the usual
//! search-over-modify priority and forwards messages to that thread over a
//! bounded channel; the thread creates all cuVS state inside its own closure, so
//! nothing that is not `Send` ever crosses a thread boundary and no `unsafe` is
//! needed to make it legal. Replies go straight back on the `oneshot::Sender`
//! carried in each search message.
//!
//! Upstream permits sharing an index across threads when each has its own
//! `raft::resources`, so this can grow into a pool of GPU threads later.

mod params;

use crate::Config;
use crate::IndexKey;
use crate::VsIndexFactory;
use crate::perf;
use crate::table::Table;
use crate::vs_index;
use crate::vs_index::Message;
use crate::vs_index::VsIndexModify;
use crate::vs_index::VsIndexSearch;
use crate::vs_index::factory::VsIndexConfiguration;
use anyhow::anyhow;
use params::CagraParams;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::warn;

pub struct CuvsIndexFactory;

impl VsIndexFactory for CuvsIndexFactory {
    fn create_index(
        &self,
        index: VsIndexConfiguration,
        _table: Arc<RwLock<Table>>,
    ) -> anyhow::Result<(mpsc::Sender<VsIndexModify>, mpsc::Sender<VsIndexSearch>)> {
        // Validated here rather than on the cuVS thread so a bad index option is
        // reported when the index is created, not silently much later.
        let params = CagraParams::try_from(&index)?;
        new(index.key, params)
    }

    fn index_engine_version(&self) -> String {
        match cuvs::version::version() {
            Ok((major, minor, patch)) => format!("cuvs-{major}.{minor}.{patch}"),
            Err(err) => format!("cuvs-unknown ({err})"),
        }
    }
}

pub fn new_cuvs(_config_rx: watch::Receiver<Arc<Config>>) -> anyhow::Result<CuvsIndexFactory> {
    // Fail at startup rather than at the first index creation if there is no
    // usable GPU.
    cuvs::Resources::new()
        .map_err(|err| anyhow!("failed to initialize cuVS/CUDA resources: {err}"))?;
    Ok(CuvsIndexFactory)
}

fn new(
    index_key: IndexKey,
    params: CagraParams,
) -> anyhow::Result<(mpsc::Sender<VsIndexModify>, mpsc::Sender<VsIndexSearch>)> {
    let channel_size = perf::channel_size().into();
    let (tx_modify, mut rx_modify) = mpsc::channel(channel_size);
    let (tx_search, mut rx_search) = mpsc::channel(channel_size);
    let (tx_gpu, mut rx_gpu) = mpsc::channel(channel_size);

    let thread_key = index_key.clone();
    thread::Builder::new()
        .name(format!("cuvs-{index_key}"))
        .spawn(move || {
            // All cuVS state is created here and never leaves this thread. The
            // parameters are held for as long as the index lives, because that
            // is what the graph will be built from.
            let _index_params = match params.to_index_params() {
                Ok(index_params) => index_params,
                Err(err) => {
                    error!("unable to create cuVS index for {thread_key}: {err}");
                    // Draining keeps senders from blocking; every search gets an
                    // error rather than hanging.
                    while let Some(msg) = rx_gpu.blocking_recv() {
                        reject(msg, || anyhow!("cuVS index is unavailable: {err}"));
                    }
                    return;
                }
            };

            debug!("cuVS thread starting for {thread_key}");
            while let Some(msg) = rx_gpu.blocking_recv() {
                handle(msg);
            }
            debug!("cuVS thread finished for {thread_key}");
        })
        .map_err(|err| anyhow!("unable to spawn cuVS thread for {index_key}: {err}"))?;

    let span_key = index_key.clone();
    tokio::spawn(perf::hotpath_async(
        async move {
            debug!("starting");

            while let Some(msg) = vs_index::recv(&mut rx_search, &mut rx_modify).await {
                if tx_gpu.send(msg).await.is_err() {
                    break;
                }
            }

            debug!("finished");
        }
        .instrument(debug_span!("cuvs", "{span_key}")),
    ));

    Ok((tx_modify, tx_search))
}

/// Answers a message with an error, for when the index cannot serve it at all.
fn reject(msg: Message, err: impl Fn() -> anyhow::Error) {
    match msg {
        Message::Search(VsIndexSearch::Ann { tx, .. } | VsIndexSearch::FilteredAnn { tx, .. }) => {
            _ = tx.send(Err(err()));
        }
        Message::Search(VsIndexSearch::Count { tx, .. }) => {
            _ = tx.send(Err(err()));
        }
        Message::Modify(_) => {}
    }
}

fn handle(msg: Message) {
    match msg {
        Message::Modify(
            VsIndexModify::AddVector { .. }
            | VsIndexModify::RemoveVector { .. }
            | VsIndexModify::RemovePartition { .. },
        ) => {
            warn!("not implemented yet");
        }
        Message::Search(_) => {
            reject(msg, || anyhow!("GPU index is not implemented yet"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connectivity;
    use crate::Dimensions;
    use crate::ExpansionAdd;
    use crate::ExpansionSearch;
    use crate::IndexKey;
    use crate::NonemptyArc;
    use crate::Quantization;
    use crate::SpaceType;
    use crate::vs_index::VsIndexSearchExt;
    use scylla::cluster::metadata::NativeType;
    use std::collections::HashMap;
    use std::num::NonZeroUsize;

    #[test]
    fn index_engine_version_reports_cuvs_library_version() {
        let factory = CuvsIndexFactory;
        let (major, minor, patch) = cuvs::version::version().unwrap();
        assert_eq!(
            factory.index_engine_version(),
            format!("cuvs-{major}.{minor}.{patch}")
        );
    }

    #[tokio::test]
    async fn create_index_returns_actor_that_reports_not_yet_implemented() {
        let factory = CuvsIndexFactory;

        let index_key = IndexKey::new(&"vector".into(), &"store".into());
        let table = Arc::new(RwLock::new(
            Table::new(
                index_key.clone(),
                NonemptyArc::new(["pk"]).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                None,
                NonZeroUsize::new(1).unwrap(),
                Arc::new([]),
                Arc::new(HashMap::from([("pk".into(), NativeType::Int)])),
            )
            .unwrap(),
        ));

        let (_modify, search) = factory
            .create_index(
                VsIndexConfiguration {
                    key: index_key.clone(),
                    dimensions: Dimensions::from(NonZeroUsize::new(3).unwrap()),
                    connectivity: Connectivity::default(),
                    expansion_add: ExpansionAdd::default(),
                    expansion_search: ExpansionSearch::default(),
                    space_type: SpaceType::default(),
                    quantization: Quantization::default(),
                },
                table,
            )
            .expect("index creation itself should succeed");

        let err = search.count(index_key).await.unwrap_err().to_string();
        assert!(err.contains("not implemented yet"));
    }
}
