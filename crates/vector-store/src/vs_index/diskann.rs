/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Config;
use crate::VsIndexFactory;
use crate::memory::Memory;
use crate::perf;
use crate::table::Table;
use crate::vs_index::actor::VsIndex;
use crate::vs_index::factory::VsIndexConfiguration;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::warn;

pub struct DiskannIndexFactory;

impl VsIndexFactory for DiskannIndexFactory {
    fn create_index(
        &self,
        index: VsIndexConfiguration,
        _table: Arc<RwLock<Table>>,
        _memory: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<VsIndex>> {
        new(index.key)
    }

    fn index_engine_version(&self) -> String {
        format!("diskann-{}", diskann::version())
    }
}

pub fn new_diskann(
    _config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<DiskannIndexFactory> {
    Ok(DiskannIndexFactory)
}

fn new(index_key: crate::IndexKey) -> anyhow::Result<mpsc::Sender<VsIndex>> {
    let (tx, mut rx) = mpsc::channel(perf::channel_size().into());

    tokio::spawn(perf::hotpath_async(
        {
            async move {
                debug!("starting");

                while let Some(msg) = rx.recv().await {
                    match msg {
                        VsIndex::AddVector { .. }
                        | VsIndex::RemoveVector { .. }
                        | VsIndex::RemovePartition { .. } => {
                            warn!("not implemented yet");
                        }
                        VsIndex::Ann { tx, .. } | VsIndex::FilteredAnn { tx, .. } => {
                            _ = tx
                                .send(Err(anyhow::anyhow!("DiskANN index is not implemented yet")));
                        }
                        VsIndex::Count { tx, .. } => {
                            _ = tx
                                .send(Err(anyhow::anyhow!("DiskANN index is not implemented yet")));
                        }
                    }
                }

                debug!("finished");
            }
        }
        .instrument(debug_span!("diskann", "{index_key}")),
    ));

    Ok(tx)
}
