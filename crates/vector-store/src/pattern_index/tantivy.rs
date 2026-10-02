/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::memory::Memory;
use crate::pattern_index::PatternIndex;
use crate::pattern_index::PatternIndexConfiguration;
use crate::pattern_index::PatternIndexFactory;
use crate::perf;
use crate::table::Table;
use crate::table::TableSearch;
use crate::worker::Worker;
use anyhow::anyhow;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tracing::debug;

pub(crate) struct Factory {
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
}

impl PatternIndexFactory for Factory {
    fn create_index(
        &self,
        index: PatternIndexConfiguration,
        table: Arc<RwLock<Table>>,
    ) -> mpsc::Sender<PatternIndex> {
        new(index, table, self.worker.clone(), self.memory.clone())
    }
}

pub(super) fn new_factory(
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> Factory {
    Factory { worker, memory }
}

pub(crate) fn new(
    config: PatternIndexConfiguration,
    _table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    _worker: async_channel::Sender<Worker>,
    _memory: mpsc::Sender<Memory>,
) -> mpsc::Sender<PatternIndex> {
    let (tx, mut rx) = mpsc::channel::<PatternIndex>(perf::channel_size().into());
    tokio::spawn(async move {
        let key = config.key.clone();
        debug!("pattern index actor starting for {key}");

        while let Some(msg) = rx.recv().await {
            match msg {
                PatternIndex::AddDocument {
                    primary_id: _primary_id,
                    document: _document,
                    in_progress: _in_progress,
                } => {
                    // TODO: implement add document logic
                }
                PatternIndex::RemoveDocument {
                    primary_id: _primary_id,
                    in_progress: _in_progress,
                } => {
                    // TODO: implement remove document logic
                }
                PatternIndex::Count {
                    index_key: _index_key,
                    tx,
                } => {
                    _ = tx.send(Ok(0));
                }
                PatternIndex::Like {
                    index_key: _index_key,
                    pattern: _pattern,
                    limit: _limit,
                    tx,
                } => {
                    _ = tx.send(Err(anyhow!("pattern: like not implemented")));
                }
            }
        }

        debug!("pattern index actor finished for {key}");
    });
    tx
}
