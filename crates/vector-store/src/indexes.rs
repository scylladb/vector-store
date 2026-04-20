/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::IndexKey;
use crate::Progress;
use crate::Quantization;
use crate::db_index::DbIndex;
use crate::db_index::DbIndexExt;
use crate::index::Index;
use crate::monitor_items::MonitorItems;
use scylla::cluster::metadata::NativeType;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) struct IndexCache {
    index: mpsc::Sender<Index>,
    _monitor_items: mpsc::Sender<MonitorItems>,
    db_index: mpsc::Sender<DbIndex>,
    quantization: Quantization,
    primary_key_columns: Arc<Vec<ColumnName>>,
    table_columns: Arc<HashMap<ColumnName, NativeType>>,
    progress: Progress,
}

impl IndexCache {
    pub(crate) async fn new(
        index: mpsc::Sender<Index>,
        monitor_items: mpsc::Sender<MonitorItems>,
        db_index: mpsc::Sender<DbIndex>,
        quantization: Quantization,
    ) -> Self {
        let primary_key_columns = db_index.get_primary_key_columns().await;
        let table_columns = db_index.get_table_columns().await;
        let progress = db_index.full_scan_progress().await;
        Self {
            index,
            _monitor_items: monitor_items,
            db_index,
            quantization,
            primary_key_columns,
            table_columns,
            progress,
        }
    }

    pub(crate) fn index(&self) -> mpsc::Sender<Index> {
        self.index.clone()
    }

    pub(crate) fn db_index(&self) -> mpsc::Sender<DbIndex> {
        self.db_index.clone()
    }

    pub(crate) fn quantization(&self) -> Quantization {
        self.quantization
    }

    pub(crate) fn primary_key_columns(&self) -> Arc<Vec<ColumnName>> {
        Arc::clone(&self.primary_key_columns)
    }

    pub(crate) fn table_columns(&self) -> Arc<HashMap<ColumnName, NativeType>> {
        Arc::clone(&self.table_columns)
    }

    pub(crate) fn progress(&self) -> Progress {
        self.progress
    }

    pub(crate) fn set_progress(&mut self, progress: Progress) {
        self.progress = progress;
    }
}

pub(crate) struct Indexes(HashMap<IndexKey, IndexCache>);

impl Indexes {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn contains_key(&self, key: &IndexKey) -> bool {
        self.0.contains_key(key)
    }

    pub(crate) fn get(&self, key: &IndexKey) -> Option<&IndexCache> {
        self.0.get(key)
    }

    pub(crate) fn get_mut(&mut self, key: &IndexKey) -> Option<&mut IndexCache> {
        self.0.get_mut(key)
    }

    pub(crate) fn insert(&mut self, key: IndexKey, value: IndexCache) {
        self.0.insert(key, value);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&IndexKey, &IndexCache)> {
        self.0.iter()
    }

    pub(crate) fn remove(&mut self, key: &IndexKey) {
        self.0.remove(key);
    }
}
