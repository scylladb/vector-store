/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Connectivity;
use crate::Dimensions;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexId;
use crate::Quantization;
use crate::SpaceType;
use crate::index::actor::Index;
use crate::memory::Memory;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct IndexConfiguration {
    pub id: IndexId,
    pub dimensions: Dimensions,
    pub connectivity: Connectivity,
    pub expansion_add: ExpansionAdd,
    pub expansion_search: ExpansionSearch,
    pub space_type: SpaceType,
    pub quantization: Quantization,
}

pub trait IndexFactory {
    fn create_index(
        &self,
        index: IndexConfiguration,
        primary_key_columns: Arc<Vec<ColumnName>>,
        memory: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<Index>>;
    fn index_engine_version(&self) -> String;
}
