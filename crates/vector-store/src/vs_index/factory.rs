/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Connectivity;
use crate::Dimensions;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexKey;
use crate::Quantization;
use crate::SpaceType;
use crate::table::Table;
use crate::vs_index::actor::VsIndex;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;

pub(crate) struct VsIndexConfiguration {
    pub key: IndexKey,
    pub dimensions: Dimensions,
    pub connectivity: Connectivity,
    pub expansion_add: ExpansionAdd,
    pub expansion_search: ExpansionSearch,
    pub space_type: SpaceType,
    pub quantization: Quantization,
}

pub(crate) trait VsIndexFactory {
    fn create_index(
        &self,
        index: VsIndexConfiguration,
        table: Arc<RwLock<Table>>,
    ) -> anyhow::Result<mpsc::Sender<VsIndex>>;
    fn index_engine_version(&self) -> String;
}
