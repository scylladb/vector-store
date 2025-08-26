/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Connectivity;
use crate::Dimensions;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexId;
use crate::SpaceType;
use crate::index::actor::Index;
use tokio::sync::mpsc;

pub trait IndexFactory {
    fn create_index(
        &self,
        id: IndexId,
        dimensions: Dimensions,
        connectivity: Connectivity,
        expansion_add: ExpansionAdd,
        expansion_search: ExpansionSearch,
        space_type: SpaceType,
    ) -> anyhow::Result<mpsc::Sender<Index>>;
    fn index_engine_version(&self) -> String;
}
