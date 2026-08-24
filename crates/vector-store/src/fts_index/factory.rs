/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Analyzer;
use crate::IndexKey;
use crate::Positions;
use crate::fts_index::actor::FtsIndex;
use crate::table::Table;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub(crate) struct FtsIndexConfiguration {
    pub key: IndexKey,
    pub analyzer: Analyzer,
    pub positions: Positions,
}

pub(crate) trait FtsIndexFactory {
    fn create_index(
        &self,
        index: FtsIndexConfiguration,
        table: Arc<RwLock<Table>>,
    ) -> mpsc::Sender<FtsIndex>;
}
