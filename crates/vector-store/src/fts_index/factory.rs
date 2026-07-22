/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::IndexKey;
use crate::fts_index::actor::FtsIndex;
use crate::table::Table;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;

pub(crate) trait FtsIndexFactory {
    fn create_index(&self, key: IndexKey, table: Arc<RwLock<Table>>) -> mpsc::Sender<FtsIndex>;
}
