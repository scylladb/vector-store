/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod actor;
mod factory;
mod tantivy;

pub(crate) use actor::FtsIndex;
pub(crate) use actor::FtsIndexExt;
pub(crate) use factory::FtsIndexFactory;
use tantivy::TantivyIndexFactory;

pub(crate) fn new_fts_index_factory_tantivy() -> Box<dyn FtsIndexFactory + Send + Sync> {
    Box::new(TantivyIndexFactory::new())
}
