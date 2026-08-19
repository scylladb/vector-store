/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use super::DiskannBackend;
use super::DiskannParams;
use crate::PrimaryId;
use anyhow::Context;
use diskann::graph::DiskANNIndex;
use diskann_inmem::Context as InmemContext;
use diskann_inmem::Provider as InmemProvider;
use diskann_inmem::Strategy as InmemStrategy;
use diskann_inmem::layers::Full;
use diskann_inmem::provider::Config as InmemProviderConfig;

/// The fully in-memory DiskANN backend: both vectors and adjacency are
/// resident in RAM.
#[derive(Clone)]
pub(super) struct InmemBackend {
    strategy: InmemStrategy,
    context: InmemContext,
}

impl InmemBackend {
    pub(super) fn new() -> Self {
        Self {
            strategy: InmemStrategy,
            context: InmemContext,
        }
    }
}

impl DiskannBackend for InmemBackend {
    type Provider = InmemProvider<Full<f32>, PrimaryId>;
    type Strategy = InmemStrategy;

    fn create_index(
        &self,
        params: &DiskannParams,
        start_point: &[f32],
    ) -> anyhow::Result<DiskANNIndex<Self::Provider>> {
        let layer = Full::<f32>::new(usize::from(params.dim.0), params.metric);
        let cfg = InmemProviderConfig::new(
            usize::from(params.max_points),
            params.config.max_degree().get(),
        );
        let provider = InmemProvider::<_, PrimaryId>::new(layer, cfg, [start_point])
            .context("failed to create InmemProvider")?;
        Ok(DiskANNIndex::new(params.config.clone(), provider, None))
    }

    fn strategy(&self) -> &InmemStrategy {
        &self.strategy
    }

    fn context(&self) -> &InmemContext {
        &self.context
    }
}
