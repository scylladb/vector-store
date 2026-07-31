/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Config;
use crate::Dimensions;
use crate::DiskannAlpha;
use crate::IndexKey;
use crate::PrimaryId;
use crate::SpaceType;
use crate::VsIndexFactory;
use crate::memory::Allocate;
use crate::memory::Memory;
use crate::memory::MemoryExt;
use crate::perf;
use crate::table::Table;
use crate::vs_index::actor::VsIndex;
use crate::vs_index::factory::VsIndexConfiguration;
use crate::worker::Worker;
use anyhow::Context;
use diskann::graph::Config as DiskannConfig;
use diskann::graph::DiskANNIndex;
use diskann::graph::config::Builder;
use diskann::graph::config::MaxDegree;
use diskann::graph::config::defaults::ALPHA as DISKANN_DEFAULT_ALPHA;
use diskann_inmem::Provider as InmemProvider;
use diskann_inmem::layers::Full;
use diskann_inmem::provider::Config as InmemProviderConfig;
use diskann_vector::distance::Metric;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::warn;

const MAX_POINTS: NonZeroUsize = NonZeroUsize::new(1_000_000).unwrap();

type DiskannProvider = InmemProvider<Full<f32>, PrimaryId>;

pub struct DiskannIndexFactory {
    _worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
    alpha: DiskannAlpha,
}

impl VsIndexFactory for DiskannIndexFactory {
    fn create_index(
        &self,
        index: VsIndexConfiguration,
        _table: Arc<RwLock<Table>>,
    ) -> anyhow::Result<mpsc::Sender<VsIndex>> {
        let params = DiskannParams::new(&index, self.alpha, MAX_POINTS)?;
        let layer = Full::<f32>::new(usize::from(params.dim.0), params.metric);
        let cfg = InmemProviderConfig::new(
            usize::from(params.max_points),
            params.config.max_degree().get(),
        );
        let provider = InmemProvider::<_, PrimaryId>::new(layer, cfg, vec![])
            .context("failed to create InmemProvider")?;
        let diskann_index = DiskANNIndex::new(params.config, provider, None);

        new(index.key, diskann_index, self.memory.clone())
    }

    fn index_engine_version(&self) -> String {
        format!("diskann-{}", diskann::version())
    }
}

pub fn new_diskann(
    mut config_rx: watch::Receiver<Arc<Config>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> anyhow::Result<DiskannIndexFactory> {
    let config = config_rx.borrow_and_update();

    Ok(DiskannIndexFactory {
        _worker: worker,
        memory,
        alpha: config
            .diskann_alpha
            .unwrap_or(DiskannAlpha::new(DISKANN_DEFAULT_ALPHA).unwrap()),
    })
}

fn new(
    index_key: crate::IndexKey,
    index: DiskANNIndex<DiskannProvider>,
    memory: mpsc::Sender<Memory>,
) -> anyhow::Result<mpsc::Sender<VsIndex>> {
    let (tx, mut rx) = mpsc::channel(perf::channel_size().into());

    let span_key = index_key.clone();

    tokio::spawn(perf::hotpath_async(
        {
            async move {
                debug!("starting");

                let mut allocate_prev = Allocate::Can;
                let allocate_rx = memory.subscribe_allocate().await;

                while let Some(msg) = rx.recv().await {
                    if !check_memory_allocation(&msg, &allocate_rx, &mut allocate_prev, &index_key)
                    {
                        continue;
                    }

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
                drop(index);

                debug!("finished");
            }
        }
        .instrument(debug_span!("diskann", "{span_key}")),
    ));

    Ok(tx)
}

#[hotpath::measure]
fn check_memory_allocation(
    msg: &VsIndex,
    rx_allocate: &watch::Receiver<Allocate>,
    allocate_prev: &mut Allocate,
    key: &IndexKey,
) -> bool {
    if !matches!(msg, VsIndex::AddVector { .. }) {
        return true;
    }

    let allocate = *rx_allocate.borrow();
    if allocate == Allocate::Cannot {
        if *allocate_prev == Allocate::Can {
            error!("Unable to add vector for index {key}: not enough memory to reserve more space");
        }
        *allocate_prev = allocate;
        return false;
    }
    *allocate_prev = allocate;
    true
}
#[derive(Clone)]
struct DiskannParams {
    config: DiskannConfig,
    metric: Metric,
    dim: Dimensions,
    max_points: NonZeroUsize,
}

impl DiskannParams {
    fn new(
        cfg: &VsIndexConfiguration,
        alpha: DiskannAlpha,
        max_points: NonZeroUsize,
    ) -> anyhow::Result<Self> {
        let metric: Metric = cfg.space_type.try_into()?;

        let mut builder = Builder::new(
            cfg.connectivity.0,
            MaxDegree::default_slack(),
            cfg.expansion_add.0,
            metric.into(),
        );

        builder.alpha(alpha.get());

        let config = builder
            .build()
            .context("failed to build DiskANN configuration")?;

        Ok(Self {
            config,
            metric,
            dim: cfg.dimensions,
            max_points,
        })
    }
}

impl TryFrom<SpaceType> for Metric {
    type Error = anyhow::Error;

    fn try_from(space_type: SpaceType) -> anyhow::Result<Self> {
        match space_type {
            SpaceType::Euclidean => Ok(Self::L2),
            SpaceType::Cosine => Ok(Self::Cosine),
            SpaceType::DotProduct => Ok(Self::InnerProduct),
            SpaceType::Hamming => {
                anyhow::bail!("DiskANN does not support Hamming space type")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connectivity;
    use crate::ExpansionAdd;
    use crate::ExpansionSearch;
    use crate::IndexKey;
    use crate::IndexName;
    use crate::KeyspaceName;
    use crate::Quantization;

    #[test]
    fn diskann_metric_try_from_space_type() {
        assert_eq!(Metric::try_from(SpaceType::Euclidean).unwrap(), Metric::L2);
        assert_eq!(Metric::try_from(SpaceType::Cosine).unwrap(), Metric::Cosine);
        assert_eq!(
            Metric::try_from(SpaceType::DotProduct).unwrap(),
            Metric::InnerProduct
        );
        assert!(Metric::try_from(SpaceType::Hamming).is_err());
    }

    #[test]
    fn diskann_params_try_from_index_configuration() {
        let vs_config = VsIndexConfiguration {
            key: IndexKey::new(
                &KeyspaceName::from("ks".to_string()),
                &IndexName::from("tbl".to_string()),
            ),
            dimensions: NonZeroUsize::new(3).unwrap().into(),
            connectivity: Connectivity(16),
            expansion_add: ExpansionAdd(64),
            expansion_search: ExpansionSearch(32),
            space_type: SpaceType::Euclidean,
            quantization: Quantization::F32,
        };

        let params = DiskannParams::new(
            &vs_config,
            DiskannAlpha::new(DISKANN_DEFAULT_ALPHA).unwrap(),
            MAX_POINTS,
        )
        .unwrap();

        assert_eq!(
            params.config.pruned_degree(),
            NonZeroUsize::new(16).unwrap()
        );
        assert_eq!(usize::from(params.dim.0), 3);
        assert_eq!(params.config.l_build(), NonZeroUsize::new(64).unwrap());
        assert_eq!(params.metric, Metric::L2);
    }
}
