/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Config;
use crate::Dimensions;
use crate::DiskannAlpha;
use crate::Distance;
use crate::IndexKey;
use crate::Limit;
use crate::PartitionId;
use crate::PrimaryId;
use crate::SpaceType;
use crate::Vector;
use crate::VsIndexFactory;
use crate::memory::Allocate;
use crate::memory::Memory;
use crate::memory::MemoryExt;
use crate::perf;
use crate::table::IndexId;
use crate::table::Table;
use crate::table::TableSearch;
use crate::vs_index::actor::AnnR;
use crate::vs_index::actor::CountR;
use crate::vs_index::actor::VsIndex;
use crate::vs_index::factory::VsIndexConfiguration;
use crate::vs_index::validator;
use crate::worker::Worker;
use anyhow::Context;
use diskann::graph::Config as DiskannConfig;
use diskann::graph::DiskANNIndex;
use diskann::graph::InplaceDeleteMethod;
use diskann::graph::config::Builder;
use diskann::graph::config::MaxDegree;
use diskann::graph::config::defaults::ALPHA as DISKANN_DEFAULT_ALPHA;
use diskann::graph::index::SearchStats;
use diskann::graph::search::Knn;
use diskann::neighbor::Neighbor;
use diskann_inmem::Context as InmemContext;
use diskann_inmem::Provider as InmemProvider;
use diskann_inmem::Strategy as InmemStrategy;
use diskann_inmem::layers::Full;
use diskann_inmem::provider::Config as InmemProviderConfig;
use diskann_vector::distance::Metric;
use itertools::Itertools;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::trace;
use tracing::warn;

const MAX_POINTS: NonZeroUsize = NonZeroUsize::new(1_000_000).unwrap();

type DiskannProvider = InmemProvider<Full<f32>, PrimaryId>;

type DiskannIndex = DiskANNIndex<DiskannProvider>;

pub struct DiskannIndexFactory {
    _worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
    alpha: DiskannAlpha,
}

impl VsIndexFactory for DiskannIndexFactory {
    fn create_index(
        &self,
        index: VsIndexConfiguration,
        table: Arc<RwLock<Table>>,
    ) -> anyhow::Result<mpsc::Sender<VsIndex>> {
        let params = DiskannParams::new(&index, self.alpha, MAX_POINTS)
            .context("failed to create DiskANN parameters")?;

        new(index.key, self.memory.clone(), table, params)
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
    index_key: IndexKey,
    memory: mpsc::Sender<Memory>,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    params: DiskannParams,
) -> anyhow::Result<mpsc::Sender<VsIndex>> {
    let (tx, mut rx) = mpsc::channel(perf::channel_size().into());

    let span_key = index_key.clone();

    tokio::spawn(perf::hotpath_async(
        {
            async move {
                debug!("starting");

                let mut state = State::new(table, params);

                let mut allocate_prev = Allocate::Can;
                let allocate_rx = memory.subscribe_allocate().await;

                while let Some(msg) = rx.recv().await {
                    if !check_memory_allocation(&msg, &allocate_rx, &mut allocate_prev, &index_key)
                    {
                        continue;
                    }

                    match msg {
                        VsIndex::AddVector {
                            partition_id,
                            primary_id,
                            embedding,
                            in_progress: _in_progress,
                        } => {
                            state.add_vector(partition_id, primary_id, embedding).await;
                        }
                        VsIndex::RemoveVector {
                            partition_id,
                            primary_id,
                            in_progress: _in_progress,
                        } => {
                            state.remove_vector(partition_id, primary_id).await;
                        }
                        VsIndex::RemovePartition { partition_id } => {
                            state.remove_partition(partition_id);
                        }
                        VsIndex::Ann {
                            index_key,
                            embedding,
                            limit,
                            tx,
                        } => {
                            if let Some(tx) = validate_dimensions(tx, &embedding, state.params.dim)
                            {
                                _ = tx.send(state.ann(index_key, embedding, limit).await);
                            }
                        }
                        VsIndex::FilteredAnn { tx, .. } => {
                            _ = tx.send(Err(anyhow::anyhow!(
                                "DiskANN index does not support filtered search"
                            )));
                        }
                        VsIndex::Count { index_key, tx } => {
                            _ = tx.send(state.count(&index_key));
                        }
                    }
                }

                debug!("finished");
            }
        }
        .instrument(debug_span!("diskann", "{span_key}")),
    ));

    Ok(tx)
}

fn create_diskann_index(
    params: &DiskannParams,
    start_point: Option<&[f32]>,
) -> anyhow::Result<DiskannIndex> {
    let layer = Full::<f32>::new(usize::from(params.dim.0), params.metric);
    let cfg = InmemProviderConfig::new(
        usize::from(params.max_points),
        params.config.max_degree().get(),
    );
    let start_points: Vec<&[f32]> = start_point.into_iter().collect();
    let provider = InmemProvider::<_, PrimaryId>::new(layer, cfg, start_points)
        .context("failed to create InmemProvider")?;
    Ok(DiskANNIndex::new(params.config.clone(), provider, None))
}

struct Partition {
    index: DiskANNIndex<DiskannProvider>,
}

struct State<T>
where
    T: TableSearch + Send + Sync + 'static,
{
    partitions: BTreeMap<PartitionId, Partition>,
    sizes: BTreeMap<IndexId, usize>,
    table: Arc<RwLock<T>>,
    params: DiskannParams,
}

impl<T> State<T>
where
    T: TableSearch + Send + Sync + 'static,
{
    fn new(table: Arc<RwLock<T>>, params: DiskannParams) -> Self {
        Self {
            partitions: BTreeMap::new(),
            sizes: BTreeMap::new(),
            table,
            params,
        }
    }

    fn get_or_create_partition(
        &mut self,
        partition_id: &PartitionId,
        start_point: Option<&[f32]>,
    ) -> anyhow::Result<&mut Partition> {
        match self.partitions.entry(*partition_id) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let index = create_diskann_index(&self.params, start_point).context(format!(
                    "failed to create index for partition {partition_id:?}"
                ))?;

                self.sizes.entry(partition_id.index_id()).or_insert(0);

                Ok(entry.insert(Partition { index }))
            }
        }
    }

    async fn add_vector(
        &mut self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
    ) {
        let partition =
            match self.get_or_create_partition(&partition_id, Some(embedding.as_slice())) {
                Ok(partition) => partition,
                Err(err) => {
                    warn!("add_vector failed: {err}");
                    return;
                }
            };
        if let Err(err) = partition
            .index
            .insert(
                &InmemStrategy,
                &InmemContext,
                &primary_id,
                embedding.as_slice(),
            )
            .await
        {
            warn!("add_vector: failed to insert vector: {err}");
        } else {
            let size = self.sizes.entry(partition_id.index_id()).or_insert(0);
            *size = size.saturating_add(1);
        }
    }

    async fn remove_vector(&mut self, partition_id: PartitionId, primary_id: PrimaryId) {
        let Some(partition) = self.partitions.get_mut(&partition_id) else {
            debug!("remove_vector: partition {partition_id:?} not found");
            return;
        };
        if let Err(err) = partition
            .index
            .inplace_delete(
                InmemStrategy,
                &InmemContext,
                &primary_id,
                self.params.config.pruned_degree().get(),
                InplaceDeleteMethod::OneHop,
            )
            .await
        {
            warn!("remove_vector: failed to delete vector: {err}");
        } else {
            let size = self.sizes.entry(partition_id.index_id()).or_insert(0);
            *size = size.saturating_sub(1);
        }
    }

    async fn search(
        &self,
        embedding: &Vector,
        k: usize,
        partition: &Partition,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(PrimaryId, Distance)>>> {
        let space_type = self.params.space_type;
        let dimensions = embedding.dim();

        let k = k.min(self.params.max_points.get());
        let l_value = self.params.l_default.get().max(k);
        let params = Knn::new(k, l_value, Some(self.params.beam_width.get()))
            .context("failed to build DiskANN search parameters")?;

        let mut neighbors: Vec<Neighbor<PrimaryId>> = Vec::with_capacity(params.k_value().get());
        let SearchStats { result_count, .. } = partition
            .index
            .search(
                params,
                &InmemStrategy,
                &InmemContext,
                embedding.as_slice(),
                &mut neighbors,
            )
            .await?;

        neighbors.truncate((result_count as usize).min(k));
        Ok(neighbors.into_iter().map(move |neighbor| {
            let raw_distance = match space_type {
                SpaceType::DotProduct => neighbor.distance() + 1.0,
                _ => neighbor.distance(),
            };
            Distance::try_from((raw_distance, space_type, dimensions))
                .map(|distance| (*neighbor.id(), distance))
        }))
    }

    async fn ann(&self, index_key: IndexKey, embedding: Vector, limit: Limit) -> AnnR {
        let partition_id: PartitionId = {
            let table = self.table.read().unwrap();
            if let Some((partition_id, _)) = table.partition_id(&index_key, None) {
                partition_id
            } else {
                warn!(
                    "partition id not found for index key {} during ann",
                    index_key
                );
                return Ok((vec![], vec![]));
            }
        };

        let Some(partition) = self.partitions.get(&partition_id) else {
            return Ok((vec![], vec![]));
        };

        let matches = self
            .search(&embedding, limit.0.get(), partition)
            .await
            .context("ann search failed")?;

        let table = self.table.read().unwrap();
        let (primary_keys, distances) = itertools::process_results(
            matches.filter_map_ok(|(primary_id, distance)| {
                table
                    .primary_key(partition_id, primary_id)
                    .or_else(|| {
                        debug!(
                            "not defined primary key for partition_id {partition_id:?} \
                                        and primary_id {primary_id:?}",
                        );
                        None
                    })
                    .map(|primary_key| (primary_key, distance))
            }),
            |it| it.unzip(),
        )?;
        Ok((primary_keys, distances))
    }

    fn remove_partition(&mut self, partition_id: PartitionId) {
        if self.partitions.remove(&partition_id).is_none() {
            debug!("remove_partition: partition {partition_id:?} not found");
        }
    }

    fn count(&self, index_key: &IndexKey) -> CountR {
        let index_id = {
            let table = self.table.read().unwrap();
            let Some(index_id) = table.index_id(index_key) else {
                anyhow::bail!("index id not found for index key {index_key}");
            };
            index_id
        };

        Ok(self.sizes.get(&index_id).copied().unwrap_or(0))
    }
}

#[hotpath::measure]
fn validate_dimensions(
    tx_ann: oneshot::Sender<AnnR>,
    embedding: &Vector,
    dimensions: Dimensions,
) -> Option<oneshot::Sender<AnnR>> {
    if let Err(err) = validator::embedding_dimensions(embedding, dimensions) {
        tx_ann
            .send(Err(err))
            .unwrap_or_else(|_| trace!("validate_dimensions: unable to send response"));
        None
    } else {
        Some(tx_ann)
    }
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
    space_type: SpaceType,
    dim: Dimensions,
    max_points: NonZeroUsize,
    l_default: NonZeroUsize,
    beam_width: NonZeroUsize,
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

        let l_default = NonZeroUsize::new(cfg.expansion_search.0)
            .context("expansion_search (DiskANN query search list size L) must be non-zero")?;
        let beam_width = NonZeroUsize::new(cfg.expansion_search.0)
            .context("expansion_search (DiskANN beam width) must be non-zero")?;

        Ok(Self {
            config,
            metric,
            space_type: cfg.space_type,
            dim: cfg.dimensions,
            max_points,
            l_default,
            beam_width,
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
        assert_eq!(params.l_default, NonZeroUsize::new(32).unwrap());
        assert_eq!(params.beam_width, NonZeroUsize::new(32).unwrap());
        assert_eq!(params.space_type, SpaceType::Euclidean);
    }
}
