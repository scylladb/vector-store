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
use crate::vs_index;
use crate::vs_index::AnnR;
use crate::vs_index::CountR;
use crate::vs_index::Message;
use crate::vs_index::VsIndexConfiguration;
use crate::vs_index::VsIndexModify;
use crate::vs_index::VsIndexSearch;
use crate::vs_index::validator;
use crate::worker::Worker;
use crate::worker::WorkerExt;
use anyhow::Context;
use diskann::graph::Config as DiskannConfig;
use diskann::graph::DiskANNIndex;
use diskann::graph::InplaceDeleteMethod;
use diskann::graph::config::Builder;
use diskann::graph::config::MaxDegree;
use diskann::graph::config::defaults::ALPHA as DISKANN_DEFAULT_ALPHA;
use diskann::graph::glue::DefaultPostProcessor;
use diskann::graph::glue::InplaceDeleteStrategy;
use diskann::graph::glue::InsertStrategy;
use diskann::graph::index::SearchStats;
use diskann::graph::search::Knn;
use diskann::neighbor::Neighbor;
use diskann::provider::DataProvider;
use diskann::provider::Delete;
use diskann::provider::SetElement;
use diskann_vector::distance::Metric;
use itertools::Itertools;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::trace;
use tracing::warn;

mod inmem;
mod scylla;

const DISKANN_DEFAULT_MAX_POINTS: NonZeroUsize = NonZeroUsize::new(1_000_000).unwrap();

/// A DiskANN index backend: a data provider together with the strategy and
/// context driving it. The trait bounds spell out everything the actor needs
/// to insert into, delete from, and search a [`DiskANNIndex`] built over the
/// backend's provider.
trait DiskannBackend: Send + Sync + 'static
where
    Self::Provider: for<'a> SetElement<&'a [f32]> + Delete,
    Self::Strategy: for<'a> InsertStrategy<'a, Self::Provider, &'a [f32]>
        + for<'a> DefaultPostProcessor<'a, Self::Provider, &'a [f32], PrimaryId>
        + InplaceDeleteStrategy<Self::Provider>
        + Clone,
{
    type Provider: DataProvider<ExternalId = PrimaryId>;
    type Strategy;

    fn create_index(
        &self,
        params: &DiskannParams,
        start_point: &[f32],
    ) -> anyhow::Result<DiskANNIndex<Self::Provider>>;

    fn strategy(&self) -> &Self::Strategy;

    fn context(&self) -> &<Self::Provider as DataProvider>::Context;
}

pub struct DiskannIndexFactory {
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
    alpha: DiskannAlpha,
    max_points: NonZeroUsize,
}

impl VsIndexFactory for DiskannIndexFactory {
    fn create_index(
        &self,
        index: VsIndexConfiguration,
        table: Arc<RwLock<Table>>,
    ) -> anyhow::Result<(mpsc::Sender<VsIndexModify>, mpsc::Sender<VsIndexSearch>)> {
        let params = DiskannParams::new(&index, self.alpha, self.max_points)
            .context("failed to create DiskANN parameters")?;

        new(
            inmem::InmemBackend::new(),
            index.key,
            self.worker.clone(),
            self.memory.clone(),
            table,
            params,
        )
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
        worker,
        memory,
        alpha: config
            .diskann_alpha
            .unwrap_or(DiskannAlpha::new(DISKANN_DEFAULT_ALPHA).unwrap()),
        max_points: config
            .diskann_max_points
            .unwrap_or(DISKANN_DEFAULT_MAX_POINTS),
    })
}

fn new<B: DiskannBackend>(
    backend: B,
    index_key: IndexKey,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    params: DiskannParams,
) -> anyhow::Result<(mpsc::Sender<VsIndexModify>, mpsc::Sender<VsIndexSearch>)> {
    let (tx_modify, mut rx_modify) = mpsc::channel(perf::channel_size().into());
    let (tx_search, mut rx_search) = mpsc::channel(perf::channel_size().into());

    let span_key = index_key.clone();
    let params = Arc::new(params);

    tokio::spawn(perf::hotpath_async(
        {
            async move {
                debug!("starting");

                let mut partitions = BTreeMap::new();
                let mut sizes = BTreeMap::new();
                let backend = Arc::new(backend);

                let mut allocate_prev = Allocate::Can;
                let allocate_rx = memory.subscribe_allocate().await;

                while let Some(msg) = vs_index::recv(&mut rx_search, &mut rx_modify).await {
                    if !check_memory_allocation(&msg, &allocate_rx, &mut allocate_prev, &index_key)
                    {
                        continue;
                    }

                    let Some((partition, size, msg)) = preprocess(
                        &backend,
                        &mut partitions,
                        &mut sizes,
                        table.as_ref(),
                        &params,
                        msg,
                    ) else {
                        continue;
                    };

                    dispatch_task(&backend, partition, &table, &params, &worker, size, msg).await;
                }

                debug!("finished");
            }
        }
        .instrument(debug_span!("diskann", "{span_key}")),
    ));

    Ok((tx_modify, tx_search))
}

#[hotpath::measure]
fn preprocess<T, B>(
    backend: &Arc<B>,
    partitions: &mut BTreeMap<PartitionId, Arc<Partition<B>>>,
    sizes: &mut BTreeMap<IndexId, Arc<AtomicUsize>>,
    table: &RwLock<T>,
    params: &DiskannParams,
    msg: Message,
) -> Option<(Arc<Partition<B>>, Arc<AtomicUsize>, Message)>
where
    T: TableSearch + Send + Sync + 'static,
    B: DiskannBackend,
{
    match msg {
        Message::Modify(VsIndexModify::AddVector {
            partition_id,
            ref embedding,
            ..
        }) => {
            let partition = match partitions.entry(partition_id) {
                Entry::Occupied(entry) => Arc::clone(entry.get()),
                Entry::Vacant(entry) => {
                    // The first vector of a partition becomes the start point of the graph, so the
                    // index is created here to make it depend on the order of the messages only.
                    let index = backend
                        .create_index(params, embedding.as_slice())
                        .context(format!(
                            "failed to create index for partition {partition_id:?}"
                        ))
                        .inspect_err(|err| warn!("add_vector failed: {err}"))
                        .ok()?;
                    Arc::clone(entry.insert(Arc::new(Partition {
                        partition_id,
                        index,
                    })))
                }
            };
            let size = Arc::clone(sizes.entry(partition_id.index_id()).or_default());
            Some((partition, size, msg))
        }

        Message::Modify(VsIndexModify::RemoveVector { partition_id, .. }) => {
            let Some(partition) = partitions.get(&partition_id).cloned() else {
                debug!("remove_vector: partition {partition_id:?} not found");
                return None;
            };
            let size = Arc::clone(sizes.entry(partition_id.index_id()).or_default());
            Some((partition, size, msg))
        }

        Message::Search(VsIndexSearch::Ann {
            index_key,
            embedding,
            limit,
            tx,
        }) => {
            let Some((partition_id, _)) = table.read().unwrap().partition_id(&index_key, None)
            else {
                warn!("partition id not found for index key {index_key} during ann");
                _ = tx.send(Ok((vec![], vec![])));
                return None;
            };
            let Some(partition) = partitions.get(&partition_id).cloned() else {
                _ = tx.send(Ok((vec![], vec![])));
                return None;
            };
            let size = Arc::clone(sizes.entry(partition_id.index_id()).or_default());
            Some((
                partition,
                size,
                Message::Search(VsIndexSearch::Ann {
                    index_key,
                    embedding,
                    limit,
                    tx,
                }),
            ))
        }

        Message::Search(VsIndexSearch::FilteredAnn { tx, .. }) => {
            _ = tx.send(Err(anyhow::anyhow!(
                "DiskANN index does not support filtered search"
            )));
            None
        }

        Message::Search(VsIndexSearch::Count { index_key, tx }) => {
            let count: CountR = match table.read().unwrap().index_id(&index_key) {
                Some(index_id) => Ok(sizes
                    .get(&index_id)
                    .map(|size| size.load(Ordering::Relaxed))
                    .unwrap_or(0)),
                None => Err(anyhow::anyhow!(
                    "index id not found for index key {index_key}"
                )),
            };
            _ = tx.send(count);
            None
        }

        Message::Modify(VsIndexModify::RemovePartition { partition_id }) => {
            if partitions.remove(&partition_id).is_none() {
                debug!("remove_partition: partition {partition_id:?} not found");
            }
            None
        }
    }
}

#[hotpath::measure]
async fn dispatch_task<T, B>(
    backend: &Arc<B>,
    partition: Arc<Partition<B>>,
    table: &Arc<RwLock<T>>,
    params: &Arc<DiskannParams>,
    worker: &async_channel::Sender<Worker>,
    size: Arc<AtomicUsize>,
    msg: Message,
) where
    T: TableSearch + Send + Sync + 'static,
    B: DiskannBackend,
{
    let non_blocking = is_non_blocking(&msg);
    let table = Arc::clone(table);
    let params = Arc::clone(params);
    let backend = Arc::clone(backend);

    let task = move || process(backend, partition, table, params, size, msg);
    if non_blocking {
        worker.spawn_async_non_blocking(task).await;
    } else {
        worker.spawn_async_blocking(task).await;
    }
}

#[hotpath::measure]
fn is_non_blocking(msg: &Message) -> bool {
    matches!(msg, Message::Search(VsIndexSearch::Ann { .. }))
}

#[hotpath::measure]
async fn process<T, B>(
    backend: Arc<B>,
    partition: Arc<Partition<B>>,
    table: Arc<RwLock<T>>,
    params: Arc<DiskannParams>,
    size: Arc<AtomicUsize>,
    msg: Message,
) where
    T: TableSearch + Send + Sync + 'static,
    B: DiskannBackend,
{
    match msg {
        Message::Modify(VsIndexModify::AddVector {
            primary_id,
            embedding,
            in_progress: _in_progress,
            ..
        }) => add_vector(backend.as_ref(), &partition, &size, primary_id, embedding).await,

        Message::Modify(VsIndexModify::RemoveVector {
            primary_id,
            in_progress: _in_progress,
            ..
        }) => remove_vector(backend.as_ref(), &partition, &params, &size, primary_id).await,

        Message::Search(VsIndexSearch::Ann {
            embedding,
            limit,
            tx,
            ..
        }) => {
            if let Some(tx) = validate_dimensions(tx, &embedding, params.dim) {
                _ = tx.send(
                    ann(
                        backend.as_ref(),
                        &partition,
                        &table,
                        &params,
                        embedding,
                        limit,
                    )
                    .await,
                );
            }
        }

        Message::Search(VsIndexSearch::FilteredAnn { .. })
        | Message::Search(VsIndexSearch::Count { .. })
        | Message::Modify(VsIndexModify::RemovePartition { .. }) => {
            unreachable!()
        }
    }
}

struct Partition<B: DiskannBackend> {
    partition_id: PartitionId,
    index: DiskANNIndex<B::Provider>,
}

async fn add_vector<B: DiskannBackend>(
    backend: &B,
    partition: &Partition<B>,
    size: &AtomicUsize,
    primary_id: PrimaryId,
    embedding: Vector,
) {
    if let Err(err) = partition
        .index
        .insert(
            backend.strategy(),
            backend.context(),
            &primary_id,
            embedding.as_slice(),
        )
        .await
    {
        warn!("add_vector: failed to insert vector: {err}");
    } else {
        size.fetch_add(1, Ordering::Relaxed);
    }
}

async fn remove_vector<B: DiskannBackend>(
    backend: &B,
    partition: &Partition<B>,
    params: &DiskannParams,
    size: &AtomicUsize,
    primary_id: PrimaryId,
) {
    if let Err(err) = partition
        .index
        .inplace_delete(
            backend.strategy().clone(),
            backend.context(),
            &primary_id,
            params.config.pruned_degree().get(),
            InplaceDeleteMethod::OneHop,
        )
        .await
    {
        warn!("remove_vector: failed to delete vector: {err}");
    } else {
        size.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn search<B: DiskannBackend>(
    backend: &B,
    partition: &Partition<B>,
    params: &DiskannParams,
    embedding: &Vector,
    k: usize,
) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(PrimaryId, Distance)>>> {
    let space_type = params.space_type;
    let dimensions = embedding.dim();

    let k = k.min(params.max_points.get());
    let l_value = params.l_default.get().max(k);
    let knn = Knn::new(k, l_value, Some(params.beam_width.get()))
        .context("failed to build DiskANN search parameters")?;

    let mut neighbors: Vec<Neighbor<PrimaryId>> = Vec::with_capacity(knn.k_value().get());
    let SearchStats { result_count, .. } = partition
        .index
        .search(
            knn,
            backend.strategy(),
            backend.context(),
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

async fn ann<T, B>(
    backend: &B,
    partition: &Partition<B>,
    table: &RwLock<T>,
    params: &DiskannParams,
    embedding: Vector,
    limit: Limit,
) -> AnnR
where
    T: TableSearch + Send + Sync + 'static,
    B: DiskannBackend,
{
    let matches = search(backend, partition, params, &embedding, limit.0.get())
        .await
        .context("ann search failed")?;

    let partition_id = partition.partition_id;
    let table = table.read().unwrap();
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
    msg: &Message,
    rx_allocate: &watch::Receiver<Allocate>,
    allocate_prev: &mut Allocate,
    key: &IndexKey,
) -> bool {
    if !matches!(msg, Message::Modify(VsIndexModify::AddVector { .. })) {
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
            DISKANN_DEFAULT_MAX_POINTS,
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

    #[tokio::test]
    async fn diskann_add_vector_stops_at_max_points_cap() {
        let max_points = NonZeroUsize::new(3).unwrap();

        let vs_config = VsIndexConfiguration {
            key: IndexKey::new(
                &KeyspaceName::from("ks".to_string()),
                &IndexName::from("tbl".to_string()),
            ),
            dimensions: NonZeroUsize::new(2).unwrap().into(),
            connectivity: Connectivity(16),
            expansion_add: ExpansionAdd(64),
            expansion_search: ExpansionSearch(32),
            space_type: SpaceType::Euclidean,
            quantization: Quantization::F32,
        };

        let params = DiskannParams::new(
            &vs_config,
            DiskannAlpha::new(DISKANN_DEFAULT_ALPHA).unwrap(),
            max_points,
        )
        .unwrap();

        let embeddings: Vec<Vector> = (0..5)
            .map(|i| Vector::from(vec![i as f32, i as f32]))
            .collect();

        let backend = inmem::InmemBackend::new();

        let index = backend
            .create_index(&params, embeddings[0].as_slice())
            .unwrap();
        let partition = Partition {
            partition_id: PartitionId::from(0u64),
            index,
        };
        let size = AtomicUsize::new(0);

        // Try to insert more vectors than the cap allows.
        for (i, embedding) in embeddings.into_iter().enumerate() {
            add_vector(
                &backend,
                &partition,
                &size,
                PrimaryId::from(i as u64),
                embedding,
            )
            .await;
        }

        assert_eq!(size.load(Ordering::Relaxed), max_points.get());
    }
}
