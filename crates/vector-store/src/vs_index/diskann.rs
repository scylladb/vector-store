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
use crate::SpaceType;
use crate::Vector;
use crate::VsIndexFactory;
use crate::memory::Memory;
use crate::perf;
use crate::table::PrimaryId;
use crate::table::Table;
use crate::table::TableSearch;
use crate::vs_index::actor::AnnR;
use crate::vs_index::actor::VsIndex;
use crate::vs_index::factory::VsIndexConfiguration;
use crate::vs_index::validator;
use anyhow::Context;
use diskann::graph::Config as DiskannConfig;
use diskann::graph::DiskANNIndex;
use diskann::graph::config::Builder;
use diskann::graph::config::MaxDegree;
use diskann::graph::config::defaults::ALPHA as DISKANN_DEFAULT_ALPHA;
use diskann::graph::index::SearchStats;
use diskann::graph::search::Knn;
use diskann::graph::strategy::FullPrecision;
use diskann::neighbor::BackInserter;
use diskann::neighbor::Neighbor;
use diskann::provider::DefaultContext;
use diskann::provider::Delete;
use diskann_providers::model::graph::provider::async_::FastMemoryVectorProviderAsync;
use diskann_providers::model::graph::provider::async_::TableDeleteProviderAsync;
use diskann_providers::model::graph::provider::async_::common::NoStore;
use diskann_providers::model::graph::provider::async_::common::TableBasedDeletes;
use diskann_providers::model::graph::provider::async_::inmem::CreateFullPrecision;
use diskann_providers::model::graph::provider::async_::inmem::DefaultProvider;
use diskann_providers::model::graph::provider::async_::inmem::DefaultProviderParameters;
use diskann_vector::distance::Metric;
use itertools::Itertools;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;
use tracing::warn;

const MAX_POINTS: NonZeroUsize = NonZeroUsize::new(1_000_000).unwrap();

type DiskannProvider =
    DefaultProvider<FastMemoryVectorProviderAsync<f32>, NoStore, TableDeleteProviderAsync>;

pub struct DiskannIndexFactory {
    alpha: DiskannAlpha,
}

impl VsIndexFactory for DiskannIndexFactory {
    fn create_index(
        &self,
        index: VsIndexConfiguration,
        table: Arc<RwLock<Table>>,
        _memory: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<VsIndex>> {
        let params = DiskannParams::new(&index, self.alpha, MAX_POINTS)?;
        let provider_params = DefaultProviderParameters::simple(
            usize::from(params.max_points),
            usize::from(params.dim.0),
            params.metric,
            u32::from(params.config.max_degree_u32()),
        );

        let provider: DiskannProvider = DefaultProvider::new_empty(
            provider_params,
            CreateFullPrecision::<f32>::new(usize::from(params.dim.0), None),
            NoStore,
            TableBasedDeletes,
        )
        .context("failed to create DiskANN provider")?;

        let diskann_index = DiskANNIndex::new(params.config.clone(), provider, None);

        new(index.key, diskann_index, params, table)
    }

    fn index_engine_version(&self) -> String {
        format!("diskann-{}", diskann::version())
    }
}

pub fn new_diskann(
    mut config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<DiskannIndexFactory> {
    let config = config_rx.borrow_and_update();

    Ok(DiskannIndexFactory {
        alpha: config
            .diskann_alpha
            .unwrap_or(DiskannAlpha::new(DISKANN_DEFAULT_ALPHA).unwrap()),
    })
}

fn new(
    index_key: IndexKey,
    index: DiskANNIndex<DiskannProvider>,
    params: DiskannParams,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
) -> anyhow::Result<mpsc::Sender<VsIndex>> {
    let (tx, mut rx) = mpsc::channel(perf::channel_size().into());

    let span_key = index_key.clone();
    tokio::spawn(perf::hotpath_async(
        {
            async move {
                debug!("starting");

                let mut state = State::new(index, index_key, params, table);

                while let Some(msg) = rx.recv().await {
                    match msg {
                        VsIndex::AddVector {
                            primary_id,
                            embedding,
                            in_progress: _in_progress,
                            ..
                        } => {
                            state.add_vector(primary_id, embedding).await;
                        }
                        VsIndex::RemoveVector {
                            primary_id,
                            in_progress: _in_progress,
                            ..
                        } => {
                            state.remove_vector(primary_id).await;
                        }
                        VsIndex::RemovePartition { .. } => {
                            warn!("not implemented yet");
                        }
                        VsIndex::Ann {
                            embedding,
                            limit,
                            tx,
                            ..
                        } => {
                            if let Some(tx) = validate_dimensions(tx, &embedding, state.params.dim)
                            {
                                _ = tx.send(state.ann(embedding, limit).await);
                            }
                        }
                        VsIndex::FilteredAnn { tx, .. } => {
                            _ = tx.send(Err(anyhow::anyhow!(
                                "DiskANN index does not support filtered search"
                            )));
                        }
                        VsIndex::Count { tx, .. } => {
                            _ = tx
                                .send(Err(anyhow::anyhow!("DiskANN index is not implemented yet")));
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

/// Mapping between PrimaryId <-> u32 label used by DiskANN.
///
/// The mapping is bidirectional but ids are NOT reused once freed,
/// because the underlying DiskANN `delete()` is a soft delete
#[derive(Default)]
struct IdMap {
    forward: BTreeMap<PrimaryId, u32>,
    reverse: BTreeMap<u32, PrimaryId>,
    next_id: u32,
}

impl IdMap {
    fn insert(&mut self, primary_id: PrimaryId) -> Option<u32> {
        if self.forward.contains_key(&primary_id) {
            return None;
        }
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1)?;
        self.forward.insert(primary_id, id);
        self.reverse.insert(id, primary_id);
        Some(id)
    }

    fn remove(&mut self, primary_id: &PrimaryId) -> Option<u32> {
        let id = self.forward.remove(primary_id)?;
        self.reverse.remove(&id);
        Some(id)
    }

    fn get(&self, primary_id: &PrimaryId) -> Option<u32> {
        self.forward.get(primary_id).copied()
    }

    fn resolve(&self, id: u32) -> Option<PrimaryId> {
        self.reverse.get(&id).copied()
    }

    fn len(&self) -> usize {
        self.forward.len()
    }
}

/// Mutable actor state for the DiskANN index.
struct State<T: TableSearch + Send + Sync + 'static> {
    index: DiskANNIndex<DiskannProvider>,
    index_key: IndexKey,
    params: DiskannParams,
    table: Arc<RwLock<T>>,
    id_map: IdMap,
}

impl<T: TableSearch + Send + Sync + 'static> State<T> {
    fn new(
        index: DiskANNIndex<DiskannProvider>,
        index_key: IndexKey,
        params: DiskannParams,
        table: Arc<RwLock<T>>,
    ) -> Self {
        Self {
            index,
            index_key,
            params,
            table,
            id_map: IdMap::default(),
        }
    }

    async fn add_vector(&mut self, primary_id: PrimaryId, embedding: Vector) {
        let Some(id) = self.id_map.insert(primary_id) else {
            warn!("add_vector: failed to insert primary_id {primary_id:?}");
            return;
        };

        if let Err(err) = self
            .index
            .insert(&FullPrecision, &DefaultContext, &id, embedding.as_slice())
            .await
        {
            warn!("add_vector: failed to insert vector: {err}");
            self.id_map.remove(&primary_id);
        }
    }

    async fn remove_vector(&mut self, primary_id: PrimaryId) {
        let Some(id) = self.id_map.get(&primary_id) else {
            warn!("remove_vector: primary_id {primary_id:?} not found");
            return;
        };

        if let Err(err) = self.index.data_provider.delete(&DefaultContext, &id).await {
            warn!("remove_vector: failed to delete vector: {err}");
            return;
        }

        self.id_map.remove(&primary_id);
    }

    async fn search(
        &self,
        embedding: &Vector,
        k: usize,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(PrimaryId, Distance)>>> {
        let space_type = self.params.space_type;
        let dimensions = embedding.dim();

        let l_value = self.params.l_default.get().max(k);
        let k = k.min(self.params.max_points.get());
        let params = Knn::new(k, l_value, Some(self.params.beam_width.get()))
            .context("failed to build DiskANN search parameters")?;

        let mut neighbors = vec![Neighbor::<u32>::default(); params.k_value().get()];
        let SearchStats { result_count, .. } = self
            .index
            .search(
                params,
                &FullPrecision,
                &DefaultContext,
                embedding.as_slice(),
                &mut BackInserter::new(neighbors.as_mut_slice()),
            )
            .await?;

        neighbors.truncate(result_count as usize);
        Ok(neighbors.into_iter().filter_map(move |neighbor| {
            let primary_id = self.id_map.resolve(neighbor.id)?;
            let raw_distance = match space_type {
                SpaceType::DotProduct => neighbor.distance + 1.0,
                _ => neighbor.distance,
            };
            Some(
                Distance::try_from((raw_distance, space_type, dimensions))
                    .map(|distance| (primary_id, distance)),
            )
        }))
    }

    async fn ann(&self, embedding: Vector, limit: Limit) -> AnnR {
        let partition_id: PartitionId = {
            let table = self.table.read().unwrap();
            if let Some((partition_id, _)) = table.partition_id(&self.index_key, None) {
                partition_id
            } else {
                warn!(
                    "partition id not found for index key {} during ann",
                    self.index_key
                );
                return Ok((vec![], vec![]));
            }
        };

        let matches = self
            .search(&embedding, limit.0.get())
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

    #[test]
    fn id_map_insert_remove_resolve_round_trip() {
        let mut map = IdMap::default();
        let pid = PrimaryId::from(42u64);

        let id = map.insert(pid).unwrap();
        assert_eq!(map.resolve(id), Some(pid));
        assert_eq!(map.len(), 1);

        assert_eq!(map.remove(&pid), Some(id));
        assert_eq!(map.resolve(id), None);
        assert_eq!(map.len(), 0);
    }
}
