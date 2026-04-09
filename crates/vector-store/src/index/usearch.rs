/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Config;
use crate::Dimensions;
use crate::Distance;
use crate::Filter;
use crate::IndexFactory;
use crate::IndexKey;
use crate::Limit;
use crate::Quantization;
use crate::SpaceType;
use crate::Vector;
use crate::index::actor::AnnR;
use crate::index::actor::Index;
use crate::index::factory::IndexConfiguration;
use crate::index::validator;
use crate::memory::Allocate;
use crate::memory::Memory;
use crate::memory::MemoryExt;
use crate::table::IndexId;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use crate::table::Table;
use crate::table::TableSearch;
use anyhow::anyhow;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::iter;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::error;
use tracing::error_span;
use tracing::info;
use tracing::trace;
use tracing::warn;
use usearch::IndexOptions;
use usearch::MetricKind;
use usearch::ScalarKind;
use usearch::b1x8;

pub struct UsearchIndexFactory {
    tokio_semaphore: Arc<Semaphore>,
    rayon_semaphore: Arc<Semaphore>,
    mode: Mode,
}

impl IndexFactory for UsearchIndexFactory {
    fn create_index(
        &self,
        index: IndexConfiguration,
        table: Arc<RwLock<Table>>,
        memory: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<Index>> {
        match &self.mode {
            Mode::Usearch => {
                let options = IndexOptions {
                    dimensions: index.dimensions.0.get(),
                    connectivity: index.connectivity.0,
                    expansion_add: index.expansion_add.0,
                    expansion_search: index.expansion_search.0,
                    metric: metric_kind(index.quantization, index.space_type)?,
                    quantization: index.quantization.into(),
                    ..Default::default()
                };
                let threads =
                    Handle::current().metrics().num_workers() + rayon::current_num_threads();
                new(
                    move || Ok(Arc::new(ThreadedUsearchIndex::new(options, threads)?)),
                    index.key,
                    index.dimensions,
                    table,
                    Arc::clone(&self.tokio_semaphore),
                    Arc::clone(&self.rayon_semaphore),
                    memory,
                )
            }
            Mode::Simulator { config, config_rx } => new(
                {
                    let config = Arc::clone(config);
                    let index_key = index.key.clone();
                    let config_rx = config_rx.clone();
                    move || Ok(Simulator::new(config.clone(), config_rx.clone(), index_key))
                },
                index.key,
                index.dimensions,
                table,
                Arc::clone(&self.tokio_semaphore),
                Arc::clone(&self.rayon_semaphore),
                memory,
            ),
        }
    }

    fn index_engine_version(&self) -> String {
        match self.mode {
            Mode::Usearch => format!("usearch-{}", usearch::version()),
            Mode::Simulator { .. } => "usearch-simulator".to_string(),
        }
    }
}

pub fn new_usearch(
    tokio_semaphore: Arc<Semaphore>,
    rayon_semaphore: Arc<Semaphore>,
    mut config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<UsearchIndexFactory> {
    let config = config_rx.borrow_and_update().clone();
    Ok(UsearchIndexFactory {
        tokio_semaphore,
        rayon_semaphore,
        mode: if config.usearch_simulator.is_none() {
            Mode::Usearch
        } else {
            Mode::Simulator { config, config_rx }
        },
    })
}

enum Mode {
    Usearch,
    Simulator {
        config: Arc<Config>,
        config_rx: watch::Receiver<Arc<Config>>,
    },
}

trait UsearchIndex {
    fn reserve(&self, size: usize) -> anyhow::Result<()>;
    fn size(&self) -> usize;
    fn capacity(&self) -> usize;
    fn add(&self, primary_id: PrimaryId, vector: &Vector) -> anyhow::Result<()>;
    fn remove(&self, primary_id: PrimaryId) -> anyhow::Result<()>;
    fn search(
        &self,
        vector: &Vector,
        limit: Limit,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(PrimaryId, Distance)>>>;
    fn filtered_search(
        &self,
        vector: &Vector,
        limit: Limit,
        filter: impl Fn(PrimaryId) -> bool,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(PrimaryId, Distance)>>>;

    fn stop(&self);
}

struct ThreadedUsearchIndex {
    inner: usearch::Index,
    threads: usize,
    quantization: usearch::ScalarKind,
    space_type: usearch::MetricKind,
}

impl ThreadedUsearchIndex {
    fn new(options: IndexOptions, threads: usize) -> anyhow::Result<Self> {
        Ok(Self {
            inner: usearch::Index::new(&options)?,
            threads,
            quantization: options.quantization,
            space_type: options.metric,
        })
    }
}

impl UsearchIndex for ThreadedUsearchIndex {
    fn reserve(&self, size: usize) -> anyhow::Result<()> {
        Ok(self
            .inner
            .reserve_capacity_and_threads(size, self.threads)?)
    }

    fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn add(&self, primary_id: PrimaryId, vector: &Vector) -> anyhow::Result<()> {
        if self.quantization == ScalarKind::B1 {
            let vector = f32_to_b1x8(vector.as_slice());
            return Ok(self.inner.add(primary_id.into(), &vector)?);
        }
        Ok(self.inner.add(primary_id.into(), vector.as_slice())?)
    }

    fn remove(&self, primary_id: PrimaryId) -> anyhow::Result<()> {
        Ok(self.inner.remove(primary_id.into()).map(|_| ())?)
    }

    fn search(
        &self,
        vector: &Vector,
        limit: Limit,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(PrimaryId, Distance)>>> {
        let matches = if self.quantization == ScalarKind::B1 {
            let vector = f32_to_b1x8(vector.as_slice());
            self.inner.search(&vector, limit.0.get())?
        } else {
            self.inner.search(vector.as_slice(), limit.0.get())?
        };
        Ok(matches
            .keys
            .into_iter()
            .zip(matches.distances)
            .map(|(primary_id, distance)| {
                Distance::try_from((distance, self.space_type.try_into()?, vector.dim()))
                    .map(|dist| (primary_id.into(), dist))
            }))
    }

    fn filtered_search(
        &self,
        vector: &Vector,
        limit: Limit,
        filter: impl Fn(PrimaryId) -> bool,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(PrimaryId, Distance)>>> {
        let matches = if self.quantization == ScalarKind::B1 {
            let vector = f32_to_b1x8(vector.as_slice());
            self.inner
                .filtered_search(&vector, limit.0.get(), |row_id| filter(row_id.into()))?
        } else {
            self.inner
                .filtered_search(vector.as_slice(), limit.0.get(), |row_id| {
                    filter(row_id.into())
                })?
        };
        Ok(matches
            .keys
            .into_iter()
            .zip(matches.distances)
            .map(|(primary_id, distance)| {
                Distance::try_from((distance, self.space_type.try_into()?, vector.dim()))
                    .map(|dist| (primary_id.into(), dist))
            }))
    }

    fn stop(&self) {}
}

struct Simulator {
    config: Arc<Config>,
    search: Duration,
    add_remove: Duration,
    reserve: Duration,
    keys: RwLock<HashSet<PrimaryId>>,
    notify: Arc<Notify>,
}

impl Simulator {
    const SEARCH_IDX: usize = 0;
    const ADD_REMOVE_IDX: usize = 1;
    const RESERVE_IDX: usize = 2;

    fn new(
        config: Arc<Config>,
        mut config_rx: watch::Receiver<Arc<Config>>,
        key: IndexKey,
    ) -> Arc<RwLock<Self>> {
        let mut sim = Self {
            config: Arc::new(Config::default()),
            search: Duration::ZERO,
            add_remove: Duration::ZERO,
            reserve: Duration::ZERO,
            keys: RwLock::new(HashSet::new()),
            notify: Arc::new(Notify::new()),
        };
        sim.update(config);
        let notify = Arc::clone(&sim.notify);
        let sim = Arc::new(RwLock::new(sim));

        tokio::spawn(
            {
                let sim = Arc::clone(&sim);
                async move {
                    loop {
                        tokio::select! {
                            _ = config_rx.changed() => {
                                let config = config_rx.borrow_and_update().clone();
                                sim.write().unwrap().update(config);
                            }
                            _ = notify.notified() => {
                                break;
                            }
                        }
                    }
                }
            }
            .instrument(error_span!("simulator", "{}", key)),
        );

        sim
    }

    fn update(&mut self, config: Arc<Config>) {
        if self.config.usearch_simulator == config.usearch_simulator {
            return;
        }
        self.search = *config
            .usearch_simulator
            .as_ref()
            .and_then(|vec| vec.get(Self::SEARCH_IDX))
            .unwrap_or(&Duration::ZERO);
        self.add_remove = *config
            .usearch_simulator
            .as_ref()
            .and_then(|vec| vec.get(Self::ADD_REMOVE_IDX))
            .unwrap_or(&Duration::ZERO);
        self.reserve = *config
            .usearch_simulator
            .as_ref()
            .and_then(|vec| vec.get(Self::RESERVE_IDX))
            .unwrap_or(&Duration::ZERO);
        info!(
            "usearch simulator config updated: search = {:?}, add_remove = {:?}, reserve = {:?}",
            self.search, self.add_remove, self.reserve
        );
        self.config = config;
    }

    fn wait(&self, start: Instant, duration: Duration) {
        while start.elapsed() < duration {}
    }

    fn wait_reserve(&self, start: Instant) {
        self.wait(start, self.reserve);
    }

    fn wait_add_remove(&self, start: Instant) {
        self.wait(start, self.add_remove);
    }

    fn wait_search(&self, start: Instant) {
        self.wait(start, self.search);
    }
}

impl UsearchIndex for RwLock<Simulator> {
    fn reserve(&self, size: usize) -> anyhow::Result<()> {
        let start = Instant::now();

        // we need simulate write lock similar to real usearch index
        #[allow(clippy::readonly_write_lock)]
        let sim = self.write().unwrap();
        {
            let mut keys = sim.keys.write().unwrap();
            let len = keys.len();
            keys.reserve(size - len);
        }

        sim.wait_reserve(start);
        Ok(())
    }

    fn capacity(&self) -> usize {
        self.read().unwrap().keys.read().unwrap().capacity()
    }

    fn size(&self) -> usize {
        self.read().unwrap().keys.read().unwrap().len()
    }

    fn add(&self, row_id: PrimaryId, _: &Vector) -> anyhow::Result<()> {
        let start = Instant::now();

        let sim = self.read().unwrap();
        sim.keys.write().unwrap().insert(row_id);

        sim.wait_add_remove(start);
        Ok(())
    }

    fn remove(&self, row_id: PrimaryId) -> anyhow::Result<()> {
        let start = Instant::now();

        let sim = self.read().unwrap();
        sim.keys.write().unwrap().remove(&row_id);

        sim.wait_add_remove(start);
        Ok(())
    }

    fn search(
        &self,
        _: &Vector,
        limit: Limit,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(PrimaryId, Distance)>>> {
        let start = Instant::now();

        let sim = self.read().unwrap();
        let keys = {
            let len = sim.keys.read().unwrap().len() as u64;
            if len == 0 {
                Vec::new()
            } else {
                let keys = sim.keys.read().unwrap();
                iter::repeat_with(|| rand::random_range(0..len))
                    .map(PrimaryId::from)
                    .filter(|row_id| keys.contains(row_id))
                    .take(limit.0.get())
                    .collect()
            }
        };

        sim.wait_search(start);
        let distance = Distance::new_euclidean(0.0)?;
        Ok(keys.into_iter().map(move |row_id| Ok((row_id, distance))))
    }

    fn filtered_search(
        &self,
        vector: &Vector,
        limit: Limit,
        _filter: impl Fn(PrimaryId) -> bool,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<(PrimaryId, Distance)>>> {
        self.search(vector, limit)
    }

    fn stop(&self) {
        self.read().unwrap().notify.notify_one();
    }
}

// Initial and incremental number for the index vectors reservation.
// The value was taken for initial benchmarks (size similar to benchmark size)
const RESERVE_INCREMENT_GLOBAL: usize = 1000000;
const RESERVE_INCREMENT_LOCAL: usize = 1000;

// When free space for index vectors drops below this, will reserve more space
// The ratio was taken for initial benchmarks
const RESERVE_THRESHOLD_GLOBAL: usize = RESERVE_INCREMENT_GLOBAL / 3;
const RESERVE_THRESHOLD_LOCAL: usize = RESERVE_INCREMENT_LOCAL / 3;

struct MetricConfig {
    quantization: Quantization,
    space_type: SpaceType,
}

fn metric_kind(quantization: Quantization, space_type: SpaceType) -> anyhow::Result<MetricKind> {
    // Usearch requires a binary metric (e.g., Hamming, Jaccard) for B1 quantization.
    // Using a non-binary metric would cause a panic during index creation.
    // Since we don't currently support selecting a specific binary space type,
    // we default to Hamming for B1 quantization.
    if quantization == Quantization::B1 {
        return Ok(MetricKind::Hamming);
    }

    MetricConfig {
        quantization,
        space_type,
    }
    .try_into()
}

impl TryFrom<MetricConfig> for MetricKind {
    type Error = anyhow::Error;

    fn try_from(config: MetricConfig) -> Result<Self, Self::Error> {
        if config.quantization == Quantization::B1 {
            return match config.space_type {
                SpaceType::Hamming => Ok(MetricKind::Hamming),
                _ => anyhow::bail!(
                    "B1 quantization requires binary space type. Unsupported space type: {:?}",
                    config.space_type
                ),
            };
        }

        match config.space_type {
            SpaceType::Cosine => Ok(MetricKind::Cos),
            SpaceType::Euclidean => Ok(MetricKind::L2sq),
            SpaceType::DotProduct => Ok(MetricKind::IP),
            SpaceType::Hamming => anyhow::bail!("Binary space type requires B1 quantization."),
        }
    }
}

impl TryFrom<MetricKind> for SpaceType {
    type Error = anyhow::Error;

    fn try_from(metric_kind: MetricKind) -> Result<Self, Self::Error> {
        match metric_kind {
            MetricKind::Cos => Ok(SpaceType::Cosine),
            MetricKind::L2sq => Ok(SpaceType::Euclidean),
            MetricKind::IP => Ok(SpaceType::DotProduct),
            MetricKind::Hamming => Ok(SpaceType::Hamming),
            _ => anyhow::bail!("Unsupported MetricKind for SpaceType: {:?}", metric_kind),
        }
    }
}

impl From<Quantization> for ScalarKind {
    fn from(quantization: Quantization) -> Self {
        match quantization {
            Quantization::F32 => ScalarKind::F32,
            Quantization::F16 => ScalarKind::F16,
            Quantization::BF16 => ScalarKind::BF16,
            Quantization::I8 => ScalarKind::I8,
            Quantization::B1 => ScalarKind::B1,
        }
    }
}

mod operation {
    use super::Index;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use tokio::sync::Notify;

    #[derive(Clone, Copy, PartialEq)]
    enum Mode {
        Reserve,
        Insert,
        Remove,
        Search,
    }

    impl From<&Index> for Mode {
        fn from(msg: &Index) -> Self {
            match msg {
                Index::AddVector { .. } => Mode::Insert,
                Index::RemoveVector { .. } => Mode::Remove,
                Index::Ann { .. } | Index::FilteredAnn { .. } => Mode::Search,
                Index::RemovePartition { .. } => todo!(),
                Index::Count { .. } => unreachable!(),
            }
        }
    }

    impl Mode {
        fn is_exclusive(&self) -> bool {
            match self {
                Mode::Insert | Mode::Search => false,
                // Remove and reserve are not safe to run concurrently with other operations.
                // Therefore, we perform both exclusively.
                // See: https://github.com/unum-cloud/USearch/issues/697.
                Mode::Reserve | Mode::Remove => true,
            }
        }
    }

    pub(super) struct Permit {
        notify: Arc<Notify>,
        counter: Arc<AtomicUsize>,
    }

    impl Drop for Permit {
        fn drop(&mut self) {
            let previous = self.counter.fetch_sub(1, Ordering::Relaxed);
            if previous == 1 {
                self.notify.notify_one();
            }
        }
    }

    pub(super) struct Operation {
        mode: Mode,
        notify: Arc<Notify>,
        counter: Arc<AtomicUsize>,
    }

    impl Operation {
        pub(super) fn new() -> Self {
            Self {
                mode: Mode::Insert,
                notify: Arc::new(Notify::new()),
                counter: Arc::new(AtomicUsize::new(0)),
            }
        }

        /// Wait until it will be possible to spawn operation.
        ///
        /// The function must be called before spawning operation tasks as it blocks
        /// until only requested family of operations is in progress.
        async fn permit(&mut self, mode: Mode) -> Permit {
            while self.mode != mode {
                if self.counter.load(Ordering::Relaxed) == 0 {
                    // it is safe to switch to the operation because there are no spawned tasks
                    // and self.counter won't be changed
                    self.mode = mode;
                    break;
                }
                self.notify.notified().await;
            }

            if mode.is_exclusive() {
                while self.counter.load(Ordering::Relaxed) != 0 {
                    self.notify.notified().await;
                }
            }

            self.counter.fetch_add(1, Ordering::Relaxed);
            Permit {
                notify: Arc::clone(&self.notify),
                counter: Arc::clone(&self.counter),
            }
        }

        pub(super) async fn permit_for_message(&mut self, msg: &Index) -> Permit {
            self.permit(msg.into()).await
        }

        pub(super) async fn permit_for_reserve(&mut self) -> Permit {
            self.permit(Mode::Reserve).await
        }

        /// Capacity and size permit cannot be concurrent only with reserve mode.
        pub(super) async fn permit_for_capacity_and_size(&mut self) -> Permit {
            while self.mode == Mode::Reserve {
                if self.counter.load(Ordering::Relaxed) == 0 {
                    // checking for capacity is during add, so insert mode is fine
                    self.mode = Mode::Insert;
                    break;
                }
                self.notify.notified().await;
            }

            self.counter.fetch_add(1, Ordering::Relaxed);
            Permit {
                notify: Arc::clone(&self.notify),
                counter: Arc::clone(&self.counter),
            }
        }
    }
}

struct PartitionState<I: UsearchIndex + Send + Sync + 'static> {
    partition_id: PartitionId,
    idx: Arc<I>,
}

impl<I> PartitionState<I>
where
    I: UsearchIndex + Send + Sync + 'static,
{
    fn new(partition_id: PartitionId, idx: Arc<I>) -> Self {
        Self { partition_id, idx }
    }

    fn stop(&self) {
        self.idx.stop();
    }
}

struct IndexState {
    dimensions: Dimensions,
    size: Arc<AtomicUsize>,
    operation: operation::Operation,
}

impl IndexState {
    fn new(dimensions: Dimensions) -> Self {
        Self {
            dimensions,
            size: Arc::new(AtomicUsize::new(0)),
            operation: operation::Operation::new(),
        }
    }
}

fn new<I: UsearchIndex + Send + Sync + 'static>(
    index_fn: impl FnOnce() -> anyhow::Result<Arc<I>> + Clone + Send + Sync + 'static,
    index_key: IndexKey,
    dimensions: Dimensions,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    tokio_semaphore: Arc<Semaphore>,
    rayon_semaphore: Arc<Semaphore>,
    memory: mpsc::Sender<Memory>,
) -> anyhow::Result<mpsc::Sender<Index>> {
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        {
            let index_key = index_key.clone();
            async move {
                debug!("starting");
                let mut states = BTreeMap::new();
                let mut partitions = BTreeMap::new();

                let mut allocate_prev = Allocate::Can;

                while let Some(msg) = rx.recv().await {
                    if !check_memory_allocation(&msg, &memory, &mut allocate_prev, &index_key).await
                    {
                        continue;
                    }

                    let Some((state, partition, msg)) = preprocess(
                        index_fn.clone(),
                        &mut states,
                        &mut partitions,
                        table.as_ref(),
                        dimensions,
                        msg,
                    ) else {
                        continue;
                    };

                    dispatch_task(
                        state,
                        partition,
                        &table,
                        &tokio_semaphore,
                        &rayon_semaphore,
                        msg,
                    )
                    .await;
                }

                partitions
                    .into_values()
                    .for_each(|partition| partition.stop());

                debug!("finished");
            }
        }
        .instrument(error_span!("usearch", "{index_key}")),
    );

    Ok(tx)
}

fn preprocess<'a, I, T>(
    index_fn: impl FnOnce() -> anyhow::Result<Arc<I>>,
    states: &'a mut BTreeMap<IndexId, IndexState>,
    partitions: &mut BTreeMap<PartitionId, Arc<PartitionState<I>>>,
    table: &RwLock<T>,
    dimensions: Dimensions,
    msg: Index,
) -> Option<(&'a mut IndexState, Arc<PartitionState<I>>, Index)>
where
    I: UsearchIndex + Send + Sync + 'static,
    T: TableSearch + Send + Sync + 'static,
{
    match msg {
        Index::AddVector { partition_id, .. } => {
            let index_id = partition_id.index_id();
            if let Some(partition) = partitions.get(&partition_id) {
                let Some(state) = states.get_mut(&index_id) else {
                    error!("index state not found for index {index_id:?}");
                    return None;
                };
                return Some((state, Arc::clone(partition), msg));
            }
            let partition = Arc::new(PartitionState::new(
                partition_id,
                index_fn()
                    .inspect_err(|err| {
                        error!("failed to create index for partition {partition_id:?}: {err}")
                    })
                    .ok()?,
            ));
            let state = states
                .entry(index_id)
                .or_insert_with(|| IndexState::new(dimensions));
            partitions.insert(partition_id, Arc::clone(&partition));
            Some((state, partition, msg))
        }

        Index::Ann {
            index_key,
            embedding,
            limit,
            tx,
        } => {
            let Some((partition_id, _)) = table.read().unwrap().partition_id(&index_key, None)
            else {
                warn!("partition id not found for index key {index_key:?} during ann");
                _ = tx.send(Err(anyhow!(validator::Error::PartitionIdNotFound {
                    index_key: index_key.clone(),
                    operation: "ann",
                })));
                return None;
            };
            let index_id = partition_id.index_id();
            let Some((state, partition)) = states
                .get_mut(&index_id)
                .zip(partitions.get(&partition_id))
                .map(|(state, partition)| (state, Arc::clone(partition)))
            else {
                warn!("state or partition not found for index key {index_key:?} during ann");
                _ = tx.send(Err(anyhow!(
                    validator::Error::StateOrPartitionNotFound {
                        index_key: index_key.clone(),
                        operation: "ann",
                    }
                )));
                return None;
            };
            Some((
                state,
                partition,
                Index::Ann {
                    embedding,
                    limit,
                    tx,
                    index_key,
                },
            ))
        }

        Index::FilteredAnn {
            index_key,
            embedding,
            filter,
            limit,
            tx,
        } => {
            let Some((partition_id, restrictions)) = table
                .read()
                .unwrap()
                .partition_id(&index_key, Some(filter.restrictions))
            else {
                warn!("partition id not found for index key {index_key:?} during filtered ann");
                _ = tx.send(Err(anyhow!(validator::Error::PartitionIdNotFound {
                    index_key: index_key.clone(),
                    operation: "filtered ann",
                })));
                return None;
            };
            let index_id = partition_id.index_id();
            let Some((state, partition)) = states
                .get_mut(&index_id)
                .zip(partitions.get(&partition_id))
                .map(|(state, partition)| (state, Arc::clone(partition)))
            else {
                warn!(
                    "state or partition not found for index key {index_key:?} \
                        during filtered ann"
                );
                _ = tx.send(Err(anyhow!(
                    validator::Error::StateOrPartitionNotFound {
                        index_key: index_key.clone(),
                        operation: "filtered ann",
                    }
                )));
                return None;
            };
            let msg = if let Some(restrictions) = restrictions {
                Index::FilteredAnn {
                    embedding,
                    limit,
                    filter: Filter {
                        restrictions,
                        allow_filtering: filter.allow_filtering,
                    },
                    tx,
                    index_key,
                }
            } else {
                Index::Ann {
                    embedding,
                    limit,
                    tx,
                    index_key,
                }
            };
            Some((state, partition, msg))
        }

        Index::Count { index_key, tx } => {
            let Some(index_id) = table.read().unwrap().index_id(&index_key) else {
                let err = anyhow!("index id not found for index key {index_key:?}");
                warn!("index count: {err}");
                _ = tx.send(Err(err));
                return None;
            };
            _ = tx.send(Ok(states
                .get_mut(&index_id)
                .map(|state| state.size.load(Ordering::Relaxed))
                .unwrap_or(0)));
            None
        }

        Index::RemoveVector { partition_id, .. } => {
            let index_id = partition_id.index_id();
            states
                .get_mut(&index_id)
                .zip(partitions.get(&partition_id))
                .map(|(state, partition)| (state, Arc::clone(partition), msg))
        }

        Index::RemovePartition { partition_id } => {
            if let Some(idx) = partitions.remove(&partition_id) {
                idx.stop();
            };
            None
        }
    }
}

async fn dispatch_task<I, T>(
    state: &mut IndexState,
    partition: Arc<PartitionState<I>>,
    table: &Arc<RwLock<T>>,
    tokio_semaphore: &Arc<Semaphore>,
    rayon_semaphore: &Arc<Semaphore>,
    msg: Index,
) where
    I: UsearchIndex + Send + Sync + 'static,
    T: TableSearch + Send + Sync + 'static,
{
    if let Index::AddVector { .. } = &msg {
        let operation_permit = state.operation.permit_for_capacity_and_size().await;
        let is_global = partition.partition_id.index_id().is_global();
        if needs_more_capacity(partition.idx.as_ref(), is_global).is_some() {
            drop(operation_permit);
            let operation_permit = state.operation.permit_for_reserve().await;
            if let Some(capacity) = needs_more_capacity(partition.idx.as_ref(), is_global) {
                let permit = Arc::clone(rayon_semaphore).acquire_owned().await.unwrap();
                let idx = Arc::clone(&partition.idx);
                rayon::spawn(move || {
                    reserve(idx.as_ref(), capacity);
                    drop(permit);
                    drop(operation_permit);
                });
            }
        }
    }

    let operation_permit = state.operation.permit_for_message(&msg).await;

    let table = Arc::clone(table);
    let dimensions = state.dimensions;
    let size = Arc::clone(&state.size);
    if should_run_on_tokio(&msg) {
        let permit = Arc::clone(tokio_semaphore).acquire_owned().await.unwrap();
        tokio::spawn(async move {
            crate::move_to_the_end_of_async_runtime_queue().await;
            process(partition, table, dimensions, size, msg);
            drop(permit);
            drop(operation_permit);
        });
        return;
    }
    let permit = Arc::clone(rayon_semaphore).acquire_owned().await.unwrap();
    rayon::spawn(move || {
        process(partition, table, dimensions, size, msg);
        drop(permit);
        drop(operation_permit);
    });
}

fn should_run_on_tokio(msg: &Index) -> bool {
    matches!(msg, Index::Ann { .. })
}

fn process<I, T>(
    partition: Arc<PartitionState<I>>,
    table: Arc<RwLock<T>>,
    dimensions: Dimensions,
    size: Arc<AtomicUsize>,
    msg: Index,
) where
    I: UsearchIndex + Send + Sync + 'static,
    T: TableSearch + Send + Sync + 'static,
{
    match msg {
        Index::AddVector {
            primary_id,
            embedding,
            in_progress: _in_progress,
            ..
        } => add(partition.idx.as_ref(), primary_id, &embedding, &size),

        Index::Ann {
            embedding,
            limit,
            tx,
            ..
        } => {
            if let Some(tx) = validate_dimensions(tx, &embedding, dimensions) {
                ann(partition, tx, &table, embedding, limit);
            }
        }

        Index::FilteredAnn {
            embedding,
            limit,
            filter,
            tx,
            ..
        } => {
            if let Some(tx) = validate_dimensions(tx, &embedding, dimensions) {
                filtered_ann(partition, tx, &table, embedding, filter, limit);
            }
        }

        Index::Count { .. } => unreachable!(),

        Index::RemoveVector {
            primary_id,
            in_progress: _in_progress,
            ..
        } => remove(partition.idx.as_ref(), primary_id, &size),

        Index::RemovePartition { .. } => unreachable!(),
    }
}

fn reserve(idx: &impl UsearchIndex, capacity: usize) {
    let result = idx.reserve(capacity);
    if let Err(err) = &result {
        error!("unable to reserve index capacity for {capacity} in usearch: {err}");
    } else {
        debug!("reserve: reserved index capacity for {capacity}");
    }
}

fn needs_more_capacity(idx: &impl UsearchIndex, is_global: bool) -> Option<usize> {
    let capacity = idx.capacity();
    let free_space = capacity - idx.size();
    let (increment, threshold) = if is_global {
        (RESERVE_INCREMENT_GLOBAL, RESERVE_THRESHOLD_GLOBAL)
    } else {
        (RESERVE_INCREMENT_LOCAL, RESERVE_THRESHOLD_LOCAL)
    };

    if free_space < threshold {
        Some(capacity + increment)
    } else {
        None
    }
}

fn add(idx: &impl UsearchIndex, primary_id: PrimaryId, embedding: &Vector, size: &AtomicUsize) {
    if let Err(err) = idx.add(primary_id, embedding) {
        warn!("add: unable to add embedding: {err}");
    } else {
        size.fetch_add(1, Ordering::Relaxed);
    }
}

fn remove(idx: &impl UsearchIndex, row_id: PrimaryId, size: &AtomicUsize) {
    if let Err(err) = idx.remove(row_id) {
        warn!("remove: unable to remove embeddings: {err}");
    } else {
        size.fetch_sub(1, Ordering::Relaxed);
    }
}

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

fn ann<I>(
    partition: Arc<PartitionState<I>>,
    tx_ann: oneshot::Sender<AnnR>,
    table: &Arc<RwLock<impl TableSearch>>,
    embedding: Vector,
    limit: Limit,
) where
    I: UsearchIndex + Send + Sync + 'static,
{
    tx_ann
        .send(
            partition.idx.search(&embedding, limit)
                .map_err(|err| anyhow!("ann: search failed: {err}"))
                .and_then(|matches| {
                    let table = table.read().unwrap();
                    let (primary_keys, distances) = itertools::process_results(
                        matches.map(|result| {
                            result.and_then(|(primary_id, distance)| {
                                table
                                    .primary_key(partition.partition_id, primary_id)
                                    .ok_or(anyhow!(
                                        "not defined primary_key for partition_id {partition_id:?} and primary_id {primary_id:?}",
                                        partition_id = partition.partition_id,
                                    ))
                                    .map(|primary_key| (primary_key, distance))
                            })
                        }),
                        |it| it.unzip(),
                    )?;
                    Ok((primary_keys, distances))
                }),
        )
        .unwrap_or_else(|_| trace!("ann: unable to send response"));
}

fn filtered_ann<I>(
    partition: Arc<PartitionState<I>>,
    tx_ann: oneshot::Sender<AnnR>,
    table: &Arc<RwLock<impl TableSearch>>,
    embedding: Vector,
    filter: Filter,
    limit: Limit,
) where
    I: UsearchIndex + Send + Sync + 'static,
{
    let id_ok = |primary_id: PrimaryId| {
        let table = table.read().unwrap();
        filter
            .restrictions
            .iter()
            .all(|restriction| table.is_valid_for(partition.partition_id, primary_id, restriction))
    };

    tx_ann
        .send(
            partition.idx.filtered_search(&embedding, limit, id_ok)
                .map_err(|err| anyhow!("ann: search failed: {err}"))
                .and_then(|matches| {
                    let table = table.read().unwrap();
                    let (primary_keys, distances) = itertools::process_results(
                        matches.map(|result| {
                            result.and_then(|(primary_id, distance)| {
                                table
                                    .primary_key(partition.partition_id, primary_id)
                                    .ok_or(anyhow!(
                                        "not defined primary key for partition_id {partition_id:?} and primary_id {primary_id:?}",
                                        partition_id = partition.partition_id,
                                    ))
                                    .map(|primary_key| (primary_key, distance))
                            })
                        }),
                        |it| it.unzip(),
                    )?;
                    Ok((primary_keys, distances))
                }),
        )
        .unwrap_or_else(|_| trace!("ann: unable to send response"));
}

async fn check_memory_allocation(
    msg: &Index,
    memory: &mpsc::Sender<Memory>,
    allocate_prev: &mut Allocate,
    key: &IndexKey,
) -> bool {
    if !matches!(msg, Index::AddVector { .. }) {
        return true;
    }

    let allocate = memory.can_allocate().await;
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

fn f32_to_b1x8(f32_vec: &[f32]) -> Vec<b1x8> {
    fn chunk_to_byte(chunk: impl Iterator<Item = f32>) -> b1x8 {
        chunk.enumerate().fold(b1x8(0u8), |byte, (i, val)| {
            if val > 0.0 {
                b1x8(byte.0 | (1 << i))
            } else {
                byte
            }
        })
    }

    // Pre-calculate total capacity to avoid reallocation when pushing the remainder chunk
    let capacity = f32_vec.len().div_ceil(8);
    let mut bytes = Vec::<b1x8>::with_capacity(capacity);

    let mut iter = f32_vec.chunks_exact(8);
    bytes.extend(
        iter.by_ref()
            .map(|chunk| chunk_to_byte(chunk.iter().copied())),
    );

    let remainder = iter.remainder();
    if !remainder.is_empty() {
        bytes.push(chunk_to_byte(remainder.iter().copied()));
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use crate::IndexKey;
    use crate::Restriction;
    use crate::index::IndexExt;
    use crate::memory;
    use crate::table::IndexIdGenerator;
    use crate::table::MockTableSearch;
    use mockall::predicate::*;
    use scylla::value::CqlValue;
    use std::num::NonZeroUsize;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::sync::watch;
    use tokio::task;
    use tokio::time;
    use usearch::b1x8;

    fn add_concurrently(
        partition_id: PartitionId,
        index: mpsc::Sender<Index>,
        threads: usize,
        adds_per_worker: usize,
        dimensions: NonZeroUsize,
    ) -> Vec<task::JoinHandle<()>> {
        let mut add_handles = Vec::new();
        for worker in 0..threads {
            let actor = index.clone();
            add_handles.push(tokio::spawn(async move {
                for offset in 0..adds_per_worker {
                    let id = (worker * adds_per_worker + offset) as u64;
                    actor
                        .add_vector(
                            partition_id,
                            id.into(),
                            vec![0.0f32; dimensions.get()].into(),
                            None,
                        )
                        .await;
                }
            }));
        }
        add_handles
    }

    fn search_concurrently(
        index_key: IndexKey,
        index: mpsc::Sender<Index>,
        threads: usize,
        searches_per_worker: usize,
        dimensions: NonZeroUsize,
    ) -> Vec<task::JoinHandle<()>> {
        let mut search_handles = Vec::new();
        for _ in 0..threads {
            let actor = index.clone();
            let index_key = index_key.clone();
            search_handles.push(tokio::spawn(async move {
                for _ in 0..searches_per_worker {
                    let limit = NonZeroUsize::new(5).unwrap().into();
                    let _ = actor
                        .ann(
                            index_key.clone(),
                            vec![0.0f32; dimensions.get()].into(),
                            limit,
                        )
                        .await;
                }
            }));
        }
        search_handles
    }

    #[tokio::test]
    async fn add_or_replace_size_ann() {
        let (_, config_rx) = watch::channel(Arc::new(Config::default()));

        let options = IndexOptions {
            dimensions: 3,
            metric: MetricKind::L2sq,
            ..Default::default()
        };
        let threads = Handle::current().metrics().num_workers() + rayon::current_num_threads();
        let table = Arc::new(RwLock::new(MockTableSearch::new()));
        let index_key = IndexKey::new(&"vector".into(), &"store".into());
        let actor = new(
            move || Ok(Arc::new(ThreadedUsearchIndex::new(options, threads)?)),
            index_key.clone(),
            NonZeroUsize::new(3).unwrap().into(),
            Arc::clone(&table),
            Arc::new(Semaphore::new(4)),
            Arc::new(Semaphore::new(4)),
            memory::new(config_rx),
        )
        .unwrap();

        let index_id = IndexIdGenerator::new().next(true).unwrap();
        let partition_id = PartitionId::global(index_id);
        actor
            .add_vector(partition_id, 1.into(), vec![1., 1., 1.].into(), None)
            .await;
        actor
            .add_vector(partition_id, 2.into(), vec![2., -2., 2.].into(), None)
            .await;
        actor
            .add_vector(partition_id, 3.into(), vec![3., 3., 3.].into(), None)
            .await;

        table
            .write()
            .unwrap()
            .expect_index_id()
            .with(eq(index_key.clone()))
            .returning(move |_| Some(index_id));

        table.write().unwrap().expect_partition_id().returning({
            let index_key = index_key.clone();
            move |key, restrictions| {
                assert_eq!(key, &index_key);
                assert!(restrictions.is_none());
                Some((partition_id, None))
            }
        });
        time::timeout(Duration::from_secs(10), async {
            while actor.count(index_key.clone()).await.unwrap() != 3 {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();

        table
            .write()
            .unwrap()
            .expect_primary_key()
            .with(eq(partition_id), eq(PrimaryId::from(2)))
            .once()
            .returning(|_, _| Some([CqlValue::Int(2)].into()));

        let (primary_keys, distances) = actor
            .ann(
                index_key.clone(),
                vec![2.2, -2.2, 2.2].into(),
                NonZeroUsize::new(1).unwrap().into(),
            )
            .await
            .unwrap();
        assert_eq!(primary_keys.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(primary_keys.first().unwrap(), &[CqlValue::Int(2)].into());

        actor.remove_vector(partition_id, 3.into(), None).await;
        actor
            .add_vector(partition_id, 3.into(), vec![2.1, -2.1, 2.1].into(), None)
            .await;

        table
            .write()
            .unwrap()
            .expect_primary_key()
            .with(eq(partition_id), eq(PrimaryId::from(3)))
            .once()
            .returning(|_, _| Some([CqlValue::Int(3)].into()));

        time::timeout(Duration::from_secs(10), async {
            while actor
                .ann(
                    index_key.clone(),
                    vec![2.2, -2.2, 2.2].into(),
                    NonZeroUsize::new(1).unwrap().into(),
                )
                .await
                .unwrap()
                .0
                .first()
                .unwrap()
                != &[CqlValue::Int(3)].into()
            {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();

        actor.remove_vector(partition_id, 3.into(), None).await;

        time::timeout(Duration::from_secs(10), async {
            while actor.count(index_key.clone()).await.unwrap() != 2 {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();

        table
            .write()
            .unwrap()
            .expect_primary_key()
            .with(eq(partition_id), eq(PrimaryId::from(2)))
            .once()
            .returning(|_, _| Some([CqlValue::Int(2)].into()));

        let (primary_keys, distances) = actor
            .ann(
                index_key,
                vec![2.2, -2.2, 2.2].into(),
                NonZeroUsize::new(1).unwrap().into(),
            )
            .await
            .unwrap();
        assert_eq!(primary_keys.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(primary_keys.first().unwrap(), &[CqlValue::Int(2)].into());
    }

    #[tokio::test]
    async fn allocate_parameter_works() {
        let (memory_tx, mut memory_rx) = mpsc::channel(1);

        let options = IndexOptions {
            dimensions: 3,
            metric: MetricKind::L2sq,
            ..Default::default()
        };
        let threads = Handle::current().metrics().num_workers() + rayon::current_num_threads();
        let table = Arc::new(RwLock::new(MockTableSearch::new()));
        let index_key = IndexKey::new(&"vector".into(), &"store".into());
        let actor = new(
            move || Ok(Arc::new(ThreadedUsearchIndex::new(options, threads)?)),
            index_key.clone(),
            NonZeroUsize::new(3).unwrap().into(),
            Arc::clone(&table),
            Arc::new(Semaphore::new(4)),
            Arc::new(Semaphore::new(4)),
            memory_tx,
        )
        .unwrap();

        let memory_respond = tokio::spawn(async move {
            let Memory::CanAllocate { tx } = memory_rx.recv().await.unwrap();
            _ = tx.send(Allocate::Cannot);
            memory_rx
        });
        let index_id = IndexIdGenerator::new().next(true).unwrap();
        let partition_id = PartitionId::global(index_id);
        actor
            .add_vector(partition_id, 1.into(), vec![1., 1., 1.].into(), None)
            .await;
        let mut memory_rx = memory_respond.await.unwrap();

        table
            .write()
            .unwrap()
            .expect_index_id()
            .with(eq(index_key.clone()))
            .returning(move |_| Some(index_id));

        assert_eq!(actor.count(index_key.clone()).await.unwrap(), 0);

        let memory_respond = tokio::spawn(async move {
            let Memory::CanAllocate { tx } = memory_rx.recv().await.unwrap();
            _ = tx.send(Allocate::Can);
        });
        actor
            .add_vector(partition_id, 1.into(), vec![1., 1., 1.].into(), None)
            .await;
        memory_respond.await.unwrap();

        // Wait for the add operation to complete, as it runs in a separate task.
        time::timeout(Duration::from_secs(10), async {
            while actor.count(index_key.clone()).await.unwrap() != 1 {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ntest::timeout(10_000)]
    async fn concurrent_add_and_search() {
        // By default, Usearch limits concurrent operations (searches, adds) to the number of CPU cores.
        // Exceeding this limit results in a "No available threads to lock" error.
        // This test verifies our concurrency control by spawning a high number of parallel adds and searches (2 x num of cores).
        let (_, config_rx) = watch::channel(Arc::new(Config::default()));

        let dimensions = NonZeroUsize::new(1024).unwrap();
        let options = IndexOptions {
            dimensions: dimensions.get(),
            metric: MetricKind::L2sq,
            ..Default::default()
        };
        let threads = Handle::current().metrics().num_workers() + rayon::current_num_threads();
        let table = Arc::new(RwLock::new(MockTableSearch::new()));
        let index_key = IndexKey::new(&"vector".into(), &"store".into());
        let index = new(
            move || Ok(Arc::new(ThreadedUsearchIndex::new(options, threads)?)),
            index_key.clone(),
            dimensions.into(),
            Arc::clone(&table),
            Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            memory::new(config_rx),
        )
        .unwrap();

        let threads = Handle::current().metrics().num_workers();

        let adds_per_worker = 50;
        let index_id = IndexIdGenerator::new().next(true).unwrap();
        let partition_id = PartitionId::global(index_id);
        table.write().unwrap().expect_partition_id().returning({
            let index_key = index_key.clone();
            move |key, restrictions| {
                assert_eq!(key, &index_key);
                assert!(restrictions.is_none());
                Some((partition_id, None))
            }
        });
        let add_handles = add_concurrently(
            partition_id,
            index.clone(),
            threads,
            adds_per_worker,
            dimensions,
        );
        let search_handles = search_concurrently(
            index_key.clone(),
            index.clone(),
            threads,
            adds_per_worker,
            dimensions,
        );

        for handle in add_handles {
            handle.await.unwrap();
        }
        for handle in search_handles {
            handle.await.unwrap();
        }

        table
            .write()
            .unwrap()
            .expect_index_id()
            .with(eq(index_key.clone()))
            .returning(move |_| Some(index_id));

        // Wait for expected number of vectors to be added.
        time::timeout(Duration::from_secs(10), async {
            while index.count(index_key.clone()).await.unwrap() != threads * adds_per_worker {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn quantization_to_kind_conversion() {
        assert_eq!(ScalarKind::from(Quantization::F32), ScalarKind::F32);
        assert_eq!(ScalarKind::from(Quantization::F16), ScalarKind::F16);
        assert_eq!(ScalarKind::from(Quantization::BF16), ScalarKind::BF16);
        assert_eq!(ScalarKind::from(Quantization::I8), ScalarKind::I8);
        assert_eq!(ScalarKind::from(Quantization::B1), ScalarKind::B1);
    }

    fn b1x8_to_u8_vec(b1_vec: &[b1x8]) -> Vec<u8> {
        b1_vec.iter().map(|&b| b.0).collect()
    }

    #[test]
    fn f32_to_b1x8_empty() {
        let b1_vec = f32_to_b1x8(&[]);
        assert_eq!(b1_vec.len(), 0);
    }

    #[test]
    fn f32_to_b1x8_single_byte() {
        // =< 0 clears bits and > 0 sets bits
        let b1_vec = f32_to_b1x8(&[1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0]);
        assert_eq!(b1_vec.len(), 1);
        assert_eq!(b1x8_to_u8_vec(&b1_vec), &[0b00001111]);
    }

    #[test]
    fn f32_to_b1x8_multiple_bytes() {
        let input = vec![
            1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, // 0b01010101
            -1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, // 0b11110000
        ];
        let b1_vec = f32_to_b1x8(&input);
        assert_eq!(b1_vec.len(), 2);
        assert_eq!(b1x8_to_u8_vec(&b1_vec), &[0b01010101, 0b11110000]);
    }

    #[test]
    fn f32_to_b1x8_large_input() {
        let input = vec![1.0; 64]; // 64 elements = 8 bytes
        let b1_vec = f32_to_b1x8(&input);
        assert_eq!(b1_vec.len(), 8);
        assert_eq!(b1x8_to_u8_vec(&b1_vec), &[0b11111111; 8]);
    }

    #[test]
    fn f32_to_b1x8_remainder() {
        let input = vec![
            1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, // 0b01010101
            1.0, -1.0, 1.0, // 0b00000101
        ];
        let b1_vec = f32_to_b1x8(&input);
        assert_eq!(b1_vec.len(), 2);
        assert_eq!(b1x8_to_u8_vec(&b1_vec), &[0b01010101, 0b00000101]);
    }

    fn create_test_actor(table: &Arc<RwLock<MockTableSearch>>) -> (mpsc::Sender<Index>, IndexKey) {
        let (_, config_rx) = watch::channel(Arc::new(Config::default()));

        let options = IndexOptions {
            dimensions: 3,
            metric: MetricKind::L2sq,
            ..Default::default()
        };
        let threads = Handle::current().metrics().num_workers() + rayon::current_num_threads();
        let index_key = IndexKey::new(&"vector".into(), &"store".into());
        let actor = new(
            move || Ok(Arc::new(ThreadedUsearchIndex::new(options, threads)?)),
            index_key.clone(),
            NonZeroUsize::new(3).unwrap().into(),
            Arc::clone(table),
            Arc::new(Semaphore::new(4)),
            Arc::new(Semaphore::new(4)),
            memory::new(config_rx),
        )
        .unwrap();
        (actor, index_key)
    }

    #[tokio::test]
    async fn ann_returns_error_when_partition_id_not_found() {
        let table = Arc::new(RwLock::new(MockTableSearch::new()));
        let (actor, index_key) = create_test_actor(&table);

        // Mock partition_id to return None — simulating a missing partition
        table
            .write()
            .unwrap()
            .expect_partition_id()
            .returning(|_, _| None);

        let result = actor
            .ann(
                index_key,
                vec![1.0, 2.0, 3.0].into(),
                NonZeroUsize::new(1).unwrap().into(),
            )
            .await;

        assert!(
            result.is_err(),
            "Expected error when partition id is not found during ann"
        );
        let err = result.unwrap_err();
        assert!(
            err.downcast_ref::<validator::Error>().is_some(),
            "Error should be a validator::Error, got: {err}"
        );
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("partition id not found"),
            "Error message should mention partition id not found, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn ann_returns_error_when_state_or_partition_not_found() {
        let table = Arc::new(RwLock::new(MockTableSearch::new()));
        let (actor, index_key) = create_test_actor(&table);

        // Mock partition_id to return a valid partition_id, but don't add any vectors
        // so that the states/partitions maps remain empty
        let index_id = IndexIdGenerator::new().next(true).unwrap();
        let partition_id = PartitionId::global(index_id);
        table
            .write()
            .unwrap()
            .expect_partition_id()
            .returning(move |_, _| Some((partition_id, None)));

        let result = actor
            .ann(
                index_key,
                vec![1.0, 2.0, 3.0].into(),
                NonZeroUsize::new(1).unwrap().into(),
            )
            .await;

        assert!(
            result.is_err(),
            "Expected error when state or partition is not found during ann"
        );
        let err = result.unwrap_err();
        assert!(
            err.downcast_ref::<validator::Error>().is_some(),
            "Error should be a validator::Error, got: {err}"
        );
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("state or partition not found"),
            "Error message should mention state or partition not found, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn filtered_ann_returns_error_when_partition_id_not_found() {
        let table = Arc::new(RwLock::new(MockTableSearch::new()));
        let (actor, index_key) = create_test_actor(&table);

        // Mock partition_id to return None for filtered ann
        table
            .write()
            .unwrap()
            .expect_partition_id()
            .returning(|_, _| None);

        let filter = Filter {
            restrictions: vec![Restriction::Eq {
                lhs: "col".into(),
                rhs: CqlValue::Int(42),
            }],
            allow_filtering: false,
        };

        let result = actor
            .filtered_ann(
                index_key,
                vec![1.0, 2.0, 3.0].into(),
                filter,
                NonZeroUsize::new(1).unwrap().into(),
            )
            .await;

        assert!(
            result.is_err(),
            "Expected error when partition id is not found during filtered ann"
        );
        let err = result.unwrap_err();
        assert!(
            err.downcast_ref::<validator::Error>().is_some(),
            "Error should be a validator::Error, got: {err}"
        );
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("partition id not found"),
            "Error message should mention partition id not found, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn filtered_ann_returns_error_when_state_or_partition_not_found() {
        let table = Arc::new(RwLock::new(MockTableSearch::new()));
        let (actor, index_key) = create_test_actor(&table);

        // Mock partition_id to return a valid partition_id, but don't add any vectors
        // so that the states/partitions maps remain empty
        let index_id = IndexIdGenerator::new().next(true).unwrap();
        let partition_id = PartitionId::global(index_id);
        table
            .write()
            .unwrap()
            .expect_partition_id()
            .returning(move |_, _| Some((partition_id, None)));

        let filter = Filter {
            restrictions: vec![Restriction::Eq {
                lhs: "col".into(),
                rhs: CqlValue::Int(42),
            }],
            allow_filtering: false,
        };

        let result = actor
            .filtered_ann(
                index_key,
                vec![1.0, 2.0, 3.0].into(),
                filter,
                NonZeroUsize::new(1).unwrap().into(),
            )
            .await;

        assert!(
            result.is_err(),
            "Expected error when state or partition is not found during filtered ann"
        );
        let err = result.unwrap_err();
        assert!(
            err.downcast_ref::<validator::Error>().is_some(),
            "Error should be a validator::Error, got: {err}"
        );
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("state or partition not found"),
            "Error message should mention state or partition not found, got: {err_msg}"
        );
    }
}
