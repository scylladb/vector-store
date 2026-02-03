/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Config;
use crate::Dimensions;
use crate::Distance;
use crate::Filter;
use crate::IndexFactory;
use crate::IndexId;
use crate::Limit;
use crate::PrimaryKey;
use crate::Quantization;
use crate::Restriction;
use crate::SpaceType;
use crate::Vector;
use crate::index::actor::AnnR;
use crate::index::actor::CountR;
use crate::index::actor::Index;
use crate::index::factory::IndexConfiguration;
use crate::index::validator;
use crate::memory::Allocate;
use crate::memory::Memory;
use crate::memory::MemoryExt;
use anyhow::anyhow;
use bimap::BiMap;
use scylla::value::CqlValue;
use std::collections::HashSet;
use std::iter;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
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
use tracing::debug_span;
use tracing::error;
use tracing::info;
use tracing::trace;
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
        primary_key_columns: Arc<Vec<ColumnName>>,
        memory: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<Index>> {
        match &self.mode {
            Mode::Usearch => {
                let options = IndexOptions {
                    dimensions: index.dimensions.0.get(),
                    connectivity: index.connectivity.0,
                    expansion_add: index.expansion_add.0,
                    expansion_search: index.expansion_search.0,
                    metric: metric_kind(index.quantization, index.space_type),
                    quantization: index.quantization.into(),
                    ..Default::default()
                };
                let threads =
                    Handle::current().metrics().num_workers() + rayon::current_num_threads();
                let idx = Arc::new(ThreadedUsearchIndex::new(options, threads)?);
                new(
                    idx,
                    index.id,
                    index.dimensions,
                    primary_key_columns,
                    Arc::clone(&self.tokio_semaphore),
                    Arc::clone(&self.rayon_semaphore),
                    memory,
                )
            }
            Mode::Simulator { config, config_rx } => {
                let sim = Simulator::new(config.clone(), config_rx.clone(), index.id.clone());
                new(
                    sim,
                    index.id,
                    index.dimensions,
                    primary_key_columns,
                    Arc::clone(&self.tokio_semaphore),
                    Arc::clone(&self.rayon_semaphore),
                    memory,
                )
            }
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
    fn add(&self, key: Key, vector: &Vector) -> anyhow::Result<()>;
    fn remove(&self, key: Key) -> anyhow::Result<()>;
    fn search(
        &self,
        vector: &Vector,
        limit: Limit,
    ) -> anyhow::Result<impl Iterator<Item = (Key, Distance)>>;
    fn filtered_search(
        &self,
        vector: &Vector,
        limit: Limit,
        filter: impl Fn(Key) -> bool,
    ) -> anyhow::Result<impl Iterator<Item = (Key, Distance)>>;

    fn stop(&self);
}

struct ThreadedUsearchIndex {
    inner: usearch::Index,
    threads: usize,
    quantization: usearch::ScalarKind,
}

impl ThreadedUsearchIndex {
    fn new(options: IndexOptions, threads: usize) -> anyhow::Result<Self> {
        Ok(Self {
            inner: usearch::Index::new(&options)?,
            threads,
            quantization: options.quantization,
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

    fn add(&self, key: Key, vector: &Vector) -> anyhow::Result<()> {
        if self.quantization == ScalarKind::B1 {
            let vector = f32_to_b1x8(&vector.0);
            return Ok(self.inner.add(key.0, &vector)?);
        }
        Ok(self.inner.add(key.0, &vector.0)?)
    }

    fn remove(&self, key: Key) -> anyhow::Result<()> {
        Ok(self.inner.remove(key.0).map(|_| ())?)
    }

    fn search(
        &self,
        vector: &Vector,
        limit: Limit,
    ) -> anyhow::Result<impl Iterator<Item = (Key, Distance)>> {
        let matches = if self.quantization == ScalarKind::B1 {
            let vector = f32_to_b1x8(&vector.0);
            self.inner.search(&vector, limit.0.get())?
        } else {
            self.inner.search(&vector.0, limit.0.get())?
        };
        Ok(matches
            .keys
            .into_iter()
            .zip(matches.distances)
            .map(|(key, distance)| (key.into(), distance.into())))
    }

    fn filtered_search(
        &self,
        vector: &Vector,
        limit: Limit,
        filter: impl Fn(Key) -> bool,
    ) -> anyhow::Result<impl Iterator<Item = (Key, Distance)>> {
        let matches = if self.quantization == ScalarKind::B1 {
            let vector = f32_to_b1x8(&vector.0);
            self.inner
                .filtered_search(&vector, limit.0.get(), |key| filter(Key(key)))?
        } else {
            self.inner
                .filtered_search(&vector.0, limit.0.get(), |key| filter(Key(key)))?
        };
        Ok(matches
            .keys
            .into_iter()
            .zip(matches.distances)
            .map(|(key, distance)| (key.into(), distance.into())))
    }

    fn stop(&self) {}
}

struct Simulator {
    config: Arc<Config>,
    search: Duration,
    add_remove: Duration,
    reserve: Duration,
    keys: RwLock<HashSet<Key>>,
    notify: Arc<Notify>,
}

impl Simulator {
    const SEARCH_IDX: usize = 0;
    const ADD_REMOVE_IDX: usize = 1;
    const RESERVE_IDX: usize = 2;

    fn new(
        config: Arc<Config>,
        mut config_rx: watch::Receiver<Arc<Config>>,
        id: IndexId,
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
            .instrument(debug_span!("simulator", "{}", id)),
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

    fn add(&self, key: Key, _: &Vector) -> anyhow::Result<()> {
        let start = Instant::now();

        let sim = self.read().unwrap();
        sim.keys.write().unwrap().insert(key);

        sim.wait_add_remove(start);
        Ok(())
    }

    fn remove(&self, key: Key) -> anyhow::Result<()> {
        let start = Instant::now();

        let sim = self.read().unwrap();
        sim.keys.write().unwrap().remove(&key);

        sim.wait_add_remove(start);
        Ok(())
    }

    fn search(
        &self,
        _: &Vector,
        limit: Limit,
    ) -> anyhow::Result<impl Iterator<Item = (Key, Distance)>> {
        let start = Instant::now();

        let sim = self.read().unwrap();
        let keys = {
            let len = sim.keys.read().unwrap().len() as u64;
            if len == 0 {
                Vec::new()
            } else {
                let keys = sim.keys.read().unwrap();
                iter::repeat_with(|| rand::random_range(0..len))
                    .map(Key)
                    .filter(|key| keys.contains(key))
                    .take(limit.0.get())
                    .collect()
            }
        };

        sim.wait_search(start);
        Ok(keys.into_iter().map(|key| (key, 0.0.into())))
    }

    fn filtered_search(
        &self,
        vector: &Vector,
        limit: Limit,
        _filter: impl Fn(Key) -> bool,
    ) -> anyhow::Result<impl Iterator<Item = (Key, Distance)>> {
        self.search(vector, limit)
    }

    fn stop(&self) {}
}

// Initial and incremental number for the index vectors reservation.
// The value was taken for initial benchmarks (size similar to benchmark size)
const RESERVE_INCREMENT: usize = 1000000;

// When free space for index vectors drops below this, will reserve more space
// The ratio was taken for initial benchmarks
const RESERVE_THRESHOLD: usize = RESERVE_INCREMENT / 3;

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    derive_more::Display,
)]
/// Key for index embeddings
struct Key(u64);

fn metric_kind(quantization: Quantization, space_type: SpaceType) -> MetricKind {
    // Usearch requires a binary metric (e.g., Hamming, Jaccard) for B1 quantization.
    // Using a non-binary metric would cause a panic during index creation.
    // Since we don't currently support selecting a specific binary space type,
    // we default to Hamming for B1 quantization.
    if quantization == Quantization::B1 {
        MetricKind::Hamming
    } else {
        space_type.into()
    }
}

impl From<SpaceType> for MetricKind {
    fn from(space_type: SpaceType) -> Self {
        match space_type {
            SpaceType::Cosine => MetricKind::Cos,
            SpaceType::Euclidean => MetricKind::L2sq,
            SpaceType::DotProduct => MetricKind::IP,
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
                Index::Add { .. } => Mode::Insert,
                Index::Remove { .. } => Mode::Remove,
                Index::Ann { .. } | Index::FilteredAnn { .. } | Index::Count { .. } => Mode::Search,
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

        /// Capacity permit cannot be concurrent only with reserve mode.
        pub(super) async fn permit_for_capacity(&mut self) -> Permit {
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

struct IndexState<I: UsearchIndex + Send + Sync + 'static> {
    idx: Arc<I>,
    keys: RwLock<BiMap<PrimaryKey, Key>>,
    dimensions: Dimensions,
    usearch_key: AtomicU64,
}

impl<I: UsearchIndex + Send + Sync + 'static> IndexState<I> {
    fn new(idx: Arc<I>, dimensions: Dimensions) -> Self {
        Self {
            idx,
            keys: RwLock::new(BiMap::new()),
            dimensions,
            usearch_key: AtomicU64::new(0),
        }
    }

    fn stop(&self) {
        self.idx.stop();
    }
}

fn new<I: UsearchIndex + Send + Sync + 'static>(
    idx: Arc<I>,
    id: IndexId,
    dimensions: Dimensions,
    primary_key_columns: Arc<Vec<ColumnName>>,
    tokio_semaphore: Arc<Semaphore>,
    rayon_semaphore: Arc<Semaphore>,
    memory: mpsc::Sender<Memory>,
) -> anyhow::Result<mpsc::Sender<Index>> {
    idx.reserve(RESERVE_INCREMENT)?;

    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        {
            let id = id.clone();
            async move {
                debug!("starting");
                let idx = Arc::new(IndexState::new(Arc::clone(&idx), dimensions));

                let mut allocate_prev = Allocate::Can;
                let mut operation = operation::Operation::new();

                while let Some(msg) = rx.recv().await {
                    if !check_memory_allocation(&msg, &memory, &mut allocate_prev, &id).await {
                        continue;
                    }

                    dispatch_task(
                        &mut operation,
                        msg,
                        Arc::clone(&idx),
                        Arc::clone(&primary_key_columns),
                        &tokio_semaphore,
                        &rayon_semaphore,
                    )
                    .await;
                }

                idx.stop();

                debug!("finished");
            }
        }
        .instrument(debug_span!("usearch", "{id}")),
    );

    Ok(tx)
}

async fn dispatch_task(
    operation: &mut operation::Operation,
    msg: Index,
    idx: Arc<IndexState<impl UsearchIndex + Send + Sync + 'static>>,
    primary_key_columns: Arc<Vec<ColumnName>>,
    tokio_semaphore: &Arc<Semaphore>,
    rayon_semaphore: &Arc<Semaphore>,
) {
    if let Index::Add { .. } = &msg {
        let operation_permit = operation.permit_for_capacity().await;
        if needs_more_capacity(idx.idx.as_ref(), &idx.keys).is_some() {
            drop(operation_permit);
            let operation_permit = operation.permit_for_reserve().await;
            if let Some(capacity) = needs_more_capacity(idx.idx.as_ref(), &idx.keys) {
                let permit = Arc::clone(rayon_semaphore).acquire_owned().await.unwrap();
                let idx = Arc::clone(&idx.idx);
                rayon::spawn(move || {
                    reserve(idx.as_ref(), capacity);
                    drop(permit);
                    drop(operation_permit);
                });
            }
        }
    }

    let operation_permit = operation.permit_for_message(&msg).await;

    if should_run_on_tokio(&msg) {
        let permit = Arc::clone(tokio_semaphore).acquire_owned().await.unwrap();
        tokio::spawn(async move {
            crate::move_to_the_end_of_async_runtime_queue().await;
            process(msg, &idx, &primary_key_columns);
            drop(permit);
            drop(operation_permit);
        });
        return;
    }
    let permit = Arc::clone(rayon_semaphore).acquire_owned().await.unwrap();
    rayon::spawn(move || {
        process(msg, &idx, &primary_key_columns);
        drop(permit);
        drop(operation_permit);
    });
}

fn should_run_on_tokio(msg: &Index) -> bool {
    matches!(msg, Index::Ann { .. } | Index::Count { .. })
}

fn process<I: UsearchIndex + Send + Sync + 'static>(
    msg: Index,
    index: &IndexState<I>,
    primary_key_columns: &[ColumnName],
) {
    match msg {
        Index::Add {
            primary_key,
            embedding,
            in_progress: _in_progress,
        } => {
            add(
                index.idx.as_ref(),
                &index.keys,
                &index.usearch_key,
                primary_key,
                embedding,
            );
        }
        Index::Ann {
            embedding,
            limit,
            tx,
        } => {
            if let Some(tx) = validate_dimensions(tx, &embedding, index.dimensions) {
                ann(Arc::clone(&index.idx), tx, &index.keys, embedding, limit);
            }
        }
        Index::FilteredAnn {
            embedding,
            limit,
            filter,
            tx,
        } => {
            if let Some(tx) = validate_dimensions(tx, &embedding, index.dimensions) {
                filtered_ann(
                    Arc::clone(&index.idx),
                    tx,
                    &index.keys,
                    primary_key_columns,
                    embedding,
                    filter,
                    limit,
                );
            }
        }
        Index::Count { tx } => {
            count(Arc::clone(&index.idx), tx);
        }
        Index::Remove {
            primary_key,
            in_progress: _in_progress,
        } => remove(index.idx.as_ref(), &index.keys, primary_key),
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

fn needs_more_capacity(
    idx: &impl UsearchIndex,
    keys: &RwLock<BiMap<PrimaryKey, Key>>,
) -> Option<usize> {
    let capacity = idx.capacity();
    let free_space = capacity - keys.read().unwrap().len();

    if free_space < RESERVE_THRESHOLD {
        Some(capacity + RESERVE_INCREMENT)
    } else {
        None
    }
}

fn add(
    idx: &impl UsearchIndex,
    keys: &RwLock<BiMap<PrimaryKey, Key>>,
    usearch_key: &AtomicU64,
    primary_key: PrimaryKey,
    embedding: Vector,
) {
    let key = usearch_key.fetch_add(1, Ordering::Relaxed).into();
    if let Err(err) = idx.add(key, &embedding) {
        debug!("add: unable to add embedding for key {key}: {err}");
        return;
    };
    let _ = keys.write().unwrap().insert(primary_key.clone(), key);
}

fn remove(idx: &impl UsearchIndex, keys: &RwLock<BiMap<PrimaryKey, Key>>, primary_key: PrimaryKey) {
    let Some((_, key)) = keys.write().unwrap().remove_by_left(&primary_key) else {
        return;
    };

    if let Err(err) = idx.remove(key) {
        debug!("remove: unable to remove embeddings for key {key}: {err}");
    };
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

fn ann(
    idx: Arc<impl UsearchIndex>,
    tx_ann: oneshot::Sender<AnnR>,
    keys: &RwLock<BiMap<PrimaryKey, Key>>,
    embedding: Vector,
    limit: Limit,
) {
    tx_ann
        .send(
            idx.search(&embedding, limit)
                .map_err(|err| anyhow!("ann: search failed: {err}"))
                .and_then(|matches| {
                    let keys = keys.read().unwrap();
                    let (primary_keys, distances) = itertools::process_results(
                        matches.map(|(key, distance)| {
                            keys.get_by_right(&key)
                                .cloned()
                                .ok_or(anyhow!("not defined primary key column {key}"))
                                .map(|primary_key| (primary_key, distance))
                        }),
                        |it| it.unzip(),
                    )?;
                    Ok((primary_keys, distances))
                }),
        )
        .unwrap_or_else(|_| trace!("ann: unable to send response"));
}

/// Compare two CqlValues, returning an Ordering if they are comparable.
/// Only Numeric, Text, Date, Time, and Timestamp types support comparison operators.
fn cql_cmp(lhs: &CqlValue, rhs: &CqlValue) -> Option<std::cmp::Ordering> {
    match (lhs, rhs) {
        // Numeric types
        (CqlValue::TinyInt(a), CqlValue::TinyInt(b)) => Some(a.cmp(b)),
        (CqlValue::SmallInt(a), CqlValue::SmallInt(b)) => Some(a.cmp(b)),
        (CqlValue::Int(a), CqlValue::Int(b)) => Some(a.cmp(b)),
        (CqlValue::BigInt(a), CqlValue::BigInt(b)) => Some(a.cmp(b)),
        (CqlValue::Float(a), CqlValue::Float(b)) => a.partial_cmp(b),
        (CqlValue::Double(a), CqlValue::Double(b)) => a.partial_cmp(b),
        (CqlValue::Counter(a), CqlValue::Counter(b)) => Some(a.0.cmp(&b.0)),
        // Text types
        (CqlValue::Text(a), CqlValue::Text(b)) => Some(a.cmp(b)),
        (CqlValue::Ascii(a), CqlValue::Ascii(b)) => Some(a.cmp(b)),
        // Date and Time types (access inner values directly)
        (CqlValue::Date(a), CqlValue::Date(b)) => Some(a.0.cmp(&b.0)),
        (CqlValue::Time(a), CqlValue::Time(b)) => Some(a.0.cmp(&b.0)),
        (CqlValue::Timestamp(a), CqlValue::Timestamp(b)) => Some(a.0.cmp(&b.0)),
        // Unsupported or mismatched types
        _ => None,
    }
}

/// Lexicographically compare tuple values.
/// Returns the ordering of the first non-equal pair, or Equal if all pairs are equal.
fn cql_cmp_tuple<'a>(
    primary_key: &'a PrimaryKey,
    primary_key_value: impl Fn(&'a PrimaryKey, &ColumnName) -> Option<&'a CqlValue>,
    lhs: &[ColumnName],
    rhs: &[CqlValue],
) -> Option<std::cmp::Ordering> {
    for (col, rhs_val) in lhs.iter().zip(rhs.iter()) {
        let lhs_val = primary_key_value(primary_key, col)?;
        match cql_cmp(lhs_val, rhs_val)? {
            std::cmp::Ordering::Equal => continue,
            other => return Some(other),
        }
    }
    Some(std::cmp::Ordering::Equal)
}

fn filtered_ann(
    idx: Arc<impl UsearchIndex>,
    tx_ann: oneshot::Sender<AnnR>,
    keys: &RwLock<BiMap<PrimaryKey, Key>>,
    primary_key_columns: &[ColumnName],
    embedding: Vector,
    filter: Filter,
    limit: Limit,
) {
    fn annotate<F>(f: F) -> F
    where
        F: for<'a, 'b> Fn(&'a PrimaryKey, &'b ColumnName) -> Option<&'a CqlValue>,
    {
        f
    }

    let primary_key_value = annotate(
        |primary_key: &PrimaryKey, name: &ColumnName| -> Option<&CqlValue> {
            primary_key_columns
                .iter()
                .position(|key_column| key_column == name)
                .and_then(move |idx| primary_key.0.get(idx))
        },
    );

    let id_ok = |key: Key| {
        let Some(primary_key) = keys.read().unwrap().get_by_right(&key).cloned() else {
            return false;
        };
        filter
            .restrictions
            .iter()
            .all(|restriction| match restriction {
                Restriction::Eq { lhs, rhs } => primary_key_value(&primary_key, lhs) == Some(rhs),
                Restriction::In { lhs, rhs } => {
                    let value = primary_key_value(&primary_key, lhs);
                    rhs.iter().any(|rhs| value == Some(rhs))
                }
                Restriction::Lt { lhs, rhs } => primary_key_value(&primary_key, lhs)
                    .and_then(|value| cql_cmp(value, rhs))
                    .is_some_and(|ord| ord.is_lt()),
                Restriction::Lte { lhs, rhs } => primary_key_value(&primary_key, lhs)
                    .and_then(|value| cql_cmp(value, rhs))
                    .is_some_and(|ord| ord.is_le()),
                Restriction::Gt { lhs, rhs } => primary_key_value(&primary_key, lhs)
                    .and_then(|value| cql_cmp(value, rhs))
                    .is_some_and(|ord| ord.is_gt()),
                Restriction::Gte { lhs, rhs } => primary_key_value(&primary_key, lhs)
                    .and_then(|value| cql_cmp(value, rhs))
                    .is_some_and(|ord| ord.is_ge()),
                Restriction::EqTuple { lhs, rhs } => lhs
                    .iter()
                    .zip(rhs.iter())
                    .all(|(lhs, rhs)| primary_key_value(&primary_key, lhs) == Some(rhs)),
                Restriction::InTuple { lhs, rhs } => {
                    let values: Vec<_> = lhs
                        .iter()
                        .map(|lhs| primary_key_value(&primary_key, lhs))
                        .collect();
                    rhs.iter().any(|rhs| {
                        values
                            .iter()
                            .zip(rhs.iter())
                            .all(|(value, rhs)| value == &Some(rhs))
                    })
                }
                Restriction::LtTuple { lhs, rhs } => {
                    cql_cmp_tuple(&primary_key, primary_key_value, lhs, rhs)
                        .is_some_and(|ord| ord.is_lt())
                }
                Restriction::LteTuple { lhs, rhs } => {
                    cql_cmp_tuple(&primary_key, primary_key_value, lhs, rhs)
                        .is_some_and(|ord| ord.is_le())
                }
                Restriction::GtTuple { lhs, rhs } => {
                    cql_cmp_tuple(&primary_key, primary_key_value, lhs, rhs)
                        .is_some_and(|ord| ord.is_gt())
                }
                Restriction::GteTuple { lhs, rhs } => {
                    cql_cmp_tuple(&primary_key, primary_key_value, lhs, rhs)
                        .is_some_and(|ord| ord.is_ge())
                }
            })
    };

    tx_ann
        .send(
            idx.filtered_search(&embedding, limit, id_ok)
                .map_err(|err| anyhow!("ann: search failed: {err}"))
                .and_then(|matches| {
                    let keys = keys.read().unwrap();
                    let (primary_keys, distances) = itertools::process_results(
                        matches.map(|(key, distance)| {
                            keys.get_by_right(&key)
                                .cloned()
                                .ok_or(anyhow!("not defined primary key column {key}"))
                                .map(|primary_key| (primary_key, distance))
                        }),
                        |it| it.unzip(),
                    )?;
                    Ok((primary_keys, distances))
                }),
        )
        .unwrap_or_else(|_| trace!("ann: unable to send response"));
}

fn count(idx: Arc<impl UsearchIndex>, tx: oneshot::Sender<CountR>) {
    tx.send(Ok(idx.size()))
        .unwrap_or_else(|_| trace!("count: unable to send response"));
}

async fn check_memory_allocation(
    msg: &Index,
    memory: &mpsc::Sender<Memory>,
    allocate_prev: &mut Allocate,
    id: &IndexId,
) -> bool {
    if !matches!(msg, Index::Add { .. }) {
        return true;
    }

    let allocate = memory.can_allocate().await;
    if allocate == Allocate::Cannot {
        if *allocate_prev == Allocate::Can {
            error!("Unable to add vector for index {id}: not enough memory to reserve more space");
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
    use crate::Connectivity;
    use crate::ExpansionAdd;
    use crate::ExpansionSearch;
    use crate::IndexId;
    use crate::index::IndexExt;
    use crate::memory;
    use scylla::value::CqlValue;
    use std::num::NonZeroUsize;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::sync::watch;
    use tokio::task;
    use tokio::time;
    use usearch::b1x8;

    fn add_concurrently(
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
                    let id = worker * adds_per_worker + offset;
                    actor
                        .add(
                            vec![CqlValue::Int(id as i32)].into(),
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
        index: mpsc::Sender<Index>,
        threads: usize,
        searches_per_worker: usize,
        dimensions: NonZeroUsize,
    ) -> Vec<task::JoinHandle<()>> {
        let mut search_handles = Vec::new();
        for _ in 0..threads {
            let actor = index.clone();
            search_handles.push(tokio::spawn(async move {
                for _ in 0..searches_per_worker {
                    let limit = NonZeroUsize::new(5).unwrap().into();
                    let _ = actor
                        .ann(vec![0.0f32; dimensions.get()].into(), limit)
                        .await;
                }
            }));
        }
        search_handles
    }

    #[tokio::test]
    async fn add_or_replace_size_ann() {
        let (_, config_rx) = watch::channel(Arc::new(Config::default()));

        let factory = UsearchIndexFactory {
            tokio_semaphore: Arc::new(Semaphore::new(4)),
            rayon_semaphore: Arc::new(Semaphore::new(4)),
            mode: Mode::Usearch,
        };
        let actor = factory
            .create_index(
                IndexConfiguration {
                    id: IndexId::new(&"vector".to_string().into(), &"store".to_string().into()),
                    dimensions: NonZeroUsize::new(3).unwrap().into(),
                    connectivity: Connectivity::default(),
                    expansion_add: ExpansionAdd::default(),
                    expansion_search: ExpansionSearch::default(),
                    space_type: SpaceType::Euclidean,
                    quantization: Quantization::default(),
                },
                Arc::new(vec![]),
                memory::new(config_rx),
            )
            .unwrap();

        actor
            .add(
                vec![CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
                vec![1., 1., 1.].into(),
                None,
            )
            .await;
        actor
            .add(
                vec![CqlValue::Int(2), CqlValue::Text("two".to_string())].into(),
                vec![2., -2., 2.].into(),
                None,
            )
            .await;
        actor
            .add(
                vec![CqlValue::Int(3), CqlValue::Text("three".to_string())].into(),
                vec![3., 3., 3.].into(),
                None,
            )
            .await;

        time::timeout(Duration::from_secs(10), async {
            while actor.count().await.unwrap() != 3 {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let (primary_keys, distances) = actor
            .ann(
                vec![2.2, -2.2, 2.2].into(),
                NonZeroUsize::new(1).unwrap().into(),
            )
            .await
            .unwrap();
        assert_eq!(primary_keys.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(
            primary_keys.first().unwrap(),
            &vec![CqlValue::Int(2), CqlValue::Text("two".to_string())].into(),
        );

        actor
            .remove(
                vec![CqlValue::Int(3), CqlValue::Text("three".to_string())].into(),
                None,
            )
            .await;
        actor
            .add(
                vec![CqlValue::Int(3), CqlValue::Text("three".to_string())].into(),
                vec![2.1, -2.1, 2.1].into(),
                None,
            )
            .await;

        time::timeout(Duration::from_secs(10), async {
            while actor
                .ann(
                    vec![2.2, -2.2, 2.2].into(),
                    NonZeroUsize::new(1).unwrap().into(),
                )
                .await
                .unwrap()
                .0
                .first()
                .unwrap()
                != &vec![CqlValue::Int(3), CqlValue::Text("three".to_string())].into()
            {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();

        actor
            .remove(
                vec![CqlValue::Int(3), CqlValue::Text("three".to_string())].into(),
                None,
            )
            .await;

        time::timeout(Duration::from_secs(10), async {
            while actor.count().await.unwrap() != 2 {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let (primary_keys, distances) = actor
            .ann(
                vec![2.2, -2.2, 2.2].into(),
                NonZeroUsize::new(1).unwrap().into(),
            )
            .await
            .unwrap();
        assert_eq!(primary_keys.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(
            primary_keys.first().unwrap(),
            &vec![CqlValue::Int(2), CqlValue::Text("two".to_string())].into(),
        );
    }

    #[tokio::test]
    async fn allocate_parameter_works() {
        let (memory_tx, mut memory_rx) = mpsc::channel(1);

        let factory = UsearchIndexFactory {
            tokio_semaphore: Arc::new(Semaphore::new(4)),
            rayon_semaphore: Arc::new(Semaphore::new(4)),
            mode: Mode::Usearch,
        };
        let actor = factory
            .create_index(
                IndexConfiguration {
                    id: IndexId::new(&"vector".to_string().into(), &"store".to_string().into()),
                    dimensions: NonZeroUsize::new(3).unwrap().into(),
                    connectivity: Connectivity::default(),
                    expansion_add: ExpansionAdd::default(),
                    expansion_search: ExpansionSearch::default(),
                    space_type: SpaceType::Euclidean,
                    quantization: Quantization::default(),
                },
                Arc::new(vec![]),
                memory_tx,
            )
            .unwrap();

        let memory_respond = tokio::spawn(async move {
            let Memory::CanAllocate { tx } = memory_rx.recv().await.unwrap();
            _ = tx.send(Allocate::Cannot);
            memory_rx
        });
        actor
            .add(vec![CqlValue::Int(1)].into(), vec![1., 1., 1.].into(), None)
            .await;
        let mut memory_rx = memory_respond.await.unwrap();
        assert_eq!(actor.count().await.unwrap(), 0);

        let memory_respond = tokio::spawn(async move {
            let Memory::CanAllocate { tx } = memory_rx.recv().await.unwrap();
            _ = tx.send(Allocate::Can);
        });
        actor
            .add(vec![CqlValue::Int(1)].into(), vec![1., 1., 1.].into(), None)
            .await;
        memory_respond.await.unwrap();

        // Wait for the add operation to complete, as it runs in a separate task.
        time::timeout(Duration::from_secs(10), async {
            while actor.count().await.unwrap() != 1 {
                task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    mod cql_cmp_tests {
        use super::super::cql_cmp;
        use scylla::value::CqlValue;
        use std::cmp::Ordering;

        #[test]
        fn compare_integers() {
            assert_eq!(
                cql_cmp(&CqlValue::Int(1), &CqlValue::Int(2)),
                Some(Ordering::Less)
            );
            assert_eq!(
                cql_cmp(&CqlValue::Int(2), &CqlValue::Int(2)),
                Some(Ordering::Equal)
            );
            assert_eq!(
                cql_cmp(&CqlValue::Int(3), &CqlValue::Int(2)),
                Some(Ordering::Greater)
            );
        }

        #[test]
        fn compare_bigints() {
            assert_eq!(
                cql_cmp(&CqlValue::BigInt(100), &CqlValue::BigInt(200)),
                Some(Ordering::Less)
            );
            assert_eq!(
                cql_cmp(&CqlValue::BigInt(-50), &CqlValue::BigInt(-50)),
                Some(Ordering::Equal)
            );
        }

        #[test]
        fn compare_floats() {
            assert_eq!(
                cql_cmp(&CqlValue::Float(1.0), &CqlValue::Float(2.0)),
                Some(Ordering::Less)
            );
            assert_eq!(
                cql_cmp(&CqlValue::Float(2.5), &CqlValue::Float(2.5)),
                Some(Ordering::Equal)
            );
            // NaN comparison returns None
            assert_eq!(
                cql_cmp(&CqlValue::Float(f32::NAN), &CqlValue::Float(1.0)),
                None
            );
        }

        #[test]
        fn compare_doubles() {
            assert_eq!(
                cql_cmp(&CqlValue::Double(1.0), &CqlValue::Double(2.0)),
                Some(Ordering::Less)
            );
            assert_eq!(
                cql_cmp(&CqlValue::Double(f64::NAN), &CqlValue::Double(1.0)),
                None
            );
        }

        #[test]
        fn compare_text() {
            assert_eq!(
                cql_cmp(
                    &CqlValue::Text("apple".to_string()),
                    &CqlValue::Text("banana".to_string())
                ),
                Some(Ordering::Less)
            );
            assert_eq!(
                cql_cmp(
                    &CqlValue::Text("same".to_string()),
                    &CqlValue::Text("same".to_string())
                ),
                Some(Ordering::Equal)
            );
        }

        #[test]
        fn compare_ascii() {
            assert_eq!(
                cql_cmp(
                    &CqlValue::Ascii("aaa".to_string()),
                    &CqlValue::Ascii("bbb".to_string())
                ),
                Some(Ordering::Less)
            );
        }

        #[test]
        fn compare_smallint_and_tinyint() {
            assert_eq!(
                cql_cmp(&CqlValue::SmallInt(10), &CqlValue::SmallInt(20)),
                Some(Ordering::Less)
            );
            assert_eq!(
                cql_cmp(&CqlValue::TinyInt(5), &CqlValue::TinyInt(3)),
                Some(Ordering::Greater)
            );
        }

        #[test]
        fn mismatched_types_return_none() {
            assert_eq!(cql_cmp(&CqlValue::Int(1), &CqlValue::BigInt(1)), None);
            assert_eq!(
                cql_cmp(&CqlValue::Int(1), &CqlValue::Text("1".to_string())),
                None
            );
            assert_eq!(cql_cmp(&CqlValue::Float(1.0), &CqlValue::Double(1.0)), None);
        }

        #[test]
        fn unsupported_types_return_none() {
            assert_eq!(
                cql_cmp(
                    &CqlValue::Blob(vec![1, 2, 3]),
                    &CqlValue::Blob(vec![1, 2, 3])
                ),
                None
            );
            assert_eq!(
                cql_cmp(&CqlValue::Boolean(true), &CqlValue::Boolean(false)),
                None
            );
        }
    }

    mod cql_cmp_tuple_tests {
        use super::super::{ColumnName, PrimaryKey, cql_cmp_tuple};
        use scylla::value::CqlValue;
        use std::cmp::Ordering;

        fn make_primary_key(values: Vec<CqlValue>) -> PrimaryKey {
            values.into()
        }

        fn primary_key_value_fn<'a>(
            columns: &'a [ColumnName],
        ) -> impl Fn(&'a PrimaryKey, &ColumnName) -> Option<&'a CqlValue> {
            move |pk: &'a PrimaryKey, name: &ColumnName| {
                columns
                    .iter()
                    .position(|col| col == name)
                    .and_then(|idx| pk.0.get(idx))
            }
        }

        #[test]
        fn equal_tuples() {
            let columns: Vec<ColumnName> = vec!["a".to_string().into(), "b".to_string().into()];
            let pk = make_primary_key(vec![CqlValue::Int(1), CqlValue::Int(2)]);
            let lhs: Vec<ColumnName> = vec!["a".to_string().into(), "b".to_string().into()];
            let rhs = vec![CqlValue::Int(1), CqlValue::Int(2)];

            let result = cql_cmp_tuple(&pk, primary_key_value_fn(&columns), &lhs, &rhs);
            assert_eq!(result, Some(Ordering::Equal));
        }

        #[test]
        fn first_element_differs() {
            let columns: Vec<ColumnName> = vec!["a".to_string().into(), "b".to_string().into()];
            let pk = make_primary_key(vec![CqlValue::Int(1), CqlValue::Int(2)]);
            let lhs: Vec<ColumnName> = vec!["a".to_string().into(), "b".to_string().into()];
            let rhs = vec![CqlValue::Int(2), CqlValue::Int(2)];

            let result = cql_cmp_tuple(&pk, primary_key_value_fn(&columns), &lhs, &rhs);
            assert_eq!(result, Some(Ordering::Less));
        }

        #[test]
        fn second_element_differs() {
            let columns: Vec<ColumnName> = vec!["a".to_string().into(), "b".to_string().into()];
            let pk = make_primary_key(vec![CqlValue::Int(1), CqlValue::Int(3)]);
            let lhs: Vec<ColumnName> = vec!["a".to_string().into(), "b".to_string().into()];
            let rhs = vec![CqlValue::Int(1), CqlValue::Int(2)];

            let result = cql_cmp_tuple(&pk, primary_key_value_fn(&columns), &lhs, &rhs);
            assert_eq!(result, Some(Ordering::Greater));
        }

        #[test]
        fn missing_column_returns_none() {
            let columns: Vec<ColumnName> = vec!["a".to_string().into()];
            let pk = make_primary_key(vec![CqlValue::Int(1)]);
            let lhs: Vec<ColumnName> = vec!["a".to_string().into(), "b".to_string().into()];
            let rhs = vec![CqlValue::Int(1), CqlValue::Int(2)];

            let result = cql_cmp_tuple(&pk, primary_key_value_fn(&columns), &lhs, &rhs);
            assert_eq!(result, None);
        }

        #[test]
        fn incomparable_types_returns_none() {
            let columns: Vec<ColumnName> = vec!["a".to_string().into()];
            let pk = make_primary_key(vec![CqlValue::Int(1)]);
            let lhs: Vec<ColumnName> = vec!["a".to_string().into()];
            let rhs = vec![CqlValue::BigInt(1)]; // Different type

            let result = cql_cmp_tuple(&pk, primary_key_value_fn(&columns), &lhs, &rhs);
            assert_eq!(result, None);
        }

        #[test]
        fn empty_tuples_are_equal() {
            let columns: Vec<ColumnName> = vec![];
            let pk = make_primary_key(vec![]);
            let lhs: Vec<ColumnName> = vec![];
            let rhs: Vec<CqlValue> = vec![];

            let result = cql_cmp_tuple(&pk, primary_key_value_fn(&columns), &lhs, &rhs);
            assert_eq!(result, Some(Ordering::Equal));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ntest::timeout(10_000)]
    async fn concurrent_add_and_search() {
        // By default, Usearch limits concurrent operations (searches, adds) to the number of CPU cores.
        // Exceeding this limit results in a "No available threads to lock" error.
        // This test verifies our concurrency control by spawning a high number of parallel adds and searches (2 x num of cores).
        let (_, config_rx) = watch::channel(Arc::new(Config::default()));
        let dimensions = NonZeroUsize::new(1024).unwrap();
        let factory = UsearchIndexFactory {
            tokio_semaphore: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            rayon_semaphore: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            mode: Mode::Usearch,
        };
        let index = factory
            .create_index(
                IndexConfiguration {
                    id: IndexId::new(&"vector".to_string().into(), &"store".to_string().into()),
                    dimensions: dimensions.into(),
                    connectivity: Connectivity::default(),
                    expansion_add: ExpansionAdd::default(),
                    expansion_search: ExpansionSearch::default(),
                    space_type: SpaceType::Euclidean,
                    quantization: Quantization::default(),
                },
                Arc::new(vec![]),
                memory::new(config_rx),
            )
            .unwrap();
        let threads = Handle::current().metrics().num_workers();

        let adds_per_worker = 50;
        let add_handles = add_concurrently(index.clone(), threads, adds_per_worker, dimensions);
        let search_handles =
            search_concurrently(index.clone(), threads, adds_per_worker, dimensions);

        for handle in add_handles {
            handle.await.unwrap();
        }
        for handle in search_handles {
            handle.await.unwrap();
        }

        // Wait for expected number of vectors to be added.
        time::timeout(Duration::from_secs(10), async {
            while index.count().await.unwrap() != threads * adds_per_worker {
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
}
