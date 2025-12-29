/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Config;
use crate::Dimensions;
use crate::Distance;
use crate::IndexFactory;
use crate::IndexId;
use crate::Limit;
use crate::PrimaryKey;
use crate::Quantization;
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
use std::collections::HashSet;
use std::iter;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Notify;
use tokio::sync::RwLock as TokioRwLock;
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

pub struct UsearchIndexFactory {
    tokio_semaphore: Arc<Semaphore>,
    rayon_semaphore: Arc<Semaphore>,
    mode: Mode,
}

impl IndexFactory for UsearchIndexFactory {
    fn create_index(
        &self,
        index: IndexConfiguration,
        memory: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<Index>> {
        match &self.mode {
            Mode::Usearch => {
                let options = IndexOptions {
                    dimensions: index.dimensions.0.get(),
                    connectivity: index.connectivity.0,
                    expansion_add: index.expansion_add.0,
                    expansion_search: index.expansion_search.0,
                    metric: index.space_type.into(),
                    quantization: index.quantization.into(),
                    ..Default::default()
                };

                let idx = Arc::new(usearch::Index::new(&options)?);
                new(
                    idx,
                    index.id,
                    index.dimensions,
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

    fn stop(&self);
}

impl UsearchIndex for usearch::Index {
    fn reserve(&self, size: usize) -> anyhow::Result<()> {
        Ok(self.reserve(size)?)
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn size(&self) -> usize {
        self.size()
    }

    fn add(&self, key: Key, vector: &Vector) -> anyhow::Result<()> {
        Ok(self.add(key.0, &vector.0)?)
    }

    fn remove(&self, key: Key) -> anyhow::Result<()> {
        Ok(self.remove(key.0).map(|_| ())?)
    }

    fn search(
        &self,
        vector: &Vector,
        limit: Limit,
    ) -> anyhow::Result<impl Iterator<Item = (Key, Distance)>> {
        let matches = self.search(&vector.0, limit.0.get())?;
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

                let idx = Arc::new(TokioRwLock::new(IndexState::new(
                    Arc::clone(&idx),
                    dimensions,
                )));

                let mut allocate_prev = Allocate::Can;

                while let Some(msg) = rx.recv().await {
                    if !check_memory_allocation(&msg, &memory, &mut allocate_prev, &id).await {
                        continue;
                    }

                    dispatch_task(msg, Arc::clone(&idx), &tokio_semaphore, &rayon_semaphore).await;
                }

                idx.write().await.stop();

                debug!("finished");
            }
        }
        .instrument(debug_span!("usearch", "{id}")),
    );

    Ok(tx)
}

async fn dispatch_task(
    msg: Index,
    idx: Arc<TokioRwLock<IndexState<impl UsearchIndex + Send + Sync + 'static>>>,
    tokio_semaphore: &Arc<Semaphore>,
    rayon_semaphore: &Arc<Semaphore>,
) {
    if let Index::AddOrReplace { .. } = &msg
        && let Err(err) = reserve(Arc::clone(rayon_semaphore), Arc::clone(&idx)).await
    {
        error!("unable to reserve space for index: {err}");
        return;
    }
    let lock = idx.read_owned().await;
    if should_run_on_tokio(&msg) {
        let permit = Arc::clone(tokio_semaphore).acquire_owned().await.unwrap();
        tokio::spawn(async move {
            crate::move_to_the_end_of_async_runtime_queue().await;
            process(msg, &lock);
            drop(permit);
            drop(lock);
        });
    } else {
        let permit = Arc::clone(rayon_semaphore).acquire_owned().await.unwrap();
        rayon::spawn(move || {
            process(msg, &lock);
            drop(permit);
            drop(lock);
        });
    }
}

fn should_run_on_tokio(msg: &Index) -> bool {
    matches!(msg, Index::Ann { .. } | Index::Count { .. })
}

fn process<I: UsearchIndex + Send + Sync + 'static>(msg: Index, index: &IndexState<I>) {
    match msg {
        Index::AddOrReplace {
            primary_key,
            embedding,
            in_progress: _in_progress,
        } => {
            add_or_replace(
                Arc::clone(&index.idx),
                &index.keys,
                &index.usearch_key,
                primary_key,
                embedding,
            );
        }
        Index::Remove {
            primary_key,
            in_progress: _in_progress,
        } => {
            remove(Arc::clone(&index.idx), &index.keys, primary_key);
        }
        Index::Ann {
            embedding,
            limit,
            tx,
        } => {
            ann(
                Arc::clone(&index.idx),
                tx,
                &index.keys,
                embedding,
                index.dimensions,
                limit,
            );
        }
        Index::Count { tx } => {
            count(Arc::clone(&index.idx), tx);
        }
    }
}

async fn reserve(
    rayon_semaphore: Arc<Semaphore>,
    idx: Arc<TokioRwLock<IndexState<impl UsearchIndex + Send + Sync + 'static>>>,
) -> anyhow::Result<()> {
    // Check for capacity needs with a read lock first to avoid taking a write lock unnecessarily.
    let needed_capacity = {
        let lock = idx.read().await;
        needs_more_capacity(&lock.idx)
    };

    if needed_capacity.is_some() {
        let lock = idx.write_owned().await;
        // Re-check capacity after acquiring the write lock to avoid a race condition
        // where another thread might have already performed the reservation.
        if let Some(new_capacity) = needs_more_capacity(&lock.idx) {
            let permit = rayon_semaphore.acquire_owned().await.unwrap();
            let (tx, rx) = oneshot::channel::<anyhow::Result<()>>();
            rayon::spawn(move || {
                let result = lock.idx.reserve(new_capacity);
                if let Err(err) = &result {
                    error!("unable to reserve index capacity for {new_capacity} in usearch: {err}");
                } else {
                    debug!("reserve: reserved index capacity for {new_capacity}");
                }
                drop(permit);
                let _ = tx.send(result);
            });
            return rx
                .await
                .expect("reserve: unable to receive result from reserve task");
        }
    }
    Ok(())
}

fn needs_more_capacity(idx: &Arc<impl UsearchIndex>) -> Option<usize> {
    let capacity = idx.capacity();
    let free_space = capacity - idx.size();

    if free_space < RESERVE_THRESHOLD {
        Some(capacity + RESERVE_INCREMENT)
    } else {
        None
    }
}

fn add_or_replace(
    idx: Arc<impl UsearchIndex>,
    keys: &RwLock<BiMap<PrimaryKey, Key>>,
    usearch_key: &AtomicU64,
    primary_key: PrimaryKey,
    embedding: Vector,
) {
    let key = usearch_key.fetch_add(1, Ordering::Relaxed).into();

    let (key, remove) = if keys
        .write()
        .unwrap()
        .insert_no_overwrite(primary_key.clone(), key)
        .is_ok()
    {
        (key, false)
    } else {
        usearch_key.fetch_sub(1, Ordering::Relaxed);
        (
            *keys.read().unwrap().get_by_left(&primary_key).unwrap(),
            true,
        )
    };

    let remove_key_from_bimap = |key: &Key| {
        keys.write().unwrap().remove_by_right(key);
    };

    if remove && let Err(err) = idx.remove(key) {
        debug!("add_or_replace: unable to remove embedding for key {key}: {err}");
        return;
    }
    if let Err(err) = idx.add(key, &embedding) {
        debug!("add_or_replace: unable to add embedding for key {key}: {err}");
        remove_key_from_bimap(&key);
    };
}

fn remove(
    idx: Arc<impl UsearchIndex>,
    keys: &RwLock<BiMap<PrimaryKey, Key>>,
    primary_key: PrimaryKey,
) {
    let Some((_, key)) = keys.write().unwrap().remove_by_left(&primary_key) else {
        return;
    };

    if let Err(err) = idx.remove(key) {
        debug!("remove: unable to remove embeddings for key {key}: {err}");
    };
}

fn ann(
    idx: Arc<impl UsearchIndex>,
    tx_ann: oneshot::Sender<AnnR>,
    keys: &RwLock<BiMap<PrimaryKey, Key>>,
    embedding: Vector,
    dimensions: Dimensions,
    limit: Limit,
) {
    if let Err(err) = validator::embedding_dimensions(&embedding, dimensions) {
        return tx_ann
            .send(Err(err))
            .unwrap_or_else(|_| trace!("ann: unable to send response"));
    }

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
    if !matches!(msg, Index::AddOrReplace { .. }) {
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
    use tokio::sync::watch;
    use tokio::task;
    use tokio::time;

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
                    quantization: Quantization::F32,
                },
                memory::new(config_rx),
            )
            .unwrap();

        actor
            .add_or_replace(
                vec![CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
                vec![1., 1., 1.].into(),
                None,
            )
            .await;
        actor
            .add_or_replace(
                vec![CqlValue::Int(2), CqlValue::Text("two".to_string())].into(),
                vec![2., -2., 2.].into(),
                None,
            )
            .await;
        actor
            .add_or_replace(
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
            .add_or_replace(
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
                    quantization: Quantization::F32,
                },
                memory_tx,
            )
            .unwrap();

        let memory_respond = tokio::spawn(async move {
            let Memory::CanAllocate { tx } = memory_rx.recv().await.unwrap();
            _ = tx.send(Allocate::Cannot);
            memory_rx
        });
        actor
            .add_or_replace(vec![CqlValue::Int(1)].into(), vec![1., 1., 1.].into(), None)
            .await;
        let mut memory_rx = memory_respond.await.unwrap();
        assert_eq!(actor.count().await.unwrap(), 0);

        let memory_respond = tokio::spawn(async move {
            let Memory::CanAllocate { tx } = memory_rx.recv().await.unwrap();
            _ = tx.send(Allocate::Can);
        });
        actor
            .add_or_replace(vec![CqlValue::Int(1)].into(), vec![1., 1., 1.].into(), None)
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

    #[tokio::test]
    async fn quantization_to_kind_conversion() {
        assert_eq!(ScalarKind::from(Quantization::F32), ScalarKind::F32);
        assert_eq!(ScalarKind::from(Quantization::F16), ScalarKind::F16);
        assert_eq!(ScalarKind::from(Quantization::BF16), ScalarKind::BF16);
        assert_eq!(ScalarKind::from(Quantization::I8), ScalarKind::I8);
        assert_eq!(ScalarKind::from(Quantization::B1), ScalarKind::B1);
    }
}
