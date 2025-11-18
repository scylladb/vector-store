/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Dimensions;
use crate::Distance;
use crate::IndexFactory;
use crate::IndexId;
use crate::Limit;
use crate::PrimaryKey;
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
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::trace;
use usearch::IndexOptions;
use usearch::MetricKind;
use usearch::ScalarKind;

pub struct UsearchIndexFactory {
    semaphore: Arc<Semaphore>,
    mode: Mode,
}

impl IndexFactory for UsearchIndexFactory {
    fn create_index(
        &self,
        index: IndexConfiguration,
        memory: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<Index>> {
        match self.mode {
            Mode::Usearch => {
                let options = IndexOptions {
                    dimensions: index.dimensions.0.get(),
                    connectivity: index.connectivity.0,
                    expansion_add: index.expansion_add.0,
                    expansion_search: index.expansion_search.0,
                    metric: index.space_type.into(),
                    quantization: ScalarKind::F32,
                    ..Default::default()
                };

                let idx = Arc::new(RwLock::new(usearch::Index::new(&options)?));
                new(
                    idx,
                    index.id,
                    index.dimensions,
                    Arc::clone(&self.semaphore),
                    memory,
                )
            }
        }
    }

    fn index_engine_version(&self) -> String {
        match self.mode {
            Mode::Usearch => format!("usearch-{}", usearch::version()),
        }
    }
}

pub fn new_usearch(semaphore: Arc<Semaphore>) -> anyhow::Result<UsearchIndexFactory> {
    Ok(UsearchIndexFactory {
        semaphore,
        mode: Mode::Usearch,
    })
}

enum Mode {
    Usearch,
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
}

impl UsearchIndex for RwLock<usearch::Index> {
    fn reserve(&self, size: usize) -> anyhow::Result<()> {
        Ok(self.write().unwrap().reserve(size)?)
    }

    fn capacity(&self) -> usize {
        self.read().unwrap().capacity()
    }

    fn size(&self) -> usize {
        self.read().unwrap().size()
    }

    fn add(&self, key: Key, vector: &Vector) -> anyhow::Result<()> {
        Ok(self.read().unwrap().add(key.0, &vector.0)?)
    }

    fn remove(&self, key: Key) -> anyhow::Result<()> {
        Ok(self.read().unwrap().remove(key.0).map(|_| ())?)
    }

    fn search(
        &self,
        vector: &Vector,
        limit: Limit,
    ) -> anyhow::Result<impl Iterator<Item = (Key, Distance)>> {
        let matches = self.read().unwrap().search(&vector.0, limit.0.get())?;
        Ok(matches
            .keys
            .into_iter()
            .zip(matches.distances)
            .map(|(key, distance)| (key.into(), distance.into())))
    }
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

fn new(
    idx: Arc<impl UsearchIndex + Send + Sync + 'static>,
    id: IndexId,
    dimensions: Dimensions,
    semaphore: Arc<Semaphore>,
    memory: mpsc::Sender<Memory>,
) -> anyhow::Result<mpsc::Sender<Index>> {
    idx.reserve(RESERVE_INCREMENT)?;

    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        async move {
            debug!("starting");

            // bimap between PrimaryKey and Key for an usearch index
            let keys = Arc::new(RwLock::new(BiMap::new()));

            // Incremental key for a usearch index
            let usearch_key = Arc::new(AtomicU64::new(0));

            while let Some(msg) = rx.recv().await {
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                tokio::spawn({
                    let idx = Arc::clone(&idx);
                    let keys = Arc::clone(&keys);
                    let usearch_key = Arc::clone(&usearch_key);
                    let memory = memory.clone();
                    async move {
                        crate::move_to_the_end_of_async_runtime_queue().await;
                        let allocate = memory
                            .can_allocate()
                            .await
                            .unwrap_or(Allocate::CannotAllocate);
                        process(msg, dimensions, idx, keys, usearch_key, allocate);
                        drop(permit);
                    }
                });
            }

            debug!("finished");
        }
        .instrument(debug_span!("usearch", "{}", id)),
    );

    Ok(tx)
}

fn process(
    msg: Index,
    dimensions: Dimensions,
    idx: Arc<impl UsearchIndex>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    usearch_key: Arc<AtomicU64>,
    allocate: Allocate,
) {
    match msg {
        Index::AddOrReplace {
            primary_key,
            embedding,
            in_progress: _in_progress,
        } => {
            add_or_replace(idx, keys, usearch_key, primary_key, embedding, allocate);
        }

        Index::Remove {
            primary_key,
            in_progress: _in_progress,
        } => {
            remove(idx, keys, primary_key);
        }

        Index::Ann {
            embedding,
            limit,
            tx,
        } => {
            ann(idx, tx, keys, embedding, dimensions, limit);
        }

        Index::Count { tx } => {
            count(idx, tx);
        }
    }
}

fn add_or_replace(
    idx: Arc<impl UsearchIndex>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    usearch_key: Arc<AtomicU64>,
    primary_key: PrimaryKey,
    embedding: Vector,
    allocate: Allocate,
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

    if allocate == Allocate::CannotAllocate {
        error!(
            "add_or_replace: unable to add embedding for key {key}: not enough memory to reserve more space"
        );
        return;
    }

    let remove_key_from_bimap = |key: &Key| {
        keys.write().unwrap().remove_by_right(key);
    };

    let capacity = idx.capacity();
    let free_space = capacity - idx.size();
    if free_space < RESERVE_THRESHOLD {
        // free space below threshold, reserve more space
        let capacity = capacity + RESERVE_INCREMENT;
        if let Err(err) = idx.reserve(capacity) {
            error!("unable to reserve index capacity for {capacity} in usearch: {err}");
            remove_key_from_bimap(&key);
            return;
        }
        debug!("add_or_replace: reserved index capacity for {capacity}");
    }

    if remove {
        if let Err(err) = idx.remove(key) {
            debug!("add_or_replace: unable to remove embedding for key {key}: {err}");
            return;
        };
    }
    if let Err(err) = idx.add(key, &embedding) {
        debug!("add_or_replace: unable to add embedding for key {key}: {err}");
        remove_key_from_bimap(&key);
    };
}

fn remove(
    idx: Arc<impl UsearchIndex>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
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
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connectivity;
    use crate::ExpansionAdd;
    use crate::ExpansionSearch;
    use crate::IndexId;
    use crate::index::IndexExt;
    use crate::memory;
    use scylla::value::CqlValue;
    use std::num::NonZeroUsize;
    use std::time::Duration;
    use tokio::task;
    use tokio::time;

    #[tokio::test]
    async fn add_or_replace_size_ann() {
        let factory = UsearchIndexFactory {
            semaphore: Arc::new(Semaphore::new(4)),
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
                },
                memory::new(),
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
        let options = IndexOptions {
            dimensions: NonZeroUsize::new(3).unwrap().into(),
            connectivity: Connectivity::default().0,
            expansion_add: ExpansionAdd::default().0,
            expansion_search: ExpansionSearch::default().0,
            ..Default::default()
        };
        let idx = Arc::new(RwLock::new(usearch::Index::new(&options).unwrap()));
        idx.write().unwrap().reserve(RESERVE_INCREMENT).unwrap();
        let keys = Arc::new(RwLock::new(BiMap::new()));
        let usearch_key = Arc::new(AtomicU64::new(0));
        let primary_key: PrimaryKey =
            vec![CqlValue::Int(3), CqlValue::Text("three".to_string())].into();
        let embedding: Vector = vec![2.1, -2.1, 2.1].into();

        let (tx, rx) = oneshot::channel();
        add_or_replace(
            idx.clone(),
            keys.clone(),
            usearch_key.clone(),
            primary_key.clone(),
            embedding.clone(),
            Allocate::CannotAllocate,
        );
        count(idx.clone(), tx);

        assert_eq!(rx.await.unwrap().unwrap(), 0);

        let (tx, rx) = oneshot::channel();
        add_or_replace(
            idx.clone(),
            keys.clone(),
            usearch_key.clone(),
            primary_key.clone(),
            embedding.clone(),
            Allocate::CanAllocate,
        );
        count(idx.clone(), tx);

        assert_eq!(rx.await.unwrap().unwrap(), 1)
    }
}
