/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! A DiskANN backend that keeps the graph in RAM but not the vectors.
//!
//! The vectors we index already live in ScyllaDB. This backend fetches these
//! vectors instead of keeping a copy in RAM; the graph — adjacency lists and
//! node liveness — stays in RAM.
//!
//! # Identity ids
//!
//! We use [`crate::PrimaryId`] *as* the internal id: `to_internal_id` and
//! `to_external_id` are the identity, and there is no id map, no slot allocator
//! and no free list.
//!
//! A `PrimaryId` is a table row slot ([`PrimaryId::idx`]) plus a 16-bit epoch
//! bumped on every update of that row, so identity ids are sparse and the graph
//! is a `BTreeMap` keyed by the whole id. Every epoch of a row is its own key,
//! and that is what makes the ordering safe.
//!
//! Ids are not reclaimed, and they are not reuse-free either: the epoch wraps, so
//! after 65,536 updates to one row the same id is re-issued for different vector
//! data while this graph may still hold edges to it, but it is very unlikely.
//! That costs recall, not correctness.
//!
//! # What lives in RAM
//!
//! * The graph: one [`Node`] per vector, holding its adjacency list.
//! * Start points, vectors included. Their ids are [`PrimaryId::RESERVED`], not
//!   backed by a row, so they can never be read back from ScyllaDB.
//! * Vectors of in-flight inserts. Back-edge pruning asks for the new vector
//!   before the insert finishes. The row is already in ScyllaDB by then — every
//!   `AddVector` comes from a full scan or from CDC, both reads of the base
//!   table — so this only saves a point read. [`InflightGuard`] removes it on
//!   both success and failure.
//! * Per-operation working sets: candidates fetched for one prune, dropped when
//!   it ends.

use super::DiskannBackend;
use super::DiskannParams;
use crate::PartitionId;
use crate::PrimaryId;
use crate::PrimaryKey;
use crate::Vector;
use crate::db_index::DbIndex;
use crate::db_index::DbIndexExt;
use crate::table::TableSearch;
use anyhow::Context as _;
use anyhow::bail;
use async_trait::async_trait;
use diskann::ANNError;
use diskann::ANNResult;
use diskann::default_post_processor;
use diskann::graph::AdjacencyList;
use diskann::graph::DiskANNIndex;
use diskann::graph::SearchOutputBuffer;
use diskann::graph::glue;
use diskann::graph::workingset;
use diskann::graph::workingset::map::Project;
use diskann::graph::workingset::map::Ref;
use diskann::neighbor::Neighbor;
use diskann::provider;
use diskann::provider::DataProvider;
use diskann::provider::DefaultContext;
use diskann::provider::Delete;
use diskann::provider::ElementStatus;
use diskann::provider::HasId;
use diskann::provider::NeighborAccessor;
use diskann::provider::NeighborAccessorMut;
use diskann::provider::SetElement;
use diskann::utils::TypeStr;
use diskann::utils::VectorRepr;
use diskann_utils::Reborrow;
use diskann_vector::PreprocessedDistanceFunction;
use diskann_vector::contains::ContainsSimd;
use diskann_vector::distance::Metric;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tracing::warn;

/// Lets a [`Vector`] be stored in DiskANN's working set and handed back as a
/// `&[f32]` view. The crate provides these for `Vec<T>` and `Box<T>` only, so
/// a newtype has to supply its own.
impl Project<Ref<[f32]>> for Vector {
    fn project(&self) -> &[f32] {
        self.as_slice()
    }
}
/// Required of [`Delete::DeleteElementGuard`]: the guard's lifetime scopes the
/// borrowed element.
impl<'this> Reborrow<'this> for Vector {
    type Target = &'this [f32];

    fn reborrow(&'this self) -> Self::Target {
        self.as_slice()
    }
}

const START_ID: PrimaryId = PrimaryId::new_reserved();

impl TypeStr for PrimaryId {
    fn type_str() -> &'static str {
        u64::type_str()
    }
}

impl ContainsSimd for PrimaryId {
    fn contains_simd(vector: &[Self], target: Self) -> bool {
        u64::contains_simd(bytemuck::cast_slice(vector), u64::from(target))
    }
}

/// The source of vector data behind a [`ScyllaProvider`].
///
/// One source serves every per-partition index. Results are index-aligned with
/// the requested ids: `None` is a candidate with no vector to score, skipped at
/// a cost in recall. A source that cannot answer returns `Err`.
#[async_trait]
pub(super) trait VectorSource: Debug + Send + Sync + 'static {
    async fn get(
        &self,
        partition_id: PartitionId,
        ids: &[PrimaryId],
    ) -> anyhow::Result<Vec<Option<Vector>>>;
}

/// A [`VectorSource`] reading vectors back from the index's base table.
pub(super) struct BaseTableSource<T> {
    table: Arc<RwLock<T>>,
    db_index: mpsc::Sender<DbIndex>,
}

impl<T> BaseTableSource<T> {
    pub(super) fn new(table: Arc<RwLock<T>>, db_index: mpsc::Sender<DbIndex>) -> Self {
        Self { table, db_index }
    }
}

impl<T> Debug for BaseTableSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BaseTableSource").finish_non_exhaustive()
    }
}

#[async_trait]
impl<T> VectorSource for BaseTableSource<T>
where
    T: TableSearch + Send + Sync + 'static,
{
    async fn get(
        &self,
        partition_id: PartitionId,
        ids: &[PrimaryId],
    ) -> anyhow::Result<Vec<Option<Vector>>> {
        // Filter out ids that have no row in the table.
        let keys: Vec<Option<PrimaryKey>> = {
            let table = self.table.read().unwrap();
            ids.iter()
                .map(|&id| table.primary_key(partition_id, id))
                .collect()
        };

        let wanted: Vec<PrimaryKey> = keys.iter().flatten().cloned().collect();
        if wanted.is_empty() {
            return Ok(vec![None; ids.len()]);
        }

        let requested = wanted.len();
        let fetched = self.db_index.get_vectors(wanted).await.with_context(|| {
            format!(
                "BaseTableSource::get: failed to read {requested} vectors \
                 for partition {partition_id:?}"
            )
        })?;

        if fetched.len() != requested {
            bail!(
                "BaseTableSource::get: expected {requested} vectors, got {}",
                fetched.len()
            );
        }

        // Spread the key-aligned answers back over the id positions.
        let mut fetched = fetched.into_iter();
        Ok(keys
            .into_iter()
            .map(|key| key.and_then(|_| fetched.next().unwrap()))
            .collect())
    }
}

#[derive(Clone)]
pub(super) struct ScyllaBackend {
    strategy: ScyllaStrategy,
    context: DefaultContext,
    source: Arc<dyn VectorSource>,
}

impl ScyllaBackend {
    pub(super) fn new(source: Arc<dyn VectorSource>) -> Self {
        Self {
            strategy: ScyllaStrategy::default(),
            context: DefaultContext,
            source,
        }
    }
}

impl DiskannBackend for ScyllaBackend {
    type Provider = ScyllaProvider;
    type Strategy = ScyllaStrategy;

    fn create_index(
        &self,
        params: &DiskannParams,
        partition_id: PartitionId,
        start_point: &[f32],
    ) -> anyhow::Result<DiskANNIndex<Self::Provider>> {
        let provider = ScyllaProvider::new(
            start_point,
            usize::from(params.dim.0),
            params.metric,
            params.config.max_degree().get(),
            partition_id,
            Arc::clone(&self.source),
        )
        .context("failed to create ScyllaProvider")?;

        Ok(DiskANNIndex::new(params.config.clone(), provider, None))
    }

    fn strategy(&self) -> &Self::Strategy {
        &self.strategy
    }

    fn context(&self) -> &DefaultContext {
        &self.context
    }
}

#[derive(Debug)]
enum Node {
    Live(AdjacencyList<PrimaryId>),
    Dead(AdjacencyList<PrimaryId>),
}

impl Node {
    fn new(capacity: usize) -> Self {
        Self::Live(AdjacencyList::with_capacity(capacity))
    }

    fn is_live(&self) -> bool {
        matches!(self, Self::Live(_))
    }

    fn neighbors(&self) -> &AdjacencyList<PrimaryId> {
        let (Self::Live(neighbors) | Self::Dead(neighbors)) = self;
        neighbors
    }

    fn neighbors_mut(&mut self) -> &mut AdjacencyList<PrimaryId> {
        let (Self::Live(neighbors) | Self::Dead(neighbors)) = self;
        neighbors
    }

    fn mark_deleted(&mut self) {
        if let Self::Live(neighbors) = self {
            *self = Self::Dead(std::mem::take(neighbors));
        }
    }
}

/// Vectors held in RAM only for the duration of an insert.
type Inflight = Arc<RwLock<BTreeMap<PrimaryId, Vector>>>;

/// The graph, keyed by internal id.
type Nodes = Arc<RwLock<BTreeMap<PrimaryId, Node>>>;

/// A DiskANN provider whose vector reads go to a [`VectorSource`].
#[derive(Debug)]
pub(super) struct ScyllaProvider {
    nodes: Nodes,
    inflight: Inflight,
    start: Vector,
    start_neighbors: RwLock<AdjacencyList<PrimaryId>>,
    partition_id: PartitionId,
    source: Arc<dyn VectorSource>,
    max_degree: usize,
    dim: usize,
    metric: Metric,
}

impl ScyllaProvider {
    pub(super) fn new(
        start_point: &[f32],
        dim: usize,
        metric: Metric,
        max_degree: usize,
        partition_id: PartitionId,
        source: Arc<dyn VectorSource>,
    ) -> anyhow::Result<Self> {
        if start_point.len() != dim {
            bail!(
                "start point has dimension {} but the index expects {dim}",
                start_point.len()
            );
        }

        Ok(Self {
            nodes: Nodes::default(),
            inflight: Inflight::default(),
            start: Vector::from(start_point.to_vec()),
            start_neighbors: RwLock::new(AdjacencyList::with_capacity(max_degree)),
            partition_id,
            source,
            max_degree,
            dim,
            metric,
        })
    }

    /// Resolve internal `ids` to vector data with a single call to the source.
    ///
    /// The result is index-aligned with `ids`. `None` means "unavailable, skip
    /// this candidate": the node is gone, the source returned no row for it, or
    /// the row's vector does not match the index dimensions.
    async fn load(&self, ids: &[PrimaryId]) -> ANNResult<Vec<Option<Vector>>> {
        let mut out: Vec<Option<Vector>> = Vec::with_capacity(ids.len());

        // Positions in `out` that the source has to fill, and the ids to ask it
        // for. Kept aligned with each other.
        let mut slots: Vec<usize> = Vec::new();
        let mut wanted: Vec<PrimaryId> = Vec::new();

        // Scoped so the guard is gone before the await below
        {
            let inflight = self.inflight.read().unwrap();
            for (position, &id) in ids.iter().enumerate() {
                if id == START_ID {
                    out.push(Some(self.start.clone()));
                    continue;
                }

                if let Some(vector) = inflight.get(&id) {
                    out.push(Some(vector.clone()));
                    continue;
                }

                out.push(None);

                if self.is_live(id) {
                    slots.push(position);
                    wanted.push(id);
                }
            }
        }

        if wanted.is_empty() {
            return Ok(out);
        }

        let fetched = self
            .source
            .get(self.partition_id, &wanted)
            .await
            .map_err(|err| ANNError::message(format!("vector source failed: {err:#}")))?;

        if fetched.len() != wanted.len() {
            return Err(ANNError::message(format!(
                "vector source returned {} results for {} ids; \
                 implementations must return one entry per id, in order",
                fetched.len(),
                wanted.len()
            )));
        }

        for (position, vector) in slots.into_iter().zip(fetched) {
            if let Some(vector) = vector {
                if vector.len() != self.dim {
                    warn!(
                        "load: skipping id {}: base table holds {} dimensions, \
                        the index expects {}",
                        ids[position],
                        vector.len(),
                        self.dim,
                    );
                    continue;
                }
                out[position] = Some(vector);
            }
        }

        Ok(out)
    }

    fn is_live(&self, id: PrimaryId) -> bool {
        self.nodes
            .read()
            .unwrap()
            .get(&id)
            .is_some_and(|node| node.is_live())
    }

    fn adjacency(&self) -> NodeAccessor<'_> {
        NodeAccessor {
            nodes: &self.nodes,
            start_neighbors: &self.start_neighbors,
            max_degree: self.max_degree,
        }
    }
}

impl DataProvider for ScyllaProvider {
    type Context = DefaultContext;
    type InternalId = PrimaryId;
    type ExternalId = PrimaryId;
    type Error = ANNError;
    type Guard = InflightGuard;

    fn to_internal_id(
        &self,
        _context: &Self::Context,
        gid: &Self::ExternalId,
    ) -> Result<Self::InternalId, Self::Error> {
        if !self.is_live(*gid) {
            return Err(ANNError::message("no mapping"));
        }
        Ok(*gid)
    }

    fn to_external_id(
        &self,
        _context: &Self::Context,
        id: Self::InternalId,
    ) -> Result<Self::ExternalId, Self::Error> {
        if id == START_ID || !self.is_live(id) {
            return Err(ANNError::message("no mapping"));
        }
        Ok(id)
    }
}

impl SetElement<&[f32]> for ScyllaProvider {
    type SetError = ANNError;

    async fn set_element(
        &self,
        _context: &Self::Context,
        id: &Self::ExternalId,
        element: &[f32],
    ) -> Result<Self::Guard, Self::SetError> {
        if element.len() != self.dim {
            return Err(ANNError::message(format!(
                "wrong dimension: got {}, expected {}",
                element.len(),
                self.dim
            )));
        }

        let internal = *id;
        if internal == START_ID {
            return Err(ANNError::message(format!(
                "id {internal} is reserved for the start point"
            )));
        }

        match self.nodes.write().unwrap().entry(internal) {
            Entry::Occupied(mut occupied) => {
                if occupied.get().is_live() {
                    return Err(ANNError::message("id already exists"));
                }
                // A tombstone the graph has not finished reaping. Only reachable
                // once this id has been re-issued, which needs the `PrimaryId`
                // epoch to have wrapped, so treat it as a fresh node.
                *occupied.get_mut() = Node::new(self.max_degree);
            }
            Entry::Vacant(vacant) => {
                vacant.insert(Node::new(self.max_degree));
            }
        }

        // Back-edge pruning asks `fill` for this vector before the insert is
        // done. The row is already in ScyllaDB, since that is where it came
        // from, so holding it here just saves a read.
        self.inflight
            .write()
            .unwrap()
            .insert(internal, Vector::from(element.to_vec()));

        Ok(InflightGuard(Some(InflightState {
            inflight: Arc::clone(&self.inflight),
            id: internal,
            nodes: Arc::clone(&self.nodes),
        })))
    }
}

impl Delete for ScyllaProvider {
    /// Mark the node dead but keep its adjacency list.
    ///
    /// `inplace_delete` calls this *first* and only then reads the deleted
    /// node's neighbors to repair the graph around it; the list is dropped at
    /// the end, by `drop_adj_list` calling `set_neighbors(id, &[])`. Clearing it
    /// here would break the repair, so [`NodeAccessor::set_neighbors`] is what
    /// finally removes the entry.
    fn delete(
        &self,
        _context: &Self::Context,
        gid: &Self::ExternalId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let id = *gid;

        let result = match self.nodes.write().unwrap().get_mut(&id) {
            Some(node) if node.is_live() => {
                node.mark_deleted();
                Ok(())
            }
            _ => Err(ANNError::message("id already deleted")),
        };

        std::future::ready(result)
    }

    fn release(
        &self,
        _context: &Self::Context,
        _id: Self::InternalId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        // Nothing to reclaim, and the graph index never calls this.
        std::future::ready(Ok(()))
    }

    fn status_by_internal_id(
        &self,
        _context: &Self::Context,
        id: Self::InternalId,
    ) -> impl Future<Output = Result<ElementStatus, Self::Error>> + Send {
        std::future::ready(Ok(self.status(id)))
    }

    fn status_by_external_id(
        &self,
        _context: &Self::Context,
        gid: &Self::ExternalId,
    ) -> impl Future<Output = Result<ElementStatus, Self::Error>> + Send {
        std::future::ready(Ok(self.status(*gid)))
    }
}

impl ScyllaProvider {
    fn status(&self, id: PrimaryId) -> ElementStatus {
        if self.is_live(id) {
            ElementStatus::Valid
        } else {
            ElementStatus::Deleted
        }
    }
}

/// What an [`InflightGuard`] needs to undo the insert it guards.
struct InflightState {
    inflight: Inflight,
    id: PrimaryId,
    nodes: Nodes,
}

/// Cleans up after an insert: the in-flight vector copy always, and the node
/// `set_element` created unless the insert completed.
pub(super) struct InflightGuard(Option<InflightState>);

impl provider::Guard for InflightGuard {
    type Id = PrimaryId;

    async fn complete(mut self) {
        let state = self.0.take().expect("InflightGuard already consumed");
        state.inflight.write().unwrap().remove(&state.id);
    }

    fn id(&self) -> Self::Id {
        self.0.as_ref().expect("InflightGuard already consumed").id
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Some(state) = self.0.take() {
            state.inflight.write().unwrap().remove(&state.id);
            state.nodes.write().unwrap().remove(&state.id);
        }
    }
}

/// Read/write access to the graph's adjacency lists.
pub(super) struct NodeAccessor<'a> {
    nodes: &'a RwLock<BTreeMap<PrimaryId, Node>>,
    start_neighbors: &'a RwLock<AdjacencyList<PrimaryId>>,
    max_degree: usize,
}

impl NodeAccessor<'_> {
    /// Replace an adjacency list with `neighbors`, capped at the max degree.
    fn overwrite(list: &mut AdjacencyList<PrimaryId>, neighbors: &[PrimaryId], max_degree: usize) {
        list.clear();
        list.extend_from_slice(neighbors);
        list.truncate(max_degree);
    }
}

impl HasId for NodeAccessor<'_> {
    type Id = PrimaryId;
}

impl NeighborAccessor for NodeAccessor<'_> {
    fn get_neighbors(
        &mut self,
        id: Self::Id,
        neighbors: &mut AdjacencyList<Self::Id>,
    ) -> impl Future<Output = ANNResult<()>> + Send {
        neighbors.clear();
        if id == START_ID {
            neighbors.overwrite_trusted(&self.start_neighbors.read().unwrap());
        } else if let Some(node) = self.nodes.read().unwrap().get(&id) {
            neighbors.overwrite_trusted(node.neighbors());
        }
        std::future::ready(Ok(()))
    }
}

impl NeighborAccessorMut for NodeAccessor<'_> {
    fn set_neighbors(
        &mut self,
        id: Self::Id,
        neighbors: &[Self::Id],
    ) -> impl Future<Output = ANNResult<()>> + Send {
        if id == START_ID {
            Self::overwrite(
                &mut self.start_neighbors.write().unwrap(),
                neighbors,
                self.max_degree,
            );
            return std::future::ready(Ok(()));
        }

        let mut nodes = self.nodes.write().unwrap();
        if let Entry::Occupied(mut occupied) = nodes.entry(id) {
            if neighbors.is_empty() && !occupied.get().is_live() {
                // `drop_adj_list` at the end of an in-place delete. This is
                // the only place a completed delete removes its node; one that
                // never gets here is a tombstone, and nothing else clears it.
                occupied.remove();
            } else {
                Self::overwrite(
                    occupied.get_mut().neighbors_mut(),
                    neighbors,
                    self.max_degree,
                );
            }
        }

        // A missing node is not an error: a delete racing this insert may have
        // reaped it already. Dropping the write leaves in-edges pointing at an
        // absent id, which `load` skips without a ScyllaDB read.
        std::future::ready(Ok(()))
    }

    fn append_vector(
        &mut self,
        id: Self::Id,
        neighbors: &[Self::Id],
    ) -> impl Future<Output = ANNResult<()>> + Send {
        if id == START_ID {
            let mut start = self.start_neighbors.write().unwrap();
            start.extend_from_slice(neighbors);
            start.truncate(self.max_degree);
        } else if let Some(node) = self.nodes.write().unwrap().get_mut(&id) {
            let list = node.neighbors_mut();
            list.extend_from_slice(neighbors);
            list.truncate(self.max_degree);
        }

        std::future::ready(Ok(()))
    }
}

/// Serves graph search: adjacency from the graph, vectors from the source.
pub(super) struct ScyllaSearchAccessor<'a> {
    provider: &'a ScyllaProvider,
    adjacency: NodeAccessor<'a>,
    distance: <f32 as VectorRepr>::QueryDistance,
    neighbors: AdjacencyList<PrimaryId>,
    candidates: Vec<PrimaryId>,
}

impl<'a> ScyllaSearchAccessor<'a> {
    fn new(provider: &'a ScyllaProvider, query: &[f32]) -> ANNResult<Self> {
        if query.len() != provider.dim {
            return Err(ANNError::message(format!(
                "query has dimension {} but the index expects {}",
                query.len(),
                provider.dim
            )));
        }

        Ok(Self {
            provider,
            adjacency: provider.adjacency(),
            distance: f32::query_distance(query, provider.metric),
            neighbors: AdjacencyList::new(),
            candidates: Vec::new(),
        })
    }
}

impl HasId for ScyllaSearchAccessor<'_> {
    type Id = PrimaryId;
}

impl glue::SearchAccessor for ScyllaSearchAccessor<'_> {
    fn starting_points(&self) -> impl Future<Output = ANNResult<Vec<Self::Id>>> + Send {
        std::future::ready(Ok(vec![START_ID]))
    }

    async fn start_point_distances<F>(&mut self, mut f: F) -> ANNResult<()>
    where
        F: FnMut(Self::Id, f32) + Send,
    {
        f(
            START_ID,
            self.distance
                .evaluate_similarity(self.provider.start.as_slice()),
        );
        Ok(())
    }

    fn is_not_start_point(
        &self,
    ) -> impl Future<Output = ANNResult<impl Fn(Self::Id) -> bool + Send + Sync + 'static>> + Send
    {
        std::future::ready(Ok(|id: Self::Id| id != START_ID))
    }

    async fn expand_beam<Itr, P, F>(
        &mut self,
        ids: Itr,
        mut pred: P,
        mut on_neighbors: F,
    ) -> ANNResult<()>
    where
        Itr: Iterator<Item = Self::Id> + Send,
        P: glue::HybridPredicate<Self::Id> + Send + Sync,
        F: FnMut(Self::Id, f32) + Send,
    {
        // Collect the whole round's candidates first, so the fetch below is
        // one request per round rather than one per beam node.
        self.candidates.clear();
        for id in ids {
            self.adjacency
                .get_neighbors(id, &mut self.neighbors)
                .await?;
            // `eval_mut` is a test-and-set: it skips ids this search has
            // already scored and claims the rest, which is what keeps
            // `candidates` free of duplicates.
            self.candidates
                .extend(self.neighbors.iter().copied().filter(|i| pred.eval_mut(i)));
        }

        let loaded = self.provider.load(&self.candidates).await?;
        for (&id, vector) in self.candidates.iter().zip(loaded) {
            // A `None` is a row that vanished under us. Skipping is explicitly
            // allowed and costs recall, not correctness.
            if let Some(vector) = vector {
                on_neighbors(id, self.distance.evaluate_similarity(vector.as_slice()));
            }
        }

        Ok(())
    }
}

type WorkingSet = workingset::Map<PrimaryId, Vector, workingset::map::Ref<[f32]>>;
type WorkingSetView<'a> = workingset::map::View<'a, PrimaryId, Vector, workingset::map::Ref<[f32]>>;

/// What [`ScyllaPruneAccessor::fill`] does when the source cannot serve a
/// candidate batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum OnSourceFailure {
    #[default]
    Fail,
    Skip,
}

/// Serves index construction: element-to-element distances over a candidate set.
pub(super) struct ScyllaPruneAccessor<'a> {
    provider: &'a ScyllaProvider,
    adjacency: NodeAccessor<'a>,
    set: WorkingSet,
    distance: <f32 as VectorRepr>::Distance,
    on_source_failure: OnSourceFailure,
}

impl<'a> ScyllaPruneAccessor<'a> {
    fn new(
        provider: &'a ScyllaProvider,
        capacity: usize,
        on_source_failure: OnSourceFailure,
    ) -> Self {
        let set = workingset::map::Builder::new(workingset::map::Capacity::Default).build(capacity);

        Self {
            provider,
            adjacency: provider.adjacency(),
            set,
            distance: f32::distance(provider.metric, Some(provider.dim)),
            on_source_failure,
        }
    }
}

impl HasId for ScyllaPruneAccessor<'_> {
    type Id = PrimaryId;
}

impl<'p> glue::PruneAccessor for ScyllaPruneAccessor<'p> {
    type Neighbors<'a>
        = provider::Neighbors<'a, NodeAccessor<'p>>
    where
        Self: 'a;

    type ElementRef<'a> = &'a [f32];

    type View<'a>
        = WorkingSetView<'a>
    where
        Self: 'a;

    type Distance<'a>
        = <f32 as VectorRepr>::Distance
    where
        Self: 'a;

    fn neighbors(&mut self) -> Self::Neighbors<'_> {
        provider::Neighbors(&mut self.adjacency)
    }

    async fn fill<Itr>(&mut self, itr: Itr) -> ANNResult<(Self::View<'_>, Self::Distance<'_>)>
    where
        Itr: ExactSizeIterator<Item = Self::Id> + Clone + Send + Sync,
    {
        // `Map::fill` takes a *synchronous* closure, so the fetch cannot happen
        // inside it. Work out the misses, fetch them in one request, then let
        // `fill` drain the results.
        let misses: Vec<PrimaryId> = itr
            .clone()
            .filter(|id| !self.set.contains_key(id))
            .collect();

        let mut pending: HashMap<PrimaryId, Vector> = if misses.is_empty() {
            HashMap::new()
        } else {
            let loaded = match self.provider.load(&misses).await {
                Ok(loaded) => loaded,
                Err(err) if self.on_source_failure == OnSourceFailure::Skip => {
                    warn!(
                        "prune: scoring {} candidates as unavailable: {err}",
                        misses.len(),
                    );
                    vec![None; misses.len()]
                }
                Err(err) => return Err(err),
            };
            misses
                .iter()
                .copied()
                .zip(loaded)
                .filter_map(|(id, vector)| vector.map(|vector| (id, vector)))
                .collect()
        };

        let view = self
            .set
            .fill(itr, |id| ANNResult::Ok(pending.remove(&id)))?;

        Ok((view, self.distance))
    }
}

/// The strategy tying [`ScyllaProvider`] to DiskANN's graph operations.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ScyllaStrategy {
    on_source_failure: OnSourceFailure,
}

impl ScyllaStrategy {
    fn with(on_source_failure: OnSourceFailure) -> Self {
        Self { on_source_failure }
    }
}

impl<'a> glue::SearchStrategy<'a, ScyllaProvider, &'a [f32]> for ScyllaStrategy {
    type SearchAccessorError = ANNError;
    type SearchAccessor = ScyllaSearchAccessor<'a>;

    fn search_accessor(
        &'a self,
        provider: &'a ScyllaProvider,
        _context: &'a DefaultContext,
        query: &'a [f32],
    ) -> Result<Self::SearchAccessor, Self::SearchAccessorError> {
        ScyllaSearchAccessor::new(provider, query)
    }
}

impl<'a> glue::DefaultPostProcessor<'a, ScyllaProvider, &'a [f32], PrimaryId> for ScyllaStrategy {
    default_post_processor!(TranslateIds);
}

impl glue::PruneStrategy<ScyllaProvider> for ScyllaStrategy {
    type PruneAccessor<'a> = ScyllaPruneAccessor<'a>;
    type PruneAccessorError = ANNError;

    fn prune_accessor<'a>(
        &'a self,
        provider: &'a ScyllaProvider,
        _context: &'a DefaultContext,
        capacity: usize,
    ) -> Result<Self::PruneAccessor<'a>, Self::PruneAccessorError> {
        Ok(ScyllaPruneAccessor::new(
            provider,
            capacity,
            self.on_source_failure,
        ))
    }
}

impl<'a> glue::InsertStrategy<'a, ScyllaProvider, &'a [f32]> for ScyllaStrategy {
    type PruneStrategy = Self;

    /// An insert that cannot reach the source has no distances to prune on, and
    /// [`InflightGuard`] undoes the node, so failing leaves nothing behind.
    fn prune_strategy(&self) -> Self::PruneStrategy {
        Self::with(OnSourceFailure::Fail)
    }
}

/// In-place delete over full-precision vectors.
///
/// `VisitedAndTopK` searches *using* the deleted vector as the query, and by
/// the time we hear about a delete its row is already gone from the base table,
/// so [`Self::get_delete_element`] cannot serve it. Owning the node lifecycle
/// makes retaining it until the delete completes possible — the same trick as
/// [`InflightGuard`], in the other direction — but that is a follow-up.
impl glue::InplaceDeleteStrategy<ScyllaProvider> for ScyllaStrategy {
    type DeleteElement<'a> = &'a [f32];
    type DeleteElementGuard = Vector;
    type DeleteElementError = ANNError;
    type PruneStrategy = Self;
    type DeleteSearchAccessor<'a> = ScyllaSearchAccessor<'a>;
    type SearchPostProcessor = glue::CopyIds;
    type SearchStrategy = Self;

    /// `inplace_delete` erases the node before it prunes, and only its final
    /// `drop_adj_list` reaps the entry, so an error here would strand a
    /// tombstone. Repair what the source can serve and finish the delete.
    fn prune_strategy(&self) -> Self::PruneStrategy {
        Self::with(OnSourceFailure::Skip)
    }

    fn search_strategy(&self) -> Self::SearchStrategy {
        *self
    }

    fn search_post_processor(&self) -> Self::SearchPostProcessor {
        glue::CopyIds
    }

    async fn get_delete_element<'a>(
        &'a self,
        _provider: &'a ScyllaProvider,
        _context: &'a DefaultContext,
        id: PrimaryId,
    ) -> Result<Self::DeleteElementGuard, Self::DeleteElementError> {
        Err(ANNError::message(format!(
            "cannot supply the vector for internal id {id}: this provider retains no \
             vector data, and a deleted row is already gone from the base table by the \
             time the delete reaches us. Use InplaceDeleteMethod::OneHop or \
             TwoHopAndOneHop, neither of which reads the deleted vector."
        )))
    }
}

/// Turns internal ids into [`PrimaryId`]s for the caller.
///
/// Dropping ids with no mapping is also what removes start points and nodes
/// deleted mid-search from results, so no `FilterStartPoints` step is needed.
#[derive(Debug, Default)]
pub(super) struct TranslateIds;

impl<'a> glue::SearchPostProcess<ScyllaSearchAccessor<'a>, &'a [f32], PrimaryId> for TranslateIds {
    type Error = ANNError;

    fn post_process<I, B>(
        &self,
        accessor: &mut ScyllaSearchAccessor<'a>,
        _query: &'a [f32],
        candidates: I,
        output: &mut B,
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send
    where
        I: Iterator<Item = Neighbor<PrimaryId>> + Send,
        B: SearchOutputBuffer<PrimaryId> + Send + ?Sized,
    {
        let provider = accessor.provider;
        let start_len = output.current_len();

        for candidate in candidates {
            let Ok(external) = provider.to_external_id(&DefaultContext, *candidate.id()) else {
                // A start point, or a node deleted mid-search.
                continue;
            };

            if output.push(external, candidate.distance()).is_full() {
                break;
            }
        }

        std::future::ready(Ok(output.current_len() - start_len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Vector;
    use crate::table::MockTableSearch;
    use scylla::value::CqlValue;

    fn key(id: u64) -> PrimaryKey {
        PrimaryKey::from([CqlValue::BigInt(id as i64)])
    }

    #[tokio::test]
    async fn get_aligns_answers_with_the_requested_ids() {
        let mut table = MockTableSearch::new();
        // Ids 1, 3 and 4 resolve to primary keys; id 2 does not.
        table.expect_primary_key().returning(|_, id| {
            [1, 3, 4]
                .contains(&u64::from(id))
                .then(|| key(u64::from(id)))
        });

        let (db_index, mut rx) = mpsc::channel(1);

        let responder = async {
            let Some(DbIndex::GetVectors { keys, tx }) = rx.recv().await else {
                panic!("expected a GetVectors request");
            };
            tx.send(Ok(vec![Some(Vector::from(vec![1.0, 2.0])), None]))
                .unwrap();
            keys
        };

        let ids = [
            PrimaryId::from(1u64),
            PrimaryId::from(2u64),
            PrimaryId::from(3u64),
        ];
        let source = BaseTableSource::new(Arc::new(RwLock::new(table)), db_index);
        let (keys, vectors) = tokio::join!(responder, source.get(PartitionId::from(0u64), &ids));

        assert_eq!(keys, vec![key(1), key(3)]);
        assert_eq!(
            vectors.unwrap(),
            vec![Some(Vector::from(vec![1.0, 2.0])), None, None,]
        );
    }

    fn table_with_all_keys() -> MockTableSearch {
        let mut table = MockTableSearch::new();
        table
            .expect_primary_key()
            .returning(|_, id| Some(key(u64::from(id))));
        table
    }

    #[tokio::test]
    async fn get_propagates_a_source_failure() {
        let (db_index, mut rx) = mpsc::channel(1);

        let responder = async {
            let Some(DbIndex::GetVectors { tx, .. }) = rx.recv().await else {
                panic!("expected a GetVectors request");
            };
            tx.send(Err(anyhow::anyhow!("scylla is down"))).unwrap();
        };

        let ids = [PrimaryId::from(1u64), PrimaryId::from(2u64)];
        let source = BaseTableSource::new(Arc::new(RwLock::new(table_with_all_keys())), db_index);
        let (_, result) = tokio::join!(responder, source.get(PartitionId::from(7u64), &ids));

        // The failure must not masquerade as a batch of missing rows.
        let err = result.expect_err("a source failure must not be reported as absent rows");
        let err = format!("{err:#}");
        assert!(err.contains("PartitionId(7)"), "{err}");
        assert!(err.contains("scylla is down"), "{err}");
    }

    #[tokio::test]
    async fn get_rejects_a_short_answer() {
        let (db_index, mut rx) = mpsc::channel(1);

        let responder = async {
            let Some(DbIndex::GetVectors { tx, .. }) = rx.recv().await else {
                panic!("expected a GetVectors request");
            };
            // Two keys were asked for, one vector comes back.
            tx.send(Ok(vec![Some(Vector::from(vec![1.0, 2.0]))]))
                .unwrap();
        };

        let ids = [PrimaryId::from(1u64), PrimaryId::from(2u64)];
        let source = BaseTableSource::new(Arc::new(RwLock::new(table_with_all_keys())), db_index);
        let (_, result) = tokio::join!(responder, source.get(PartitionId::from(0u64), &ids));

        let err = format!(
            "{:#}",
            result.expect_err("a short answer is a broken contract")
        );
        assert!(err.contains("expected 2 vectors, got 1"), "{err}");
    }
}
