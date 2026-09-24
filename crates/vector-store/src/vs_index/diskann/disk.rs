/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! A DiskANN store that keeps the vectors and the graph in one local file.
//!
//! # Layout
//!
//! One file per index, one record per node: a header naming
//! the owner, the adjacency list, then the vector, padded to whole pages. Edges
//! and vector share a record on purpose. A node's edges are read when it is
//! expanded, one hop after its vector was read to score it, so the page is
//! still in cache and the edge read costs nothing extra.
//!
//! The map that says which ids exist carries the record each one owns, and the
//! record header names its owner, so a reader that resolved a slot just before
//! it was reaped and handed out again sees the mismatch and treats the node as
//! gone.
//!
//! The file is sized once, to one record per point the index may hold, and is
//! sparse, so the records nothing has been written to cost no disk. A mapping
//! that never moves needs no lock around it.

use super::scylla::GraphStore;
use super::scylla::StoreError;
use super::scylla::VectorSource;
use crate::IndexKey;
use crate::PartitionId;
use crate::PrimaryId;
use crate::Vector;
use anyhow::Context as _;
use anyhow::bail;
use async_trait::async_trait;
use diskann::graph::AdjacencyList;
use memmap2::Advice;
use memmap2::MmapRaw;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt;
use std::path::Path;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use tracing::debug;

type Key = (PartitionId, PrimaryId);

/// Records are padded to this, so one never spans more pages than it needs.
const PAGE: usize = 4096;

/// Enough that unrelated slots rarely collide, small enough to stay cheap.
const STRIPES: usize = 1024;

/// The first bytes of a record: who owns it and how many edges it holds.
#[derive(Clone, Copy, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
struct Header {
    owner: u64,
    partition_id: u64,
    n_edges: u32,
    /// [`Header::TAG`] once named, so a record never written has no owner.
    tag: u32,
}

impl Header {
    const TAG: u32 = 0x6a6a_686d;

    fn key(&self) -> Option<Key> {
        (self.tag == Self::TAG).then(|| {
            (
                PartitionId::from(self.partition_id),
                PrimaryId::from(self.owner),
            )
        })
    }
}

/// Where things are inside a record.
#[derive(Debug, Clone, Copy)]
struct Layout {
    max_degree: usize,
    dim: usize,
    record: usize,
}

impl Layout {
    const HEADER: usize = std::mem::size_of::<Header>();

    fn new(dim: usize, max_degree: usize) -> Self {
        let raw = Self::HEADER + max_degree * size_of::<PrimaryId>() + dim * size_of::<f32>();
        Self {
            max_degree,
            dim,
            record: raw.div_ceil(PAGE) * PAGE,
        }
    }

    fn offset(&self, idx: usize) -> usize {
        idx * self.record
    }

    fn header(&self) -> Span {
        Span {
            offset: 0,
            len: Self::HEADER,
        }
    }

    /// The first `n` edges, or every edge a record holds if `n` is larger.
    fn edges(&self, n: usize) -> Span {
        Span {
            offset: Self::HEADER,
            len: n.min(self.max_degree) * size_of::<PrimaryId>(),
        }
    }

    /// The first `n` components, or the whole vector if `n` is larger.
    fn vector(&self, n: usize) -> Span {
        Span {
            offset: Self::HEADER + self.max_degree * size_of::<PrimaryId>(),
            len: n.min(self.dim) * size_of::<f32>(),
        }
    }
}

/// Bytes of a record, positioned so that they are certainly inside it.
///
/// Only [`Layout`] builds these, and it caps every count it is given, so
/// `offset + len` never passes the end of the region the span names. Those
/// regions lie end to end within the length [`Layout::new`] rounded up to, so
/// no span can reach past a record. That is what lets [`Record`] copy a span
/// without checking it.
#[derive(Debug, Clone, Copy)]
struct Span {
    offset: usize,
    len: usize,
}

/// The node file, sized once and mapped whole, and the locks of its records.
struct Mapping {
    map: MmapRaw,
    layout: Layout,
    records: usize,
    stripes: Box<[RwLock<()>]>,
}

impl Mapping {
    /// Create a node file with room for `records` records and map all of it.
    ///
    /// The file is sparse, so the records nothing has been written to cost no
    /// disk. Sizing it up front is what lets the mapping stay fixed.
    fn open(data_dir: &Path, layout: Layout, records: usize) -> anyhow::Result<Self> {
        let file = tempfile::tempfile_in(data_dir)
            .with_context(|| format!("failed to create a node file in {}", data_dir.display()))?;
        file.set_len((records * layout.record) as u64)
            .context("failed to size the node file")?;

        // The mapping keeps the inode alive on its own, so the descriptor can go.
        let map = MmapRaw::map_raw(&file).context("failed to map the node file")?;
        // Records are hit at random; reading their neighbours ahead is waste.
        map.advise(Advice::Random).ok();

        Ok(Self {
            map,
            layout,
            records,
            stripes: std::iter::repeat_with(RwLock::default)
                .take(STRIPES)
                .collect(),
        })
    }

    /// The lock of a record. Different slots are disjoint byte ranges, so no
    /// other lock is needed to access one.
    fn stripe(&self, slot: usize) -> &RwLock<()> {
        &self.stripes[slot % STRIPES]
    }

    /// Where `slot`'s record starts, or `None` if the file has no such slot.
    fn ptr(&self, slot: usize) -> Option<*mut u8> {
        (slot < self.records).then(|| {
            // SAFETY: the offset is inside the mapping.
            unsafe { self.map.as_mut_ptr().add(self.layout.offset(slot)) }
        })
    }

    /// `slot`'s record with its stripe held shared.
    fn read(&self, slot: usize) -> Option<Shared<'_>> {
        Some(Record {
            ptr: self.ptr(slot)?,
            layout: &self.layout,
            _stripe: self.stripe(slot).read().unwrap(),
        })
    }

    /// `slot`'s record with its stripe held exclusively.
    fn write(&self, slot: usize) -> Option<Exclusive<'_>> {
        Some(Record {
            ptr: self.ptr(slot)?,
            layout: &self.layout,
            _stripe: self.stripe(slot).write().unwrap(),
        })
    }
}

/// One record of the node file, and the guard of its stripe.
///
/// Reads copy out and writes copy in through the raw mapping pointer, so no
/// reference into the mapping is ever made. Only [`Mapping::read`] and
/// [`Mapping::write`] build one, and the guard lives as long as the record, so
/// every access happens under the stripe. Only an exclusive guard can write.
struct Record<'a, G> {
    ptr: *mut u8,
    layout: &'a Layout,
    _stripe: G,
}

type Shared<'a> = Record<'a, RwLockReadGuard<'a, ()>>;
type Exclusive<'a> = Record<'a, RwLockWriteGuard<'a, ()>>;

impl<G> Record<'_, G> {
    /// Copy `span` out into `out`, as far as the shorter of the two reaches.
    fn read(&self, span: Span, out: &mut [u8]) {
        let len = span.len.min(out.len());
        // SAFETY: `span` is inside the record and `len` is within both it and
        // `out`, which is ours. The stripe rules out a concurrent write.
        unsafe {
            self.ptr
                .add(span.offset)
                .copy_to_nonoverlapping(out.as_mut_ptr(), len)
        }
    }

    fn header(&self) -> Header {
        let mut bytes = [0; Layout::HEADER];
        self.read(self.layout.header(), &mut bytes);
        bytemuck::pod_read_unaligned(&bytes)
    }

    fn is_owned_by(&self, key: Key) -> bool {
        self.header().key() == Some(key)
    }

    fn edges(&self) -> Vec<PrimaryId> {
        let span = self.layout.edges(self.header().n_edges as usize);
        let mut edges = vec![PrimaryId::default(); span.len / size_of::<PrimaryId>()];
        self.read(span, bytemuck::cast_slice_mut(&mut edges));
        edges
    }

    fn vector(&self) -> Vec<f32> {
        let span = self.layout.vector(self.layout.dim);
        let mut vector = vec![0.0; span.len / size_of::<f32>()];
        self.read(span, bytemuck::cast_slice_mut(&mut vector));
        vector
    }
}

impl Exclusive<'_> {
    /// Copy `bytes` into `span`, as far as the shorter of the two reaches.
    fn write(&self, span: Span, bytes: &[u8]) {
        let len = span.len.min(bytes.len());
        // SAFETY: `span` is inside the record and `len` is within both it and
        // `bytes`, which is ours. The exclusive stripe rules out any
        // concurrent access.
        unsafe {
            self.ptr
                .add(span.offset)
                .copy_from_nonoverlapping(bytes.as_ptr(), len)
        }
    }

    fn set_header(&self, header: Header) {
        self.write(self.layout.header(), bytemuck::bytes_of(&header));
    }

    fn set_edges(&self, edges: &[PrimaryId]) {
        let span = self.layout.edges(edges.len());
        let mut header = self.header();
        header.n_edges = (span.len / size_of::<PrimaryId>()) as u32;
        self.set_header(header);
        self.write(span, bytemuck::cast_slice(edges));
    }

    fn set_vector(&self, vector: &[f32]) {
        self.write(
            self.layout.vector(vector.len()),
            bytemuck::cast_slice(vector),
        );
    }
}

/// A node and the record slot it owns. A dead node keeps its record until it
/// is reaped, so the repair pass can still read its edges.
#[derive(Debug, Clone, Copy)]
enum Node {
    Live(u32),
    Dead(u32),
}

impl Node {
    fn slot(self) -> u32 {
        let (Self::Live(slot) | Self::Dead(slot)) = self;
        slot
    }

    fn is_live(self) -> bool {
        matches!(self, Self::Live(_))
    }
}

/// Hands out record slots, reusing the ones reaped nodes gave back.
#[derive(Debug)]
struct Slots {
    next: u32,
    free: Vec<u32>,
    cap: u32,
}

impl Slots {
    fn new(cap: u32) -> Self {
        Self {
            next: 0,
            free: Vec::new(),
            cap,
        }
    }

    fn take(&mut self) -> anyhow::Result<u32> {
        if let Some(slot) = self.free.pop() {
            return Ok(slot);
        }
        if self.next == self.cap {
            bail!("the node store is full at {} records", self.cap);
        }
        let slot = self.next;
        self.next += 1;
        Ok(slot)
    }

    fn put(&mut self, slot: u32) {
        self.free.push(slot);
    }
}

/// A [`GraphStore`] and [`VectorSource`] over one local, memory-mapped file.
pub(super) struct DiskNodeStore {
    mapping: Mapping,
    nodes: RwLock<BTreeMap<Key, Node>>,
    /// Taken only while the `nodes` lock is held, never on its own.
    slots: Mutex<Slots>,
}

impl DiskNodeStore {
    pub(super) fn open(
        data_dir: &Path,
        index_key: &IndexKey,
        dim: usize,
        max_degree: usize,
        max_points: usize,
    ) -> anyhow::Result<Self> {
        if dim == 0 || max_degree == 0 {
            bail!("a node store needs a non-zero dimension and max degree");
        }

        // One record per point the index may hold, so the file is sized once.
        let records = max_points;
        let cap = u32::try_from(records).context("a node store holds at most 2^32 records")?;

        let layout = Layout::new(dim, max_degree);
        let mapping = Mapping::open(data_dir, layout, records)
            .with_context(|| format!("failed to open the node file for {index_key}"))?;
        debug!(
            "{index_key}: {records} node records of {} bytes in {}",
            layout.record,
            data_dir.display()
        );

        Ok(Self {
            mapping,
            nodes: RwLock::default(),
            slots: Mutex::new(Slots::new(cap)),
        })
    }

    fn node(&self, key: Key) -> Option<Node> {
        self.nodes.read().unwrap().get(&key).copied()
    }

    /// Drop `key` and hand its record back for reuse.
    fn release(&self, key: Key, slot: u32) {
        let mut nodes = self.nodes.write().unwrap();
        if nodes.remove(&key).is_some() {
            self.slots.lock().unwrap().put(slot);
        }
    }

    /// The record at `slot`, held shared, if `key` still owns it.
    fn read_owned(&self, key: Key, slot: u32) -> Option<Shared<'_>> {
        let record = self.mapping.read(slot as usize)?;
        record.is_owned_by(key).then_some(record)
    }

    /// The record at `slot`, held exclusively, if `key` still owns it.
    fn write_owned(&self, key: Key, slot: u32) -> Option<Exclusive<'_>> {
        let record = self.mapping.write(slot as usize)?;
        record.is_owned_by(key).then_some(record)
    }
}

impl fmt::Debug for DiskNodeStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiskNodeStore")
            .field("layout", &self.mapping.layout)
            .field("nodes", &self.nodes.read().unwrap().len())
            .field("slots", &self.slots.lock().unwrap())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl GraphStore for DiskNodeStore {
    fn is_live(&self, partition_id: PartitionId, id: PrimaryId) -> bool {
        self.node((partition_id, id)).is_some_and(Node::is_live)
    }

    async fn create(&self, partition_id: PartitionId, id: PrimaryId) -> Result<(), StoreError> {
        let key = (partition_id, id);

        // Claimed under the map lock, so two creates of one id cannot both win,
        // and a revived tombstone keeps the record it already had.
        let slot = {
            let mut nodes = self.nodes.write().unwrap();
            match nodes.entry(key) {
                Entry::Occupied(occupied) if occupied.get().is_live() => {
                    return Err(StoreError::Conflict);
                }
                Entry::Occupied(mut occupied) => {
                    let slot = occupied.get().slot();
                    occupied.insert(Node::Live(slot));
                    slot
                }
                Entry::Vacant(vacant) => {
                    let slot = match self.slots.lock().unwrap().take() {
                        Ok(slot) => slot,
                        Err(err) => return Err(StoreError::Backend(err)),
                    };
                    vacant.insert(Node::Live(slot));
                    slot
                }
            }
        };

        let Some(record) = self.mapping.write(slot as usize) else {
            // Undo, or the id is live without a record to write to.
            self.release(key, slot);
            return Err(StoreError::Backend(anyhow::anyhow!(
                "the node file has no record {slot}"
            )));
        };
        // Until the stripe was taken, a delete could reap the node and hand
        // its record to another one.
        if !matches!(self.node(key), Some(Node::Live(live)) if live == slot) {
            return Err(StoreError::Backend(anyhow::anyhow!(
                "{id} was deleted while it was being created"
            )));
        }

        // Put the node's name on its record and leave it with no edges.
        record.set_header(Header {
            owner: id.into(),
            partition_id: partition_id.into(),
            n_edges: 0,
            tag: Header::TAG,
        });
        Ok(())
    }

    fn mark_dead(&self, partition_id: PartitionId, id: PrimaryId) -> Result<(), StoreError> {
        match self.nodes.write().unwrap().get_mut(&(partition_id, id)) {
            Some(node) if node.is_live() => {
                *node = Node::Dead(node.slot());
                Ok(())
            }
            _ => Err(StoreError::Conflict),
        }
    }

    async fn get(
        &self,
        partition_id: PartitionId,
        id: PrimaryId,
    ) -> anyhow::Result<Option<AdjacencyList<PrimaryId>>> {
        let key = (partition_id, id);
        let Some(node) = self.node(key) else {
            return Ok(None);
        };

        // Reaped and handed to another node since the map was read.
        let Some(record) = self.read_owned(key, node.slot()) else {
            return Ok(None);
        };
        let mut list = AdjacencyList::with_capacity(self.mapping.layout.max_degree);
        list.overwrite_trusted(&record.edges());
        Ok(Some(list))
    }

    async fn set(
        &self,
        partition_id: PartitionId,
        id: PrimaryId,
        edges: &[PrimaryId],
    ) -> anyhow::Result<()> {
        let key = (partition_id, id);
        if let Some(node) = self.node(key) {
            let mut list = AdjacencyList::with_capacity(self.mapping.layout.max_degree);
            list.extend_from_slice(edges);
            list.truncate(self.mapping.layout.max_degree);

            if let Some(record) = self.write_owned(key, node.slot()) {
                record.set_edges(&list);
            }
        }
        Ok(())
    }

    async fn append(
        &self,
        partition_id: PartitionId,
        id: PrimaryId,
        edges: &[PrimaryId],
    ) -> anyhow::Result<()> {
        let key = (partition_id, id);
        if let Some(node) = self.node(key)
            && let Some(record) = self.write_owned(key, node.slot())
        {
            let mut list = AdjacencyList::with_capacity(self.mapping.layout.max_degree);
            list.overwrite_trusted(&record.edges());
            list.extend_from_slice(edges);
            list.truncate(self.mapping.layout.max_degree);
            record.set_edges(&list);
        }
        Ok(())
    }

    async fn clear(&self, partition_id: PartitionId, id: PrimaryId) -> anyhow::Result<()> {
        let key = (partition_id, id);
        let Some(node) = self.node(key) else {
            return Ok(());
        };

        let Some(record) = self.mapping.write(node.slot() as usize) else {
            bail!("the node file has no record {}", node.slot());
        };

        // Re-check under the stripe lock, in one critical section: a
        // concurrent `create` may have revived this key (it keeps the same
        // slot) since `node` was read above, and branching on that stale
        // read would otherwise reap the revived node or blank its fresh
        // edges.
        let mut nodes = self.nodes.write().unwrap();
        match nodes.entry(key) {
            // Reap: the node goes, and its record joins the free list.
            Entry::Occupied(occupied) if !occupied.get().is_live() => {
                self.slots.lock().unwrap().put(occupied.remove().slot());
            }
            Entry::Occupied(_) => {
                drop(nodes);
                if record.is_owned_by(key) {
                    record.set_edges(&[]);
                }
            }
            Entry::Vacant(_) => {}
        }
        Ok(())
    }

    fn abandon(&self, partition_id: PartitionId, id: PrimaryId) {
        let key = (partition_id, id);
        let Some(node) = self.node(key) else {
            return;
        };

        let _stripe = self.mapping.stripe(node.slot() as usize).write().unwrap();

        // Re-check under the stripe lock and act on the current entry, not
        // the pre-lock snapshot: a concurrent `create` may have revived this
        // key (it keeps the same slot) since `node` was read above. Only a
        // still-live entry is ours to undo; if it is dead, a delete already
        // claimed it and reaping it is `clear`'s job.
        let mut nodes = self.nodes.write().unwrap();
        if let Entry::Occupied(occupied) = nodes.entry(key)
            && occupied.get().is_live()
        {
            self.slots.lock().unwrap().put(occupied.remove().slot());
        }
    }
}

#[async_trait]
impl VectorSource for DiskNodeStore {
    async fn get(
        &self,
        partition_id: PartitionId,
        ids: &[PrimaryId],
    ) -> anyhow::Result<Vec<Option<Vector>>> {
        let slots: Vec<Option<u32>> = {
            let nodes = self.nodes.read().unwrap();
            ids.iter()
                .map(|&id| nodes.get(&(partition_id, id)).map(|node| node.slot()))
                .collect()
        };

        // Ask for the whole batch first, so the kernel pages the records in
        // together rather than one fault at a time.
        {
            let layout = &self.mapping.layout;
            let span = layout.vector(layout.dim);
            for slot in slots
                .iter()
                .flatten()
                .map(|&slot| slot as usize)
                .filter(|&slot| slot < self.mapping.records)
            {
                self.mapping
                    .map
                    .advise_range(
                        Advice::WillNeed,
                        layout.offset(slot) + span.offset,
                        span.len,
                    )
                    .ok();
            }
        }

        Ok(ids
            .iter()
            .zip(&slots)
            .map(|(&id, &slot)| {
                let slot = slot?;
                let record = self.read_owned((partition_id, id), slot)?;
                Some(Vector::from(record.vector()))
            })
            .collect())
    }

    async fn put(
        &self,
        partition_id: PartitionId,
        id: PrimaryId,
        vector: &[f32],
    ) -> anyhow::Result<()> {
        let layout = &self.mapping.layout;
        if vector.len() != layout.dim {
            bail!(
                "put: {id} has {} dimensions, the store holds {}",
                vector.len(),
                layout.dim
            );
        }

        let key = (partition_id, id);
        let Some(node) = self.node(key) else {
            bail!("put: {id} is not a node");
        };

        let Some(record) = self.write_owned(key, node.slot()) else {
            bail!("put: the record of {id} was handed to another node");
        };
        record.set_vector(vector);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(dim: usize, max_degree: usize) -> DiskNodeStore {
        store_holding(dim, max_degree, 64)
    }

    fn store_holding(dim: usize, max_degree: usize, max_points: usize) -> DiskNodeStore {
        DiskNodeStore::open(
            &std::env::temp_dir(),
            &IndexKey::new(
                &crate::KeyspaceName::from("ks".to_string()),
                &crate::IndexName::from("idx".to_string()),
            ),
            dim,
            max_degree,
            max_points,
        )
        .unwrap()
    }

    fn part() -> PartitionId {
        PartitionId::from(1u64)
    }

    /// A [`PrimaryId`] with the row slot and the epoch spelled out.
    fn id(idx: u64, epoch: u64) -> PrimaryId {
        PrimaryId::from(epoch << 48 | idx)
    }

    async fn edges(store: &DiskNodeStore, id: PrimaryId) -> Option<Vec<PrimaryId>> {
        GraphStore::get(store, part(), id)
            .await
            .unwrap()
            .map(|list| list.to_vec())
    }

    async fn vectors(store: &DiskNodeStore, ids: &[PrimaryId]) -> Vec<Option<Vector>> {
        VectorSource::get(store, part(), ids).await.unwrap()
    }

    fn slot_of(store: &DiskNodeStore, id: PrimaryId) -> Option<u32> {
        store.node((part(), id)).map(Node::slot)
    }

    #[tokio::test]
    async fn a_node_is_absent_then_live_then_dead() {
        let store = store(2, 3);

        // Absent: no edges, and every write to it is dropped.
        assert_eq!(edges(&store, id(1, 0)).await, None);
        store.set(part(), id(1, 0), &[id(2, 0)]).await.unwrap();
        store.append(part(), id(1, 0), &[id(2, 0)]).await.unwrap();
        store.clear(part(), id(1, 0)).await.unwrap();
        assert_eq!(edges(&store, id(1, 0)).await, None);

        // Live: an empty list rather than an absent node.
        store.create(part(), id(1, 0)).await.unwrap();
        assert!(store.is_live(part(), id(1, 0)));
        assert_eq!(edges(&store, id(1, 0)).await, Some(vec![]));

        // Deduplicated, then capped at the max degree.
        store
            .set(
                part(),
                id(1, 0),
                &[id(2, 0), id(2, 0), id(3, 0), id(4, 0), id(5, 0)],
            )
            .await
            .unwrap();
        assert_eq!(
            edges(&store, id(1, 0)).await,
            Some(vec![id(2, 0), id(3, 0), id(4, 0)])
        );

        // A second create is refused without touching the winner's edges.
        assert!(matches!(
            store.create(part(), id(1, 0)).await,
            Err(StoreError::Conflict)
        ));

        // Dead: invisible, but the repair pass can still read the edges.
        store.mark_dead(part(), id(1, 0)).unwrap();
        assert!(!store.is_live(part(), id(1, 0)));
        assert_eq!(
            edges(&store, id(1, 0)).await,
            Some(vec![id(2, 0), id(3, 0), id(4, 0)])
        );
        assert!(matches!(
            store.mark_dead(part(), id(1, 0)),
            Err(StoreError::Conflict)
        ));

        // Clearing a dead node reaps it.
        store.clear(part(), id(1, 0)).await.unwrap();
        assert_eq!(edges(&store, id(1, 0)).await, None);
    }

    #[test]
    fn a_record_never_written_has_no_owner() {
        let store = store(2, 2);
        let key = (PartitionId::from(0u64), PrimaryId::from(0u64));

        assert!(store.read_owned(key, 0).is_none());
    }

    #[tokio::test]
    async fn two_epochs_of_one_row_do_not_share_a_record() {
        let store = store(2, 4);
        let (old, new) = (id(7, 0), id(7, 1));

        store.create(part(), old).await.unwrap();
        store.put(part(), old, &[1.0, 2.0]).await.unwrap();
        store.set(part(), old, &[id(1, 0), id(2, 0)]).await.unwrap();

        // The new epoch arrives while the old one is still being repaired.
        store.create(part(), new).await.unwrap();
        store.put(part(), new, &[3.0, 4.0]).await.unwrap();
        store.set(part(), new, &[id(3, 0)]).await.unwrap();

        assert_ne!(slot_of(&store, old), slot_of(&store, new));
        assert_eq!(edges(&store, old).await, Some(vec![id(1, 0), id(2, 0)]));
        assert_eq!(edges(&store, new).await, Some(vec![id(3, 0)]));
        assert_eq!(
            vectors(&store, &[old, new]).await,
            vec![
                Some(Vector::from(vec![1.0, 2.0])),
                Some(Vector::from(vec![3.0, 4.0])),
            ]
        );
    }

    #[tokio::test]
    async fn a_reaped_node_gives_its_record_back() {
        let store = store(2, 2);

        store.create(part(), id(1, 0)).await.unwrap();
        let reaped = slot_of(&store, id(1, 0)).unwrap();
        store.mark_dead(part(), id(1, 0)).unwrap();
        store.clear(part(), id(1, 0)).await.unwrap();

        store.create(part(), id(2, 0)).await.unwrap();
        assert_eq!(slot_of(&store, id(2, 0)), Some(reaped));
        assert_eq!(edges(&store, id(2, 0)).await, Some(vec![]));
    }

    #[tokio::test]
    async fn vectors_are_aligned_with_the_ids_and_absent_for_non_nodes() {
        let store = store(2, 2);

        store.create(part(), id(1, 0)).await.unwrap();
        store.put(part(), id(1, 0), &[1.0, 1.0]).await.unwrap();
        store.create(part(), id(3, 0)).await.unwrap();
        store.put(part(), id(3, 0), &[3.0, 3.0]).await.unwrap();

        assert_eq!(
            vectors(&store, &[id(1, 0), id(2, 0), id(3, 0), id(1, 1)]).await,
            vec![
                Some(Vector::from(vec![1.0, 1.0])),
                None,
                Some(Vector::from(vec![3.0, 3.0])),
                None,
            ]
        );
        assert!(store.put(part(), id(2, 0), &[2.0, 2.0]).await.is_err());
        assert!(store.put(part(), id(1, 0), &[1.0]).await.is_err());
    }

    #[tokio::test]
    async fn abandon_leaves_the_id_absent() {
        let store = store(2, 2);

        store.create(part(), id(1, 0)).await.unwrap();
        store.set(part(), id(1, 0), &[id(2, 0)]).await.unwrap();
        store.abandon(part(), id(1, 0));

        assert!(!store.is_live(part(), id(1, 0)));
        assert_eq!(edges(&store, id(1, 0)).await, None);

        store.create(part(), id(1, 0)).await.unwrap();
        assert_eq!(edges(&store, id(1, 0)).await, Some(vec![]));
    }

    #[tokio::test]
    async fn a_partition_does_not_see_another_partitions_nodes() {
        let store = store(2, 2);
        let other = PartitionId::from(2u64);

        store.create(part(), id(1, 0)).await.unwrap();
        store.put(part(), id(1, 0), &[1.0, 1.0]).await.unwrap();

        assert!(!store.is_live(other, id(1, 0)));
        assert!(
            GraphStore::get(&store, other, id(1, 0))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            VectorSource::get(&store, other, &[id(1, 0)]).await.unwrap(),
            vec![None]
        );
    }

    #[tokio::test]
    async fn the_store_holds_no_more_than_its_records() {
        let store = store_holding(2, 2, 2);

        store.create(part(), id(1, 0)).await.unwrap();
        store.create(part(), id(2, 0)).await.unwrap();
        assert!(matches!(
            store.create(part(), id(3, 0)).await,
            Err(StoreError::Backend(_))
        ));

        // Reaping one lets the next node in.
        store.mark_dead(part(), id(1, 0)).unwrap();
        store.clear(part(), id(1, 0)).await.unwrap();
        store.create(part(), id(3, 0)).await.unwrap();
        assert_eq!(edges(&store, id(3, 0)).await, Some(vec![]));
    }
}
