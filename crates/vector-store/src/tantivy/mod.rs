/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::btree_map;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::anyhow;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tantivy::Directory;
use tantivy::IndexWriter;
use tantivy::Opstamp;
use tantivy::ReloadPolicy;
use tantivy::Searcher;
use tantivy::TantivyDocument;
use tantivy::directory::WatchCallback;
use tantivy::directory::WatchHandle;
use tantivy::index::SegmentId;
use tantivy::indexer::IndexWriterOptions;
use tantivy::merge_policy::LogMergePolicy;
use tantivy::merge_policy::MergePolicy;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::FAST;
use tantivy::schema::INDEXED;
use tantivy::schema::Schema;
use tantivy::schema::TextOptions;
use tantivy::tokenizer::TextAnalyzer;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::error;

use crate::AsyncInProgress;
use crate::IndexKey;
use crate::memory::Allocate;
use crate::memory::Memory;
use crate::memory::MemoryExt;
use crate::perf;
use crate::table::IndexId;
use crate::table::PrimaryId;
use crate::table::TableSearch;
use crate::worker::Worker;
use crate::worker::WorkerExt;

pub(crate) const COMMIT_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const MAX_UNCOMMITTED_THRESHOLD: usize = 10_000;

struct SessionWriter {
    writer: IndexWriter,
    // Handed to the reader on commit, which holds them until the documents are searchable.
    uncommitted: Vec<AsyncInProgress>,
}

/// A writer and its threads exist only while its index is being written, so their number is
/// bounded by the number of indexes written at once, not by the number of indexes.
fn new_writer(index: &tantivy::Index) -> anyhow::Result<SessionWriter> {
    let options = IndexWriterOptions::builder()
        .num_worker_threads(1)
        .num_merge_threads(1)
        .build();
    let writer: IndexWriter = index
        .writer_with_options(options)
        .map_err(|e| anyhow!("tantivy: failed to create writer: {e}"))?;
    // The session starts the merges itself, so that it knows when they are done without
    // blocking on them.
    writer.set_merge_policy(Box::new(NoMergePolicy));
    Ok(SessionWriter {
        writer,
        uncommitted: Vec::new(),
    })
}

#[derive(Default)]
struct ReloadState {
    // Committed documents by commit opstamp, held until a reload makes them searchable.
    unsearchable_docs: BTreeMap<Opstamp, Vec<AsyncInProgress>>,
    reloaded_opstamp: Opstamp,
    reload_failed: bool,
}

/// A thin wrapper over the Tantivy reader that reloads it whenever the index metadata changes and
/// drops the in-progress guards of committed documents once they are searchable.
pub(crate) struct Reader {
    // Shared with the reload task.
    inner: Arc<ReaderInner>,
    _meta_watch: WatchHandle,
    // Lets the tick request a reload through the same channel as the watch callback.
    reload_tx: watch::Sender<()>,
}

struct ReaderInner {
    index: tantivy::Index,
    reader: tantivy::IndexReader,
    key: IndexKey,
    reload_state: Mutex<ReloadState>,
    worker: async_channel::Sender<Worker>,
}

impl ReaderInner {
    /// Reloads the reader and drops the guards of the committed documents it made searchable.
    fn reload(&self) {
        // The index meta holds the opstamp of its last commit, so a reload after reading it
        // serves at least that commit.
        let reloaded = self.index.load_metas().and_then(|meta| {
            self.reader.reload()?;
            Ok(meta.opstamp)
        });
        let mut reload_state = self.reload_state.lock().unwrap();
        match reloaded {
            Ok(opstamp) => {
                reload_state.reload_failed = false;
                reload_state.reloaded_opstamp = opstamp;
                reload_state
                    .unsearchable_docs
                    .retain(|committed, _| *committed > opstamp);
            }
            Err(err) => {
                reload_state.reload_failed = true;
                error!("tantivy: failed to reload reader for {}: {err}", self.key);
            }
        }
    }

    /// Runs the reload on the worker and returns once it has finished.
    async fn reload_on_worker(self: &Arc<Self>) {
        let (done_tx, done_rx) = oneshot::channel();
        let inner = Arc::clone(self);
        self.worker
            .spawn_blocking(move || {
                inner.reload();
                _ = done_tx.send(());
            })
            .await;
        _ = done_rx.await;
    }
}

impl Reader {
    fn new(
        index: &tantivy::Index,
        key: &IndexKey,
        worker: async_channel::Sender<Worker>,
    ) -> anyhow::Result<Self> {
        let reader: tantivy::IndexReader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| anyhow!("tantivy: failed to create reader: {e}"))?;
        let inner = Arc::new(ReaderInner {
            index: index.clone(),
            reader,
            key: key.clone(),
            reload_state: Mutex::new(ReloadState::default()),
            worker,
        });
        let (reload_tx, mut reload_rx) = watch::channel(());
        let callback = {
            let reload_tx = reload_tx.clone();
            move || _ = reload_tx.send(())
        };
        let meta_watch = index
            .directory()
            .watch(WatchCallback::new(callback))
            .map_err(|e| anyhow!("tantivy: failed to watch index metas: {e}"))?;
        tokio::spawn({
            let inner = Arc::clone(&inner);
            async move {
                // The reloads run one at a time, so a reload never stores an older searcher or
                // opstamp after a newer one. Ends when the reader and its watch callback are
                // dropped.
                while reload_rx.changed().await.is_ok() {
                    inner.reload_on_worker().await;
                }
            }
        });
        Ok(Self {
            inner,
            _meta_watch: meta_watch,
            reload_tx,
        })
    }

    pub(crate) fn searcher(&self) -> Searcher {
        self.inner.reader.searcher()
    }

    fn request_reload(&self) {
        _ = self.reload_tx.send(());
    }

    /// Whether committed documents wait for a reload or the last reload failed.
    fn needs_reload(&self) -> bool {
        let reload_state = self.inner.reload_state.lock().unwrap();
        reload_state.reload_failed || !reload_state.unsearchable_docs.is_empty()
    }

    /// Holds the guards until a reload makes its documents searchable.
    fn drop_guards_when_searchable(&self, commit_opstamp: Opstamp, guards: Vec<AsyncInProgress>) {
        let mut reload_state = self.inner.reload_state.lock().unwrap();
        // The watch callback of this commit may have reloaded before the guards got here.
        if commit_opstamp > reload_state.reloaded_opstamp {
            reload_state
                .unsearchable_docs
                .insert(commit_opstamp, guards);
        }
    }
}

#[derive(Default)]
struct Pending {
    // Changes waiting for the next flush.
    mutations: Vec<Mutation>,
    // A write session of the index is running; Tantivy allows one writer per index.
    session: bool,
    // The session is asked to flush whatever is pending.
    requested: bool,
    // The writer of the session holds changes a failed commit left behind.
    uncommitted: bool,
    // The index is gone: its session ends without finishing its merges.
    closed: bool,
}

pub(crate) struct Index {
    pub(crate) index: tantivy::Index,
    pending: Mutex<Pending>,
    // Wakes the write session when a flush is requested.
    wake: Notify,
    // Tells the mutations task that the session took the pending changes.
    taken: Notify,
    pub(crate) reader: Reader,
    pub(crate) schema: Schema,
}

impl Index {
    pub(crate) fn new(
        schema: Schema,
        tokenizer_name: &str,
        tokenizer: TextAnalyzer,
        key: &IndexKey,
        worker: async_channel::Sender<Worker>,
    ) -> anyhow::Result<Self> {
        let index = tantivy::Index::create_in_ram(schema.clone());
        index.tokenizers().register(tokenizer_name, tokenizer);
        let reader = Reader::new(&index, key, worker)?;
        Ok(Self {
            index,
            pending: Mutex::new(Pending::default()),
            wake: Notify::new(),
            taken: Notify::new(),
            reader,
            schema,
        })
    }

    /// Asks for a flush of the pending changes. Returns whether the caller has to start
    /// a write session, which is the case when none is running.
    fn request_flush(&self) -> bool {
        let mut pending = self.pending.lock().unwrap();
        if pending.mutations.is_empty() && !pending.uncommitted {
            return false;
        }
        pending.requested = true;
        if pending.session {
            self.wake.notify_one();
            return false;
        }
        pending.session = true;
        true
    }
}

pub(crate) fn build_schema(text_options: TextOptions) -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("primary_id", INDEXED | FAST);
    schema_builder.add_text_field("text", text_options);
    schema_builder.build()
}

fn create_doc(schema: &Schema, primary_id: PrimaryId, document: &str) -> TantivyDocument {
    let primary_id_field = schema.get_field("primary_id").unwrap();
    let text_field = schema.get_field("text").unwrap();

    let mut doc = TantivyDocument::new();
    doc.add_u64(primary_id_field, u64::from(primary_id));
    doc.add_text(text_field, document);
    doc
}

/// Adds the changes to the writer and commits it. The changes of a failed commit stay
/// in the writer for the next one.
fn write(index: &Index, key: &IndexKey, writer: &mut SessionWriter, mutations: Vec<Mutation>) {
    for mutation in mutations {
        match mutation {
            Mutation::Add {
                primary_id,
                document,
                in_progress,
            } => {
                let doc = create_doc(&index.schema, primary_id, &document);
                match writer.writer.add_document(doc) {
                    Ok(_) => writer.uncommitted.push(in_progress),
                    Err(err) => error!("tantivy: failed to add document {primary_id:?}: {err}"),
                }
            }
            Mutation::Remove {
                primary_id,
                in_progress,
            } => {
                writer
                    .writer
                    .delete_term(create_term(&index.schema, primary_id));
                writer.uncommitted.push(in_progress);
            }
        }
    }
    match writer.writer.commit() {
        Ok(commit_opstamp) => index
            .reader
            .drop_guards_when_searchable(commit_opstamp, mem::take(&mut writer.uncommitted)),
        Err(err) => error!("tantivy: failed to commit for {key}: {err}"),
    }
}

type Merge = BoxFuture<'static, (Vec<SegmentId>, tantivy::Result<()>)>;

/// The merges of a write session that are running.
#[derive(Default)]
struct Merges {
    running: FuturesUnordered<Merge>,
    segments: HashSet<SegmentId>,
    failed: bool,
}

impl Merges {
    /// Starts the merge the merge policy asks for, one at a time as the writer has one merge
    /// thread.
    fn start(&mut self, index: &Index, key: &IndexKey, writer: &mut SessionWriter) {
        while !self.failed && self.running.is_empty() {
            let segments = match index.index.searchable_segment_metas() {
                Ok(segments) => segments,
                Err(err) => {
                    error!("tantivy: failed to list segments of {key}: {err}");
                    self.failed = true;
                    return;
                }
            };
            let free = segments
                .into_iter()
                .filter(|segment| !self.segments.contains(&segment.id()))
                .collect::<Vec<_>>();
            let Some(candidate) = LogMergePolicy::default()
                .compute_merge_candidates(&free)
                .into_iter()
                .next()
            else {
                return;
            };
            let segment_ids = candidate.0;
            self.segments.extend(segment_ids.iter().copied());
            let merge = writer.writer.merge(&segment_ids);
            self.running.push(Box::pin(async move {
                let merged = merge.await.map(|_| ());
                (segment_ids, merged)
            }));
        }
    }

    fn finished(
        &mut self,
        key: &IndexKey,
        segment_ids: Vec<SegmentId>,
        merged: tantivy::Result<()>,
    ) {
        for segment_id in segment_ids {
            self.segments.remove(&segment_id);
        }
        if let Err(err) = merged {
            error!("tantivy: failed to merge for {key}: {err}");
            self.failed = true;
        }
    }
}

/// A write session: flushes the pending changes with a writer that lives only for the session
/// and runs its merges. The writer stays until its merges are done. Only the flushes run on
/// the worker; the session itself waits without holding a thread, so a flush never waits
/// for a merge.
async fn write_session(
    index: Arc<Index>,
    key: IndexKey,
    worker: async_channel::Sender<Worker>,
    commit_threshold: usize,
) {
    let ready = |index: &Index| {
        let pending = index.pending.lock().unwrap();
        pending.requested || pending.mutations.len() >= commit_threshold
    };
    let mut writer: Option<SessionWriter> = None;
    let mut merges = Merges::default();
    loop {
        let mutations = {
            let mut pending = index.pending.lock().unwrap();
            pending.requested = false;
            mem::take(&mut pending.mutations)
        };
        index.taken.notify_one();
        let uncommitted = writer
            .as_ref()
            .is_some_and(|writer| !writer.uncommitted.is_empty());
        if !mutations.is_empty() || uncommitted {
            let (done_tx, done_rx) = oneshot::channel();
            worker
                .spawn_blocking({
                    let index = Arc::clone(&index);
                    let key = key.clone();
                    let writer = writer.take();
                    move || {
                        let writer = match writer {
                            Some(writer) => Ok(writer),
                            None => new_writer(&index.index),
                        };
                        _ = done_tx.send(match writer {
                            Ok(mut writer) => {
                                write(&index, &key, &mut writer, mutations);
                                Ok(writer)
                            }
                            Err(err) => {
                                error!("{err} for {key}");
                                Err(mutations)
                            }
                        });
                    }
                })
                .await;
            match done_rx.await {
                Ok(Ok(written)) => {
                    index.pending.lock().unwrap().uncommitted = !written.uncommitted.is_empty();
                    writer = Some(written);
                }
                Ok(Err(mut unwritten)) => {
                    // Without a writer the changes go back to wait for the next flush, which
                    // starts a new session.
                    let mut pending = index.pending.lock().unwrap();
                    unwritten.append(&mut pending.mutations);
                    pending.mutations = unwritten;
                    pending.session = false;
                    return;
                }
                Err(_) => {}
            }
        }

        while !ready(&index) {
            if index.pending.lock().unwrap().closed {
                break;
            }
            if let Some(writer) = &mut writer {
                merges.start(&index, &key, writer);
            }
            let uncommitted = writer
                .as_ref()
                .is_some_and(|writer| !writer.uncommitted.is_empty());
            if merges.running.is_empty() && !uncommitted {
                break;
            }
            tokio::select! {
                _ = index.wake.notified() => {}
                Some((segment_ids, merged)) = merges.running.next() => {
                    merges.finished(&key, segment_ids, merged);
                }
            }
        }
        if ready(&index) {
            continue;
        }

        // The merges still running are abandoned, which happens only for a closed index.
        merges = Merges::default();
        if let Some(writer) = writer.take() {
            // Dropping a writer joins its threads.
            _ = tokio::task::spawn_blocking(move || drop(writer)).await;
        }
        let mut pending = index.pending.lock().unwrap();
        pending.uncommitted = false;
        if !pending.requested && pending.mutations.len() < commit_threshold {
            pending.session = false;
            return;
        }
    }
}

fn create_term(schema: &Schema, primary_id: PrimaryId) -> tantivy::Term {
    let primary_id_field = schema.get_field("primary_id").unwrap();
    tantivy::Term::from_field_u64(primary_id_field, u64::from(primary_id))
}

enum Mutation {
    Add {
        primary_id: PrimaryId,
        document: String,
        in_progress: AsyncInProgress,
    },
    Remove {
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    },
}

/// Spawns the task that takes the changes of the index. The changes waiting in the queue
/// are buffered together; once the buffer reaches the threshold, the write session of the index
/// is asked to flush it, and the task stops taking changes while two thresholds are buffered.
fn spawn_mutations(
    index: Arc<Index>,
    key: IndexKey,
    worker: async_channel::Sender<Worker>,
    commit_threshold: usize,
) -> mpsc::Sender<Mutation> {
    let capacity = perf::channel_size().into();
    let (tx, mut rx) = mpsc::channel(capacity);
    tokio::spawn(async move {
        let mut mutations = Vec::with_capacity(capacity);
        while rx.recv_many(&mut mutations, capacity).await > 0 {
            // The buffer keeps the order of the changes, so a remove of a row may follow
            // its add from here on.
            for mutation in &mut mutations {
                if let Mutation::Add { in_progress, .. } = mutation {
                    in_progress.applied();
                }
            }
            let mut pending = {
                let mut pending = index.pending.lock().unwrap();
                pending.mutations.append(&mut mutations);
                pending.mutations.len()
            };
            if pending < commit_threshold {
                continue;
            }
            flush(&index, &key, &worker, commit_threshold);
            while pending >= 2 * commit_threshold {
                index.taken.notified().await;
                pending = index.pending.lock().unwrap().mutations.len();
            }
        }
    });
    tx
}

/// Has the pending changes of the index flushed, by its running write session or a new one.
fn flush(
    index: &Arc<Index>,
    key: &IndexKey,
    worker: &async_channel::Sender<Worker>,
    commit_threshold: usize,
) {
    if index.request_flush() {
        tokio::spawn(write_session(
            Arc::clone(index),
            key.clone(),
            worker.clone(),
            commit_threshold,
        ));
    }
}

struct IndexHandle {
    index: Arc<Index>,
    mutations: mpsc::Sender<Mutation>,
}

impl Drop for IndexHandle {
    fn drop(&mut self) {
        self.index.pending.lock().unwrap().closed = true;
        self.index.wake.notify_one();
    }
}

/// The tantivy indexes of one index key, one per index id.
pub(crate) struct Indexes<T, F> {
    key: IndexKey,
    table: Arc<RwLock<T>>,
    worker: async_channel::Sender<Worker>,
    commit_interval: tokio::time::Interval,
    commit_threshold: usize,
    indexes: BTreeMap<IndexId, IndexHandle>,
    allocate_rx: watch::Receiver<Allocate>,
    allocate_prev: Allocate,
    new_index: F,
}

impl<T, F> Indexes<T, F>
where
    T: TableSearch,
    F: Fn(&IndexKey, async_channel::Sender<Worker>) -> anyhow::Result<Index>,
{
    pub(crate) async fn new(
        key: IndexKey,
        table: Arc<RwLock<T>>,
        worker: async_channel::Sender<Worker>,
        memory: mpsc::Sender<Memory>,
        commit_interval: Duration,
        commit_threshold: usize,
        new_index: F,
    ) -> Self {
        let allocate_rx = memory.subscribe_allocate().await;
        let mut commit_interval = tokio::time::interval(commit_interval);
        commit_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            key,
            table,
            worker,
            commit_interval,
            commit_threshold,
            indexes: BTreeMap::new(),
            allocate_rx,
            allocate_prev: Allocate::Can,
            new_index,
        }
    }

    pub(crate) async fn add_document(
        &mut self,
        primary_id: PrimaryId,
        document: String,
        in_progress: AsyncInProgress,
    ) {
        if !self.can_allocate_memory() {
            return;
        }
        let Some(mutations) = self.get_or_create_queue() else {
            return;
        };
        _ = mutations
            .send(Mutation::Add {
                primary_id,
                document,
                in_progress,
            })
            .await;
    }

    pub(crate) async fn remove_document(
        &mut self,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) {
        let Some(mutations) = self.get_or_create_queue() else {
            return;
        };
        _ = mutations
            .send(Mutation::Remove {
                primary_id,
                in_progress,
            })
            .await;
    }

    pub(crate) fn get(&self, index_key: &IndexKey) -> Option<Arc<Index>> {
        let index_id = self.table.read().unwrap().index_id(index_key)?;
        self.indexes
            .get(&index_id)
            .map(|handle| Arc::clone(&handle.index))
    }

    pub(crate) fn count(&self, index_key: &IndexKey) -> usize {
        self.get(index_key)
            .map(|index| index.reader.searcher().num_docs() as usize)
            .unwrap_or(0)
    }

    pub(crate) async fn commit_interval_tick(&mut self) {
        self.commit_interval.tick().await;
    }

    /// Commits every index with uncommitted documents and requests a reload of every other index
    /// whose committed documents are not searchable yet.
    pub(crate) async fn commit_or_reload(&self) {
        for IndexHandle { index, .. } in self.indexes.values() {
            let pending = {
                let pending = index.pending.lock().unwrap();
                !pending.mutations.is_empty() || pending.uncommitted
            };
            if pending {
                flush(index, &self.key, &self.worker, self.commit_threshold);
            } else if index.reader.needs_reload() {
                index.reader.request_reload();
            }
        }
    }

    fn get_or_create_queue(&mut self) -> Option<&mpsc::Sender<Mutation>> {
        let index_id = self.table.read().unwrap().index_id(&self.key)?;
        let handle = match self.indexes.entry(index_id) {
            btree_map::Entry::Occupied(entry) => entry.into_mut(),
            btree_map::Entry::Vacant(entry) => {
                match (self.new_index)(&self.key, self.worker.clone()) {
                    Ok(index) => {
                        let index = Arc::new(index);
                        let mutations = spawn_mutations(
                            Arc::clone(&index),
                            self.key.clone(),
                            self.worker.clone(),
                            self.commit_threshold,
                        );
                        entry.insert(IndexHandle { index, mutations })
                    }
                    Err(err) => {
                        error!("tantivy: failed to create index for {}: {err}", self.key);
                        return None;
                    }
                }
            }
        };
        Some(&handle.mutations)
    }

    fn can_allocate_memory(&mut self) -> bool {
        let allocate = *self.allocate_rx.borrow();
        if allocate == Allocate::Cannot {
            if self.allocate_prev == Allocate::Can {
                error!(
                    "Unable to add document for index {}: not enough memory",
                    self.key
                );
            }
            self.allocate_prev = allocate;
            return false;
        }
        self.allocate_prev = allocate;
        true
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;
    use std::sync::RwLock;

    use scylla::value::CqlValue;
    use tokio::sync::mpsc;
    use tokio::sync::watch;

    use crate::IndexKey;
    use crate::PrimaryKey;
    use crate::memory::Allocate;
    use crate::memory::Memory;
    use crate::table::IndexIdGenerator;
    use crate::table::MockTableSearch;
    use crate::table::PartitionId;

    pub(crate) fn make_table_with_keys() -> Arc<RwLock<MockTableSearch>> {
        let index_id = IndexIdGenerator::new().next(true).unwrap();
        let partition_id = PartitionId::global(index_id);
        let mut mock = MockTableSearch::new();
        mock.expect_index_id()
            .returning(move |_index_key| Some(index_id));
        mock.expect_partition_id()
            .returning(move |_index_key, _restrictions| Some((partition_id, None)));
        mock.expect_primary_key()
            .returning(|_partition_id, primary_id| {
                let id_val = u64::from(primary_id);
                Some(PrimaryKey::from(vec![CqlValue::BigInt(id_val as i64)]))
            });
        Arc::new(RwLock::new(mock))
    }

    pub(crate) fn make_index_key() -> IndexKey {
        IndexKey::new(&"ks".into(), &"idx".into())
    }

    pub(crate) fn make_memory_actor(allocate: Allocate) -> mpsc::Sender<Memory> {
        let (tx, mut rx) = mpsc::channel::<Memory>(1);
        tokio::spawn(async move {
            let (watch_tx, _) = watch::channel(allocate);
            while let Some(msg) = rx.recv().await {
                match msg {
                    Memory::SubscribeAllocate { tx } => {
                        let _ = tx.send(watch_tx.subscribe());
                    }
                }
            }
        });
        tx
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::path::Path;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use rstest::rstest;
    use tantivy::IndexSettings;
    use tantivy::directory::FileHandle;
    use tantivy::directory::INDEX_WRITER_LOCK;
    use tantivy::directory::RamDirectory;
    use tantivy::directory::WritePtr;
    use tantivy::directory::error::DeleteError;
    use tantivy::directory::error::OpenReadError;
    use tantivy::directory::error::OpenWriteError;
    use tantivy::schema::TEXT;

    use super::test_support::make_index_key;
    use super::test_support::make_memory_actor;
    use super::test_support::make_table_with_keys;
    use super::*;
    use crate::table::MockTableSearch;
    use crate::worker;

    const TEST_COMMIT_THRESHOLD: usize = 3;
    const TEST_COMMIT_INTERVAL: Duration = Duration::from_millis(50);

    /// An in-memory directory that fails a set number of the next writes of the index metadata,
    /// which fails the commits that make them.
    #[derive(Clone, Debug, Default)]
    struct FailingDirectory {
        directory: RamDirectory,
        failures: Arc<AtomicUsize>,
    }

    impl Directory for FailingDirectory {
        fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
            self.directory.get_file_handle(path)
        }

        fn delete(&self, path: &Path) -> Result<(), DeleteError> {
            self.directory.delete(path)
        }

        fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
            self.directory.exists(path)
        }

        fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
            self.directory.open_write(path)
        }

        fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
            self.directory.atomic_read(path)
        }

        fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
            let fail = path == Path::new("meta.json")
                && self
                    .failures
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |failures| {
                        failures.checked_sub(1)
                    })
                    .is_ok();
            if fail {
                return Err(io::Error::other("injected failure"));
            }
            self.directory.atomic_write(path, data)
        }

        fn sync_directory(&self) -> io::Result<()> {
            self.directory.sync_directory()
        }

        fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
            self.directory.watch(watch_callback)
        }
    }

    fn new_index(
        directory: impl Directory,
        key: &IndexKey,
        worker: async_channel::Sender<Worker>,
    ) -> anyhow::Result<Index> {
        let schema = build_schema(TEXT);
        let index = tantivy::Index::create(directory, schema.clone(), IndexSettings::default())?;
        let reader = Reader::new(&index, key, worker)?;
        Ok(Index {
            index,
            pending: Mutex::new(Pending::default()),
            wake: Notify::new(),
            taken: Notify::new(),
            reader,
            schema,
        })
    }

    type NewIndex = Box<dyn Fn(&IndexKey, async_channel::Sender<Worker>) -> anyhow::Result<Index>>;

    /// Indexes that commit only when the test asks them to or on reaching the threshold.
    async fn make_indexes(directory: impl Directory + Clone) -> Indexes<MockTableSearch, NewIndex> {
        let new_index: NewIndex =
            Box::new(move |key, worker| new_index(directory.clone(), key, worker));
        Indexes::new(
            make_index_key(),
            make_table_with_keys(),
            worker::new(),
            make_memory_actor(Allocate::Can),
            Duration::from_secs(3600),
            TEST_COMMIT_THRESHOLD,
            new_index,
        )
        .await
    }

    /// Commits like the interval does until the documents holding the guards are searchable.
    async fn commit_until_searchable(
        indexes: &Indexes<MockTableSearch, NewIndex>,
        mut searchable: mpsc::Receiver<()>,
    ) {
        loop {
            indexes.commit_or_reload().await;
            let recv = tokio::time::timeout(TEST_COMMIT_INTERVAL, searchable.recv());
            if recv.await.is_ok() {
                return;
            }
        }
    }

    fn has_writer(index: &Index) -> bool {
        // A writer holds the writer lock of its index, which is a file in the directory.
        index
            .index
            .directory()
            .exists(&INDEX_WRITER_LOCK.filepath)
            .unwrap()
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn idle_index_holds_no_writer() {
        let mut indexes = make_indexes(RamDirectory::create()).await;
        let key = make_index_key();

        let (tx, rx) = mpsc::channel(1);
        indexes
            .add_document(1.into(), "first".into(), AsyncInProgress::fullscan(tx))
            .await;
        commit_until_searchable(&indexes, rx).await;
        let index = indexes.get(&key).unwrap();
        while has_writer(&index) {
            tokio::time::sleep(TEST_COMMIT_INTERVAL).await;
        }

        // The next write gets a writer again.
        let (tx, rx) = mpsc::channel(1);
        indexes
            .add_document(2.into(), "second".into(), AsyncInProgress::fullscan(tx))
            .await;
        commit_until_searchable(&indexes, rx).await;

        assert_eq!(indexes.count(&key), 2);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn failed_commit_is_retried() {
        let directory = FailingDirectory::default();
        let failures = Arc::clone(&directory.failures);
        let mut indexes = make_indexes(directory).await;
        let key = make_index_key();

        let (tx, rx) = mpsc::channel(1);
        indexes
            .add_document(1.into(), "first".into(), AsyncInProgress::fullscan(tx))
            .await;
        // The index is created by now, so the failure hits the first commit.
        failures.store(1, Ordering::Relaxed);
        commit_until_searchable(&indexes, rx).await;

        assert_eq!(failures.load(Ordering::Relaxed), 0);
        assert_eq!(indexes.count(&key), 1);
    }
}
