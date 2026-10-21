/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::anyhow;
use tantivy::Directory;
use tantivy::IndexWriter;
use tantivy::Opstamp;
use tantivy::ReloadPolicy;
use tantivy::Searcher;
use tantivy::TantivyDocument;
use tantivy::directory::WatchCallback;
use tantivy::directory::WatchHandle;
use tantivy::indexer::IndexWriterOptions;
use tantivy::schema::FAST;
use tantivy::schema::INDEXED;
use tantivy::schema::Schema;
use tantivy::schema::TextOptions;
use tantivy::tokenizer::TextAnalyzer;
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

struct Writer {
    writer: IndexWriter,
    // Handed to the reader on commit, which holds them until the documents are searchable.
    uncommitted_docs_in_progress_guards: Vec<AsyncInProgress>,
}

impl Writer {
    fn new(index: &tantivy::Index) -> anyhow::Result<Self> {
        let options = IndexWriterOptions::builder()
            .num_worker_threads(perf::num_workers().into())
            .build();
        let writer = index
            .writer_with_options(options)
            .map_err(|e| anyhow!("tantivy: failed to create writer: {e}"))?;
        Ok(Self {
            writer,
            uncommitted_docs_in_progress_guards: Vec::new(),
        })
    }

    fn add_document(
        &mut self,
        doc: TantivyDocument,
        in_progress: AsyncInProgress,
    ) -> tantivy::Result<usize> {
        self.writer.add_document(doc)?;
        self.uncommitted_docs_in_progress_guards.push(in_progress);
        Ok(self.uncommitted_docs())
    }

    fn rm_document(&mut self, term: tantivy::Term, in_progress: AsyncInProgress) -> usize {
        self.writer.delete_term(term);
        self.uncommitted_docs_in_progress_guards.push(in_progress);
        self.uncommitted_docs()
    }

    /// Commits the documents to the index and returns the commit opstamp with their in-progress
    /// guards.
    fn commit(&mut self) -> tantivy::Result<(Opstamp, Vec<AsyncInProgress>)> {
        let opstamp = self.writer.commit()?;
        Ok((
            opstamp,
            mem::take(&mut self.uncommitted_docs_in_progress_guards),
        ))
    }

    fn uncommitted_docs(&self) -> usize {
        self.uncommitted_docs_in_progress_guards.len()
    }

    fn has_uncommitted_docs(&self) -> bool {
        !self.uncommitted_docs_in_progress_guards.is_empty()
    }
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

pub(crate) struct Index {
    pub(crate) index: tantivy::Index,
    writer: RwLock<Writer>,
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
        let writer = RwLock::new(Writer::new(&index)?);
        Ok(Self {
            index,
            writer,
            reader,
            schema,
        })
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

fn commit(index: &Index, key: &IndexKey) {
    let result = index.writer.write().unwrap().commit();
    match result {
        Ok((commit_opstamp, guards)) => index
            .reader
            .drop_guards_when_searchable(commit_opstamp, guards),
        Err(err) => error!("tantivy: failed to commit for {key}: {err}"),
    }
}

fn add_document(
    index: &Index,
    primary_id: PrimaryId,
    document: String,
    in_progress: AsyncInProgress,
) -> usize {
    let doc = create_doc(&index.schema, primary_id, &document);
    let mut writer = index.writer.write().unwrap();
    match writer.add_document(doc, in_progress) {
        Ok(pending) => pending,
        Err(err) => {
            error!("tantivy: failed to add document {primary_id:?}: {err}");
            writer.uncommitted_docs()
        }
    }
}

fn create_term(schema: &Schema, primary_id: PrimaryId) -> tantivy::Term {
    let primary_id_field = schema.get_field("primary_id").unwrap();
    tantivy::Term::from_field_u64(primary_id_field, u64::from(primary_id))
}

fn remove_document(index: &Index, primary_id: PrimaryId, in_progress: AsyncInProgress) -> usize {
    let term = create_term(&index.schema, primary_id);
    index.writer.write().unwrap().rm_document(term, in_progress)
}

/// The tantivy indexes of one index key, one per index id.
pub(crate) struct Indexes<T, F> {
    key: IndexKey,
    table: Arc<RwLock<T>>,
    worker: async_channel::Sender<Worker>,
    commit_interval: tokio::time::Interval,
    commit_threshold: usize,
    indexes: BTreeMap<IndexId, Arc<Index>>,
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
        let Some(index) = self.get_or_create() else {
            return;
        };
        if !self.can_allocate_memory() {
            return;
        }
        let key = self.key.clone();
        let commit_threshold = self.commit_threshold;
        self.worker
            .spawn_blocking(move || {
                let pending = add_document(&index, primary_id, document, in_progress);
                if pending >= commit_threshold {
                    commit(&index, &key);
                }
            })
            .await;
    }

    pub(crate) async fn remove_document(
        &mut self,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) {
        let Some(index) = self.get_or_create() else {
            return;
        };
        let key = self.key.clone();
        let commit_threshold = self.commit_threshold;
        self.worker
            .spawn_blocking(move || {
                let pending = remove_document(&index, primary_id, in_progress);
                if pending >= commit_threshold {
                    commit(&index, &key);
                }
            })
            .await;
    }

    pub(crate) fn get(&self, index_key: &IndexKey) -> Option<Arc<Index>> {
        let index_id = self.table.read().unwrap().index_id(index_key)?;
        self.indexes.get(&index_id).cloned()
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
        for index in self.indexes.values() {
            if index.writer.read().unwrap().has_uncommitted_docs() {
                let index = Arc::clone(index);
                let key = self.key.clone();
                self.worker
                    .spawn_blocking(move || commit(&index, &key))
                    .await;
            } else if index.reader.needs_reload() {
                index.reader.request_reload();
            }
        }
    }

    fn get_or_create(&mut self) -> Option<Arc<Index>> {
        let index_id = self.table.read().unwrap().index_id(&self.key)?;
        if let Some(index) = self.indexes.get(&index_id) {
            return Some(Arc::clone(index));
        }
        match (self.new_index)(&self.key, self.worker.clone()) {
            Ok(index) => {
                let index = Arc::new(index);
                self.indexes.insert(index_id, Arc::clone(&index));
                Some(index)
            }
            Err(err) => {
                error!("tantivy: failed to create index for {}: {err}", self.key);
                None
            }
        }
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
