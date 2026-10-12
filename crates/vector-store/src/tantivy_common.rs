/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Plumbing shared by every Tantivy-backed index kind, parametrised by a [`TantivyBackend`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::anyhow;
use tantivy::IndexWriter;
use tantivy::ReloadPolicy;
use tantivy::Searcher;
use tantivy::TantivyDocument;
use tantivy::indexer::IndexWriterOptions;
use tantivy::schema::FAST;
use tantivy::schema::INDEXED;
use tantivy::schema::Schema;
use tantivy::schema::TextOptions;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::debug;
use tracing::error;

use crate::AsyncInProgress;
use crate::IndexKey;
use crate::memory::Allocate;
use crate::memory::Memory;
use crate::memory::MemoryExt;
use crate::perf;
use crate::table::IndexId;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use crate::table::TableSearch;
use crate::worker::Worker;
use crate::worker::WorkerExt;

pub(crate) const PRIMARY_ID_FIELD: &str = "primary_id";

pub(crate) const COMMIT_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const MAX_UNCOMMITTED_THRESHOLD: usize = 10_000;

/// Kind-specific behaviour of a Tantivy-backed index.
pub(crate) trait TantivyBackend: Clone + Send + Sync + 'static {
    /// Prefix of log and error messages, e.g. `fts`.
    const NAME: &'static str;

    /// Builds the schema of the index. It must contain a u64 [`PRIMARY_ID_FIELD`] field that is
    /// at least `INDEXED`, so documents can be deleted by their primary id.
    fn build_schema(&self) -> Schema;

    fn register_tokenizers(&self, index: &tantivy::Index) -> anyhow::Result<()>;

    fn create_doc(&self, schema: &Schema, primary_id: PrimaryId, text: &str) -> TantivyDocument;
}

struct Writer {
    writer: IndexWriter,
    // In-progress guards for documents written to the writer but not yet committed. They are held
    // here so the index is not reported as caught up (SERVING) until the commit that makes those
    // documents searchable has succeeded.
    uncommitted_docs_in_progress_guards: Vec<AsyncInProgress>,
}

impl Writer {
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

    fn commit(&mut self, reload: impl FnOnce() -> tantivy::Result<()>) -> tantivy::Result<()> {
        self.writer.commit()?;
        reload()?;
        self.uncommitted_docs_in_progress_guards.clear();
        Ok(())
    }

    fn uncommitted_docs(&self) -> usize {
        self.uncommitted_docs_in_progress_guards.len()
    }

    fn has_uncommitted_docs(&self) -> bool {
        !self.uncommitted_docs_in_progress_guards.is_empty()
    }
}

pub(crate) struct IndexState<B: TantivyBackend> {
    index: tantivy::Index,
    writer: RwLock<Writer>,
    reader: tantivy::IndexReader,
    schema: Schema,
    backend: B,
}

impl<B: TantivyBackend> IndexState<B> {
    fn new(backend: B) -> anyhow::Result<Self> {
        let schema = backend.build_schema();
        let index = tantivy::Index::create_in_ram(schema.clone());
        backend.register_tokenizers(&index)?;
        let options = IndexWriterOptions::builder()
            .num_worker_threads(perf::num_workers().into())
            .build();
        let writer = index
            .writer_with_options(options)
            .map_err(|e| anyhow!("{}: failed to create writer: {e}", B::NAME))?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| anyhow!("{}: failed to create reader: {e}", B::NAME))?;
        Ok(Self {
            index,
            writer: RwLock::new(Writer {
                writer,
                uncommitted_docs_in_progress_guards: Vec::new(),
            }),
            reader,
            schema,
            backend,
        })
    }

    pub(crate) fn index(&self) -> &tantivy::Index {
        &self.index
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn searcher(&self) -> Searcher {
        self.reader.searcher()
    }

    fn num_docs(&self) -> u64 {
        self.searcher().num_docs()
    }

    fn has_uncommitted_docs(&self) -> bool {
        self.writer.read().unwrap().has_uncommitted_docs()
    }
}

fn commit<B: TantivyBackend>(state: &IndexState<B>, key: &IndexKey) {
    let result = state
        .writer
        .write()
        .unwrap()
        .commit(|| state.reader.reload());
    if let Err(err) = result {
        error!("{}: failed to commit for {key}: {err}", B::NAME);
    }
}

fn handle_add_document<B: TantivyBackend>(
    state: &IndexState<B>,
    primary_id: PrimaryId,
    document: String,
    in_progress: AsyncInProgress,
) -> usize {
    let doc = state
        .backend
        .create_doc(&state.schema, primary_id, &document);
    let mut writer = state.writer.write().unwrap();
    match writer.add_document(doc, in_progress) {
        Ok(pending) => pending,
        Err(err) => {
            error!("{}: failed to add document {primary_id:?}: {err}", B::NAME);
            writer.uncommitted_docs()
        }
    }
}

fn create_term(schema: &Schema, primary_id: PrimaryId) -> tantivy::Term {
    let primary_id_field = schema.get_field(PRIMARY_ID_FIELD).unwrap();
    tantivy::Term::from_field_u64(primary_id_field, u64::from(primary_id))
}

fn handle_remove_document<B: TantivyBackend>(
    state: &IndexState<B>,
    primary_id: PrimaryId,
    in_progress: AsyncInProgress,
) -> usize {
    let term = create_term(&state.schema, primary_id);
    state.writer.write().unwrap().rm_document(term, in_progress)
}

pub(crate) fn find_partition_id<B: TantivyBackend>(
    table: &impl TableSearch,
    index_key: &IndexKey,
) -> anyhow::Result<PartitionId> {
    let (partition_id, _) = table.partition_id(index_key, None).ok_or_else(|| {
        anyhow!(
            "{}: partition id not found for index key {index_key:?}",
            B::NAME
        )
    })?;
    Ok(partition_id)
}

/// Kind-specific messages of a Tantivy-backed index actor. Each kind handles its own queries and
/// hands writes and counts to the shared [`TantivyIndexer`].
pub(crate) trait TantivyMessage: Send + 'static {
    type Backend: TantivyBackend;

    fn handle<T: TableSearch + Send + Sync + 'static>(
        self,
        indexer: &mut TantivyIndexer<Self::Backend, T>,
    ) -> impl Future<Output = ()> + Send;
}

/// The part of a Tantivy-backed index actor shared by every index kind: the index states, the
/// document writes, the commits and the memory gating.
pub(crate) struct TantivyIndexer<B: TantivyBackend, T> {
    key: IndexKey,
    backend: B,
    states: BTreeMap<IndexId, Arc<IndexState<B>>>,
    table: Arc<RwLock<T>>,
    worker: async_channel::Sender<Worker>,
    allocate_rx: watch::Receiver<Allocate>,
    allocate_prev: Allocate,
    commit_threshold: usize,
}

impl<B: TantivyBackend, T: TableSearch + Send + Sync + 'static> TantivyIndexer<B, T> {
    async fn new(
        key: IndexKey,
        backend: B,
        table: Arc<RwLock<T>>,
        worker: async_channel::Sender<Worker>,
        memory: mpsc::Sender<Memory>,
        commit_threshold: usize,
    ) -> Self {
        Self {
            key,
            backend,
            states: BTreeMap::new(),
            table,
            worker,
            allocate_prev: Allocate::Can,
            allocate_rx: memory.subscribe_allocate().await,
            commit_threshold,
        }
    }

    pub(crate) fn table(&self) -> &Arc<RwLock<T>> {
        &self.table
    }

    pub(crate) async fn add_document(
        &mut self,
        primary_id: PrimaryId,
        document: String,
        in_progress: AsyncInProgress,
    ) {
        let Some(state) = self.get_or_create_state() else {
            return;
        };
        if !self.can_allocate_memory() {
            return;
        }
        self.spawn_write(state, move |state| {
            handle_add_document(state, primary_id, document, in_progress)
        })
        .await;
    }

    pub(crate) async fn remove_document(
        &mut self,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) {
        let Some(state) = self.get_or_create_state() else {
            return;
        };
        self.spawn_write(state, move |state| {
            handle_remove_document(state, primary_id, in_progress)
        })
        .await;
    }

    pub(crate) fn count(&self, index_key: &IndexKey) -> usize {
        self.state(index_key)
            .map(|s| s.num_docs() as usize)
            .unwrap_or(0)
    }

    /// Runs `query` on a worker against the state of `index_key` and sends its result to `tx`.
    /// Sends the result of `on_missing` instead when the index has no state yet; queries never
    /// create one.
    pub(crate) async fn spawn_query<R: Send + 'static>(
        &self,
        index_key: IndexKey,
        tx: oneshot::Sender<R>,
        on_missing: impl FnOnce(&IndexKey) -> R,
        query: impl FnOnce(&IndexState<B>, &IndexKey) -> R + Send + 'static,
    ) {
        let Some(state) = self.state(&index_key) else {
            _ = tx.send(on_missing(&index_key));
            return;
        };
        self.worker
            .spawn_blocking(move || {
                _ = tx.send(query(&state, &index_key));
            })
            .await;
    }

    async fn spawn_write(
        &self,
        state: Arc<IndexState<B>>,
        write: impl FnOnce(&IndexState<B>) -> usize + Send + 'static,
    ) {
        let key = self.key.clone();
        let commit_threshold = self.commit_threshold;
        self.worker
            .spawn_blocking(move || {
                let pending = write(&state);
                if pending >= commit_threshold {
                    commit(&state, &key);
                }
            })
            .await;
    }

    async fn commit_pending(&self) {
        for state in self.states.values() {
            if !state.has_uncommitted_docs() {
                continue;
            }
            let state = Arc::clone(state);
            let key = self.key.clone();
            self.worker
                .spawn_blocking(move || commit(&state, &key))
                .await;
        }
    }

    fn get_or_create_state(&mut self) -> Option<Arc<IndexState<B>>> {
        let key = &self.key;
        let index_id = self.table.read().unwrap().index_id(key)?;
        if let Some(state) = self.states.get(&index_id) {
            return Some(Arc::clone(state));
        }
        match IndexState::new(self.backend.clone()) {
            Ok(state) => {
                let state = Arc::new(state);
                self.states.insert(index_id, Arc::clone(&state));
                Some(state)
            }
            Err(err) => {
                error!("{}: failed to create index state for {key}: {err}", B::NAME);
                None
            }
        }
    }

    fn state(&self, index_key: &IndexKey) -> Option<Arc<IndexState<B>>> {
        let index_id = self.table.read().unwrap().index_id(index_key)?;
        self.states.get(&index_id).cloned()
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

/// Spawns the actor of a Tantivy-backed index. Messages go to [`TantivyMessage::handle`];
/// pending writes are committed every `commit_interval`, or as soon as `commit_threshold` of them
/// are waiting.
pub(crate) fn spawn_actor<M: TantivyMessage>(
    key: IndexKey,
    backend: M::Backend,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
    commit_interval: Duration,
    commit_threshold: usize,
) -> mpsc::Sender<M> {
    let (tx, rx) = mpsc::channel::<M>(perf::channel_size().into());
    tokio::spawn(async move {
        let name = <M::Backend as TantivyBackend>::NAME;
        debug!("{name} index actor starting for {key}");
        let indexer = TantivyIndexer::new(
            key.clone(),
            backend,
            table,
            worker,
            memory,
            commit_threshold,
        )
        .await;
        run_actor_loop(indexer, rx, commit_interval).await;
        debug!("{name} index actor finished for {key}");
    });
    tx
}

async fn run_actor_loop<M: TantivyMessage, T: TableSearch + Send + Sync + 'static>(
    mut indexer: TantivyIndexer<M::Backend, T>,
    mut rx: mpsc::Receiver<M>,
    commit_interval: Duration,
) {
    let mut interval = tokio::time::interval(commit_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    break;
                };
                msg.handle(&mut indexer).await;
            }
            _ = interval.tick() => {
                indexer.commit_pending().await;
            }
        }
    }
}

pub(crate) fn build_schema(text_field: &str, text_options: TextOptions) -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field(PRIMARY_ID_FIELD, INDEXED | FAST);
    schema_builder.add_text_field(text_field, text_options);
    schema_builder.build()
}

pub(crate) fn create_doc(
    schema: &Schema,
    primary_id: PrimaryId,
    text_field: &str,
    text: &str,
) -> TantivyDocument {
    let primary_id_field = schema.get_field(PRIMARY_ID_FIELD).unwrap();
    let text_field = schema.get_field(text_field).unwrap();

    let mut doc = TantivyDocument::new();
    doc.add_u64(primary_id_field, u64::from(primary_id));
    doc.add_text(text_field, text);
    doc
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
