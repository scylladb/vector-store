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
use tokio::sync::watch;
use tracing::error;

use crate::AsyncInProgress;
use crate::IndexKey;
use crate::memory::Allocate;
use crate::perf;
use crate::table::IndexId;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use crate::table::TableSearch;

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

    pub(crate) fn num_docs(&self) -> u64 {
        self.searcher().num_docs()
    }

    pub(crate) fn has_uncommitted_docs(&self) -> bool {
        self.writer.read().unwrap().has_uncommitted_docs()
    }
}

pub(crate) fn commit<B: TantivyBackend>(state: &IndexState<B>, key: &IndexKey) {
    let result = state
        .writer
        .write()
        .unwrap()
        .commit(|| state.reader.reload());
    if let Err(err) = result {
        error!("{}: failed to commit for {key}: {err}", B::NAME);
    }
}

pub(crate) fn handle_add_document<B: TantivyBackend>(
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

pub(crate) fn handle_remove_document<B: TantivyBackend>(
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

pub(crate) fn get_or_create_state<B: TantivyBackend, T: TableSearch>(
    states: &mut BTreeMap<IndexId, Arc<IndexState<B>>>,
    table: &RwLock<T>,
    key: &IndexKey,
    backend: &B,
) -> Option<Arc<IndexState<B>>> {
    let index_id = table.read().unwrap().index_id(key)?;
    if let Some(state) = states.get(&index_id) {
        return Some(Arc::clone(state));
    }
    match IndexState::new(backend.clone()) {
        Ok(state) => {
            let state = Arc::new(state);
            states.insert(index_id, Arc::clone(&state));
            Some(state)
        }
        Err(err) => {
            error!("{}: failed to create index state for {key}: {err}", B::NAME);
            None
        }
    }
}

pub(crate) fn get_state<B: TantivyBackend, T: TableSearch>(
    states: &BTreeMap<IndexId, Arc<IndexState<B>>>,
    table: &RwLock<T>,
    key: &IndexKey,
) -> Option<Arc<IndexState<B>>> {
    let index_id = table.read().unwrap().index_id(key)?;
    states.get(&index_id).cloned()
}

pub(crate) fn can_allocate_memory(
    rx_allocate: &watch::Receiver<Allocate>,
    allocate_prev: &mut Allocate,
    key: &IndexKey,
) -> bool {
    let allocate = *rx_allocate.borrow();
    if allocate == Allocate::Cannot {
        if *allocate_prev == Allocate::Can {
            error!("Unable to add document for index {key}: not enough memory");
        }
        *allocate_prev = allocate;
        return false;
    }
    *allocate_prev = allocate;
    true
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
