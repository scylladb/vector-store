/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;

use anyhow::anyhow;
use tantivy::IndexWriter;
use tantivy::TantivyDocument;
use tantivy::collector::TopDocs;
use tantivy::indexer::IndexWriterOptions;
use tantivy::query::QueryParser;
use tantivy::schema::INDEXED;
use tantivy::schema::STORED;
use tantivy::schema::Schema;
use tantivy::schema::TEXT;
use tantivy::schema::Value;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::debug;
use tracing::error;

use crate::IndexKey;
use crate::Limit;
use crate::fts_index::factory::FtsIndexFactory;
use crate::memory::Allocate;
use crate::memory::Memory;
use crate::memory::MemoryExt;
use crate::perf;
use crate::table::IndexId;
use crate::table::PrimaryId;
use crate::table::Table;
use crate::table::TableSearch;
use crate::worker;
use crate::worker::Worker;
use crate::worker::WorkerExt;

use super::actor::FtsIndex;
use super::actor::FtsSearchR;

pub(crate) struct TantivyIndexFactory {
    worker: async_channel::Sender<Worker>,
}

impl TantivyIndexFactory {
    pub(crate) fn new() -> Self {
        Self {
            worker: worker::new(),
        }
    }
}

impl FtsIndexFactory for TantivyIndexFactory {
    fn create_index(
        &self,
        key: IndexKey,
        table: Arc<RwLock<Table>>,
        memory: mpsc::Sender<Memory>,
    ) -> mpsc::Sender<FtsIndex> {
        new(key, table, self.worker.clone(), memory)
    }
}

struct IndexState {
    index: tantivy::Index,
    writer: RwLock<IndexWriter>,
    reader: tantivy::IndexReader,
    schema: Schema,
}

impl IndexState {
    fn new() -> anyhow::Result<Self> {
        let schema = build_schema();
        let index = tantivy::Index::create_in_ram(schema.clone());
        let options = IndexWriterOptions::builder()
            .num_worker_threads(perf::num_workers().into())
            .build();
        let writer = index
            .writer_with_options(options)
            .map_err(|e| anyhow!("fts: failed to create writer: {e}"))?;
        let reader = index
            .reader()
            .map_err(|e| anyhow!("fts: failed to create reader: {e}"))?;
        Ok(Self {
            index,
            writer: RwLock::new(writer),
            reader,
            schema,
        })
    }
}

fn build_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("primary_id", INDEXED | STORED);
    schema_builder.add_text_field("body", TEXT);
    schema_builder.build()
}

fn create_doc(schema: &Schema, primary_id: PrimaryId, document: &str) -> TantivyDocument {
    let primary_id_field = schema.get_field("primary_id").unwrap();
    let body_field = schema.get_field("body").unwrap();

    let mut doc = TantivyDocument::new();
    doc.add_u64(primary_id_field, u64::from(primary_id));
    doc.add_text(body_field, document);
    doc
}

fn handle_add_document(
    state: &IndexState,
    key: &IndexKey,
    primary_id: PrimaryId,
    document: String,
) {
    let doc = create_doc(&state.schema, primary_id, &document);
    if let Err(err) = state.writer.read().unwrap().add_document(doc) {
        error!("fts: failed to add document {primary_id:?}: {err}");
        return;
    }
    if let Err(err) = state.writer.write().unwrap().commit() {
        error!("fts: failed to commit add for {key}: {err}");
        return;
    }
    if let Err(err) = state.reader.reload() {
        error!("fts: failed to reload reader for {key}: {err}");
    }
}

fn create_term(schema: &Schema, primary_id: PrimaryId) -> tantivy::Term {
    let primary_id_field = schema.get_field("primary_id").unwrap();
    tantivy::Term::from_field_u64(primary_id_field, u64::from(primary_id))
}

fn handle_remove_document(state: &IndexState, key: &IndexKey, primary_id: PrimaryId) {
    let term = create_term(&state.schema, primary_id);
    state.writer.read().unwrap().delete_term(term);
    if let Err(err) = state.writer.write().unwrap().commit() {
        error!("fts: failed to commit remove for {key}: {err}");
        return;
    }
    if let Err(err) = state.reader.reload() {
        error!("fts: failed to reload reader for {key}: {err}");
    }
}

fn make_query(
    index: &tantivy::Index,
    body_field: tantivy::schema::Field,
    query_str: &str,
) -> anyhow::Result<Box<dyn tantivy::query::Query>> {
    let query_parser = QueryParser::for_index(index, vec![body_field]);
    query_parser
        .parse_query(query_str)
        .map_err(|e| anyhow!("fts: failed to parse query: {e}"))
}

fn find_partition_id(
    table: &impl TableSearch,
    index_key: &IndexKey,
) -> anyhow::Result<crate::table::PartitionId> {
    let (partition_id, _) = table
        .partition_id(index_key, None)
        .ok_or_else(|| anyhow!("fts: partition id not found for index key {index_key:?}"))?;
    Ok(partition_id)
}

fn handle_search(
    state: &IndexState,
    table: &RwLock<impl TableSearch>,
    index_key: &IndexKey,
    query_str: &str,
    limit: Limit,
) -> FtsSearchR {
    let body_field = state.schema.get_field("body").unwrap();
    let primary_id_field = state.schema.get_field("primary_id").unwrap();

    let searcher = state.reader.searcher();
    let query = make_query(&state.index, body_field, query_str)?;
    let limit: usize = (*limit.as_ref()).into();

    let top_docs = searcher
        .search(&query, &TopDocs::with_limit(limit).order_by_score())
        .map_err(|e| anyhow!("fts: search failed: {e}"))?;

    let table = table.read().unwrap();
    let partition_id = find_partition_id(table.deref(), index_key)?;

    let (primary_keys, scores) = top_docs
        .into_iter()
        .map(|(score, doc_address)| {
            let doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| anyhow!("fts: failed to retrieve doc: {e}"))?;
            let raw_id = doc
                .get_first(primary_id_field)
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow!("fts: missing primary_id in doc"))?;
            Ok((score, PrimaryId::from(raw_id)))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .filter_map(|(score, primary_id)| {
            table
                .primary_key(partition_id, primary_id)
                .map(|pk| (pk, score))
        })
        .unzip();

    Ok((primary_keys, scores))
}

fn get_or_create_state<T: TableSearch>(
    states: &mut BTreeMap<IndexId, Arc<IndexState>>,
    table: &RwLock<T>,
    key: &IndexKey,
) -> Option<Arc<IndexState>> {
    let index_id = table.read().unwrap().index_id(key)?;
    if let Some(state) = states.get(&index_id) {
        return Some(Arc::clone(state));
    }
    match IndexState::new() {
        Ok(state) => {
            let state = Arc::new(state);
            states.insert(index_id, Arc::clone(&state));
            Some(state)
        }
        Err(err) => {
            error!("fts: failed to create index state for {key}: {err}");
            None
        }
    }
}

fn get_state<T: TableSearch>(
    states: &BTreeMap<IndexId, Arc<IndexState>>,
    table: &RwLock<T>,
    key: &IndexKey,
) -> Option<Arc<IndexState>> {
    let index_id = table.read().unwrap().index_id(key)?;
    states.get(&index_id).cloned()
}

fn can_allocate_memory(
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

pub(crate) fn new(
    key: IndexKey,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> mpsc::Sender<FtsIndex> {
    let (tx, mut rx) = mpsc::channel::<FtsIndex>(perf::channel_size().into());
    tokio::spawn(async move {
        debug!("fts index actor starting for {key}");
        let mut states: BTreeMap<IndexId, Arc<IndexState>> = BTreeMap::new();

        let mut allocate_prev = Allocate::Can;
        let allocate_rx = memory.subscribe_allocate().await;

        while let Some(msg) = rx.recv().await {
            match msg {
                FtsIndex::AddDocument {
                    primary_id,
                    document,
                    in_progress,
                } => {
                    let Some(state) = get_or_create_state(&mut states, table.as_ref(), &key) else {
                        continue;
                    };
                    if !can_allocate_memory(&allocate_rx, &mut allocate_prev, &key) {
                        continue;
                    }
                    let key = key.clone();
                    worker
                        .spawn_blocking(move || {
                            handle_add_document(&state, &key, primary_id, document);
                            drop(in_progress);
                        })
                        .await;
                }
                FtsIndex::RemoveDocument {
                    primary_id,
                    in_progress,
                } => {
                    let Some(state) = get_or_create_state(&mut states, table.as_ref(), &key) else {
                        continue;
                    };
                    let key = key.clone();
                    worker
                        .spawn_blocking(move || {
                            handle_remove_document(&state, &key, primary_id);
                            drop(in_progress);
                        })
                        .await;
                }
                FtsIndex::Count { tx, index_key, .. } => {
                    let result = get_state(&states, table.as_ref(), &index_key)
                        .map(|s| s.reader.searcher().num_docs() as usize)
                        .unwrap_or(0);
                    _ = tx.send(Ok(result));
                }
                FtsIndex::Search {
                    index_key,
                    query,
                    limit,
                    tx,
                } => {
                    let Some(state) = get_state(&states, table.as_ref(), &index_key) else {
                        _ = tx.send(Ok((vec![], vec![])));
                        continue;
                    };
                    let table = Arc::clone(&table);
                    worker
                        .spawn_blocking(move || {
                            let result =
                                handle_search(&state, table.as_ref(), &index_key, &query, limit);
                            _ = tx.send(result);
                        })
                        .await;
                }
            }
        }
        debug!("fts index actor finished for {key}");
    });
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AsyncInProgress;
    use crate::IndexKey;
    use crate::PrimaryKey;
    use crate::table::IndexIdGenerator;
    use crate::table::MockTableSearch;
    use crate::table::PartitionId;
    use scylla::value::CqlValue;

    use super::super::actor::FtsIndexExt;

    fn make_table_with_keys() -> Arc<RwLock<MockTableSearch>> {
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

    fn make_index_key() -> IndexKey {
        IndexKey::new(&"ks".into(), &"idx".into())
    }

    fn make_memory_actor() -> mpsc::Sender<Memory> {
        let (tx, mut rx) = mpsc::channel::<Memory>(1);
        tokio::spawn(async move {
            let (watch_tx, _) = watch::channel(Allocate::Can);
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

    fn make_sender(table: Arc<RwLock<MockTableSearch>>) -> mpsc::Sender<FtsIndex> {
        let key = make_index_key();
        let memory = make_memory_actor();
        new(key, table, worker::new(), memory)
    }

    async fn add_doc(sender: &mpsc::Sender<FtsIndex>, primary: u64, content: &str) {
        let (tx, mut rx) = mpsc::channel(1);
        sender
            .add_document(
                primary.into(),
                content.into(),
                AsyncInProgress::Fullscan(tx),
            )
            .await;
        rx.recv().await;
    }

    async fn rm_doc(sender: &mpsc::Sender<FtsIndex>, primary: u64) {
        let (tx, mut rx) = mpsc::channel(1);
        sender
            .remove_document(primary.into(), AsyncInProgress::Fullscan(tx))
            .await;
        rx.recv().await;
    }

    fn make_memory_actor_cannot_allocate() -> mpsc::Sender<Memory> {
        let (tx, mut rx) = mpsc::channel::<Memory>(1);
        tokio::spawn(async move {
            let (watch_tx, _) = watch::channel(Allocate::Cannot);
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

    #[tokio::test]
    async fn add_document_increments_count() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        add_doc(&sender, 1, "hello world").await;
        add_doc(&sender, 2, "foo bar").await;

        let key = make_index_key();
        let count = sender.count(key).await.unwrap();

        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn remove_document_decrements_count() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        add_doc(&sender, 1, "hello").await;
        add_doc(&sender, 2, "world").await;
        rm_doc(&sender, 2).await;

        let key = make_index_key();
        let count = sender.count(key).await.unwrap();

        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn search_returns_matching_docs() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        add_doc(&sender, 1, "the quick brown fox").await;
        add_doc(&sender, 2, "lazy dog sleeps").await;

        let key = make_index_key();
        let (keys, scores) = sender
            .search(
                key,
                "fox".into(),
                Limit::from(std::num::NonZeroUsize::new(10).unwrap()),
            )
            .await
            .unwrap();

        assert_eq!(keys.len(), 1);
        assert_eq!(scores.len(), 1);
        assert!(scores.iter().all(|&s| s > 0.0));
    }

    #[tokio::test]
    async fn search_orders_by_bm25_relevance() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        add_doc(&sender, 1, "rust rust rust programming language").await;
        add_doc(&sender, 2, "rust is a systems programming language").await;

        let key = make_index_key();
        let (keys, scores) = sender
            .search(
                key,
                "rust".into(),
                Limit::from(std::num::NonZeroUsize::new(10).unwrap()),
            )
            .await
            .unwrap();

        assert!(keys.len() >= 2);
        for i in 1..scores.len() {
            assert!(scores[i - 1] >= scores[i], "scores should be descending");
        }
    }

    #[tokio::test]
    async fn search_returns_empty_for_no_match() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        add_doc(&sender, 1, "hello world").await;

        let key = make_index_key();
        let (keys, scores) = sender
            .search(
                key,
                "nonexistentterm".into(),
                Limit::from(std::num::NonZeroUsize::new(10).unwrap()),
            )
            .await
            .unwrap();

        assert!(keys.is_empty());
        assert!(scores.is_empty());
    }

    #[tokio::test]
    async fn remove_then_search_excludes_removed() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        add_doc(&sender, 1, "unique document alpha").await;
        add_doc(&sender, 2, "unique document beta").await;

        rm_doc(&sender, 1).await;

        let key = make_index_key();
        let (keys, scores) = sender
            .search(
                key,
                "unique".into(),
                Limit::from(std::num::NonZeroUsize::new(10).unwrap()),
            )
            .await
            .unwrap();

        assert_eq!(keys.len(), 1);
        assert_eq!(scores.len(), 1);
    }

    #[tokio::test]
    async fn add_document_rejected_when_memory_exhausted() {
        let table = make_table_with_keys();
        let key = make_index_key();
        let memory = make_memory_actor_cannot_allocate();
        let sender = new(key, table, worker::new(), memory);

        add_doc(&sender, 1, "should not be indexed").await;

        let key = make_index_key();
        let count = sender.count(key).await.unwrap();
        assert_eq!(count, 0);
    }
}
