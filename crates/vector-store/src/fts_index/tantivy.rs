/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::anyhow;
use tantivy::IndexWriter;
use tantivy::TantivyDocument;
use tantivy::collector::TopDocs;
use tantivy::indexer::IndexWriterOptions;
use tantivy::query::QueryParser;
use tantivy::schema::INDEXED;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::STORED;
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::schema::Value;
use tantivy::tokenizer::Language;
use tantivy::tokenizer::LowerCaser;
use tantivy::tokenizer::SimpleTokenizer;
use tantivy::tokenizer::StopWordFilter;
use tantivy::tokenizer::TextAnalyzer;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::debug;
use tracing::error;

use crate::AsyncInProgress;
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
use super::actor::FtsStats;
use super::actor::FtsStatsR;

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
        new(
            key,
            table,
            self.worker.clone(),
            memory,
            COMMIT_INTERVAL,
            MAX_UNCOMMITTED_THRESHOLD,
        )
    }
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

struct IndexState {
    index: tantivy::Index,
    writer: RwLock<Writer>,
    reader: tantivy::IndexReader,
    schema: Schema,
}

const TOKENIZER_NAME: &str = "standard";
const COMMIT_INTERVAL: Duration = Duration::from_secs(3);
const MAX_UNCOMMITTED_THRESHOLD: usize = 10_000;

impl IndexState {
    fn new() -> anyhow::Result<Self> {
        let schema = build_schema();
        let index = tantivy::Index::create_in_ram(schema.clone());
        index
            .tokenizers()
            .register(TOKENIZER_NAME, build_standard_analyzer()?);
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
            writer: RwLock::new(Writer {
                writer,
                uncommitted_docs_in_progress_guards: Vec::new(),
            }),
            reader,
            schema,
        })
    }
}

fn build_standard_analyzer() -> anyhow::Result<TextAnalyzer> {
    let stop_words = StopWordFilter::new(Language::English)
        .ok_or_else(|| anyhow!("fts: english stop words unavailable"))?;
    Ok(TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .filter(stop_words)
        .build())
}

fn body_text_options() -> TextOptions {
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER_NAME)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    TextOptions::default().set_indexing_options(indexing)
}

fn build_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("primary_id", INDEXED | STORED);
    schema_builder.add_text_field("body", body_text_options());
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

fn commit(state: &IndexState, key: &IndexKey) {
    let result = state
        .writer
        .write()
        .unwrap()
        .commit(|| state.reader.reload());
    if let Err(err) = result {
        error!("fts: failed to commit for {key}: {err}");
    }
}

fn handle_add_document(
    state: &IndexState,
    primary_id: PrimaryId,
    document: String,
    in_progress: AsyncInProgress,
) -> usize {
    let doc = create_doc(&state.schema, primary_id, &document);
    let mut writer = state.writer.write().unwrap();
    match writer.add_document(doc, in_progress) {
        Ok(pending) => pending,
        Err(err) => {
            error!("fts: failed to add document {primary_id:?}: {err}");
            writer.uncommitted_docs()
        }
    }
}

fn create_term(schema: &Schema, primary_id: PrimaryId) -> tantivy::Term {
    let primary_id_field = schema.get_field("primary_id").unwrap();
    tantivy::Term::from_field_u64(primary_id_field, u64::from(primary_id))
}

fn handle_remove_document(
    state: &IndexState,
    primary_id: PrimaryId,
    in_progress: AsyncInProgress,
) -> usize {
    let term = create_term(&state.schema, primary_id);
    state.writer.write().unwrap().rm_document(term, in_progress)
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

fn handle_stats(state: &IndexState) -> FtsStatsR {
    let searcher = state.reader.searcher();
    let num_docs = searcher.num_docs();
    let segment_count = searcher.segment_readers().len();
    let size_bytes = searcher
        .space_usage()
        .map_err(|e| anyhow!("fts: failed to compute space usage: {e}"))?
        .total()
        .get_bytes();
    Ok(FtsStats {
        num_docs,
        size_bytes,
        segment_count,
    })
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
    commit_interval: Duration,
    commit_threshold: usize,
) -> mpsc::Sender<FtsIndex> {
    let (tx, mut rx) = mpsc::channel::<FtsIndex>(perf::channel_size().into());
    tokio::spawn(async move {
        debug!("fts index actor starting for {key}");
        let mut states: BTreeMap<IndexId, Arc<IndexState>> = BTreeMap::new();

        let mut allocate_prev = Allocate::Can;
        let allocate_rx = memory.subscribe_allocate().await;

        let mut interval = tokio::time::interval(commit_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };
                    match msg {
                        FtsIndex::AddDocument {
                            primary_id,
                            document,
                            in_progress,
                        } => {
                            let Some(state) = get_or_create_state(
                                &mut states,
                                table.as_ref(),
                                &key,
                            ) else {
                                continue;
                            };
                            if !can_allocate_memory(&allocate_rx, &mut allocate_prev, &key) {
                                continue;
                            }
                            let key = key.clone();
                            worker
                                .spawn_blocking(move || {
                                    let pending = handle_add_document(
                                        &state,
                                        primary_id,
                                        document,
                                        in_progress,
                                    );
                                    if pending >= commit_threshold {
                                        commit(&state, &key);
                                    }
                                })
                                .await;
                        }
                        FtsIndex::RemoveDocument {
                            primary_id,
                            in_progress,
                        } => {
                            let Some(state) = get_or_create_state(
                                &mut states,
                                table.as_ref(),
                                &key,
                            ) else {
                                continue;
                            };
                            let key = key.clone();
                            worker
                                .spawn_blocking(move || {
                                    let pending =
                                        handle_remove_document(&state, primary_id, in_progress);
                                    if pending >= commit_threshold {
                                        commit(&state, &key);
                                    }
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
                                    let result = handle_search(
                                        &state,
                                        table.as_ref(),
                                        &index_key,
                                        &query,
                                        limit,
                                    );
                                    _ = tx.send(result);
                                })
                                .await;
                        }
                        FtsIndex::Stats { index_key, tx } => {
                            let Some(state) = get_state(&states, table.as_ref(), &index_key)
                            else {
                                _ = tx.send(Ok(FtsStats::default()));
                                continue;
                            };
                            worker
                                .spawn_blocking(move || {
                                    let result = handle_stats(&state);
                                    _ = tx.send(result);
                                })
                                .await;
                        }
                    }
                }
                _ = interval.tick() => {
                    for state in states.values() {
                        if !state.writer.read().unwrap().has_uncommitted_docs() {
                            continue;
                        }
                        let state = Arc::clone(state);
                        let key = key.clone();
                        worker.spawn_blocking(move || commit(&state, &key)).await;
                    }
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
    use std::time::Duration;

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

    const TEST_COMMIT_INTERVAL: Duration = Duration::from_millis(50);
    const TEST_COMMIT_THRESHOLD: usize = 3;

    fn make_sender(table: Arc<RwLock<MockTableSearch>>) -> mpsc::Sender<FtsIndex> {
        let key = make_index_key();
        let memory = make_memory_actor();
        new(
            key,
            table,
            worker::new(),
            memory,
            TEST_COMMIT_INTERVAL,
            TEST_COMMIT_THRESHOLD,
        )
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

    fn tokenize_with_standard_analyzer(text: &str) -> Vec<String> {
        let mut analyzer = build_standard_analyzer().unwrap();
        let mut stream = analyzer.token_stream(text);
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().text.clone());
        }
        tokens
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
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
    #[ntest::timeout(10_000)]
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
    #[ntest::timeout(10_000)]
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
    #[ntest::timeout(10_000)]
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
    #[ntest::timeout(10_000)]
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
    #[ntest::timeout(10_000)]
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
    #[ntest::timeout(10_000)]
    async fn stats_reflects_doc_count_and_segments() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        add_doc(&sender, 1, "hello world").await;
        add_doc(&sender, 2, "foo bar").await;

        let key = make_index_key();
        let stats = sender.stats(key).await.unwrap();

        assert_eq!(stats.num_docs, 2);
        assert!(stats.segment_count > 0);
        assert!(stats.size_bytes > 0);
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn stats_for_unknown_index_returns_default() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        let key = make_index_key();
        let stats = sender.stats(key).await.unwrap();

        assert_eq!(stats.num_docs, 0);
        assert_eq!(stats.segment_count, 0);
        assert_eq!(stats.size_bytes, 0);
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn add_document_rejected_when_memory_exhausted() {
        let table = make_table_with_keys();
        let key = make_index_key();
        let memory = make_memory_actor_cannot_allocate();
        let sender = new(
            key,
            table,
            worker::new(),
            memory,
            TEST_COMMIT_INTERVAL,
            TEST_COMMIT_THRESHOLD,
        );

        add_doc(&sender, 1, "should not be indexed").await;

        let key = make_index_key();
        let count = sender.count(key).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    #[ntest::timeout(10_000)]
    async fn threshold_forces_commit_before_interval() {
        let table = make_table_with_keys();
        let key = make_index_key();
        let memory = make_memory_actor();
        let sender = new(
            key.clone(),
            table,
            worker::new(),
            memory,
            Duration::from_secs(3600),
            TEST_COMMIT_THRESHOLD,
        );
        let (tx, mut rx) = mpsc::channel(1);

        for primary in 1..=TEST_COMMIT_THRESHOLD as u64 {
            sender
                .add_document(
                    primary.into(),
                    "content".into(),
                    AsyncInProgress::Fullscan(tx.clone()),
                )
                .await;
        }
        // Each added document holds a Fullscan sender clone in the writer's uncommitted guards.
        // Dropping our own sender leaves only those clones alive, so `recv` returns `None` exactly
        // when the threshold-forced commit clears the guards - i.e. once the commit has completed.
        drop(tx);
        rx.recv().await;
        let count = sender.count(key).await.unwrap();

        assert_eq!(count, TEST_COMMIT_THRESHOLD);
    }

    #[test]
    fn tokenize_lowercases_mixed_case() {
        assert_eq!(
            tokenize_with_standard_analyzer("Hello WORLD Rust"),
            vec!["hello", "world", "rust"]
        );
    }

    #[test]
    fn tokenize_splits_on_punctuation() {
        assert_eq!(
            tokenize_with_standard_analyzer("hello,world!rust.programming"),
            vec!["hello", "world", "rust", "programming"]
        );
    }

    #[test]
    fn tokenize_removes_english_stop_words() {
        assert_eq!(
            tokenize_with_standard_analyzer("the quick brown fox and a lazy dog"),
            vec!["quick", "brown", "fox", "lazy", "dog"]
        );
    }

    #[test]
    fn tokenize_preserves_unicode_alphanumerics() {
        assert_eq!(
            tokenize_with_standard_analyzer("Café Über Naïve Straße"),
            vec!["café", "über", "naïve", "straße"]
        );
    }

    #[test]
    fn tokenize_empty_string_yields_no_tokens() {
        assert!(tokenize_with_standard_analyzer("").is_empty());
    }

    #[test]
    fn tokenize_whitespace_only_yields_no_tokens() {
        assert!(tokenize_with_standard_analyzer("   \t\n  ").is_empty());
    }

    #[test]
    fn tokenize_punctuation_only_yields_no_tokens() {
        assert!(tokenize_with_standard_analyzer("!@#$ ,.;:").is_empty());
    }
}
