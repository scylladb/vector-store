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
use tantivy::ReloadPolicy;
use tantivy::TantivyDocument;
use tantivy::collector::TopDocs;
use tantivy::indexer::IndexWriterOptions;
use tantivy::query::BooleanQuery;
use tantivy::query::BoostQuery;
use tantivy::query::Occur;
use tantivy::query::Query;
use tantivy::query::QueryParser;
use tantivy::schema::INDEXED;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::STORED;
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::schema::Value;
use tantivy::snippet::SnippetGenerator;
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
use crate::worker::Worker;
use crate::worker::WorkerExt;

use super::actor::FtsHighlightR;
use super::actor::FtsIndex;
use super::actor::FtsSearchR;
use super::actor::FtsStats;
use super::actor::FtsStatsR;

pub(crate) struct TantivyIndexFactory {
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
}

impl TantivyIndexFactory {
    pub(crate) fn new(worker: async_channel::Sender<Worker>, memory: mpsc::Sender<Memory>) -> Self {
        Self { worker, memory }
    }
}

impl FtsIndexFactory for TantivyIndexFactory {
    fn create_index(&self, key: IndexKey, table: Arc<RwLock<Table>>) -> mpsc::Sender<FtsIndex> {
        new(
            key,
            table,
            self.worker.clone(),
            self.memory.clone(),
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
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
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

/// A query-related failure caused by the caller's input (an unparsable query, or a query
/// construct that this endpoint cannot process) rather than an internal/actor failure.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct QueryError(pub(crate) String);

fn make_query(
    index: &tantivy::Index,
    body_field: tantivy::schema::Field,
    query_str: &str,
) -> anyhow::Result<Box<dyn tantivy::query::Query>> {
    let query_parser = QueryParser::for_index(index, vec![body_field]);
    query_parser
        .parse_query(query_str)
        .map_err(|e| QueryError(format!("fts: failed to parse query: {e}")).into())
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

const HIGHLIGHT_MAX_NUM_CHARS: usize = 150;
const HIGHLIGHT_PRE_TAG: &str = "<b>";
const HIGHLIGHT_POST_TAG: &str = "</b>";

/// Rebuilds `query`, dropping any `MustNot` clause at every nesting level.
///
/// Works around a Tantivy limitation.
/// `SnippetGenerator` uses `BooleanQuery::query_terms` to determine which terms to highlight.
/// That method walks every subquery regardless of `Occur`, discarding the sign entirely.
/// Without this, a negated term (e.g. `-dog` in `fox -dog`) would still get highlighted
/// in caller-supplied text even though the query excludes documents containing it.
///
/// Returns `Err` for a query construct we cannot see inside since we cannot rule
/// out a `MustNot` clause hidden behind it. The caller surfaces this as a query error
/// rather than silently returning no highlights, since the query may still positively
/// match the document.
fn strip_negated_clauses(query: &dyn Query) -> anyhow::Result<Box<dyn Query>> {
    let Some(boolean_query) = query.downcast_ref::<BooleanQuery>() else {
        if query.downcast_ref::<BoostQuery>().is_some() {
            return Err(QueryError(
                "fts: boosted queries are not supported for highlighting".to_string(),
            )
            .into());
        }
        return Ok(query.box_clone());
    };
    let clauses = boolean_query
        .clauses()
        .iter()
        .filter(|(occur, _)| *occur != Occur::MustNot)
        .map(|(occur, subquery)| Ok((*occur, strip_negated_clauses(subquery.as_ref())?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Box::new(BooleanQuery::new(clauses)))
}

fn handle_highlight(state: &IndexState, query_str: &str, documents: &[String]) -> FtsHighlightR {
    let body_field = state.schema.get_field("body").unwrap();
    let searcher = state.reader.searcher();
    let query = make_query(&state.index, body_field, query_str)?;
    let query = strip_negated_clauses(query.as_ref())?;

    // The generator uses the live index to weight terms by document frequency,
    // prioritizing rarer indexed terms when picking which fragment of a long text to show.
    let mut generator = SnippetGenerator::create(&searcher, query.as_ref(), body_field)
        .map_err(|e| anyhow!("fts: failed to create snippet generator: {e}"))?;
    generator.set_max_num_chars(HIGHLIGHT_MAX_NUM_CHARS);

    Ok(documents
        .iter()
        .map(|text| {
            let mut snippet = generator.snippet(text);
            if snippet.is_empty() {
                return None;
            }
            snippet.set_snippet_prefix_postfix(HIGHLIGHT_PRE_TAG, HIGHLIGHT_POST_TAG);
            Some(snippet.to_html())
        })
        .collect())
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
                        FtsIndex::_Highlight {
                            index_key,
                            query,
                            documents,
                            tx,
                        } => {
                            let Some(state) = get_state(&states, table.as_ref(), &index_key)
                            else {
                                _ = tx.send(Err(anyhow!("fts: missing index {index_key}")));
                                continue;
                            };
                            worker
                                .spawn_blocking(move || {
                                    let result = handle_highlight(&state, &query, &documents);
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
    use crate::worker;
    use rstest::rstest;
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
            .await
            .unwrap();
        rx.recv().await;
    }

    async fn rm_doc(sender: &mpsc::Sender<FtsIndex>, primary: u64) {
        let (tx, mut rx) = mpsc::channel(1);
        sender
            .remove_document(primary.into(), AsyncInProgress::Fullscan(tx))
            .await
            .unwrap();
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn stats_for_unknown_index_returns_default() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        let key = make_index_key();
        let stats = sender.stats(key).await.unwrap();

        assert_eq!(stats.num_docs, 0);
        assert_eq!(stats.segment_count, 0);
        assert_eq!(stats.size_bytes, 0);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
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
                .await
                .unwrap();
        }
        // Each added document holds a Fullscan sender clone in the writer's uncommitted guards.
        // Dropping our own sender leaves only those clones alive, so `recv` returns `None` exactly
        // when the threshold-forced commit clears the guards - i.e. once the commit has completed.
        drop(tx);
        rx.recv().await;
        let count = sender.count(key).await.unwrap();

        assert_eq!(count, TEST_COMMIT_THRESHOLD);
    }

    async fn highlight(
        sender: &mpsc::Sender<FtsIndex>,
        query: &str,
        documents: &[&str],
    ) -> Vec<Option<String>> {
        sender
            ._highlight(
                make_index_key(),
                query.into(),
                documents.iter().map(|doc| doc.to_string()).collect(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn highlight_marks_query_terms_in_caller_supplied_text() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "the quick brown fox jumps").await;

        let highlights = highlight(&sender, "fox", &["a completely different fox story"]).await;

        assert_eq!(highlights.len(), 1);
        assert_eq!(
            highlights[0].as_deref(),
            Some("a completely different <b>fox</b> story")
        );
    }

    #[tokio::test]
    async fn highlight_returns_none_when_query_term_was_never_indexed() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "unrelated content about turtles").await;

        let highlights = highlight(&sender, "fox", &["a completely different fox story"]).await;

        assert_eq!(highlights[0], None);
    }

    #[tokio::test]
    async fn highlight_returns_one_entry_per_document_in_order() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 13, "turtles").await;
        add_doc(&sender, 21, "fox").await;
        add_doc(&sender, 34, "dog").await;

        let highlights = highlight(
            &sender,
            "fox OR dog OR turtles",
            &["quick fox jumps", "turtles swim slowly", "lazy dog sleeps"],
        )
        .await;

        assert_eq!(highlights.len(), 3);
        assert_eq!(highlights[0].as_deref(), Some("quick <b>fox</b> jumps"));
        assert_eq!(highlights[1].as_deref(), Some("<b>turtles</b> swim slowly"));
        assert_eq!(highlights[2].as_deref(), Some("lazy <b>dog</b> sleeps"));
    }

    #[tokio::test]
    async fn highlight_returns_none_without_matches() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;

        let highlights = highlight(&sender, "fox", &["turtles all the way down"]).await;

        assert_eq!(highlights[0], None);
    }

    #[tokio::test]
    async fn highlight_returns_none_for_not_matched_or_indexed_documents() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;
        add_doc(&sender, 2, "dog").await;
        // "turtles" is not indexed
        add_doc(&sender, 4, "cat").await;

        let highlights = highlight(
            &sender,
            // "dog" does not appear in the query
            "fox OR turtles OR cat",
            &[
                "quick fox jumps",
                "turtles swim slowly",
                "lazy dog sleeps",
                "cat nap time",
            ],
        )
        .await;

        assert_eq!(highlights.len(), 4);
        assert_eq!(highlights[0].as_deref(), Some("quick <b>fox</b> jumps"));
        assert_eq!(highlights[1], None);
        assert_eq!(highlights[2], None);
        assert_eq!(highlights[3].as_deref(), Some("<b>cat</b> nap time"));
    }

    #[tokio::test]
    async fn highlight_escapes_html_in_fragment_but_not_in_tags() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;

        let highlights =
            highlight(&sender, "fox", &["<script>fox & \"friends\"</script> end"]).await;

        assert_eq!(
            highlights[0].as_deref(),
            Some("&lt;script&gt;<b>fox</b> &amp; &quot;friends&quot;&lt;/script&gt; end")
        );
    }

    #[tokio::test]
    async fn highlight_truncates_to_default_max_num_chars() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "needle").await;

        let padding = "haystack ".repeat(40);
        let text = format!("{padding}needle {padding}");
        let highlights = highlight(&sender, "needle", &[text.as_str()]).await;

        let highlight = highlights[0].as_deref().unwrap();
        let highlight_len = highlight.len();
        assert!(
            highlight_len
                <= HIGHLIGHT_MAX_NUM_CHARS + HIGHLIGHT_PRE_TAG.len() + HIGHLIGHT_POST_TAG.len(),
            "highlight of {highlight_len} chars exceeds the default max_num_chars of {HIGHLIGHT_MAX_NUM_CHARS} plus tag overhead: {highlight}",
        );
        assert!(highlight.contains("<b>needle</b>"));
    }

    #[tokio::test]
    async fn highlight_empty_documents_returns_no_highlights() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;

        assert!(highlight(&sender, "fox", &[]).await.is_empty());
    }

    #[tokio::test]
    async fn highlight_marks_terms_for_phrase_query() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "brown fox").await;

        let highlights = highlight(&sender, "\"brown fox\"", &["the quick brown fox jumps"]).await;

        assert_eq!(
            highlights[0].as_deref(),
            Some("the quick <b>brown</b> <b>fox</b> jumps")
        );
    }

    #[tokio::test]
    async fn highlight_does_not_mark_negated_query_terms() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;
        add_doc(&sender, 2, "dog").await;

        let highlights = highlight(&sender, "fox -dog", &["fox dog"]).await;

        assert_eq!(highlights[0].as_deref(), Some("<b>fox</b> dog"));
    }

    #[tokio::test]
    async fn highlight_marks_positive_terms_in_nested_query_with_negation() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;
        add_doc(&sender, 2, "cat").await;
        add_doc(&sender, 3, "dog").await;

        let highlights = highlight(&sender, "(fox OR cat) -dog", &["fox cat dog"]).await;

        assert_eq!(highlights[0].as_deref(), Some("<b>fox</b> <b>cat</b> dog"));
    }

    #[tokio::test]
    async fn highlight_marks_non_ascii_terms() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "café").await;

        let highlights = highlight(&sender, "café", &["I love a nice café over über coffee"]).await;

        assert_eq!(
            highlights[0].as_deref(),
            Some("I love a nice <b>café</b> over über coffee")
        );
    }

    #[tokio::test]
    async fn highlight_fails_on_unparsable_query() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;

        let result = sender
            ._highlight(make_index_key(), "fox AND".into(), vec!["a fox".into()])
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn highlight_fails_for_negated_boosted_query() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;
        add_doc(&sender, 2, "dog").await;

        let result = sender
            ._highlight(
                make_index_key(),
                "(fox -dog)^2".into(),
                vec!["fox dog".into()],
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn highlight_fails_for_boosted_query() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;
        add_doc(&sender, 2, "dog").await;

        // A plain boost carries no negation, but we still cannot see inside it
        // to rule one out, so we report it as unsupported rather than a false "no match".
        let result = sender
            ._highlight(make_index_key(), "fox^2".into(), vec!["fox dog".into()])
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn highlight_fails_without_index() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        let result = sender
            ._highlight(make_index_key(), "fox".into(), vec!["a fox".into()])
            .await;

        assert!(result.is_err());
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
