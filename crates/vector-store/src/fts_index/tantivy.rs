/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use anyhow::anyhow;
use tantivy::collector::TopDocs;
use tantivy::query::BooleanQuery;
use tantivy::query::BoostQuery;
use tantivy::query::Occur;
use tantivy::query::Query;
use tantivy::query::QueryParser;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::snippet::SnippetGenerator;
use tantivy::tokenizer::Language;
use tantivy::tokenizer::LowerCaser;
use tantivy::tokenizer::SimpleTokenizer;
use tantivy::tokenizer::Stemmer;
use tantivy::tokenizer::StopWordFilter;
use tantivy::tokenizer::TextAnalyzer;
use tantivy::tokenizer::WhitespaceTokenizer;
use tokio::sync::mpsc;
use tracing::debug;

use crate::Analyzer;
use crate::IndexKey;
use crate::Limit;
use crate::Positions;
use crate::QueryError;
use crate::fts_index::factory::FtsIndexConfiguration;
use crate::fts_index::factory::FtsIndexFactory;
use crate::memory::Memory;
use crate::metrics::Metrics;
use crate::perf;
use crate::table::PrimaryId;
use crate::table::Table;
use crate::table::TableSearch;
use crate::tantivy::COMMIT_INTERVAL;
use crate::tantivy::Index;
use crate::tantivy::IndexOptions;
use crate::tantivy::IndexSchema;
use crate::tantivy::Indexes;
use crate::tantivy::MAX_UNCOMMITTED_THRESHOLD;
use crate::tantivy::build_schema;
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
    metrics: Arc<Metrics>,
}

impl TantivyIndexFactory {
    pub(crate) fn new(
        worker: async_channel::Sender<Worker>,
        memory: mpsc::Sender<Memory>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            worker,
            memory,
            metrics,
        }
    }
}

impl FtsIndexFactory for TantivyIndexFactory {
    fn create_index(
        &self,
        index: FtsIndexConfiguration,
        table: Arc<RwLock<Table>>,
    ) -> mpsc::Sender<FtsIndex> {
        new(
            index,
            table,
            self.worker.clone(),
            self.memory.clone(),
            COMMIT_INTERVAL,
            MAX_UNCOMMITTED_THRESHOLD,
            Arc::clone(&self.metrics),
        )
    }
}

fn index_schema(analyzer: Analyzer, positions: Positions) -> anyhow::Result<IndexSchema> {
    let tokenizer_name = analyzer.to_string();
    Ok(IndexSchema {
        schema: build_schema(text_options(&tokenizer_name, positions)),
        tokenizer_name,
        tokenizer: build_token_pipeline(analyzer)?,
    })
}

fn stop_words(language: Language) -> anyhow::Result<StopWordFilter> {
    StopWordFilter::new(language)
        .ok_or_else(|| anyhow!("fts: stop words unavailable for {language:?}"))
}

/// The same token pipeline runs over the indexed documents and over the queries,
/// so both sides produce matching tokens.
fn build_token_pipeline(analyzer: Analyzer) -> anyhow::Result<TextAnalyzer> {
    // The language analyzers share the pipeline of `standard` and add the stop
    // words and the stemming of their own language.
    let language = match analyzer {
        // Splits on whitespace only, keeping punctuation and letter case.
        Analyzer::Whitespace => {
            return Ok(TextAnalyzer::builder(WhitespaceTokenizer::default()).build());
        }
        // Splits on non-alphanumeric characters and lowercases, nothing more.
        Analyzer::Simple => {
            return Ok(TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .build());
        }
        // Removes English stop words, but does not stem.
        Analyzer::Standard => {
            return Ok(TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .filter(stop_words(Language::English)?)
                .build());
        }
        Analyzer::English => Language::English,
        Analyzer::German => Language::German,
        Analyzer::French => Language::French,
        Analyzer::Spanish => Language::Spanish,
        Analyzer::Italian => Language::Italian,
        Analyzer::Portuguese => Language::Portuguese,
        Analyzer::Russian => Language::Russian,
    };
    Ok(TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .filter(stop_words(language)?)
        .filter(Stemmer::new(language))
        .build())
}

fn text_options(tokenizer: &str, positions: Positions) -> TextOptions {
    // Positions are what phrase queries match on.
    // Dropping them makes the index smaller at the cost of rejecting phrase queries against it.
    let index_option = if *positions.as_ref() {
        IndexRecordOption::WithFreqsAndPositions
    } else {
        IndexRecordOption::WithFreqs
    };
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(tokenizer)
        .set_index_option(index_option);
    TextOptions::default().set_indexing_options(indexing)
}

fn make_query(
    index: &tantivy::Index,
    text_field: tantivy::schema::Field,
    query_str: &str,
) -> anyhow::Result<Box<dyn tantivy::query::Query>> {
    let query_parser = QueryParser::for_index(index, vec![text_field]);
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
    index: &Index,
    table: &RwLock<impl TableSearch>,
    index_key: &IndexKey,
    query_str: &str,
    limit: Limit,
) -> FtsSearchR {
    let text_field = index.schema.get_field("text").unwrap();

    let searcher = index.reader.searcher();
    let query = make_query(&index.index, text_field, query_str)?;
    let limit: usize = (*limit.as_ref()).into();

    let top_docs = searcher
        .search(&query, &TopDocs::with_limit(limit).order_by_score())
        .map_err(|e| anyhow!("fts: search failed: {e}"))?;

    let table = table.read().unwrap();
    let partition_id = find_partition_id(table.deref(), index_key)?;

    let (primary_keys, scores) = top_docs
        .into_iter()
        .map(|(score, doc_address)| {
            let raw_id = searcher
                .segment_reader(doc_address.segment_ord)
                .fast_fields()
                .u64("primary_id")
                .map_err(|e| anyhow!("fts: failed to open the primary id column: {e}"))?
                .first(doc_address.doc_id)
                .ok_or_else(|| anyhow!("fts: missing primary_id in the column"))?;
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

fn handle_highlight(index: &Index, query_str: &str, documents: &[String]) -> FtsHighlightR {
    let text_field = index.schema.get_field("text").unwrap();
    let searcher = index.reader.searcher();
    let query = make_query(&index.index, text_field, query_str)?;
    let query = strip_negated_clauses(query.as_ref())?;

    // The generator uses the live index to weight terms by document frequency,
    // prioritizing rarer indexed terms when picking which fragment of a long text to show.
    let mut generator = SnippetGenerator::create(&searcher, query.as_ref(), text_field)
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

fn handle_stats(index: &Index) -> FtsStatsR {
    let searcher = index.reader.searcher();
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

pub(crate) fn new(
    index: FtsIndexConfiguration,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
    commit_interval: Duration,
    commit_threshold: usize,
    metrics: Arc<Metrics>,
) -> mpsc::Sender<FtsIndex> {
    let (tx, mut rx) = mpsc::channel::<FtsIndex>(perf::channel_size().into());
    tokio::spawn(async move {
        let key = index.key;
        debug!("fts index actor starting for {key}");
        let mut indexes = Indexes::new(
            IndexOptions {
                key: key.clone(),
                commit_interval,
                commit_threshold,
                index_schema: move || index_schema(index.analyzer, index.positions),
            },
            Arc::clone(&table),
            worker.clone(),
            memory,
            metrics,
        )
        .await;

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
                            indexes.add_document(primary_id, document, in_progress).await;
                        }
                        FtsIndex::RemoveDocument {
                            primary_id,
                            in_progress,
                        } => {
                            indexes.remove_document(primary_id, in_progress).await;
                        }
                        FtsIndex::Count { tx, index_key, .. } => {
                            _ = tx.send(Ok(indexes.count(&index_key)));
                        }
                        FtsIndex::Search {
                            index_key,
                            query,
                            limit,
                            tx,
                        } => {
                            let Some(index) = indexes.get(&index_key) else {
                                _ = tx.send(Ok((vec![], vec![])));
                                continue;
                            };
                            let table = Arc::clone(&table);
                            worker
                                .spawn_blocking(move || {
                                    let result = handle_search(
                                        &index,
                                        table.as_ref(),
                                        &index_key,
                                        &query,
                                        limit,
                                    );
                                    _ = tx.send(result);
                                })
                                .await;
                        }
                        FtsIndex::Highlight {
                            index_key,
                            query,
                            documents,
                            tx,
                        } => {
                            let Some(index) = indexes.get(&index_key) else {
                                _ = tx.send(Err(anyhow!("fts: missing index {index_key}")));
                                continue;
                            };
                            worker
                                .spawn_blocking(move || {
                                    let result = handle_highlight(&index, &query, &documents);
                                    _ = tx.send(result);
                                })
                                .await;
                        }
                        FtsIndex::Stats { index_key, tx } => {
                            let Some(index) = indexes.get(&index_key) else {
                                _ = tx.send(Ok(FtsStats::default()));
                                continue;
                            };
                            worker
                                .spawn_blocking(move || {
                                    let result = handle_stats(&index);
                                    _ = tx.send(result);
                                })
                                .await;
                        }
                    }
                }
                _ = indexes.commit_interval_tick() => indexes.commit_or_reload().await,
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
    use crate::memory::Allocate;
    use crate::table::MockTableSearch;
    use crate::tantivy::test_support::make_index_key;
    use crate::tantivy::test_support::make_memory_actor;
    use crate::tantivy::test_support::make_table_with_keys;
    use crate::worker;
    use rstest::rstest;
    use std::time::Duration;

    use super::super::actor::FtsIndexExt;

    const TEST_COMMIT_INTERVAL: Duration = Duration::from_millis(50);
    const TEST_COMMIT_THRESHOLD: usize = 3;

    fn make_configuration() -> FtsIndexConfiguration {
        FtsIndexConfiguration {
            key: make_index_key(),
            analyzer: Analyzer::default(),
            positions: Positions::default(),
        }
    }

    fn make_sender(table: Arc<RwLock<MockTableSearch>>) -> mpsc::Sender<FtsIndex> {
        make_sender_with_options(table, Analyzer::default(), Positions::default())
    }

    fn make_sender_with_options(
        table: Arc<RwLock<MockTableSearch>>,
        analyzer: Analyzer,
        positions: Positions,
    ) -> mpsc::Sender<FtsIndex> {
        let memory = make_memory_actor(Allocate::Can);
        new(
            FtsIndexConfiguration {
                analyzer,
                positions,
                ..make_configuration()
            },
            table,
            worker::new(),
            memory,
            TEST_COMMIT_INTERVAL,
            TEST_COMMIT_THRESHOLD,
            Arc::new(Metrics::new()),
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

    fn tokenize_with(analyzer: Analyzer, text: &str) -> Vec<String> {
        let mut analyzer = build_token_pipeline(analyzer).unwrap();
        let mut stream = analyzer.token_stream(text);
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().text.clone());
        }
        tokens
    }

    fn tokenize_with_standard_analyzer(text: &str) -> Vec<String> {
        tokenize_with(Analyzer::Standard, text)
    }

    async fn add_committed_segment(sender: &mpsc::Sender<FtsIndex>, segment: u64) {
        let (tx, mut rx) = mpsc::channel(1);
        // Reaching the commit threshold forces a commit, and each commit adds a new segment.
        let docs_per_segment = TEST_COMMIT_THRESHOLD as u64;
        for doc in 0..docs_per_segment {
            sender
                .add_document(
                    (segment * docs_per_segment + doc).into(),
                    format!("segment {segment} document {doc} body text"),
                    AsyncInProgress::Fullscan(tx.clone()),
                )
                .await
                .unwrap();
        }
        drop(tx);
        rx.recv().await;
    }

    async fn segment_count(sender: &mpsc::Sender<FtsIndex>) -> usize {
        sender.stats(make_index_key()).await.unwrap().segment_count
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
        let memory = make_memory_actor(Allocate::Cannot);
        let sender = new(
            make_configuration(),
            table,
            worker::new(),
            memory,
            TEST_COMMIT_INTERVAL,
            TEST_COMMIT_THRESHOLD,
            Arc::new(Metrics::new()),
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
        let memory = make_memory_actor(Allocate::Can);
        let sender = new(
            make_configuration(),
            table,
            worker::new(),
            memory,
            Duration::from_secs(3600),
            TEST_COMMIT_THRESHOLD,
            Arc::new(Metrics::new()),
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
        // when the guards are cleared - i.e. once the reload after the threshold-forced commit has
        // made the documents searchable.
        drop(tx);
        rx.recv().await;
        let count = sender.count(make_index_key()).await.unwrap();

        assert_eq!(count, TEST_COMMIT_THRESHOLD);
    }

    async fn highlight(
        sender: &mpsc::Sender<FtsIndex>,
        query: &str,
        documents: &[&str],
    ) -> Vec<Option<String>> {
        sender
            .highlight(
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
    async fn highlight_marks_every_occurrence_of_a_term() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "fox").await;

        let highlights = highlight(&sender, "fox", &["fox jumps, fox runs, fox sleeps"]).await;

        assert_eq!(
            highlights[0].as_deref(),
            Some("<b>fox</b> jumps, <b>fox</b> runs, <b>fox</b> sleeps")
        );
    }

    #[tokio::test]
    async fn highlight_matches_case_insensitively_and_keeps_the_text_casing() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "The Quick Brown Fox").await;

        let highlights = highlight(&sender, "FOX", &["The Quick Brown Fox jumps"]).await;

        assert_eq!(
            highlights[0].as_deref(),
            Some("The Quick Brown <b>Fox</b> jumps")
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
    async fn highlight_returns_one_fragment_for_matches_too_far_apart_to_share_one() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "needle").await;

        let filler = "haystack ".repeat(HIGHLIGHT_MAX_NUM_CHARS);
        let text = format!("needle {filler} needle");
        let highlights = highlight(&sender, "needle", &[text.as_str()]).await;

        let highlight = highlights[0].as_deref().unwrap();
        let marks = highlight.matches("<b>needle</b>").count();
        assert_eq!(
            marks, 1,
            "expected a single fragment holding one of the two matches: {highlight}"
        );
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
    async fn highlight_marks_the_required_term_of_a_grouped_conjunction() {
        let table = make_table_with_keys();
        let sender = make_sender(table);
        add_doc(&sender, 1, "turtle meadow").await;
        add_doc(&sender, 2, "fox meadow").await;

        let highlights = highlight(
            &sender,
            "(fox OR turtle) AND meadow",
            &["a turtle in the meadow", "a fox in the meadow"],
        )
        .await;

        assert_eq!(
            highlights[0].as_deref(),
            Some("a <b>turtle</b> in the <b>meadow</b>")
        );
        assert_eq!(
            highlights[1].as_deref(),
            Some("a <b>fox</b> in the <b>meadow</b>")
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
            .highlight(make_index_key(), "fox AND".into(), vec!["a fox".into()])
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
            .highlight(
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
            .highlight(make_index_key(), "fox^2".into(), vec!["fox dog".into()])
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn highlight_fails_without_index() {
        let table = make_table_with_keys();
        let sender = make_sender(table);

        let result = sender
            .highlight(make_index_key(), "fox".into(), vec!["a fox".into()])
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn highlight_uses_the_configured_analyzer() {
        let table = make_table_with_keys();
        let sender = make_sender_with_options(table, Analyzer::English, Positions::default());
        add_doc(&sender, 1, "the runners are running").await;

        // The English analyzer stems both the query and the highlighted text,
        // so "run" marks the "running" occurrence in the caller-supplied document.
        let highlights = highlight(&sender, "run", &["a story about running fast"]).await;

        assert_eq!(
            highlights[0].as_deref(),
            Some("a story about <b>running</b> fast")
        );
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

    #[test]
    fn standard_analyzer_does_not_stem() {
        assert_eq!(
            tokenize_with_standard_analyzer("the running runners"),
            vec!["running", "runners"]
        );
    }

    #[rstest]
    #[case(Analyzer::English, "the running runners", vec!["run", "runner"])]
    #[case(Analyzer::German, "die laufenden Läufer", vec!["laufend", "lauf"])]
    #[case(Analyzer::French, "les coureurs courants", vec!["coureur", "cour"])]
    #[case(Analyzer::Spanish, "los corredores corriendo", vec!["corredor", "corr"])]
    #[case(Analyzer::Italian, "i corridori correnti", vec!["corridor", "corrent"])]
    #[case(Analyzer::Portuguese, "os corredores correndo", vec!["corredor", "corr"])]
    #[case(Analyzer::Russian, "и бегущие бегуны", vec!["бегущ", "бегун"])]
    fn language_analyzers_stem_and_remove_stop_words(
        #[case] analyzer: Analyzer,
        #[case] text: &str,
        #[case] expected: Vec<&str>,
    ) {
        assert_eq!(tokenize_with(analyzer, text), expected);
    }

    #[test]
    fn simple_analyzer_lowercases_without_stop_words_or_stemming() {
        assert_eq!(
            tokenize_with(Analyzer::Simple, "The Running Runners, and a dog"),
            vec!["the", "running", "runners", "and", "a", "dog"]
        );
    }

    #[test]
    fn whitespace_analyzer_keeps_case_and_punctuation() {
        assert_eq!(
            tokenize_with(Analyzer::Whitespace, "The Running, Runners!"),
            vec!["The", "Running,", "Runners!"]
        );
    }

    async fn search_phrase(positions: bool) -> FtsSearchR {
        let sender = make_sender_with_options(
            make_table_with_keys(),
            Analyzer::default(),
            Positions::from(positions),
        );

        add_doc(&sender, 1, "quick brown fox").await;
        add_doc(&sender, 2, "brown quick fox").await;

        sender
            .search(
                make_index_key(),
                "\"quick brown\"".into(),
                Limit::from(std::num::NonZeroUsize::new(10).unwrap()),
            )
            .await
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn phrase_query_matches_with_positions() {
        let (keys, _) = search_phrase(true).await.unwrap();

        assert_eq!(keys.len(), 1);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn phrase_query_is_rejected_without_positions() {
        let err = search_phrase(false)
            .await
            .expect_err("phrase query should not be answerable");

        // A QueryError is what makes the endpoint answer 400 rather than 500.
        // The index cannot serve the query, but the service is healthy.
        assert!(
            err.downcast_ref::<QueryError>().is_some(),
            "expected a query error, got: {err}"
        );
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn term_query_works_without_positions() {
        let table = make_table_with_keys();
        let sender = make_sender_with_options(table, Analyzer::default(), Positions::from(false));

        add_doc(&sender, 1, "quick brown fox").await;
        add_doc(&sender, 2, "lazy dog").await;

        let (keys, _) = sender
            .search(
                make_index_key(),
                "fox".into(),
                Limit::from(std::num::NonZeroUsize::new(10).unwrap()),
            )
            .await
            .unwrap();

        assert_eq!(keys.len(), 1);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn search_uses_the_configured_analyzer() {
        let table = make_table_with_keys();
        let sender = make_sender_with_options(table, Analyzer::English, Positions::default());

        add_doc(&sender, 1, "the runners are running").await;

        // Only an analyzer that stems both sides matches "run" against "running".
        let (keys, _) = sender
            .search(
                make_index_key(),
                "run".into(),
                Limit::from(std::num::NonZeroUsize::new(10).unwrap()),
            )
            .await
            .unwrap();

        assert_eq!(keys.len(), 1);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn reader_segment_count_drops_after_background_merge() {
        /// Once the index has this many small segments, the default merge policy merges them into
        /// one segment.
        const MERGE_POLICY_MIN_NUM_SEGMENTS: u64 = 8;
        let table = make_table_with_keys();
        let sender = make_sender(table);

        // The last commit leaves exactly enough segments to start a background merge, which
        // finishes after that commit has already reloaded the reader.
        for segment in 0..MERGE_POLICY_MIN_NUM_SEGMENTS {
            add_committed_segment(&sender, segment).await;
        }

        // Wait until the reader serves the merged segments without any further writes. A tick
        // between two documents can commit a partial segment, so the merge may leave more than
        // one segment. Without a reload after the merge, the reader keeps all 8 segments and the
        // test times out.
        while segment_count(&sender).await >= MERGE_POLICY_MIN_NUM_SEGMENTS as usize {
            tokio::time::sleep(TEST_COMMIT_INTERVAL).await;
        }
    }
}
