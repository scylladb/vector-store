/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::memory::Memory;
use crate::pattern_index::PatternIndex;
use crate::pattern_index::PatternIndexConfiguration;
use crate::pattern_index::PatternIndexFactory;
use crate::perf;
use crate::table::Table;
use crate::table::TableSearch;
use crate::tantivy::COMMIT_INTERVAL;
use crate::tantivy::IndexSchema;
use crate::tantivy::Indexes;
use crate::tantivy::MAX_UNCOMMITTED_THRESHOLD;
use crate::tantivy::build_schema;
use crate::worker::Worker;
use anyhow::anyhow;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::tokenizer::NgramTokenizer;
use tantivy::tokenizer::TextAnalyzer;
use tokio::sync::mpsc;
use tracing::debug;

pub(crate) struct Factory {
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
}

impl PatternIndexFactory for Factory {
    fn create_index(
        &self,
        index: PatternIndexConfiguration,
        table: Arc<RwLock<Table>>,
    ) -> mpsc::Sender<PatternIndex> {
        new(
            index,
            table,
            self.worker.clone(),
            self.memory.clone(),
            COMMIT_INTERVAL,
            MAX_UNCOMMITTED_THRESHOLD,
        )
    }
}

pub(super) fn new_factory(
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> Factory {
    Factory { worker, memory }
}

const TOKENIZER_NAME: &str = "pattern_ngram";
const MIN_GRAM: usize = 1;
const MAX_GRAM: usize = 3;

/// Every value is indexed between these two marks; they are C0 control characters, which text
/// values are not expected to carry.
const VALUE_START: char = '\u{2}';
const VALUE_END: char = '\u{3}';

fn index_schema() -> anyhow::Result<IndexSchema> {
    Ok(IndexSchema {
        schema: build_schema(text_options()),
        tokenizer_name: TOKENIZER_NAME.to_owned(),
        tokenizer: build_ngram_analyzer()?,
    })
}

fn build_ngram_analyzer() -> anyhow::Result<TextAnalyzer> {
    let tokenizer = NgramTokenizer::new(MIN_GRAM, MAX_GRAM, false)
        .map_err(|e| anyhow!("pattern: failed to create the n-gram tokenizer: {e}"))?;
    Ok(TextAnalyzer::builder(tokenizer).build())
}

fn text_options() -> TextOptions {
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER_NAME)
        .set_index_option(IndexRecordOption::Basic);
    TextOptions::default().set_indexing_options(indexing)
}

fn framed(value: &str) -> String {
    format!("{VALUE_START}{value}{VALUE_END}")
}

pub(crate) fn new(
    config: PatternIndexConfiguration,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
    commit_interval: Duration,
    commit_threshold: usize,
) -> mpsc::Sender<PatternIndex> {
    let (tx, mut rx) = mpsc::channel::<PatternIndex>(perf::channel_size().into());
    tokio::spawn(async move {
        let key = config.key;
        debug!("pattern index actor starting for {key}");
        let mut indexes = Indexes::new(
            key.clone(),
            table,
            worker,
            memory,
            commit_interval,
            commit_threshold,
            index_schema,
        )
        .await;

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };
                    match msg {
                        PatternIndex::AddDocument {
                            primary_id,
                            document,
                            in_progress,
                        } => {
                            indexes.add_document(primary_id, framed(&document), in_progress).await;
                        }
                        PatternIndex::RemoveDocument {
                            primary_id,
                            in_progress,
                        } => {
                            indexes.remove_document(primary_id, in_progress).await;
                        }
                        PatternIndex::Count { tx, index_key, .. } => {
                            _ = tx.send(Ok(indexes.count(&index_key)));
                        }
                        PatternIndex::Like {
                            index_key: _index_key,
                            pattern: _pattern,
                            limit: _limit,
                            tx,
                        } => {
                            _ = tx.send(Err(anyhow!("pattern: like not implemented")));
                        }
                    }
                }
                _ = indexes.commit_interval_tick() => indexes.commit_or_reload().await,
            }
        }
        debug!("pattern index actor finished for {key}");
    });
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AsyncInProgress;
    use crate::memory::Allocate;
    use crate::pattern_index::PatternIndexExt;
    use crate::table::MockTableSearch;
    use crate::tantivy::test_support::make_index_key;
    use crate::tantivy::test_support::make_memory_actor;
    use crate::tantivy::test_support::make_table_with_keys;
    use crate::worker;
    use rstest::rstest;
    use std::collections::BTreeSet;

    const TEST_COMMIT_INTERVAL: Duration = Duration::from_millis(50);
    const TEST_COMMIT_THRESHOLD: usize = 3;

    fn make_sender_with_memory(
        table: Arc<RwLock<MockTableSearch>>,
        allocate: Allocate,
    ) -> mpsc::Sender<PatternIndex> {
        new(
            PatternIndexConfiguration {
                key: make_index_key(),
            },
            table,
            worker::new(),
            make_memory_actor(allocate),
            TEST_COMMIT_INTERVAL,
            TEST_COMMIT_THRESHOLD,
        )
    }

    fn make_sender(table: Arc<RwLock<MockTableSearch>>) -> mpsc::Sender<PatternIndex> {
        make_sender_with_memory(table, Allocate::Can)
    }

    async fn add_doc(sender: &mpsc::Sender<PatternIndex>, primary: u64, content: &str) {
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

    async fn rm_doc(sender: &mpsc::Sender<PatternIndex>, primary: u64) {
        let (tx, mut rx) = mpsc::channel(1);
        sender
            .remove_document(primary.into(), AsyncInProgress::Fullscan(tx))
            .await
            .unwrap();
        rx.recv().await;
    }

    async fn count(sender: &mpsc::Sender<PatternIndex>) -> usize {
        sender.count(make_index_key()).await.unwrap()
    }

    fn grams_of(text: &str) -> BTreeSet<String> {
        let mut analyzer = build_ngram_analyzer().unwrap();
        let mut stream = analyzer.token_stream(text);
        let mut grams = BTreeSet::new();
        while stream.advance() {
            grams.insert(stream.token().text.clone());
        }
        grams
    }

    fn set_of(grams: &[&str]) -> BTreeSet<String> {
        grams.iter().map(|gram| gram.to_string()).collect()
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn count_before_any_document_is_zero() {
        let sender = make_sender(make_table_with_keys());

        assert_eq!(count(&sender).await, 0);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn add_document_increments_count() {
        let sender = make_sender(make_table_with_keys());

        add_doc(&sender, 1, "AB-1024-XL").await;
        add_doc(&sender, 2, "CD-2048-S").await;

        assert_eq!(count(&sender).await, 2);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn remove_document_decrements_count() {
        let sender = make_sender(make_table_with_keys());

        add_doc(&sender, 1, "AB-1024-XL").await;
        add_doc(&sender, 2, "CD-2048-S").await;
        rm_doc(&sender, 2).await;

        assert_eq!(count(&sender).await, 1);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn add_document_rejected_when_memory_exhausted() {
        let sender = make_sender_with_memory(make_table_with_keys(), Allocate::Cannot);

        add_doc(&sender, 1, "should not be indexed").await;

        assert_eq!(count(&sender).await, 0);
    }

    #[test]
    fn ngram_analyzer_emits_a_gram_for_the_empty_value() {
        assert_eq!(
            grams_of(&framed("")),
            set_of(&["\u{2}", "\u{2}\u{3}", "\u{3}"])
        );
    }

    #[test]
    fn ngram_analyzer_emits_grams_with_frame_marks() {
        assert_eq!(
            grams_of(&framed("ab")),
            set_of(&[
                "\u{2}", "\u{2}a", "\u{2}ab", "a", "ab", "ab\u{3}", "b", "b\u{3}", "\u{3}",
            ])
        );
    }

    #[test]
    fn ngram_analyzer_keeps_whitespace_and_letter_case() {
        let grams = grams_of("a B");

        assert!(grams.contains("a "));
        assert!(grams.contains(" B"));
        assert!(!grams.contains("b"));
    }

    #[test]
    fn ngram_analyzer_emits_cjk_grams_of_up_to_max_gram_chars() {
        assert_eq!(
            grams_of(&framed("将军来了")),
            set_of(&[
                "\u{2}",
                "\u{2}将",
                "\u{2}将军",
                "将",
                "将军",
                "将军来",
                "军",
                "军来",
                "军来了",
                "来",
                "来了",
                "来了\u{3}",
                "了",
                "了\u{3}",
                "\u{3}",
            ])
        );
    }
}
