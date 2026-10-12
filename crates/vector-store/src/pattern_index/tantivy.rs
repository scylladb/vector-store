/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::CaseSensitive;
use crate::memory::Memory;
use crate::pattern_index::PatternIndex;
use crate::pattern_index::PatternIndexConfiguration;
use crate::pattern_index::PatternIndexFactory;
use crate::pattern_index::PatternMessage;
use crate::table::PrimaryId;
use crate::table::Table;
use crate::table::TableSearch;
use crate::tantivy_common::COMMIT_INTERVAL;
use crate::tantivy_common::MAX_UNCOMMITTED_THRESHOLD;
use crate::tantivy_common::TantivyBackend;
use crate::tantivy_common::TantivyIndexer;
use crate::tantivy_common::TantivyMessage;
use crate::tantivy_common::build_schema;
use crate::tantivy_common::create_doc;
use crate::tantivy_common::spawn_actor;
use crate::worker::Worker;
use anyhow::anyhow;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tantivy::TantivyDocument;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::tokenizer::NgramTokenizer;
use tantivy::tokenizer::TextAnalyzer;
use tokio::sync::mpsc;

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

const TEXT_FIELD: &str = "text";
const TOKENIZER_NAME: &str = "pattern_ngram";
const MIN_GRAM: usize = 1;
const MAX_GRAM: usize = 3;

/// Every value is indexed and stored between these two marks, so a pattern anchored at the start
/// or at the end of the value, and the empty value itself, are found through the very same grams
/// as a pattern that is not anchored. They are C0 control characters, which text values are not
/// expected to carry.
const VALUE_START: char = '\u{2}';
const VALUE_END: char = '\u{3}';

#[derive(Clone)]
pub(crate) struct PatternBackend {
    case_sensitive: CaseSensitive,
}

impl TantivyBackend for PatternBackend {
    const NAME: &'static str = "pattern";

    fn build_schema(&self) -> Schema {
        build_schema(TEXT_FIELD, text_options())
    }

    fn register_tokenizers(&self, index: &tantivy::Index) -> anyhow::Result<()> {
        index
            .tokenizers()
            .register(TOKENIZER_NAME, build_ngram_analyzer()?);
        Ok(())
    }

    fn create_doc(&self, schema: &Schema, primary_id: PrimaryId, text: &str) -> TantivyDocument {
        create_doc(
            schema,
            primary_id,
            TEXT_FIELD,
            &framed(&normalize(text, self.case_sensitive)),
        )
    }
}

/// Every gram of one up to `MAX_GRAM` characters is a term, whitespace and punctuation included,
/// as a LIKE pattern matches characters and not words.
fn build_ngram_analyzer() -> anyhow::Result<TextAnalyzer> {
    let tokenizer = NgramTokenizer::new(MIN_GRAM, MAX_GRAM, false)
        .map_err(|e| anyhow!("pattern: failed to create the n-gram tokenizer: {e}"))?;
    Ok(TextAnalyzer::builder(tokenizer).build())
}

fn text_options() -> TextOptions {
    // Neither term frequencies nor positions are needed: a gram is either present in a value or
    // not, and candidates are verified against the stored value.
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER_NAME)
        .set_index_option(IndexRecordOption::Basic);
    TextOptions::default()
        .set_indexing_options(indexing)
        .set_stored()
}

/// The form a value is compared in: unchanged for a case-sensitive index, lowercased one
/// character at a time otherwise. Unlike `str::to_lowercase`, which lowercases a final 'Σ' to
/// 'ς', this does not depend on the neighbouring characters, so a lowercased literal of a pattern
/// is found in every lowercased value that holds the literal.
fn normalize(text: &str, case_sensitive: CaseSensitive) -> Cow<'_, str> {
    if *case_sensitive.as_ref() {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(text.chars().flat_map(char::to_lowercase).collect())
    }
}

fn framed(normalized: &str) -> String {
    format!("{VALUE_START}{normalized}{VALUE_END}")
}

pub(crate) fn new(
    config: PatternIndexConfiguration,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
    commit_interval: Duration,
    commit_threshold: usize,
) -> mpsc::Sender<PatternIndex> {
    let backend = PatternBackend {
        case_sensitive: config.case_sensitive,
    };
    spawn_actor(
        config.key,
        backend,
        table,
        worker,
        memory,
        commit_interval,
        commit_threshold,
    )
}

impl TantivyMessage for PatternMessage {
    type Backend = PatternBackend;

    async fn handle<T: TableSearch + Send + Sync + 'static>(
        self,
        _indexer: &mut TantivyIndexer<PatternBackend, T>,
    ) {
        match self {
            PatternMessage::Like {
                index_key: _index_key,
                pattern: _pattern,
                limit: _limit,
                tx,
            } => {
                _ = tx.send(Err(anyhow!("pattern: like not implemented")));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AsyncInProgress;
    use crate::memory::Allocate;
    use crate::pattern_index::PatternIndexExt;
    use crate::table::MockTableSearch;
    use crate::tantivy_common::TantivyIndexExt;
    use crate::tantivy_common::test_support::make_index_key;
    use crate::tantivy_common::test_support::make_memory_actor;
    use crate::tantivy_common::test_support::make_table_with_keys;
    use crate::worker;
    use rstest::rstest;
    use std::collections::BTreeSet;
    use tantivy::schema::Value;

    const TEST_COMMIT_INTERVAL: Duration = Duration::from_millis(50);
    const TEST_COMMIT_THRESHOLD: usize = 3;

    fn make_sender_with_memory(
        table: Arc<RwLock<MockTableSearch>>,
        allocate: Allocate,
    ) -> mpsc::Sender<PatternIndex> {
        new(
            PatternIndexConfiguration {
                key: make_index_key(),
                case_sensitive: CaseSensitive::default(),
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
    async fn update_as_remove_and_add_keeps_count() {
        let sender = make_sender(make_table_with_keys());

        add_doc(&sender, 1, "AB-1024-XL").await;
        rm_doc(&sender, 1).await;
        add_doc(&sender, 1, "AB-1024-XXL").await;

        assert_eq!(count(&sender).await, 1);
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
    async fn like_is_not_implemented_yet() {
        let sender = make_sender(make_table_with_keys());
        add_doc(&sender, 1, "AB-1024-XL").await;

        let result = sender
            .like(
                make_index_key(),
                "AB-%".into(),
                std::num::NonZeroUsize::new(10).unwrap().into(),
            )
            .await;

        assert!(result.is_err());
    }

    #[rstest]
    #[case::latin("NGgamer", "nggamer")]
    #[case::non_ascii("ŻÓŁW Ärger", "żółw ärger")]
    #[case::cjk("将军", "将军")]
    #[case::final_sigma("ΟΣ", "οσ")]
    fn normalize_lowercases_a_case_insensitive_value(#[case] value: &str, #[case] expected: &str) {
        assert_eq!(normalize(value, CaseSensitive::from(false)), expected);
    }

    #[rstest]
    #[case::case_sensitive(true, "AB-1024-XL", "\u{2}AB-1024-XL\u{3}")]
    #[case::case_insensitive(false, "AB-1024-XL", "\u{2}ab-1024-xl\u{3}")]
    #[case::empty(true, "", "\u{2}\u{3}")]
    fn document_stores_the_framed_normalized_value(
        #[case] case_sensitive: bool,
        #[case] value: &str,
        #[case] expected: &str,
    ) {
        let backend = PatternBackend {
            case_sensitive: CaseSensitive::from(case_sensitive),
        };
        let schema = backend.build_schema();

        let doc = backend.create_doc(&schema, PrimaryId::from(1), value);

        let stored = doc
            .get_first(schema.get_field(TEXT_FIELD).unwrap())
            .and_then(|v| v.as_str());
        assert_eq!(stored, Some(expected));
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
    fn ngram_analyzer_emits_a_gram_for_the_empty_value() {
        assert_eq!(
            grams_of(&framed("")),
            set_of(&["\u{2}", "\u{2}\u{3}", "\u{3}"])
        );
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

    #[test]
    fn ngram_analyzer_keeps_whitespace_and_letter_case() {
        let grams = grams_of("a B");

        assert!(grams.contains("a "));
        assert!(grams.contains(" B"));
        assert!(!grams.contains("b"));
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn add_document_rejected_when_memory_exhausted() {
        let sender = make_sender_with_memory(make_table_with_keys(), Allocate::Cannot);

        add_doc(&sender, 1, "should not be indexed").await;

        assert_eq!(count(&sender).await, 0);
    }
}
