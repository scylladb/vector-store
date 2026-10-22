/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::IndexKey;
use crate::Limit;
use crate::PrimaryKey;
use crate::memory::Memory;
use crate::pattern_index::PatternIndex;
use crate::pattern_index::PatternIndexConfiguration;
use crate::pattern_index::PatternIndexFactory;
use crate::pattern_index::actor::PatternLikeR;
use crate::pattern_index::gram;
use crate::pattern_index::like::LikeQuery;
use crate::perf;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use crate::table::Table;
use crate::table::TableSearch;
use crate::tantivy::COMMIT_INTERVAL;
use crate::tantivy::Index;
use crate::tantivy::Indexes;
use crate::tantivy::MAX_UNCOMMITTED_THRESHOLD;
use crate::tantivy::build_schema;
use crate::worker::Worker;
use crate::worker::WorkerExt;
use anyhow::anyhow;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tantivy::Searcher;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
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

fn new_index(key: &IndexKey, worker: async_channel::Sender<Worker>) -> anyhow::Result<Index> {
    Index::new(
        build_schema(text_options()),
        TOKENIZER_NAME,
        gram::build_ngram_analyzer()?,
        key,
        worker,
    )
}

fn text_options() -> TextOptions {
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER_NAME)
        .set_index_option(IndexRecordOption::Basic);
    TextOptions::default()
        .set_indexing_options(indexing)
        .set_stored()
}

/// Returns the keys of up to `limit` rows whose value matches `pattern`, each row once.
fn handle_like(
    index: &Index,
    table: &RwLock<impl TableSearch>,
    index_key: &IndexKey,
    pattern: &str,
    limit: Limit,
) -> PatternLikeR {
    let text_field = index.schema.get_field("text").unwrap();
    let like = LikeQuery::new(&index.schema, text_field, pattern)?;
    let (partition_id, _) = table
        .read()
        .unwrap()
        .partition_id(index_key, None)
        .ok_or_else(|| anyhow!("pattern: partition id not found for index key {index_key:?}"))?;
    let searcher = index.reader.searcher();
    let limit: usize = (*limit.as_ref()).into();
    search_matching_primary_keys(table, partition_id, &searcher, &like, limit)
}

/// Returns the keys of up to `limit` rows that match `like` in `searcher`, each row once.
fn search_matching_primary_keys(
    table: &RwLock<impl TableSearch>,
    partition_id: PartitionId,
    searcher: &Searcher,
    like: &LikeQuery,
    limit: usize,
) -> anyhow::Result<Vec<PrimaryKey>> {
    let mut seen = HashSet::new();
    let mut primary_keys = Vec::new();
    for segment in searcher.segment_readers() {
        let primary_ids = segment
            .fast_fields()
            .u64("primary_id")
            .map_err(|e| anyhow!("pattern: failed to open the primary id column: {e}"))?;
        for doc_id in like.matching_docs(segment)? {
            let primary_id: PrimaryId = primary_ids
                .first(doc_id?)
                .ok_or_else(|| anyhow!("pattern: missing primary_id in the column"))?
                .into();
            if seen.insert(primary_id)
                && let Some(primary_key) =
                    table.read().unwrap().primary_key(partition_id, primary_id)
            {
                primary_keys.push(primary_key);
                if primary_keys.len() == limit {
                    return Ok(primary_keys);
                }
            }
        }
    }
    Ok(primary_keys)
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
            Arc::clone(&table),
            worker.clone(),
            memory,
            commit_interval,
            commit_threshold,
            new_index,
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
                            let document = gram::framed(&document);
                            indexes.add_document(primary_id, document, in_progress).await;
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
                            index_key,
                            pattern,
                            limit,
                            tx,
                        } => {
                            let Some(index) = indexes.get(&index_key) else {
                                _ = tx.send(Ok(vec![]));
                                continue;
                            };
                            let table = Arc::clone(&table);
                            worker
                                .spawn_blocking(move || {
                                    let result = handle_like(
                                        &index,
                                        table.as_ref(),
                                        &index_key,
                                        &pattern,
                                        limit,
                                    );
                                    _ = tx.send(result);
                                })
                                .await;
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
    use crate::QueryError;
    use crate::memory::Allocate;
    use crate::pattern_index::PatternIndexExt;
    use crate::table::MockTableSearch;
    use crate::tantivy::test_support::make_index_key;
    use crate::tantivy::test_support::make_memory_actor;
    use crate::tantivy::test_support::make_table_with_keys;
    use crate::worker;
    use rstest::rstest;
    use scylla::value::CqlValue;
    use std::num::NonZeroUsize;

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
        add_docs(sender, &[(primary, content)]).await;
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

    /// Adds `docs` without waiting between them, then waits until all of them are searchable.
    async fn add_docs(sender: &mpsc::Sender<PatternIndex>, docs: &[(u64, &str)]) {
        let mut receivers = Vec::new();
        for &(primary, content) in docs {
            let (tx, rx) = mpsc::channel(1);
            sender
                .add_document(
                    primary.into(),
                    content.into(),
                    AsyncInProgress::Fullscan(tx),
                )
                .await
                .unwrap();
            receivers.push(rx);
        }
        for mut rx in receivers {
            rx.recv().await;
        }
    }

    async fn like(
        sender: &mpsc::Sender<PatternIndex>,
        pattern: &str,
        limit: usize,
    ) -> PatternLikeR {
        sender
            .like(
                make_index_key(),
                pattern.into(),
                NonZeroUsize::new(limit).unwrap().into(),
            )
            .await
    }

    /// The sorted ids of the rows matching `pattern`, as the walk order is not fixed.
    async fn like_ids(sender: &mpsc::Sender<PatternIndex>, pattern: &str) -> Vec<i64> {
        let mut ids: Vec<i64> = like(sender, pattern, 100)
            .await
            .unwrap()
            .into_iter()
            .map(|key| match key.get(0).unwrap() {
                CqlValue::BigInt(id) => id,
                other => panic!("unexpected primary key value {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        ids
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

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_before_any_document_returns_nothing() {
        let sender = make_sender(make_table_with_keys());

        assert_eq!(like_ids(&sender, "%abc%").await, Vec::<i64>::new());
    }

    #[rstest]
    #[case::exact("AB-1024", &[1])]
    #[case::prefix("AB-1024%", &[1, 2])]
    #[case::suffix("%AB-1024", &[1, 3])]
    #[case::infix("%AB-1024%", &[1, 2, 3, 4])]
    #[case::short_infix("%XL%", &[2, 3, 4])]
    #[case::any_char("AB-_024", &[1, 6])]
    #[case::prefix_and_suffix("AB-%-XL", &[2])]
    #[case::two_infixes("%1024%S%", &[4])]
    #[case::case_sensitive("ab-%", &[7])]
    #[case::empty("", &[8])]
    #[case::percent("AB-50%", &[9, 10, 12])]
    #[case::escaped_percent(r"AB-50\%", &[9])]
    #[case::underscore("AB_50", &[11, 12, 13])]
    #[case::escaped_underscore(r"AB\_50", &[11])]
    #[case::escaped_backslash(r"AB\\50", &[13])]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_matches_values(#[case] pattern: &str, #[case] expected: &[i64]) {
        let sender = make_sender(make_table_with_keys());
        // Row 5 matches no pattern. Every value that holds all grams of a pattern also matches it,
        // so the result does not depend on the regex.
        add_docs(
            &sender,
            &[
                (1, "AB-1024"),
                (2, "AB-1024-XL"),
                (3, "XL-AB-1024"),
                (4, "XL-AB-1024-S"),
                (5, "CD-2048-M"),
                (6, "AB-2024"),
                (7, "ab-1024"),
                (8, ""),
                (9, "AB-50%"),
                (10, "AB-500"),
                (11, "AB_50"),
                (12, "AB-50"),
                (13, r"AB\50"),
            ],
        )
        .await;

        assert_eq!(like_ids(&sender, pattern).await, expected);
    }

    #[rstest]
    #[case::exact("将军", &[1])]
    #[case::prefix("将军%", &[1, 2])]
    #[case::suffix("%将军", &[1, 3])]
    #[case::infix("%将军%", &[1, 2, 3, 4])]
    #[case::any_char("将_", &[1, 6])]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_supports_cjk(#[case] pattern: &str, #[case] expected: &[i64]) {
        let sender = make_sender(make_table_with_keys());
        // Row 5 matches no pattern and row 6 only the one with '_'.
        add_docs(
            &sender,
            &[
                (1, "将军"),
                (2, "将军来了"),
                (3, "李将军"),
                (4, "李将军来了"),
                (5, "元帅"),
                (6, "将领"),
            ],
        )
        .await;

        assert_eq!(like_ids(&sender, pattern).await, expected);
    }

    #[rstest]
    // 'AB-10-1024' holds every 3-character piece of 'AB-1024' (AB-, B-1, -10, 102, 024), but the
    // pieces are not next to each other.
    #[case::grams_apart("%AB-1024%", "AB-10-1024")]
    // 'AB-1024-XL' holds 'XL' and '1024', but '1024' comes before 'XL'.
    #[case::wrong_order("%XL%1024%", "AB-1024-XL")]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_filters_out_values_that_grams_cannot_prove_by_applying_regex(
        #[case] pattern: &str,
        #[case] value_holding_all_grams: &str,
    ) {
        let sender = make_sender(make_table_with_keys());
        // Both rows hold every gram of the pattern, so the gram query returns both of them as
        // candidates. Only row 2 really matches the pattern.
        add_docs(&sender, &[(1, value_holding_all_grams), (2, "XL-AB-1024")]).await;

        // The regex reads the stored value of each candidate and drops row 1.
        assert_eq!(like_ids(&sender, pattern).await, [2]);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_excludes_a_removed_document() {
        let sender = make_sender(make_table_with_keys());
        add_docs(&sender, &[(1, "abc"), (2, "abcd")]).await;
        rm_doc(&sender, 2).await;

        assert_eq!(like_ids(&sender, "abc%").await, [1]);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_returns_a_row_indexed_twice_once() {
        let sender = make_sender(make_table_with_keys());
        add_docs(&sender, &[(1, "abc"), (1, "abc")]).await;

        assert_eq!(like_ids(&sender, "%abc%").await, [1]);
    }

    #[rstest]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_respects_limit() {
        let sender = make_sender(make_table_with_keys());
        add_docs(&sender, &[(1, "abc"), (2, "abc"), (3, "abc")]).await;

        assert_eq!(like(&sender, "%abc%", 2).await.unwrap().len(), 2);
    }

    #[rstest]
    #[case::any_seq("%")]
    #[case::any_seqs("%%")]
    #[case::any_char("_")]
    #[case::any_chars("___")]
    #[case::any_char_between_any_seqs("%_%")]
    #[timeout(Duration::from_secs(10))]
    #[tokio::test]
    async fn like_rejects_a_pattern_without_literal_characters(#[case] pattern: &str) {
        let sender = make_sender(make_table_with_keys());
        // The pattern is checked only once the index exists.
        add_doc(&sender, 1, "abc").await;

        let err = like(&sender, pattern, 10).await.unwrap_err();

        assert!(err.downcast_ref::<QueryError>().is_some(), "got: {err}");
    }
}
