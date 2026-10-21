/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::IndexKey;
use crate::memory::Memory;
use crate::pattern_index::PatternIndex;
use crate::pattern_index::PatternIndexConfiguration;
use crate::pattern_index::PatternIndexFactory;
use crate::pattern_index::gram;
use crate::perf;
use crate::table::Table;
use crate::table::TableSearch;
use crate::tantivy::COMMIT_INTERVAL;
use crate::tantivy::Index;
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
    TextOptions::default().set_indexing_options(indexing)
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
}
