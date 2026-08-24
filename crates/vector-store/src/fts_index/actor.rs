/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::AsyncInProgress;
use crate::IndexKey;
use crate::Limit;
use crate::PrimaryKey;
use crate::table::PrimaryId;
use crate::vs_index::CountR;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) type FtsSearchR = anyhow::Result<(Vec<PrimaryKey>, Vec<f32>)>;
pub(crate) type FtsHighlightR = anyhow::Result<Vec<Option<String>>>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FtsStats {
    pub(crate) num_docs: u64,
    pub(crate) size_bytes: u64,
    pub(crate) segment_count: usize,
}

pub(crate) type FtsStatsR = anyhow::Result<FtsStats>;

pub(crate) enum FtsIndex {
    AddDocument {
        primary_id: PrimaryId,
        document: String,
        in_progress: AsyncInProgress,
    },
    RemoveDocument {
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    },
    Count {
        index_key: IndexKey,
        tx: oneshot::Sender<CountR>,
    },
    Search {
        index_key: IndexKey,
        query: String,
        limit: Limit,
        tx: oneshot::Sender<FtsSearchR>,
    },
    Highlight {
        index_key: IndexKey,
        query: String,
        documents: Vec<String>,
        tx: oneshot::Sender<FtsHighlightR>,
    },
    Stats {
        index_key: IndexKey,
        tx: oneshot::Sender<FtsStatsR>,
    },
}

pub(crate) trait FtsIndexExt {
    async fn add_document(
        &self,
        primary_id: PrimaryId,
        document: String,
        in_progress: AsyncInProgress,
    ) -> anyhow::Result<()>;
    async fn remove_document(
        &self,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) -> anyhow::Result<()>;
    async fn count(&self, index_key: IndexKey) -> CountR;
    async fn search(&self, index_key: IndexKey, query: String, limit: Limit) -> FtsSearchR;
    async fn highlight(
        &self,
        index_key: IndexKey,
        query: String,
        documents: Vec<String>,
    ) -> FtsHighlightR;
    async fn stats(&self, index_key: IndexKey) -> FtsStatsR;
}

impl FtsIndexExt for mpsc::Sender<FtsIndex> {
    async fn add_document(
        &self,
        primary_id: PrimaryId,
        document: String,
        in_progress: AsyncInProgress,
    ) -> anyhow::Result<()> {
        Ok(self
            .send(FtsIndex::AddDocument {
                primary_id,
                document,
                in_progress,
            })
            .await?)
    }

    async fn remove_document(
        &self,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) -> anyhow::Result<()> {
        Ok(self
            .send(FtsIndex::RemoveDocument {
                primary_id,
                in_progress,
            })
            .await?)
    }

    async fn count(&self, index_key: IndexKey) -> CountR {
        let (tx, rx) = oneshot::channel();
        self.send(FtsIndex::Count { index_key, tx }).await?;
        rx.await?
    }

    async fn search(&self, index_key: IndexKey, query: String, limit: Limit) -> FtsSearchR {
        let (tx, rx) = oneshot::channel();
        self.send(FtsIndex::Search {
            index_key,
            query,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn highlight(
        &self,
        index_key: IndexKey,
        query: String,
        documents: Vec<String>,
    ) -> FtsHighlightR {
        let (tx, rx) = oneshot::channel();
        self.send(FtsIndex::Highlight {
            index_key,
            query,
            documents,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn stats(&self, index_key: IndexKey) -> FtsStatsR {
        let (tx, rx) = oneshot::channel();
        self.send(FtsIndex::Stats { index_key, tx }).await?;
        rx.await?
    }
}
