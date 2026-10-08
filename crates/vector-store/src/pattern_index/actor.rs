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

pub(crate) type PatternLikeR = anyhow::Result<Vec<PrimaryKey>>;

pub(crate) enum PatternIndex {
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
    Like {
        index_key: IndexKey,
        pattern: String,
        limit: Limit,
        tx: oneshot::Sender<PatternLikeR>,
    },
}

pub(crate) trait PatternIndexExt {
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
    async fn like(&self, index_key: IndexKey, pattern: String, limit: Limit) -> PatternLikeR;
}

impl PatternIndexExt for mpsc::Sender<PatternIndex> {
    async fn add_document(
        &self,
        primary_id: PrimaryId,
        document: String,
        in_progress: AsyncInProgress,
    ) -> anyhow::Result<()> {
        Ok(self
            .send(PatternIndex::AddDocument {
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
            .send(PatternIndex::RemoveDocument {
                primary_id,
                in_progress,
            })
            .await?)
    }

    async fn count(&self, index_key: IndexKey) -> CountR {
        let (tx, rx) = oneshot::channel();
        self.send(PatternIndex::Count { index_key, tx }).await?;
        rx.await?
    }

    async fn like(&self, index_key: IndexKey, pattern: String, limit: Limit) -> PatternLikeR {
        let (tx, rx) = oneshot::channel();
        self.send(PatternIndex::Like {
            index_key,
            pattern,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }
}
