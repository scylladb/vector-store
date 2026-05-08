/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod analyzer;
mod index;
mod query;

pub(crate) use index::FtsIndex;

use crate::Limit;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Result type for FTS search: list of `(PrimaryId, relevance_score)` pairs.
pub(crate) type FtsSearchR = anyhow::Result<Vec<(PrimaryId, f32)>>;

/// Actor messages for the FTS index.
pub(crate) enum FtsMessage {
    AddDocument {
        partition_id: PartitionId,
        primary_id: PrimaryId,
        text_content: String,
    },
    RemoveDocument {
        partition_id: PartitionId,
        primary_id: PrimaryId,
    },
    Search {
        query: String,
        limit: Limit,
        tx: oneshot::Sender<FtsSearchR>,
    },
    Count {
        tx: oneshot::Sender<anyhow::Result<usize>>,
    },
}

/// Async interface for sending messages to the FTS actor.
pub(crate) trait FtsIndexExt {
    async fn add_document(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        text_content: String,
    );
    async fn remove_document(&self, partition_id: PartitionId, primary_id: PrimaryId);
    async fn search(&self, query: String, limit: Limit) -> FtsSearchR;
    async fn count(&self) -> anyhow::Result<usize>;
}

impl FtsIndexExt for mpsc::Sender<FtsMessage> {
    async fn add_document(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        text_content: String,
    ) {
        self.send(FtsMessage::AddDocument {
            partition_id,
            primary_id,
            text_content,
        })
        .await
        .expect("fts actor should receive request");
    }

    async fn remove_document(&self, partition_id: PartitionId, primary_id: PrimaryId) {
        self.send(FtsMessage::RemoveDocument {
            partition_id,
            primary_id,
        })
        .await
        .expect("fts actor should receive request");
    }

    async fn search(&self, query: String, limit: Limit) -> FtsSearchR {
        let (tx, rx) = oneshot::channel();
        self.send(FtsMessage::Search { query, limit, tx })
            .await?;
        rx.await?
    }

    async fn count(&self) -> anyhow::Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.send(FtsMessage::Count { tx }).await?;
        rx.await?
    }
}
