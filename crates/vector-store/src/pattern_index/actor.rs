/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::IndexKey;
use crate::Limit;
use crate::PrimaryKey;
use crate::tantivy::TantivyIndex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) type PatternLikeR = anyhow::Result<Vec<PrimaryKey>>;

pub(crate) type PatternIndex = TantivyIndex<PatternMessage>;

pub(crate) enum PatternMessage {
    Like {
        index_key: IndexKey,
        pattern: String,
        limit: Limit,
        tx: oneshot::Sender<PatternLikeR>,
    },
}

pub(crate) trait PatternIndexExt {
    async fn like(&self, index_key: IndexKey, pattern: String, limit: Limit) -> PatternLikeR;
}

impl PatternIndexExt for mpsc::Sender<PatternIndex> {
    async fn like(&self, index_key: IndexKey, pattern: String, limit: Limit) -> PatternLikeR {
        let (tx, rx) = oneshot::channel();
        self.send(TantivyIndex::Kind(PatternMessage::Like {
            index_key,
            pattern,
            limit,
            tx,
        }))
        .await?;
        rx.await?
    }
}
