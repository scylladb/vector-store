/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Distance;
use crate::Embeddings;
use crate::Limit;
use crate::PrimaryKey;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::warn;

pub(crate) type AnnR = anyhow::Result<(Vec<PrimaryKey>, Vec<Distance>)>;
pub(crate) type CountR = anyhow::Result<usize>;

pub(crate) enum Index {
    Add {
        primary_key: PrimaryKey,
        embeddings: Embeddings,
    },
    Ann {
        embeddings: Embeddings,
        limit: Limit,
        tx: oneshot::Sender<AnnR>,
    },
    Count {
        tx: oneshot::Sender<CountR>,
    },
}

pub(crate) trait IndexExt {
    async fn add(&self, primary_key: PrimaryKey, embeddings: Embeddings);
    async fn ann(&self, embeddings: Embeddings, limit: Limit) -> AnnR;
    async fn count(&self) -> CountR;
}

impl IndexExt for mpsc::Sender<Index> {
    async fn add(&self, primary_key: PrimaryKey, embeddings: Embeddings) {
        self.send(Index::Add {
            primary_key,
            embeddings,
        })
        .await
        .unwrap_or_else(|err| warn!("IndexExt::add: unable to send request: {err}"));
    }

    async fn ann(&self, embeddings: Embeddings, limit: Limit) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Ann {
            embeddings,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn count(&self) -> CountR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Count { tx }).await?;
        rx.await?
    }
}
