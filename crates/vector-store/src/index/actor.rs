/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::Distance;
use crate::Limit;
use crate::PrimaryKey;
use crate::Vector;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) type AnnR = anyhow::Result<(Vec<PrimaryKey>, Vec<Distance>)>;
pub(crate) type CountR = anyhow::Result<usize>;
pub(crate) type MemoryUsageR = anyhow::Result<usize>;

pub enum Index {
    AddOrReplace {
        primary_key: PrimaryKey,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    },
    Remove {
        primary_key: PrimaryKey,
        in_progress: Option<AsyncInProgress>,
    },
    Ann {
        embedding: Vector,
        limit: Limit,
        tx: oneshot::Sender<AnnR>,
    },
    Count {
        tx: oneshot::Sender<CountR>,
    },
    MemoryUsage {
        tx: oneshot::Sender<MemoryUsageR>,
    },
}

pub(crate) trait IndexExt {
    async fn add_or_replace(
        &self,
        primary_key: PrimaryKey,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    );
    async fn remove(&self, primary_key: PrimaryKey, in_progress: Option<AsyncInProgress>);
    async fn ann(&self, embedding: Vector, limit: Limit) -> AnnR;
    async fn count(&self) -> CountR;
    async fn memory_usage(&self) -> MemoryUsageR;
}

impl IndexExt for mpsc::Sender<Index> {
    async fn add_or_replace(
        &self,
        primary_key: PrimaryKey,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    ) {
        self.send(Index::AddOrReplace {
            primary_key,
            embedding,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn remove(&self, primary_key: PrimaryKey, in_progress: Option<AsyncInProgress>) {
        self.send(Index::Remove {
            primary_key,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn ann(&self, embedding: Vector, limit: Limit) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Ann {
            embedding,
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

    async fn memory_usage(&self) -> MemoryUsageR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::MemoryUsage { tx }).await?;
        rx.await?
    }
}
