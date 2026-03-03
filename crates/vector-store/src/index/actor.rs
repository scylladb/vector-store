/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::Distance;
use crate::Filter;
use crate::IndexKey;
use crate::Limit;
use crate::PrimaryKey;
use crate::Vector;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) type AnnR = anyhow::Result<(Vec<Arc<PrimaryKey>>, Vec<Distance>)>;
pub(crate) type CountR = anyhow::Result<usize>;

pub enum Index {
    AddVector {
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    },
    RemoveVector {
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: Option<AsyncInProgress>,
    },
    RemovePartition {
        partition_id: PartitionId,
    },
    Ann {
        index_key: IndexKey,
        embedding: Vector,
        limit: Limit,
        tx: oneshot::Sender<AnnR>,
    },
    FilteredAnn {
        index_key: IndexKey,
        embedding: Vector,
        filter: Filter,
        limit: Limit,
        tx: oneshot::Sender<AnnR>,
    },
    Count {
        index_key: IndexKey,
        tx: oneshot::Sender<CountR>,
    },
}

pub(crate) trait IndexExt {
    async fn add_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    );
    async fn remove_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: Option<AsyncInProgress>,
    );
    async fn remove_partition(&self, partition_id: PartitionId);
    async fn ann(&self, index_key: IndexKey, embedding: Vector, limit: Limit) -> AnnR;
    async fn filtered_ann(
        &self,
        index_key: IndexKey,
        embedding: Vector,
        filter: Filter,
        limit: Limit,
    ) -> AnnR;
    async fn count(&self, index_key: IndexKey) -> CountR;
}

impl IndexExt for mpsc::Sender<Index> {
    async fn add_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    ) {
        self.send(Index::AddVector {
            partition_id,
            primary_id,
            embedding,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn remove_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: Option<AsyncInProgress>,
    ) {
        self.send(Index::RemoveVector {
            partition_id,
            primary_id,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn remove_partition(&self, partition_id: PartitionId) {
        self.send(Index::RemovePartition { partition_id })
            .await
            .expect("internal actor should receive request");
    }

    async fn ann(&self, index_key: IndexKey, embedding: Vector, limit: Limit) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Ann {
            index_key,
            embedding,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn filtered_ann(
        &self,
        index_key: IndexKey,
        embedding: Vector,
        filter: Filter,
        limit: Limit,
    ) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::FilteredAnn {
            index_key,
            embedding,
            filter,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn count(&self, index_key: IndexKey) -> CountR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Count { index_key, tx }).await?;
        rx.await?
    }
}
