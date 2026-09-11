/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::AsyncInProgress;
use crate::ColumnName;
use crate::Distance;
use crate::Filter;
use crate::IndexKey;
use crate::Limit;
use crate::PrimaryKey;
use crate::Vector;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) type AnnR = anyhow::Result<(
    Vec<PrimaryKey>,
    Vec<Distance>,
    HashMap<ColumnName, Vec<Option<CqlValue>>>,
)>;
pub(crate) type CountR = anyhow::Result<usize>;

/// The `column_values` shape for zero result rows: every requested column
/// present, mapped to an empty Vec - never an empty map - so a client can
/// always rely on `column_values` having exactly the requested columns as
/// keys whenever `return_columns` isn't empty, no matter how many rows
/// matched.
pub(crate) fn empty_column_values(
    return_columns: &[ColumnName],
) -> HashMap<ColumnName, Vec<Option<CqlValue>>> {
    return_columns
        .iter()
        .cloned()
        .map(|column| (column, Vec::new()))
        .collect()
}

pub enum VsIndexModify {
    AddVector {
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
        in_progress: AsyncInProgress,
    },
    RemoveVector {
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    },
    RemovePartition {
        partition_id: PartitionId,
    },
}

pub enum VsIndexSearch {
    Ann {
        index_key: IndexKey,
        embedding: Vector,
        limit: Limit,
        return_columns: Arc<[ColumnName]>,
        tx: oneshot::Sender<AnnR>,
    },
    FilteredAnn {
        index_key: IndexKey,
        embedding: Vector,
        filter: Filter,
        limit: Limit,
        return_columns: Arc<[ColumnName]>,
        tx: oneshot::Sender<AnnR>,
    },
    Count {
        index_key: IndexKey,
        tx: oneshot::Sender<CountR>,
    },
}

pub(super) enum Message {
    Modify(VsIndexModify),
    Search(VsIndexSearch),
}

pub(crate) trait VsIndexModifyExt {
    async fn add_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
        in_progress: AsyncInProgress,
    ) -> anyhow::Result<()>;
    async fn remove_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) -> anyhow::Result<()>;
    async fn remove_partition(&self, partition_id: PartitionId) -> anyhow::Result<()>;
}

pub(crate) trait VsIndexSearchExt {
    async fn ann(
        &self,
        index_key: IndexKey,
        embedding: Vector,
        limit: Limit,
        return_columns: Arc<[ColumnName]>,
    ) -> AnnR;
    async fn filtered_ann(
        &self,
        index_key: IndexKey,
        embedding: Vector,
        filter: Filter,
        limit: Limit,
        return_columns: Arc<[ColumnName]>,
    ) -> AnnR;
    async fn count(&self, index_key: IndexKey) -> CountR;
}

impl VsIndexModifyExt for mpsc::Sender<VsIndexModify> {
    #[hotpath::measure]
    async fn add_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
        in_progress: AsyncInProgress,
    ) -> anyhow::Result<()> {
        Ok(self
            .send(VsIndexModify::AddVector {
                partition_id,
                primary_id,
                embedding,
                in_progress,
            })
            .await?)
    }

    #[hotpath::measure]
    async fn remove_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) -> anyhow::Result<()> {
        Ok(self
            .send(VsIndexModify::RemoveVector {
                partition_id,
                primary_id,
                in_progress,
            })
            .await?)
    }

    #[hotpath::measure]
    async fn remove_partition(&self, partition_id: PartitionId) -> anyhow::Result<()> {
        Ok(self
            .send(VsIndexModify::RemovePartition { partition_id })
            .await?)
    }
}

impl VsIndexSearchExt for mpsc::Sender<VsIndexSearch> {
    #[hotpath::measure]
    async fn ann(
        &self,
        index_key: IndexKey,
        embedding: Vector,
        limit: Limit,
        return_columns: Arc<[ColumnName]>,
    ) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(VsIndexSearch::Ann {
            index_key,
            embedding,
            limit,
            return_columns,
            tx,
        })
        .await?;
        rx.await?
    }

    #[hotpath::measure]
    async fn filtered_ann(
        &self,
        index_key: IndexKey,
        embedding: Vector,
        filter: Filter,
        limit: Limit,
        return_columns: Arc<[ColumnName]>,
    ) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(VsIndexSearch::FilteredAnn {
            index_key,
            embedding,
            filter,
            limit,
            return_columns,
            tx,
        })
        .await?;
        rx.await?
    }

    #[hotpath::measure]
    async fn count(&self, index_key: IndexKey) -> CountR {
        let (tx, rx) = oneshot::channel();
        self.send(VsIndexSearch::Count { index_key, tx }).await?;
        rx.await?
    }
}
