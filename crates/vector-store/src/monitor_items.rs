/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::DbIndexedOperation;
use crate::DbIndexedRow;
use crate::DbIndexedValue;
use crate::IndexKey;
use crate::Metrics;
use crate::NonemptyBox;
use crate::PrimaryKey;
use crate::Timestamp;
use crate::Timestamped;
use crate::Vector;
use crate::fts_index::FtsIndex;
use crate::fts_index::FtsIndexExt;
use crate::metrics::OP_INSERT;
use crate::metrics::OP_REMOVE;
use crate::metrics::OP_UPDATE;
use crate::perf;
use crate::table::Operation;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use crate::table::TableModify;
use crate::vs_index::VsIndexModify;
use crate::vs_index::VsIndexModifyExt;
use std::future::Future;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::Instrument;
use tracing::debug;
use tracing::error;
use tracing::error_span;

pub(crate) enum IndexStatus {
    Live,
    Dead,
}

impl From<anyhow::Result<()>> for IndexStatus {
    fn from(result: anyhow::Result<()>) -> Self {
        match result {
            Ok(()) => IndexStatus::Live,
            Err(_) => IndexStatus::Dead,
        }
    }
}

impl IndexStatus {
    pub(crate) fn is_dead(&self) -> bool {
        matches!(self, IndexStatus::Dead)
    }
}

pub(crate) trait IndexDispatch {
    fn add_vector(
        &self,
        _partition_id: PartitionId,
        _primary_id: PrimaryId,
        _vector: Vector,
        _in_progress: AsyncInProgress,
    ) -> impl Future<Output = IndexStatus> + Send {
        async move {
            error!("ignoring add vector for an index that does not support it");
            IndexStatus::Live
        }
    }

    fn add_document(
        &self,
        _partition_id: PartitionId,
        _primary_id: PrimaryId,
        _document: String,
        _in_progress: AsyncInProgress,
    ) -> impl Future<Output = IndexStatus> + Send {
        async move {
            error!("ignoring add document for an index that does not support it");
            IndexStatus::Live
        }
    }

    fn remove_value(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) -> impl Future<Output = IndexStatus> + Send;

    fn remove_partition(
        &self,
        partition_id: PartitionId,
    ) -> impl Future<Output = IndexStatus> + Send;
}

impl IndexDispatch for mpsc::Sender<VsIndexModify> {
    async fn add_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        vector: Vector,
        in_progress: AsyncInProgress,
    ) -> IndexStatus {
        VsIndexModifyExt::add_vector(self, partition_id, primary_id, vector, in_progress)
            .await
            .into()
    }

    async fn remove_value(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) -> IndexStatus {
        self.remove_vector(partition_id, primary_id, in_progress)
            .await
            .into()
    }

    async fn remove_partition(&self, partition_id: PartitionId) -> IndexStatus {
        VsIndexModifyExt::remove_partition(self, partition_id)
            .await
            .into()
    }
}

impl IndexDispatch for mpsc::Sender<FtsIndex> {
    async fn add_document(
        &self,
        _partition_id: PartitionId,
        primary_id: PrimaryId,
        document: String,
        in_progress: AsyncInProgress,
    ) -> IndexStatus {
        FtsIndexExt::add_document(self, primary_id, document, in_progress)
            .await
            .into()
    }

    async fn remove_value(
        &self,
        _partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) -> IndexStatus {
        self.remove_document(primary_id, in_progress).await.into()
    }

    async fn remove_partition(&self, _partition_id: PartitionId) -> IndexStatus {
        IndexStatus::Live
    }
}

pub(crate) enum MonitorItems {}

pub(crate) async fn new<T>(
    key: IndexKey,
    table: Arc<RwLock<impl TableModify + Send + Sync + 'static>>,
    mut db_rows: Receiver<(DbIndexedRow, AsyncInProgress)>,
    index: mpsc::Sender<T>,
    metrics: Arc<Metrics>,
) -> anyhow::Result<Sender<MonitorItems>>
where
    T: Send + 'static,
    mpsc::Sender<T>: IndexDispatch,
{
    let (tx, mut rx) = mpsc::channel(perf::channel_size().into());
    let key_for_span = key.clone();

    tokio::spawn(perf::hotpath_async(
        async move {
            debug!("starting");

            while !rx.is_closed() {
                tokio::select! {
                    db_row = db_rows.recv() => {
                        let Some((db_row, in_progress)) = db_row else {
                            break;
                        };
                        let primary_key = db_row.primary_key;
                        match db_row.operation {
                            DbIndexedOperation::Upsert(values) => {
                                if upsert(&table, &index, primary_key, values, in_progress, &metrics, &key).await.is_dead() {
                                    error!("Index for {key:?} disappeared while processing upsert");
                                    break;
                                }
                            }
                            DbIndexedOperation::Delete(timestamp) => {
                                if delete(&table, &index, primary_key, timestamp, in_progress, &metrics, &key).await.is_dead() {
                                    error!("Index for {key:?} disappeared while processing delete");
                                    break;
                                }
                            }
                        }
                    }
                    _ = rx.recv() => { }
                }
            }

            debug!("finished");
        }
        .instrument(error_span!("monitor items", "{key_for_span}")),
    ));
    Ok(tx)
}

async fn upsert<I: IndexDispatch>(
    table: &Arc<RwLock<impl TableModify>>,
    index: &I,
    primary_key: PrimaryKey,
    values: NonemptyBox<Timestamped<DbIndexedValue>>,
    in_progress: AsyncInProgress,
    metrics: &Metrics,
    index_key: &IndexKey,
) -> IndexStatus {
    let Ok(operations) = table
        .write()
        .unwrap()
        .upsert(index_key, primary_key, values)
        .inspect_err(|err| {
            error!("failed to upsert values to a table: {err}");
        })
    else {
        return IndexStatus::Live;
    };
    process_operations(operations, index, in_progress, metrics, index_key).await
}

async fn delete<I: IndexDispatch>(
    table: &Arc<RwLock<impl TableModify>>,
    index: &I,
    primary_key: PrimaryKey,
    timestamp: Timestamp,
    in_progress: AsyncInProgress,
    metrics: &Metrics,
    index_key: &IndexKey,
) -> IndexStatus {
    let Ok(operations) = table
        .write()
        .unwrap()
        .delete(index_key, primary_key, timestamp)
        .inspect_err(|err| {
            error!("failed to delete row from a table: {err}");
        })
    else {
        return IndexStatus::Live;
    };
    process_operations(operations, index, in_progress, metrics, index_key).await
}

async fn process_operations<I: IndexDispatch>(
    operations: Vec<Operation>,
    index: &I,
    mut in_progress: AsyncInProgress,
    metrics: &Metrics,
    index_key: &IndexKey,
) -> IndexStatus {
    let in_progress = &mut in_progress;
    for operation in operations.into_iter() {
        match operation {
            Operation::AddVector {
                primary_id,
                partition_id,
                vector,
                is_update,
            } => {
                let op_label = if is_update { OP_UPDATE } else { OP_INSERT };
                if index
                    .add_vector(partition_id, primary_id, vector, in_progress.take())
                    .await
                    .is_dead()
                {
                    return IndexStatus::Dead;
                }
                metrics
                    .modified
                    .with_label_values(&[
                        index_key.keyspace().as_ref(),
                        index_key.index().as_ref(),
                        op_label,
                    ])
                    .inc();
            }
            Operation::AddDocument {
                primary_id,
                partition_id,
                document,
                is_update,
            } => {
                let op_label = if is_update { OP_UPDATE } else { OP_INSERT };
                if index
                    .add_document(partition_id, primary_id, document, in_progress.take())
                    .await
                    .is_dead()
                {
                    return IndexStatus::Dead;
                }
                metrics
                    .modified
                    .with_label_values(&[
                        index_key.keyspace().as_ref(),
                        index_key.index().as_ref(),
                        op_label,
                    ])
                    .inc();
            }
            Operation::RemoveBeforeAddValue {
                primary_id,
                partition_id,
            } => {
                if index
                    .remove_value(partition_id, primary_id, AsyncInProgress::None)
                    .await
                    .is_dead()
                {
                    return IndexStatus::Dead;
                }
            }
            Operation::RemoveValue {
                primary_id,
                partition_id,
            } => {
                if index
                    .remove_value(partition_id, primary_id, in_progress.take())
                    .await
                    .is_dead()
                {
                    return IndexStatus::Dead;
                }
                metrics
                    .modified
                    .with_label_values(&[
                        index_key.keyspace().as_ref(),
                        index_key.index().as_ref(),
                        OP_REMOVE,
                    ])
                    .inc();
            }
            Operation::RemovePartition { partition_id } => {
                if index.remove_partition(partition_id).await.is_dead() {
                    return IndexStatus::Dead;
                }
            }
        }
    }

    metrics.mark_dirty(index_key.keyspace().as_ref(), index_key.index().as_ref());
    IndexStatus::Live
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbIndexedOperation;
    use crate::DbIndexedValue;
    use crate::NonemptyBox;
    use crate::Timestamp;
    use crate::Timestamped;
    use crate::metrics::Metrics;
    use crate::table::MockTableModify;
    use crate::vs_index::VsIndexModify;
    use anyhow::anyhow;
    use mockall::predicate::*;
    use scylla::value::CqlValue;

    // prometheus counter returns f64, so we need to compare with f64 values
    fn assert_modified_metric_counts(metrics: &Metrics, insert: f64, update: f64, remove: f64) {
        assert_eq!(
            metrics
                .modified
                .with_label_values(&["vector", "store", OP_INSERT])
                .get(),
            insert,
        );
        assert_eq!(
            metrics
                .modified
                .with_label_values(&["vector", "store", OP_UPDATE])
                .get(),
            update,
        );
        assert_eq!(
            metrics
                .modified
                .with_label_values(&["vector", "store", OP_REMOVE])
                .get(),
            remove,
        );
    }

    fn indexing_lag_sample_count(metrics: &Metrics) -> u64 {
        metrics
            .indexing_lag
            .with_label_values(&["vector", "store"])
            .get_sample_count()
    }

    #[tokio::test]
    async fn do_nothing_on_error() {
        let (tx_db_rows, rx_db_rows) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndexModify>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableModify::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_db_rows,
            tx_index,
            metrics,
        )
        .await
        .unwrap();

        let primary_key: PrimaryKey = [CqlValue::Int(1)].into();
        let values = NonemptyBox::new([Timestamped::new(
            Timestamp::from_millis(10),
            Some(DbIndexedValue::Vector(vec![1.].into())),
        )])
        .unwrap();
        table
            .write()
            .unwrap()
            .expect_upsert()
            .with(eq(index_key), eq(primary_key.clone()), eq(values.clone()))
            .once()
            .returning(|_, _, _| Err(anyhow!("some error")));
        tx_db_rows
            .send((
                DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Upsert(values),
                },
                AsyncInProgress::None,
            ))
            .await
            .unwrap();

        drop(tx_db_rows);
        assert!(rx_index.recv().await.is_none());
    }

    #[tokio::test]
    async fn add_vector_with_progress() {
        let (tx_db_rows, rx_db_rows) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndexModify>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableModify::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_db_rows,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let primary_key: PrimaryKey = [CqlValue::Int(1)].into();
        let values = NonemptyBox::new([Timestamped::new(
            Timestamp::from_millis(10),
            Some(DbIndexedValue::Vector(vec![1.].into())),
        )])
        .unwrap();
        let (tx_progress, _rx_progress) = mpsc::channel(1);
        table
            .write()
            .unwrap()
            .expect_upsert()
            .with(eq(index_key), eq(primary_key.clone()), eq(values.clone()))
            .once()
            .returning(|_, _, _| {
                Ok(vec![Operation::AddVector {
                    primary_id: 2.into(),
                    partition_id: 3.into(),
                    vector: vec![4.].into(),
                    is_update: false,
                }])
            });
        tx_db_rows
            .send((
                DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Upsert(values),
                },
                AsyncInProgress::Fullscan(tx_progress),
            ))
            .await
            .unwrap();
        let VsIndexModify::AddVector {
            primary_id,
            partition_id,
            embedding,
            in_progress,
        } = rx_index.recv().await.unwrap()
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 2.into());
        assert_eq!(partition_id, 3.into());
        assert_eq!(embedding, vec![4.].into());
        assert!(matches!(in_progress, AsyncInProgress::Fullscan(_)));

        drop(tx_db_rows);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 1., 0., 0.);
        assert_eq!(
            indexing_lag_sample_count(&metrics),
            0,
            "full-scan rows must not record indexing lag"
        );
    }

    #[tokio::test]
    async fn add_vector_with_cdc_progress() {
        let (tx_db_rows, rx_db_rows) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndexModify>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableModify::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_db_rows,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let primary_key: PrimaryKey = [CqlValue::Int(1)].into();
        let values = NonemptyBox::new([Timestamped::new(
            Timestamp::from_millis(10),
            Some(DbIndexedValue::Vector(vec![1.].into())),
        )])
        .unwrap();
        table
            .write()
            .unwrap()
            .expect_upsert()
            .with(eq(index_key), eq(primary_key.clone()), eq(values.clone()))
            .once()
            .returning(|_, _, _| {
                Ok(vec![Operation::AddVector {
                    primary_id: 2.into(),
                    partition_id: 3.into(),
                    vector: vec![4.].into(),
                    is_update: false,
                }])
            });
        tx_db_rows
            .send((
                DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Upsert(values),
                },
                AsyncInProgress::cdc(
                    metrics.indexing_lag.with_label_values(&["vector", "store"]),
                    Timestamp::now(),
                ),
            ))
            .await
            .unwrap();
        let Some(VsIndexModify::AddVector {
            partition_id,
            primary_id,
            embedding,
            in_progress,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 2.into());
        assert_eq!(partition_id, 3.into());
        assert_eq!(embedding, vec![4.].into());
        drop(in_progress);

        drop(tx_db_rows);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 1., 0., 0.);
        assert_eq!(
            indexing_lag_sample_count(&metrics),
            1,
            "CDC rows must record indexing lag"
        );
    }

    #[tokio::test]
    async fn update_vector() {
        let (tx_db_rows, rx_db_rows) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndexModify>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableModify::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_db_rows,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let primary_key: PrimaryKey = [CqlValue::Int(1)].into();
        let values = NonemptyBox::new([Timestamped::new(
            Timestamp::from_millis(10),
            Some(DbIndexedValue::Vector(vec![1.].into())),
        )])
        .unwrap();
        table
            .write()
            .unwrap()
            .expect_upsert()
            .with(eq(index_key), eq(primary_key.clone()), eq(values.clone()))
            .once()
            .returning(|_, _, _| {
                Ok(vec![
                    Operation::RemoveBeforeAddValue {
                        primary_id: 2.into(),
                        partition_id: 3.into(),
                    },
                    Operation::AddVector {
                        primary_id: 3.into(),
                        partition_id: 3.into(),
                        vector: vec![4.].into(),
                        is_update: true,
                    },
                ])
            });
        tx_db_rows
            .send((
                DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Upsert(values),
                },
                AsyncInProgress::None,
            ))
            .await
            .unwrap();

        let Some(VsIndexModify::RemoveVector {
            partition_id,
            primary_id,
            in_progress: AsyncInProgress::None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 2.into());
        assert_eq!(partition_id, 3.into());

        let Some(VsIndexModify::AddVector {
            partition_id,
            primary_id,
            embedding,
            ..
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 3.into());
        assert_eq!(partition_id, 3.into());
        assert_eq!(embedding, vec![4.].into());

        drop(tx_db_rows);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 0., 1., 0.);
    }

    #[tokio::test]
    async fn insert_and_update_in_single_batch() {
        let (tx_db_rows, rx_db_rows) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndexModify>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableModify::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_db_rows,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let primary_key: PrimaryKey = [CqlValue::Int(1)].into();
        let values = NonemptyBox::new([Timestamped::new(
            Timestamp::from_millis(10),
            Some(DbIndexedValue::Vector(vec![1.].into())),
        )])
        .unwrap();
        table
            .write()
            .unwrap()
            .expect_upsert()
            .with(eq(index_key), eq(primary_key.clone()), eq(values.clone()))
            .once()
            .returning(|_, _, _| {
                Ok(vec![
                    // Plain insert
                    Operation::AddVector {
                        primary_id: 1.into(),
                        partition_id: 2.into(),
                        vector: vec![10.].into(),
                        is_update: false,
                    },
                    // Update (remove + add)
                    Operation::RemoveBeforeAddValue {
                        primary_id: 3.into(),
                        partition_id: 4.into(),
                    },
                    Operation::AddVector {
                        primary_id: 5.into(),
                        partition_id: 4.into(),
                        vector: vec![20.].into(),
                        is_update: true,
                    },
                ])
            });
        tx_db_rows
            .send((
                DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Upsert(values),
                },
                AsyncInProgress::None,
            ))
            .await
            .unwrap();

        // First: plain insert
        let Some(VsIndexModify::AddVector {
            partition_id,
            primary_id,
            embedding,
            ..
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 1.into());
        assert_eq!(partition_id, 2.into());
        assert_eq!(embedding, vec![10.].into());

        // Second: remove half of the update
        let Some(VsIndexModify::RemoveVector {
            partition_id,
            primary_id,
            in_progress: AsyncInProgress::None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 3.into());
        assert_eq!(partition_id, 4.into());

        // Third: add half of the update
        let Some(VsIndexModify::AddVector {
            partition_id,
            primary_id,
            embedding,
            in_progress: AsyncInProgress::None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 5.into());
        assert_eq!(partition_id, 4.into());
        assert_eq!(embedding, vec![20.].into());

        drop(tx_db_rows);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 1., 1., 0.);
    }

    #[tokio::test]
    async fn remove_vector_with_none_value() {
        let (tx_db_rows, rx_db_rows) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndexModify>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableModify::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_db_rows,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let primary_key: PrimaryKey = [CqlValue::Int(1)].into();
        let values =
            NonemptyBox::new([Timestamped::new(Timestamp::from_millis(10), None)]).unwrap();
        table
            .write()
            .unwrap()
            .expect_upsert()
            .with(eq(index_key), eq(primary_key.clone()), eq(values.clone()))
            .once()
            .returning(|_, _, _| {
                Ok(vec![Operation::RemoveValue {
                    primary_id: 5.into(),
                    partition_id: 6.into(),
                }])
            });
        tx_db_rows
            .send((
                DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Upsert(values),
                },
                AsyncInProgress::None,
            ))
            .await
            .unwrap();

        let Some(VsIndexModify::RemoveVector {
            partition_id,
            primary_id,
            ..
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 5.into());
        assert_eq!(partition_id, 6.into());

        drop(tx_db_rows);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 0., 0., 1.);
    }

    #[tokio::test]
    async fn remove_vector_with_delete() {
        let (tx_db_rows, rx_db_rows) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndexModify>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableModify::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_db_rows,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let primary_key: PrimaryKey = [CqlValue::Int(1)].into();
        table
            .write()
            .unwrap()
            .expect_delete()
            .with(
                eq(index_key),
                eq(primary_key.clone()),
                eq(Timestamp::from_millis(10)),
            )
            .once()
            .returning(|_, _, _| {
                Ok(vec![Operation::RemoveValue {
                    primary_id: 5.into(),
                    partition_id: 6.into(),
                }])
            });
        tx_db_rows
            .send((
                DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Delete(Timestamp::from_millis(10)),
                },
                AsyncInProgress::None,
            ))
            .await
            .unwrap();

        let Some(VsIndexModify::RemoveVector {
            partition_id,
            primary_id,
            ..
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 5.into());
        assert_eq!(partition_id, 6.into());

        drop(tx_db_rows);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 0., 0., 1.);
    }

    #[tokio::test]
    async fn remove_partition() {
        let (tx_db_rows, rx_db_rows) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndexModify>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableModify::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_db_rows,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let primary_key: PrimaryKey = [CqlValue::Int(1)].into();
        let values = NonemptyBox::new([Timestamped::<DbIndexedValue>::new(
            Timestamp::from_millis(10),
            None,
        )])
        .unwrap();
        table
            .write()
            .unwrap()
            .expect_upsert()
            .with(eq(index_key), eq(primary_key.clone()), eq(values.clone()))
            .once()
            .returning(|_, _, _| {
                Ok(vec![Operation::RemovePartition {
                    partition_id: 6.into(),
                }])
            });
        tx_db_rows
            .send((
                DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Upsert(values),
                },
                AsyncInProgress::None,
            ))
            .await
            .unwrap();

        let Some(VsIndexModify::RemovePartition { partition_id }) = rx_index.recv().await else {
            unreachable!();
        };
        assert_eq!(partition_id, 6.into());

        drop(tx_db_rows);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 0., 0., 0.);
    }
}
