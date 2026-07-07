/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::DbIndexedRow;
use crate::DbIndexedValue;
use crate::IndexKey;
use crate::Metrics;
use crate::fts_index::FtsIndex;
use crate::fts_index::FtsIndexExt;
use crate::metrics::OP_INSERT;
use crate::metrics::OP_REMOVE;
use crate::metrics::OP_UPDATE;
use crate::perf;
use crate::table::Operation;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use crate::table::TableAdd;
use crate::vs_index::VsIndex;
use crate::vs_index::VsIndexExt;
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

pub(crate) trait IndexDispatch {
    fn add_value(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        value: DbIndexedValue,
        in_progress: AsyncInProgress,
    ) -> impl Future<Output = ()> + Send;

    fn remove_value(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) -> impl Future<Output = ()> + Send;

    fn remove_partition(&self, partition_id: PartitionId) -> impl Future<Output = ()> + Send;
}

impl IndexDispatch for mpsc::Sender<VsIndex> {
    async fn add_value(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        value: DbIndexedValue,
        in_progress: AsyncInProgress,
    ) {
        match value {
            DbIndexedValue::Vector(vector) => {
                self.add_vector(partition_id, primary_id, vector, in_progress)
                    .await;
            }
            DbIndexedValue::Document(_) => {
                error!("received document for vector-search index, ignoring");
            }
        }
    }

    async fn remove_value(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) {
        self.remove_vector(partition_id, primary_id, in_progress)
            .await;
    }

    async fn remove_partition(&self, partition_id: PartitionId) {
        VsIndexExt::remove_partition(self, partition_id).await;
    }
}

impl IndexDispatch for mpsc::Sender<FtsIndex> {
    async fn add_value(
        &self,
        _partition_id: PartitionId,
        primary_id: PrimaryId,
        value: DbIndexedValue,
        in_progress: AsyncInProgress,
    ) {
        match value {
            DbIndexedValue::Document(document) => {
                self.add_document(primary_id, document, in_progress).await;
            }
            DbIndexedValue::Vector(_) => {
                error!("received vector for full-text-search index, ignoring");
            }
        }
    }

    async fn remove_value(
        &self,
        _partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: AsyncInProgress,
    ) {
        self.remove_document(primary_id, in_progress).await;
    }

    async fn remove_partition(&self, _partition_id: PartitionId) {}
}

pub(crate) enum MonitorItems {}

pub(crate) async fn new<T>(
    key: IndexKey,
    table: Arc<RwLock<impl TableAdd + Send + Sync + 'static>>,
    mut embeddings: Receiver<(DbIndexedRow, AsyncInProgress)>,
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
                    embedding = embeddings.recv() => {
                        let Some((embedding, in_progress)) = embedding else {
                            break;
                        };
                        add(&table, &index, embedding, in_progress, &metrics, &key).await;
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

async fn add<I: IndexDispatch>(
    table: &Arc<RwLock<impl TableAdd>>,
    index: &I,
    embedding: DbIndexedRow,
    mut in_progress: AsyncInProgress,
    metrics: &Metrics,
    key: &IndexKey,
) {
    // Rows arriving without a progress marker come from CDC. Wrap them in a
    // Cdc marker so that the indexing-lag metric is observed when the marker
    // is dropped (i.e. when the index actor finishes processing the row).
    if matches!(in_progress, AsyncInProgress::None) {
        in_progress = AsyncInProgress::cdc(
            metrics
                .indexing_lag
                .with_label_values(&[key.keyspace().as_ref(), key.index().as_ref()]),
            embedding.timestamp,
        );
    }

    let Ok(operations) = table
        .write()
        .unwrap()
        .add(key, embedding)
        .inspect_err(|err| {
            error!("failed to add embedding to table: {err}");
        })
    else {
        return;
    };
    let in_progress = &mut in_progress;
    for operation in operations.into_iter() {
        match operation {
            Operation::AddValue {
                primary_id,
                partition_id,
                value,
                is_update,
            } => {
                let op_label = if is_update { OP_UPDATE } else { OP_INSERT };
                index
                    .add_value(partition_id, primary_id, value, in_progress.take())
                    .await;
                metrics
                    .modified
                    .with_label_values(&[key.keyspace().as_ref(), key.index().as_ref(), op_label])
                    .inc();
            }
            Operation::RemoveBeforeAddValue {
                primary_id,
                partition_id,
            } => {
                index
                    .remove_value(partition_id, primary_id, AsyncInProgress::None)
                    .await;
            }
            Operation::RemoveValue {
                primary_id,
                partition_id,
            } => {
                index
                    .remove_value(partition_id, primary_id, in_progress.take())
                    .await;
                metrics
                    .modified
                    .with_label_values(&[key.keyspace().as_ref(), key.index().as_ref(), OP_REMOVE])
                    .inc();
            }
            Operation::RemovePartition { partition_id } => {
                index.remove_partition(partition_id).await;
            }
        }
    }

    metrics.mark_dirty(key.keyspace().as_ref(), key.index().as_ref());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbIndexedValue;
    use crate::Timestamp;
    use crate::metrics::Metrics;
    use crate::table::MockTableAdd;
    use crate::vs_index::VsIndex;
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
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndex>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableAdd::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_embeddings,
            tx_index,
            metrics,
        )
        .await
        .unwrap();

        let embedding = DbIndexedRow {
            primary_key: [CqlValue::Int(1)].into(),
            value: Some(DbIndexedValue::Vector(vec![1.].into())),
            timestamp: Timestamp::from_millis(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| Err(anyhow!("some error")));
        tx_embeddings
            .send((embedding, AsyncInProgress::None))
            .await
            .unwrap();

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
    }

    #[tokio::test]
    async fn add_vector_with_progress() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndex>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableAdd::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_embeddings,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let embedding = DbIndexedRow {
            primary_key: [CqlValue::Int(1)].into(),
            value: Some(DbIndexedValue::Vector(vec![1.].into())),
            timestamp: Timestamp::from_millis(10),
        };
        let (tx_progress, _rx_progress) = mpsc::channel(1);
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![Operation::AddValue {
                    primary_id: 2.into(),
                    partition_id: 3.into(),
                    value: DbIndexedValue::Vector(vec![4.].into()),
                    is_update: false,
                }])
            });
        tx_embeddings
            .send((embedding, AsyncInProgress::Fullscan(tx_progress)))
            .await
            .unwrap();
        let VsIndex::AddVector {
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

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 1., 0., 0.);
        assert_eq!(
            indexing_lag_sample_count(&metrics),
            0,
            "full-scan rows must not record indexing lag"
        );
    }

    #[tokio::test]
    async fn add_vector_without_progress() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndex>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableAdd::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_embeddings,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let embedding = DbIndexedRow {
            primary_key: [CqlValue::Int(1)].into(),
            value: Some(DbIndexedValue::Vector(vec![1.].into())),
            timestamp: Timestamp::from_millis(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![Operation::AddValue {
                    primary_id: 2.into(),
                    partition_id: 3.into(),
                    value: DbIndexedValue::Vector(vec![4.].into()),
                    is_update: false,
                }])
            });
        tx_embeddings
            .send((embedding, AsyncInProgress::None))
            .await
            .unwrap();
        let Some(VsIndex::AddVector {
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
        assert!(matches!(in_progress, AsyncInProgress::Cdc(_)));
        drop(in_progress);

        drop(tx_embeddings);
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
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndex>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableAdd::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_embeddings,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let embedding = DbIndexedRow {
            primary_key: [CqlValue::Int(1)].into(),
            value: Some(DbIndexedValue::Vector(vec![1.].into())),
            timestamp: Timestamp::from_millis(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![
                    Operation::RemoveBeforeAddValue {
                        primary_id: 2.into(),
                        partition_id: 3.into(),
                    },
                    Operation::AddValue {
                        primary_id: 3.into(),
                        partition_id: 3.into(),
                        value: DbIndexedValue::Vector(vec![4.].into()),
                        is_update: true,
                    },
                ])
            });
        tx_embeddings
            .send((embedding, AsyncInProgress::None))
            .await
            .unwrap();

        let Some(VsIndex::RemoveVector {
            partition_id,
            primary_id,
            in_progress: AsyncInProgress::None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 2.into());
        assert_eq!(partition_id, 3.into());

        let Some(VsIndex::AddVector {
            partition_id,
            primary_id,
            embedding,
            in_progress,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 3.into());
        assert_eq!(partition_id, 3.into());
        assert_eq!(embedding, vec![4.].into());
        assert!(matches!(in_progress, AsyncInProgress::Cdc(_)));

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 0., 1., 0.);
    }

    #[tokio::test]
    async fn insert_and_update_in_single_batch() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndex>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableAdd::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_embeddings,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let embedding = DbIndexedRow {
            primary_key: [CqlValue::Int(1)].into(),
            value: Some(DbIndexedValue::Vector(vec![1.].into())),
            timestamp: Timestamp::from_millis(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![
                    // Plain insert
                    Operation::AddValue {
                        primary_id: 1.into(),
                        partition_id: 2.into(),
                        value: DbIndexedValue::Vector(vec![10.].into()),
                        is_update: false,
                    },
                    // Update (remove + add)
                    Operation::RemoveBeforeAddValue {
                        primary_id: 3.into(),
                        partition_id: 4.into(),
                    },
                    Operation::AddValue {
                        primary_id: 5.into(),
                        partition_id: 4.into(),
                        value: DbIndexedValue::Vector(vec![20.].into()),
                        is_update: true,
                    },
                ])
            });
        tx_embeddings
            .send((embedding, AsyncInProgress::None))
            .await
            .unwrap();

        // First: plain insert
        let Some(VsIndex::AddVector {
            partition_id,
            primary_id,
            embedding,
            in_progress,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 1.into());
        assert_eq!(partition_id, 2.into());
        assert_eq!(embedding, vec![10.].into());
        assert!(matches!(in_progress, AsyncInProgress::Cdc(_)));

        // Second: remove half of the update
        let Some(VsIndex::RemoveVector {
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
        let Some(VsIndex::AddVector {
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

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 1., 1., 0.);
    }

    #[tokio::test]
    async fn remove_vector() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndex>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableAdd::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_embeddings,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let embedding = DbIndexedRow {
            primary_key: [CqlValue::Int(1)].into(),
            value: None,
            timestamp: Timestamp::from_millis(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![Operation::RemoveValue {
                    primary_id: 5.into(),
                    partition_id: 6.into(),
                }])
            });
        tx_embeddings
            .send((embedding, AsyncInProgress::None))
            .await
            .unwrap();

        let Some(VsIndex::RemoveVector {
            partition_id,
            primary_id,
            in_progress,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 5.into());
        assert_eq!(partition_id, 6.into());
        assert!(matches!(in_progress, AsyncInProgress::Cdc(_)));

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 0., 0., 1.);
    }

    #[tokio::test]
    async fn remove_partition() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel::<VsIndex>(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let table = Arc::new(RwLock::new(MockTableAdd::new()));
        let index_key = IndexKey::new(&"vector".to_string().into(), &"store".to_string().into());
        let _actor = new(
            index_key.clone(),
            Arc::clone(&table),
            rx_embeddings,
            tx_index,
            Arc::clone(&metrics),
        )
        .await
        .unwrap();

        let embedding = DbIndexedRow {
            primary_key: [CqlValue::Int(1)].into(),
            value: None,
            timestamp: Timestamp::from_millis(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![Operation::RemovePartition {
                    partition_id: 6.into(),
                }])
            });
        tx_embeddings
            .send((embedding, AsyncInProgress::None))
            .await
            .unwrap();

        let Some(VsIndex::RemovePartition { partition_id }) = rx_index.recv().await else {
            unreachable!();
        };
        assert_eq!(partition_id, 6.into());

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
        assert_modified_metric_counts(&metrics, 0., 0., 0.);
    }
}
