/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::DbEmbedding;
use crate::IndexKey;
use crate::Metrics;
use crate::index::Index;
use crate::index::IndexExt;
use crate::table::Operation;
use crate::table::TableAdd;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;

pub(crate) enum MonitorItems {}

pub(crate) async fn new(
    key: IndexKey,
    table: Arc<RwLock<impl TableAdd + Send + Sync + 'static>>,
    mut embeddings: Receiver<(DbEmbedding, Option<AsyncInProgress>)>,
    index: Sender<Index>,
    metrics: Arc<Metrics>,
) -> anyhow::Result<Sender<MonitorItems>> {
    // The value was taken from initial benchmarks
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);
    let key_for_span = key.clone();

    tokio::spawn(
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
        .instrument(debug_span!("monitor items", "{key_for_span}")),
    );
    Ok(tx)
}

async fn add(
    table: &Arc<RwLock<impl TableAdd>>,
    index: &Sender<Index>,
    embedding: DbEmbedding,
    mut in_progress: Option<AsyncInProgress>,
    metrics: &Metrics,
    key: &IndexKey,
) {
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
            Operation::AddVector {
                primary_id,
                partition_id,
                vector,
            } => {
                index
                    .add_vector(partition_id, primary_id, vector, in_progress.take())
                    .await;
                metrics
                    .modified
                    .with_label_values(&[key.keyspace().as_ref(), key.index().as_ref(), "update"])
                    .inc();
            }
            Operation::RemoveBeforeAddVector {
                primary_id,
                partition_id,
            } => {
                index.remove_vector(partition_id, primary_id, None).await;
            }
            Operation::RemoveVector {
                primary_id,
                partition_id,
            } => {
                index
                    .remove_vector(partition_id, primary_id, in_progress.take())
                    .await;
                metrics
                    .modified
                    .with_label_values(&[key.keyspace().as_ref(), key.index().as_ref(), "remove"])
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
    use crate::Timestamp;
    use crate::metrics::Metrics;
    use crate::table::MockTableAdd;
    use anyhow::anyhow;
    use mockall::predicate::*;
    use scylla::value::CqlValue;

    #[tokio::test]
    async fn do_nothing_on_error() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel(10);
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

        let embedding = DbEmbedding {
            primary_key: [CqlValue::Int(1)].into(),
            embedding: Some(vec![1.].into()),
            timestamp: Timestamp::from_unix_timestamp(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| Err(anyhow!("some error")));
        tx_embeddings.send((embedding, None)).await.unwrap();

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
    }

    #[tokio::test]
    async fn add_vector_with_progress() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel(10);
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

        let embedding = DbEmbedding {
            primary_key: [CqlValue::Int(1)].into(),
            embedding: Some(vec![1.].into()),
            timestamp: Timestamp::from_unix_timestamp(10),
        };
        let (tx_progress, _rx_progress) = mpsc::channel(1);
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![Operation::AddVector {
                    primary_id: 2.into(),
                    partition_id: 3.into(),
                    vector: vec![4.].into(),
                }])
            });
        tx_embeddings
            .send((embedding, Some(AsyncInProgress(tx_progress))))
            .await
            .unwrap();
        let Index::AddVector {
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
        assert!(in_progress.is_some());

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
    }

    #[tokio::test]
    async fn add_vector_without_progress() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel(10);
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

        let embedding = DbEmbedding {
            primary_key: [CqlValue::Int(1)].into(),
            embedding: Some(vec![1.].into()),
            timestamp: Timestamp::from_unix_timestamp(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![Operation::AddVector {
                    primary_id: 2.into(),
                    partition_id: 3.into(),
                    vector: vec![4.].into(),
                }])
            });
        tx_embeddings.send((embedding, None)).await.unwrap();
        let Some(Index::AddVector {
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
        assert!(in_progress.is_none());

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
    }

    #[tokio::test]
    async fn update_vector() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel(10);
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

        let embedding = DbEmbedding {
            primary_key: [CqlValue::Int(1)].into(),
            embedding: Some(vec![1.].into()),
            timestamp: Timestamp::from_unix_timestamp(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![
                    Operation::RemoveBeforeAddVector {
                        primary_id: 2.into(),
                        partition_id: 3.into(),
                    },
                    Operation::AddVector {
                        primary_id: 3.into(),
                        partition_id: 3.into(),
                        vector: vec![4.].into(),
                    },
                ])
            });
        tx_embeddings.send((embedding, None)).await.unwrap();

        let Some(Index::RemoveVector {
            partition_id,
            primary_id,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 2.into());
        assert_eq!(partition_id, 3.into());

        let Some(Index::AddVector {
            partition_id,
            primary_id,
            embedding,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 3.into());
        assert_eq!(partition_id, 3.into());
        assert_eq!(embedding, vec![4.].into());

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
    }

    #[tokio::test]
    async fn remove_vector() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel(10);
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

        let embedding = DbEmbedding {
            primary_key: [CqlValue::Int(1)].into(),
            embedding: None,
            timestamp: Timestamp::from_unix_timestamp(10),
        };
        table
            .write()
            .unwrap()
            .expect_add()
            .with(eq(index_key), eq(embedding.clone()))
            .once()
            .returning(|_, _| {
                Ok(vec![Operation::RemoveVector {
                    primary_id: 5.into(),
                    partition_id: 6.into(),
                }])
            });
        tx_embeddings.send((embedding, None)).await.unwrap();

        let Some(Index::RemoveVector {
            partition_id,
            primary_id,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_id, 5.into());
        assert_eq!(partition_id, 6.into());

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
    }

    #[tokio::test]
    async fn remove_partition() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel(10);
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

        let embedding = DbEmbedding {
            primary_key: [CqlValue::Int(1)].into(),
            embedding: None,
            timestamp: Timestamp::from_unix_timestamp(10),
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
        tx_embeddings.send((embedding, None)).await.unwrap();

        let Some(Index::RemovePartition { partition_id }) = rx_index.recv().await else {
            unreachable!();
        };
        assert_eq!(partition_id, 6.into());

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
    }
}
