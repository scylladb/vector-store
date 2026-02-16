/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::DbEmbedding;
use crate::IndexId;
use crate::Metrics;
use crate::PrimaryKey;
use crate::Timestamp;
use crate::index::Index;
use crate::index::IndexExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub(crate) enum MonitorItems {}

pub(crate) async fn new(
    id: IndexId,
    mut embeddings: Receiver<(DbEmbedding, Option<AsyncInProgress>)>,
    index: Sender<Index>,
    metrics: Arc<Metrics>,
) -> anyhow::Result<Sender<MonitorItems>> {
    // The value was taken from initial benchmarks
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);
    let id_for_span = id.clone();

    tokio::spawn(
        async move {
            debug!("starting");

            let mut timestamps: HashMap<PrimaryKey, Timestamp> = HashMap::new();

            while !rx.is_closed() {
                tokio::select! {
                    embedding = embeddings.recv() => {
                        let Some((embedding, in_progress)) = embedding else {
                            break;
                        };
                        add(&mut timestamps, &index, embedding, in_progress, &metrics, &id).await;
                    }
                    _ = rx.recv() => { }
                }
            }

            debug!("finished");
        }
        .instrument(debug_span!("monitor items", "{id_for_span}")),
    );
    Ok(tx)
}

async fn add(
    timestamps: &mut HashMap<PrimaryKey, Timestamp>,
    index: &Sender<Index>,
    embedding: DbEmbedding,
    in_progress: Option<AsyncInProgress>,
    metrics: &Metrics,
    id: &IndexId,
) {
    let mut modify = true;
    let mut remove_before_add = false;
    timestamps
        .entry(embedding.primary_key.clone())
        .and_modify(|timestamp| {
            if timestamp.0 < embedding.timestamp.0 {
                *timestamp = embedding.timestamp;
                remove_before_add = true;
            } else {
                modify = false;
            }
        })
        .or_insert(embedding.timestamp);
    if modify {
        let primary_key = embedding.primary_key;
        if let Some(embedding) = embedding.embedding {
            metrics
                .modified
                .with_label_values(&[id.keyspace().as_ref(), id.index().as_ref(), "update"])
                .inc();
            if remove_before_add {
                index.remove(primary_key.clone(), None).await;
            }
            index.add(primary_key, embedding, in_progress).await;
        } else {
            metrics
                .modified
                .with_label_values(&[id.keyspace().as_ref(), id.index().as_ref(), "remove"])
                .inc();
            index.remove(primary_key, in_progress).await;
        }
        metrics.mark_dirty(id.keyspace().as_ref(), id.index().as_ref());
    }
}

#[cfg(test)]
mod tests {
    use crate::metrics::Metrics;

    use super::*;
    use scylla::value::CqlValue;
    use time::OffsetDateTime;

    #[tokio::test]
    async fn flow() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel(10);
        let metrics: Arc<Metrics> = Arc::new(Metrics::new());
        let _actor = new(
            IndexId::new(&"vector".to_string().into(), &"store".to_string().into()),
            rx_embeddings,
            tx_index,
            metrics,
        )
        .await
        .unwrap();

        tx_embeddings
            .send((
                DbEmbedding {
                    primary_key: vec![CqlValue::Int(1)].into(),
                    embedding: Some(vec![1.].into()),
                    timestamp: OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
                },
                None,
            ))
            .await
            .unwrap();
        tx_embeddings
            .send((
                DbEmbedding {
                    primary_key: vec![CqlValue::Int(2)].into(),
                    embedding: Some(vec![2.].into()),
                    timestamp: OffsetDateTime::from_unix_timestamp(11).unwrap().into(),
                },
                None,
            ))
            .await
            .unwrap();
        tx_embeddings
            .send((
                DbEmbedding {
                    // should be dropped
                    primary_key: vec![CqlValue::Int(1)].into(),
                    embedding: Some(vec![3.].into()),
                    timestamp: OffsetDateTime::from_unix_timestamp(5).unwrap().into(),
                },
                None,
            ))
            .await
            .unwrap();
        tx_embeddings
            .send((
                DbEmbedding {
                    // should be accepted
                    primary_key: vec![CqlValue::Int(2)].into(),
                    embedding: Some(vec![4.].into()),
                    timestamp: OffsetDateTime::from_unix_timestamp(15).unwrap().into(),
                },
                None,
            ))
            .await
            .unwrap();
        tx_embeddings
            .send((
                DbEmbedding {
                    primary_key: vec![CqlValue::Int(1)].into(),
                    embedding: None,
                    timestamp: OffsetDateTime::from_unix_timestamp(25).unwrap().into(),
                },
                None,
            ))
            .await
            .unwrap();
        tx_embeddings
            .send((
                DbEmbedding {
                    // should be dropped
                    primary_key: vec![CqlValue::Int(1)].into(),
                    embedding: Some(vec![5.].into()),
                    timestamp: OffsetDateTime::from_unix_timestamp(24).unwrap().into(),
                },
                None,
            ))
            .await
            .unwrap();
        tx_embeddings
            .send((
                DbEmbedding {
                    primary_key: vec![CqlValue::Int(1)].into(),
                    embedding: Some(vec![6.].into()),
                    timestamp: OffsetDateTime::from_unix_timestamp(26).unwrap().into(),
                },
                None,
            ))
            .await
            .unwrap();

        let Some(Index::Add {
            primary_key,
            embedding,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(1)].into());
        assert_eq!(embedding, vec![1.].into());

        let Some(Index::Add {
            primary_key,
            embedding,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(2)].into());
        assert_eq!(embedding, vec![2.].into());

        // The entry is already present, so it's removed first.
        let Some(Index::Remove {
            primary_key,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(2)].into());
        let Some(Index::Add {
            primary_key,
            embedding,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(2)].into());
        assert_eq!(embedding, vec![4.].into());

        let Some(Index::Remove {
            primary_key,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(1)].into());

        // The entry is already present, so it's removed first.
        let Some(Index::Remove {
            primary_key,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(1)].into());
        let Some(Index::Add {
            primary_key,
            embedding,
            in_progress: None,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(1)].into());
        assert_eq!(embedding, vec![6.].into());

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
    }
}
