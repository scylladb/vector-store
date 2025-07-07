/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::DbEmbedding;
use crate::IndexId;
use crate::PrimaryKey;
use crate::Timestamp;
use crate::index::Index;
use crate::index::IndexExt;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub(crate) enum MonitorItems {}

pub(crate) async fn new(
    id: IndexId,
    mut embeddings: Receiver<DbEmbedding>,
    index: Sender<Index>,
) -> anyhow::Result<Sender<MonitorItems>> {
    // The value was taken from initial benchmarks
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        async move {
            debug!("starting");

            let mut timestamps: HashMap<PrimaryKey, Timestamp> = HashMap::new();

            while !rx.is_closed() {
                tokio::select! {
                    embedding = embeddings.recv() => {
                        let Some(embedding) = embedding else {
                            break;
                        };
                        add(&mut timestamps, &index, embedding).await;
                    }
                    _ = rx.recv() => { }
                }
            }

            debug!("finished");
        }
        .instrument(debug_span!("monitor items", "{id}")),
    );
    Ok(tx)
}

async fn add(
    timestamps: &mut HashMap<PrimaryKey, Timestamp>,
    index: &Sender<Index>,
    embedding: DbEmbedding,
) {
    let mut modify = true;
    timestamps
        .entry(embedding.primary_key.clone())
        .and_modify(|timestamp| {
            if timestamp.0 < embedding.timestamp.0 {
                *timestamp = embedding.timestamp;
            } else {
                modify = false;
            }
        })
        .or_insert(embedding.timestamp);
    if modify {
        let primary_key = embedding.primary_key;
        if let Some(embedding) = embedding.embedding {
            index.add_or_replace(primary_key, embedding).await;
        } else {
            index.remove(primary_key).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::value::CqlValue;
    use time::OffsetDateTime;

    #[tokio::test]
    async fn flow() {
        let (tx_embeddings, rx_embeddings) = mpsc::channel(10);
        let (tx_index, mut rx_index) = mpsc::channel(10);
        let _actor = new(
            IndexId::new(&"vector".to_string().into(), &"store".to_string().into()),
            rx_embeddings,
            tx_index,
        )
        .await
        .unwrap();

        tx_embeddings
            .send(DbEmbedding {
                primary_key: vec![CqlValue::Int(1)].into(),
                embedding: Some(vec![1.].into()),
                timestamp: OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            })
            .await
            .unwrap();
        tx_embeddings
            .send(DbEmbedding {
                primary_key: vec![CqlValue::Int(2)].into(),
                embedding: Some(vec![2.].into()),
                timestamp: OffsetDateTime::from_unix_timestamp(11).unwrap().into(),
            })
            .await
            .unwrap();
        tx_embeddings
            .send(DbEmbedding {
                // should be dropped
                primary_key: vec![CqlValue::Int(1)].into(),
                embedding: Some(vec![3.].into()),
                timestamp: OffsetDateTime::from_unix_timestamp(5).unwrap().into(),
            })
            .await
            .unwrap();
        tx_embeddings
            .send(DbEmbedding {
                // should be accepted
                primary_key: vec![CqlValue::Int(2)].into(),
                embedding: Some(vec![4.].into()),
                timestamp: OffsetDateTime::from_unix_timestamp(15).unwrap().into(),
            })
            .await
            .unwrap();
        tx_embeddings
            .send(DbEmbedding {
                primary_key: vec![CqlValue::Int(1)].into(),
                embedding: None,
                timestamp: OffsetDateTime::from_unix_timestamp(25).unwrap().into(),
            })
            .await
            .unwrap();
        tx_embeddings
            .send(DbEmbedding {
                // should be dropped
                primary_key: vec![CqlValue::Int(1)].into(),
                embedding: Some(vec![5.].into()),
                timestamp: OffsetDateTime::from_unix_timestamp(24).unwrap().into(),
            })
            .await
            .unwrap();
        tx_embeddings
            .send(DbEmbedding {
                primary_key: vec![CqlValue::Int(1)].into(),
                embedding: Some(vec![6.].into()),
                timestamp: OffsetDateTime::from_unix_timestamp(26).unwrap().into(),
            })
            .await
            .unwrap();

        let Some(Index::AddOrReplace {
            primary_key,
            embedding,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(1)].into());
        assert_eq!(embedding, vec![1.].into());

        let Some(Index::AddOrReplace {
            primary_key,
            embedding,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(2)].into());
        assert_eq!(embedding, vec![2.].into());

        let Some(Index::AddOrReplace {
            primary_key,
            embedding,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(2)].into());
        assert_eq!(embedding, vec![4.].into());

        let Some(Index::Remove { primary_key }) = rx_index.recv().await else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(1)].into());

        let Some(Index::AddOrReplace {
            primary_key,
            embedding,
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
