/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::DbEmbeddings;
use crate::IndexId;
use crate::index::Index;
use crate::index::IndexExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub(crate) enum MonitorItems {}

pub(crate) async fn new(
    id: IndexId,
    mut embeddings: Receiver<DbEmbeddings>,
    index: Sender<Index>,
) -> anyhow::Result<Sender<MonitorItems>> {
    // The value was taken from initial benchmarks
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        async move {
            debug!("starting");

            while !rx.is_closed() {
                tokio::select! {
                    embeddings = embeddings.recv() => {
                        let Some(embeddings) = embeddings else {
                            break;
                        };
                        index.add_or_replace(embeddings.primary_key, embeddings.embeddings).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::value::CqlValue;

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
            .send(DbEmbeddings {
                primary_key: vec![CqlValue::Int(1)].into(),
                embeddings: vec![1.].into(),
            })
            .await
            .unwrap();
        tx_embeddings
            .send(DbEmbeddings {
                primary_key: vec![CqlValue::Int(2)].into(),
                embeddings: vec![2.].into(),
            })
            .await
            .unwrap();

        let Some(Index::AddOrReplace {
            primary_key,
            embeddings,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(1)].into());
        assert_eq!(embeddings, vec![1.].into());

        let Some(Index::AddOrReplace {
            primary_key,
            embeddings,
        }) = rx_index.recv().await
        else {
            unreachable!();
        };
        assert_eq!(primary_key, vec![CqlValue::Int(2)].into());
        assert_eq!(embeddings, vec![2.].into());

        drop(tx_embeddings);
        assert!(rx_index.recv().await.is_none());
    }
}
