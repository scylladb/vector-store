/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_index::DbIndex;
use crate::db_index::DbIndexExt;
use crate::index::Index;
use crate::index::IndexExt;
use futures::TryStreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::Instrument;
use tracing::debug;
use tracing::info_span;
use tracing::warn;

pub(crate) enum MonitorItems {}

pub(crate) async fn new(
    db_index: Sender<DbIndex>,
    index: Sender<Index>,
) -> anyhow::Result<Sender<MonitorItems>> {
    // The value was taken from initial benchmarks
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        async move {
            initial_read(&mut rx, &db_index, &index)
                .await
                .unwrap_or_else(|err| {
                    warn!("monitor_items: unable to do initial read: {err}");
                });

            while rx.recv().await.is_some() {}
        }
        .instrument(info_span!("monitor items")),
    );
    Ok(tx)
}

/// Get all current embeddings from the db table and add to the index
async fn initial_read(
    rx: &mut Receiver<MonitorItems>,
    db_index: &Sender<DbIndex>,
    index: &Sender<Index>,
) -> anyhow::Result<()> {
    let mut rows = db_index.get_items().await?;

    // The accumulator for number of processed embeddings
    let mut count = 0;

    while !rx.is_closed() {
        tokio::select! {
            row = rows.try_next() => {
                let Some((primary_key, embeddings)) = row? else {
                    break;
                };
                count += 1;
                index.add(primary_key, embeddings).await;
            }

            _ = rx.recv() => { }
        }
    }

    if count > 0 {
        debug!("initial_read: processed {count} items");
    }
    Ok(())
}
