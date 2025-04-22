/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexId;
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
use tracing::debug_span;
use tracing::warn;

pub(crate) enum MonitorItems {}

pub(crate) async fn new(
    db_index: Sender<DbIndex>,
    id: IndexId,
    index: Sender<Index>,
) -> anyhow::Result<Sender<MonitorItems>> {
    // The value was taken from initial benchmarks
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        {
            let id = id.clone();
            async move {
                debug!("starting");

                initial_read(&mut rx, &db_index, &index)
                    .await
                    .unwrap_or_else(|err| {
                        warn!("unable to do initial read from db for {id}: {err}");
                    });

                while rx.recv().await.is_some() {}

                debug!("finished");
            }
        }
        .instrument(debug_span!("monitor items", "{id}")),
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

    while !rx.is_closed() {
        tokio::select! {
            row = rows.try_next() => {
                let Some((primary_key, embeddings)) = row? else {
                    break;
                };
                index.add(primary_key, embeddings).await;
            }

            _ = rx.recv() => { }
        }
    }

    Ok(())
}
