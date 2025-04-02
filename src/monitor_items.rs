/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_index::DbIndex;
use crate::db_index::DbIndexExt;
use crate::index::Index;
use crate::index::IndexExt;
use futures::TryStreamExt;
use std::mem;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::info;
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

    tokio::spawn(async move {
        info!("resetting items");
        let mut state = State::Reset;

        const INTERVAL: Duration = Duration::from_secs(1);
        let mut interval = time::interval(INTERVAL);

        while !rx.is_closed() {
            tokio::select! {
                _ = interval.tick() => {
                    match state {
                        State::Reset => {
                            if !reset_items(&db_index)
                                .await
                                .unwrap_or_else(|err| {
                                    warn!("monitor_items: unable to reset items in table: {err}");
                                    false
                                })
                            {
                                info!("copying items");
                                state = State::Copy;
                            } else {
                                interval.reset_immediately();
                            }
                        }
                        State::Copy => {
                            if table_to_index(&db_index, &index)
                                .await
                                .unwrap_or_else(|err| {
                                    warn!("monitor_items: unable to copy data from table to index: {err}");
                                    false
                                })
                            {
                                interval.reset_immediately();
                            }
                        }
                    }
                }

                _ = rx.recv() => { }
            }
        }
    }.instrument(info_span!("monitor items")));
    Ok(tx)
}

enum State {
    Reset,
    Copy,
}

async fn reset_items(db_index: &Sender<DbIndex>) -> anyhow::Result<bool> {
    // The value was taken from initial benchmarks
    const CHUNK_SIZE: usize = 100;
    let mut keys_chunks = db_index.get_processed_ids().await?.try_chunks(CHUNK_SIZE);

    let mut count = 0;
    while let Some(keys) = keys_chunks.try_next().await? {
        count += keys.len();
        db_index.reset_items(keys).await?;
    }
    debug!("processed new items: {count}");
    Ok(count > 0)
}

/// Get new embeddings from db and add to the index. Then mark embeddings in db as processed
async fn table_to_index(db_index: &Sender<DbIndex>, index: &Sender<Index>) -> anyhow::Result<bool> {
    let mut rows = db_index.get_items().await?;

    // The value was taken from initial benchmarks
    const PROCESSED_CHUNK_SIZE: usize = 100;

    // The container for processed keys
    let mut processed = Vec::new();
    // The accumulator for number of processed embeddings
    let mut count = 0;

    while let Some((key, embeddings)) = rows.try_next().await? {
        processed.push(key);
        count += 1;

        if processed.len() == PROCESSED_CHUNK_SIZE {
            // A new chunk of processed keys is prepared. Mark them as processed in db in the
            // background.
            let processed: Vec<_> = mem::take(&mut processed);
            db_index
                .update_items(processed)
                .await
                .unwrap_or_else(|err| {
                    warn!("monitor_items::table_to_index: unable to update items: {err}")
                });
        }

        // Add the embeddings in the background
        tokio::spawn({
            let index = index.clone();
            async move {
                index.add(key, embeddings).await;
            }
        });
    }

    if !processed.is_empty() {
        db_index.update_items(processed).await?;
    }

    if count > 0 {
        debug!("table_to_index: processed {count} items",);
    }

    Ok(count > 0)
}
