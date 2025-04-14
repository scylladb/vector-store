/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_index::DbIndex;
use crate::db_index::DbIndexExt;
use crate::index::Index;
use crate::index::IndexExt;
use futures::TryStreamExt;
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
    let mut keys = db_index.get_processed_ids().await?;

    let mut count = 0;
    while let Some(key) = keys.try_next().await? {
        count += 1;
        db_index.reset_item(key).await?;
    }
    debug!("processed new items: {count}");
    Ok(count > 0)
}

/// Get new embeddings from db and add to the index. Then mark embeddings in db as processed
async fn table_to_index(db_index: &Sender<DbIndex>, index: &Sender<Index>) -> anyhow::Result<bool> {
    let mut rows = db_index.get_items().await?;

    // The accumulator for number of processed embeddings
    let mut count = 0;

    while let Some((key, embeddings)) = rows.try_next().await? {
        count += 1;
        db_index.update_item(key).await?;
        index.add(key, embeddings).await;
    }

    if count > 0 {
        debug!("table_to_index: processed {count} items",);
    }

    Ok(count > 0)
}
