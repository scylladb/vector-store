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
                        index.add(embeddings.primary_key, embeddings.embeddings).await;
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
