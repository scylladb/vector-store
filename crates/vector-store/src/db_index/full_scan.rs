/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::AsyncInProgress;
use crate::DbIndexedRow;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

pub(crate) type RangeRows = BoxStream<'static, DbIndexedRow>;

/// Streams the rows of every range into `tx`, limiting the number of concurrently scanned ranges
/// to `concurrency`. Each row is tagged with an `AsyncInProgress::Fullscan` guard; a range
/// contributes `range_length(&range)` to `completed_scan_length` once the pipeline has dropped all
/// of its guards. The future resolves only after every range has been acknowledged this way.
pub(crate) async fn scan_ranges<R, Fut>(
    ranges: impl IntoIterator<Item = R>,
    concurrency: NonZeroUsize,
    open: impl Fn(R) -> Fut,
    range_length: impl Fn(&R) -> u64,
    tx: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
    completed_scan_length: Arc<AtomicU64>,
) where
    Fut: Future<Output = anyhow::Result<RangeRows>>,
{
    let semaphore_capacity = concurrency.get();
    let semaphore = Arc::new(Semaphore::new(semaphore_capacity));

    for range in ranges {
        let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();

        let length = range_length(&range);
        let range_scan = open(range).await;
        if let Ok(embeddings) = range_scan {
            let tx = tx.clone();
            let scan_length = completed_scan_length.clone();
            tokio::spawn(async move {
                let (tx_in_progress, mut rx_in_progress) = mpsc::channel(1);
                embeddings
                    .for_each(move |embedding| {
                        let tx = tx.clone();
                        let tx_in_progress = tx_in_progress.clone();
                        async move {
                            _ = tx
                                .send((embedding, AsyncInProgress::Fullscan(tx_in_progress)))
                                .await;
                        }
                    })
                    .await;

                // wait until all in-progress markers are dropped
                while rx_in_progress.recv().await.is_some() {
                    rx_in_progress.len();
                }

                scan_length.fetch_add(length, Ordering::Relaxed);
                drop(permit);
            });
        } else {
            drop(permit);
        }
    }

    // Acquire all permits to wait until all spawned tasks have finished and released their permits.
    let _permits = semaphore
        .acquire_many(semaphore_capacity as u32)
        .await
        .unwrap();
}
