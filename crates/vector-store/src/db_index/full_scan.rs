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
use tokio::task::JoinSet;
use tracing::error;

pub(crate) type RangeRows = BoxStream<'static, DbIndexedRow>;

/// Streams the rows of every range into `tx`, keeping at most `concurrency` range streams open
/// at once. Each row is tagged with an `AsyncInProgress::Fullscan` guard; a range contributes
/// `range_length(&range)` to `completed_scan_length` once the pipeline has dropped all of its
/// guards. The future resolves only after every range has been acknowledged this way. Dropping
/// the future aborts the in-flight range tasks.
///
/// A range whose `open` fails is skipped: `open` is expected to have logged the failure, and the
/// length of the range is not accounted, so the progress stays below `Progress::Done`.
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
    let semaphore = Arc::new(Semaphore::new(concurrency.get()));
    let mut in_flight = JoinSet::new();

    for range in ranges {
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("full scan semaphore is never closed");
        let length = range_length(&range);
        let Ok(rows) = open(range).await else {
            continue;
        };
        let tx = tx.clone();
        let completed_scan_length = Arc::clone(&completed_scan_length);
        in_flight.spawn(async move {
            let (tx_in_progress, mut rx_in_progress) = mpsc::channel(1);
            rows.for_each(move |row| {
                let tx = tx.clone();
                let tx_in_progress = tx_in_progress.clone();
                async move {
                    _ = tx
                        .send((row, AsyncInProgress::Fullscan(tx_in_progress)))
                        .await;
                }
            })
            .await;

            // wait until all in-progress markers are dropped
            while rx_in_progress.recv().await.is_some() {}

            completed_scan_length.fetch_add(length, Ordering::Relaxed);
            drop(permit);
        });
    }

    while let Some(result) = in_flight.join_next().await {
        if let Err(err) = result {
            error!("full scan range task failed: {err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbIndexedOperation;
    use crate::PrimaryKey;
    use crate::Timestamp;
    use anyhow::anyhow;
    use futures::future;
    use futures::stream;
    use rstest::rstest;
    use scylla::value::CqlValue;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::sync::watch;
    use tokio::task;

    type Rx = mpsc::Receiver<(DbIndexedRow, AsyncInProgress)>;

    fn row(i: usize) -> DbIndexedRow {
        DbIndexedRow {
            primary_key: PrimaryKey::from(vec![CqlValue::Int(i as i32)]),
            operation: DbIndexedOperation::Delete(Timestamp::from_millis(0)),
        }
    }

    fn single_row_range(i: usize) -> anyhow::Result<RangeRows> {
        Ok(stream::iter([row(i)]).boxed())
    }

    fn concurrency(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    fn spawn_consumer_acking_immediately(mut rx: Rx) -> task::JoinHandle<usize> {
        tokio::spawn(async move {
            let mut received = 0;
            while let Some((_row, guard)) = rx.recv().await {
                drop(guard);
                received += 1;
            }
            received
        })
    }

    async fn wait_until(condition: impl Fn() -> bool) {
        while !condition() {
            task::yield_now().await;
        }
    }

    #[rstest]
    #[timeout(Duration::from_secs(5))]
    #[tokio::test]
    async fn progress_reaches_sum_of_range_lengths_once_rows_are_acknowledged() {
        const RANGES: usize = 5;
        let (tx, rx) = mpsc::channel(1);
        let completed = Arc::new(AtomicU64::new(0));
        let consumer = spawn_consumer_acking_immediately(rx);

        scan_ranges(
            1..=RANGES,
            concurrency(2),
            |i| future::ready(single_row_range(i)),
            |i| *i as u64 * 10,
            tx,
            Arc::clone(&completed),
        )
        .await;

        assert_eq!(consumer.await.unwrap(), RANGES);
        assert_eq!(completed.load(Ordering::Relaxed), 10 + 20 + 30 + 40 + 50);
    }

    #[rstest]
    #[timeout(Duration::from_secs(5))]
    #[tokio::test]
    async fn range_failing_to_open_is_skipped_without_progress() {
        let (tx, rx) = mpsc::channel(1);
        let completed = Arc::new(AtomicU64::new(0));
        let consumer = spawn_consumer_acking_immediately(rx);

        scan_ranges(
            0..3,
            concurrency(1),
            |i| {
                future::ready(if i == 1 {
                    Err(anyhow!("range unavailable"))
                } else {
                    single_row_range(i)
                })
            },
            |_| 1,
            tx,
            Arc::clone(&completed),
        )
        .await;

        assert_eq!(consumer.await.unwrap(), 2);
        assert_eq!(completed.load(Ordering::Relaxed), 2);
    }

    #[rstest]
    #[timeout(Duration::from_secs(5))]
    #[tokio::test]
    async fn never_opens_more_range_streams_than_concurrency() {
        const RANGES: usize = 6;
        const CONCURRENCY: usize = 2;
        let (tx, rx) = mpsc::channel(1);
        let completed = Arc::new(AtomicU64::new(0));
        let (gate_tx, gate_rx) = watch::channel(false);
        let opened_total = Arc::new(AtomicUsize::new(0));
        let open_now = Arc::new(AtomicUsize::new(0));
        let max_open = Arc::new(AtomicUsize::new(0));
        let consumer = spawn_consumer_acking_immediately(rx);

        let scan = {
            let opened_total = Arc::clone(&opened_total);
            let open_now = Arc::clone(&open_now);
            let max_open = Arc::clone(&max_open);
            let completed = Arc::clone(&completed);
            tokio::spawn(async move {
                scan_ranges(
                    0..RANGES,
                    concurrency(CONCURRENCY),
                    |i| {
                        opened_total.fetch_add(1, Ordering::Relaxed);
                        let now_open = open_now.fetch_add(1, Ordering::Relaxed) + 1;
                        max_open.fetch_max(now_open, Ordering::Relaxed);
                        let mut gate_rx = gate_rx.clone();
                        let open_now = Arc::clone(&open_now);
                        future::ready(Ok(stream::iter([row(i)])
                            .chain(
                                stream::once(async move {
                                    _ = gate_rx.wait_for(|open| *open).await;
                                    open_now.fetch_sub(1, Ordering::Relaxed);
                                })
                                .filter_map(|()| async { None }),
                            )
                            .boxed()))
                    },
                    |_| 1,
                    tx,
                    completed,
                )
                .await
            })
        };

        wait_until(|| opened_total.load(Ordering::Relaxed) == CONCURRENCY).await;
        for _ in 0..10 {
            task::yield_now().await;
        }
        assert_eq!(opened_total.load(Ordering::Relaxed), CONCURRENCY);

        gate_tx.send(true).unwrap();
        scan.await.unwrap();

        assert_eq!(consumer.await.unwrap(), RANGES);
        assert_eq!(opened_total.load(Ordering::Relaxed), RANGES);
        assert_eq!(max_open.load(Ordering::Relaxed), CONCURRENCY);
        assert_eq!(completed.load(Ordering::Relaxed), RANGES as u64);
    }
}
