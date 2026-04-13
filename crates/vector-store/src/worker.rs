/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::perf;
use async_channel::Sender;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use tokio::sync::mpsc;
use tracing::Instrument;
use tracing::debug;
use tracing::error_span;

pub(crate) enum Worker {
    SpawnShortTask { f: Box<dyn FnOnce() + Send> },
    SpawnLongTask { f: Box<dyn FnOnce() + Send> },
}

pub(crate) trait WorkerExt {
    async fn spawn_short_task(&self, f: impl FnOnce() + Send + 'static);
    async fn spawn_long_task(&self, f: impl FnOnce() + Send + 'static);
}

impl WorkerExt for Sender<Worker> {
    #[hotpath::measure]
    async fn spawn_short_task(&self, f: impl FnOnce() + Send + 'static) {
        self.send(Worker::SpawnShortTask { f: Box::new(f) })
            .await
            .expect("WorkerExt::spawn_short_task: internal actor should receive request");
    }

    #[hotpath::measure]
    async fn spawn_long_task(&self, f: impl FnOnce() + Send + 'static) {
        self.send(Worker::SpawnLongTask { f: Box::new(f) })
            .await
            .expect("WorkerExt::spawn_long_task: internal actor should receive request");
    }
}

pub(crate) fn new() -> Sender<Worker> {
    let (tx_worker, rx_worker) = async_channel::bounded(perf::channel_size());
    let (tx_thread, mut rx_thread) = mpsc::channel::<Box<dyn FnOnce() + Send>>(1);

    // Dedicated thread for long tasks to avoid starving runtime.
    thread::spawn(move || {
        while let Some(f) = rx_thread.blocking_recv() {
            f();
        }
    });

    let operations_in_flow = Arc::new(AtomicUsize::new(0));
    let workers = perf::num_workers();
    (0..workers).for_each(|id| {
        let rx_worker = rx_worker.clone();
        let tx_thread = tx_thread.clone();
        let operations_in_flow = Arc::clone(&operations_in_flow);
        tokio::spawn(perf::hotpath_async(
            async move {
                debug!("starting");

                while let Ok(msg) = rx_worker.recv().await {
                    let in_flow = operations_in_flow.fetch_add(1, Ordering::Relaxed) + 1;
                    match msg {
                        Worker::SpawnShortTask { f } => {
                            f();
                        }
                        Worker::SpawnLongTask { f } => {
                            if in_flow == workers {
                                // If all workers are busy, we need to execute the long task in a
                                // separate thread to avoid starving runtime.
                                tx_thread
                                    .send(f)
                                    .await
                                    .expect("Worker: internal thread should receive task");
                            } else {
                                f();
                            }
                        }
                    }
                    operations_in_flow.fetch_sub(1, Ordering::Relaxed);
                }

                debug!("finished");
            }
            .instrument(error_span!("worker", id)),
        ));
    });
    tx_worker
}
