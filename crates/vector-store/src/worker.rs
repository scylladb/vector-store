/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::perf;
use async_channel::Sender;
use futures::future::BoxFuture;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::error_span;

/// A lambda creating a future, instead of the future itself. The future has to be
/// created by the runtime which polls it.
type NewTaskFuture = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

fn new_task_future<Fut>(f: impl FnOnce() -> Fut + Send + 'static) -> NewTaskFuture
where
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move || -> BoxFuture<'static, ()> { Box::pin(f()) })
}

#[allow(clippy::enum_variant_names)]
pub(crate) enum Worker {
    SpawnNonBlocking { f: Box<dyn FnOnce() + Send> },
    SpawnBlocking { f: Box<dyn FnOnce() + Send> },
    SpawnAsyncNonBlocking { f: NewTaskFuture },
    SpawnAsyncBlocking { f: NewTaskFuture },
}

pub(crate) trait WorkerExt {
    async fn spawn_non_blocking(&self, f: impl FnOnce() + Send + 'static);
    async fn spawn_blocking(&self, f: impl FnOnce() + Send + 'static);
    async fn spawn_async_non_blocking<Fut>(&self, f: impl FnOnce() -> Fut + Send + 'static)
    where
        Fut: Future<Output = ()> + Send + 'static;
    async fn spawn_async_blocking<Fut>(&self, f: impl FnOnce() -> Fut + Send + 'static)
    where
        Fut: Future<Output = ()> + Send + 'static;
}

impl WorkerExt for Sender<Worker> {
    #[hotpath::measure]
    async fn spawn_non_blocking(&self, f: impl FnOnce() + Send + 'static) {
        self.send(Worker::SpawnNonBlocking { f: Box::new(f) })
            .await
            .expect("WorkerExt::spawn_non_blocking: internal actor should receive request");
    }

    #[hotpath::measure]
    async fn spawn_blocking(&self, f: impl FnOnce() + Send + 'static) {
        self.send(Worker::SpawnBlocking { f: Box::new(f) })
            .await
            .expect("WorkerExt::spawn_blocking: internal actor should receive request");
    }

    #[hotpath::measure]
    async fn spawn_async_non_blocking<Fut>(&self, f: impl FnOnce() -> Fut + Send + 'static)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.send(Worker::SpawnAsyncNonBlocking {
            f: new_task_future(f),
        })
        .await
        .expect("WorkerExt::spawn_async_non_blocking: internal actor should receive request");
    }

    #[hotpath::measure]
    async fn spawn_async_blocking<Fut>(&self, f: impl FnOnce() -> Fut + Send + 'static)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.send(Worker::SpawnAsyncBlocking {
            f: new_task_future(f),
        })
        .await
        .expect("WorkerExt::spawn_async_blocking: internal actor should receive request");
    }
}

fn try_acquire_thread(
    id: usize,
    in_flow: usize,
    workers: usize,
    thread_operations_in_flow: &AtomicUsize,
) -> bool {
    // If all workers are busy, we need to execute the blocking task in a
    // separate thread to avoid starving runtime.
    if in_flow != workers {
        return false;
    }
    let thread_in_flow = thread_operations_in_flow.fetch_add(1, Ordering::Relaxed) + 1;
    hotpath::val!("worker::thread_in_flow").set(&(id, in_flow, thread_in_flow));
    if thread_in_flow == 1 {
        true
    } else {
        thread_operations_in_flow.fetch_sub(1, Ordering::Relaxed);
        false
    }
}

async fn run_in_thread(
    tx_thread: &mpsc::Sender<(NewTaskFuture, oneshot::Sender<()>)>,
    thread_operations_in_flow: &AtomicUsize,
    f: NewTaskFuture,
) {
    let (tx_call, rx_call) = oneshot::channel();
    tx_thread
        .send((f, tx_call))
        .await
        .expect("Worker: internal thread should receive task");
    _ = rx_call.await;
    thread_operations_in_flow.fetch_sub(1, Ordering::Relaxed);
}

pub(crate) fn new() -> Sender<Worker> {
    let (tx_worker, rx_worker) = async_channel::bounded(perf::channel_size().into());
    let (tx_thread, mut rx_thread) = mpsc::channel::<(NewTaskFuture, oneshot::Sender<()>)>(1);

    // Dedicated thread for long tasks to avoid starving runtime.
    thread::spawn(move || {
        let runtime = Builder::new_current_thread().build().unwrap();

        runtime.block_on(async move {
            while let Some((f, tx_call)) = rx_thread.recv().await {
                f().await;
                _ = tx_call.send(());
            }
        });
    });

    let operations_in_flow = Arc::new(AtomicUsize::new(0));
    let thread_operations_in_flow = Arc::new(AtomicUsize::new(0));
    let workers = perf::num_workers().into();
    (0..workers).for_each(|id| {
        let rx_worker = rx_worker.clone();
        let tx_thread = tx_thread.clone();
        let operations_in_flow = Arc::clone(&operations_in_flow);
        let thread_operations_in_flow = Arc::clone(&thread_operations_in_flow);
        tokio::spawn(perf::hotpath_async(
            async move {
                debug!("starting");

                while let Ok(msg) = rx_worker.recv().await {
                    let in_flow = operations_in_flow.fetch_add(1, Ordering::Relaxed) + 1;
                    hotpath::val!("worker::in_flow").set(&(id, in_flow));
                    match msg {
                        Worker::SpawnNonBlocking { f } => {
                            f();
                        }
                        Worker::SpawnAsyncNonBlocking { f } => {
                            f().await;
                        }
                        Worker::SpawnBlocking { f } => {
                            if try_acquire_thread(id, in_flow, workers, &thread_operations_in_flow)
                            {
                                run_in_thread(
                                    &tx_thread,
                                    &thread_operations_in_flow,
                                    new_task_future(move || async move { f() }),
                                )
                                .await;
                            } else {
                                f();
                            }
                        }
                        Worker::SpawnAsyncBlocking { f } => {
                            if try_acquire_thread(id, in_flow, workers, &thread_operations_in_flow)
                            {
                                run_in_thread(&tx_thread, &thread_operations_in_flow, f).await;
                            } else {
                                f().await;
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
