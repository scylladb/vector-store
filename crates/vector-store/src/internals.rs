/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;

pub type CountersR = BTreeMap<String, u64>;

pub enum Internals {
    StartCounter {
        name: String,
        tx: oneshot::Sender<()>,
    },
    IncrementCounter {
        name: String,
    },
    ClearCounters {
        tx: oneshot::Sender<()>,
    },
    Counters {
        tx: oneshot::Sender<CountersR>,
    },
}

pub trait InternalsExt {
    async fn start_counter(&self, name: String);
    async fn increment_counter(&self, name: String);
    async fn clear_counters(&self);
    async fn counters(&self) -> CountersR;
}

impl InternalsExt for mpsc::Sender<Internals> {
    async fn start_counter(&self, name: String) {
        let (tx, rx) = oneshot::channel();
        self.send(Internals::StartCounter { name, tx })
            .await
            .expect("Internals::start_counter: internal actor should receive request");
        rx.await
            .expect("Internals::start_counter: internal actor should send response")
    }

    async fn increment_counter(&self, name: String) {
        self.send(Internals::IncrementCounter { name })
            .await
            .expect("Internals::increment_counter: internal actor should receive request");
    }

    async fn clear_counters(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(Internals::ClearCounters { tx })
            .await
            .expect("Internals::clear_counter: internal actor should receive request");
        rx.await
            .expect("Internals::clear_counter: internal actor should send response")
    }

    async fn counters(&self) -> CountersR {
        let (tx, rx) = oneshot::channel();
        self.send(Internals::Counters { tx })
            .await
            .expect("Internals::counters: internal actor should receive request");
        rx.await
            .expect("Internals::counters: internal actor should send response")
    }
}

pub(crate) fn new() -> mpsc::Sender<Internals> {
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        async move {
            debug!("starting");

            let state = Arc::new(RwLock::new(State::new()));

            while let Some(msg) = rx.recv().await {
                tokio::spawn(process(msg, state.clone()));
            }

            debug!("finished");
        }
        .instrument(debug_span!("internals")),
    );

    tx
}

struct State {
    counters: BTreeMap<String, AtomicU64>,
}

impl State {
    fn new() -> Self {
        Self {
            counters: BTreeMap::new(),
        }
    }
}

async fn process(msg: Internals, state: Arc<RwLock<State>>) {
    match msg {
        Internals::StartCounter { name, tx } => {
            state
                .write()
                .unwrap()
                .counters
                .entry(name)
                .or_insert(AtomicU64::new(0));
            _ = tx.send(());
        }
        Internals::IncrementCounter { name } => {
            if let Some(counter) = state.read().unwrap().counters.get(&name) {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
        Internals::ClearCounters { tx } => {
            state.write().unwrap().counters.clear();
            _ = tx.send(());
        }
        Internals::Counters { tx } => {
            _ = tx.send(
                state
                    .read()
                    .unwrap()
                    .counters
                    .iter()
                    .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                    .collect(),
            )
        }
    }
}
