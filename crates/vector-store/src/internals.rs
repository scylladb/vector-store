/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use scylla::client::session::Session;
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
    CreateSession {
        session: Option<Arc<Session>>,
    },
    SessionCounters {
        tx: oneshot::Sender<CountersR>,
    },
}

pub trait InternalsExt {
    async fn start_counter(&self, name: String);
    async fn increment_counter(&self, name: String);
    async fn clear_counters(&self);
    async fn counters(&self) -> CountersR;
    async fn create_session(&self, session: Option<Arc<Session>>);
    async fn session_counters(&self) -> CountersR;
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

    async fn create_session(&self, session: Option<Arc<Session>>) {
        self.send(Internals::CreateSession { session })
            .await
            .expect("Internals::create_session: internal actor should receive request");
    }

    async fn session_counters(&self) -> CountersR {
        let (tx, rx) = oneshot::channel();
        self.send(Internals::SessionCounters { tx })
            .await
            .expect("Internals::session_counters: internal actor should receive request");
        rx.await
            .expect("Internals::session_counters: internal actor should send response")
    }
}

type Counters = RwLock<BTreeMap<String, AtomicU64>>;

pub(crate) fn new() -> mpsc::Sender<Internals> {
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        async move {
            debug!("starting");

            let counters = Arc::new(RwLock::new(BTreeMap::new()));
            let mut session = None;

            while let Some(msg) = rx.recv().await {
                match processing(&msg) {
                    Processing::Session => {
                        process_session(msg, &mut session, counters.as_ref());
                    }
                    Processing::Counters => {
                        tokio::spawn(process_counters(msg, counters.clone()));
                    }
                }
            }

            debug!("finished");
        }
        .instrument(debug_span!("internals")),
    );

    tx
}

enum Processing {
    Counters,
    Session,
}

fn processing(msg: &Internals) -> Processing {
    match msg {
        Internals::StartCounter { .. }
        | Internals::IncrementCounter { .. }
        | Internals::ClearCounters { .. }
        | Internals::Counters { .. } => Processing::Counters,
        Internals::CreateSession { .. } | Internals::SessionCounters { .. } => Processing::Session,
    }
}

async fn process_counters(msg: Internals, counters: Arc<Counters>) {
    match msg {
        Internals::StartCounter { name, tx } => {
            counters
                .write()
                .unwrap()
                .entry(name)
                .or_insert(AtomicU64::new(0));
            _ = tx.send(());
        }
        Internals::IncrementCounter { name } => {
            increment_counter(counters.as_ref(), &name);
        }
        Internals::ClearCounters { tx } => {
            counters.write().unwrap().clear();
            _ = tx.send(());
        }
        Internals::Counters { tx } => {
            _ = tx.send(
                counters
                    .read()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
                    .collect(),
            )
        }
        _ => unreachable!(),
    }
}

fn increment_counter(counters: &Counters, name: &str) {
    if let Some(counter) = counters.read().unwrap().get(name) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

fn process_session(msg: Internals, session_saved: &mut Option<Arc<Session>>, counters: &Counters) {
    match msg {
        Internals::CreateSession { session } => {
            if session.is_some() {
                increment_counter(counters, "session-create-success");
            } else {
                increment_counter(counters, "session-create-failure");
            }
            *session_saved = session;
        }
        Internals::SessionCounters { tx } => {
            _ = tx.send(if let Some(session) = session_saved {
                [
                    (
                        "total-connections".to_string(),
                        session.get_metrics().get_total_connections(),
                    ),
                    (
                        "connection-timeouts".to_string(),
                        session.get_metrics().get_connection_timeouts(),
                    ),
                ]
                .into_iter()
                .collect()
            } else {
                BTreeMap::new()
            });
        }
        _ => unreachable!(),
    }
}
