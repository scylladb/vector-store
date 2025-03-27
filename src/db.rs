/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use scylla::client::session::Session;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug_span;
use tracing::Instrument;

pub(crate) enum Db {
    #[allow(dead_code)]
    DummyTemp,
}

#[allow(dead_code)]
pub(crate) trait DbExt {}

impl DbExt for mpsc::Sender<Db> {}

pub(crate) async fn new(db_session: Arc<Session>) -> anyhow::Result<mpsc::Sender<Db>> {
    let statements = Arc::new(Statements::new(db_session).await?);
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(
        async move {
            while let Some(msg) = rx.recv().await {
                tokio::spawn(process(Arc::clone(&statements), msg));
            }
        }
        .instrument(debug_span!("db")),
    );
    Ok(tx)
}

async fn process(_statements: Arc<Statements>, msg: Db) {
    match msg {
        Db::DummyTemp => unreachable!(),
    }
}

#[allow(dead_code)]
struct Statements {
    session: Arc<Session>,
}

impl Statements {
    async fn new(session: Arc<Session>) -> anyhow::Result<Self> {
        Ok(Self { session })
    }
}
