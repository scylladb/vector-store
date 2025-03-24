/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::db_index;
use crate::db_index::DbIndex;
use crate::IndexMetadata;
use scylla::client::session::Session;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug_span;
use tracing::warn;
use tracing::Instrument;

pub(crate) enum Db {
    #[allow(clippy::enum_variant_names)]
    GetIndexDb {
        metadata: IndexMetadata,
        tx: oneshot::Sender<anyhow::Result<mpsc::Sender<DbIndex>>>,
    },
}

pub(crate) trait DbExt {
    async fn get_index_db(&self, metadata: IndexMetadata) -> anyhow::Result<mpsc::Sender<DbIndex>>;
}

impl DbExt for mpsc::Sender<Db> {
    async fn get_index_db(&self, metadata: IndexMetadata) -> anyhow::Result<mpsc::Sender<DbIndex>> {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexDb { metadata, tx }).await?;
        rx.await?
    }
}

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

async fn process(statements: Arc<Statements>, msg: Db) {
    match msg {
        Db::GetIndexDb { metadata, tx } => tx
            .send(statements.get_index_db(metadata).await)
            .unwrap_or_else(|_| warn!("db::process: Db::GetIndexDb: unable to send response")),
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

    async fn get_index_db(&self, metadata: IndexMetadata) -> anyhow::Result<mpsc::Sender<DbIndex>> {
        db_index::new(Arc::clone(&self.session), metadata).await
    }
}
