/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexMetadata;
use scylla::client::session::Session;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug_span, Instrument};

pub(crate) enum DbIndex {}

#[allow(dead_code)]
pub(crate) trait DbIndexExt {}

impl DbIndexExt for mpsc::Sender<DbIndex> {}

pub(crate) async fn new(
    db_session: Arc<Session>,
    metadata: IndexMetadata,
) -> anyhow::Result<mpsc::Sender<DbIndex>> {
    let statements = Arc::new(Statements::new(db_session, metadata).await?);
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(
        async move {
            while let Some(msg) = rx.recv().await {
                tokio::spawn(process(Arc::clone(&statements), msg));
            }
        }
        .instrument(debug_span!("db_index")),
    );
    Ok(tx)
}

async fn process(_statements: Arc<Statements>, msg: DbIndex) {
    match msg {}
}

#[allow(dead_code)]
struct Statements {
    session: Arc<Session>,
}

impl Statements {
    async fn new(session: Arc<Session>, _metadata: IndexMetadata) -> anyhow::Result<Self> {
        Ok(Self { session })
    }
}
