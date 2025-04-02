/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexId;
use crate::db;
use anyhow::Context;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tracing::warn;

pub(crate) enum ModifyIndexes {
    Del { id: IndexId },
}

pub(crate) trait ModifyIndexesExt {
    async fn del(&self, id: IndexId);
}

impl ModifyIndexesExt for Sender<ModifyIndexes> {
    async fn del(&self, id: IndexId) {
        self.send(ModifyIndexes::Del { id })
            .await
            .unwrap_or_else(|err| warn!("ModifyIndexesExt::del: unable to send request: {err}"));
    }
}

pub(crate) async fn new(
    db_session: Arc<Session>,
    _db_actor: Sender<db::Db>,
) -> anyhow::Result<Sender<ModifyIndexes>> {
    let db = Db::new(db_session).await?;

    // The value was taken from initial benchmarks
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                ModifyIndexes::Del { id } => db.remove_index(id).await.unwrap_or_else(|err| {
                    warn!("modify_indexes: unable to remove index from db: {err}")
                }),
            }
        }
    });
    Ok(tx)
}

struct Db {
    session: Arc<Session>,
    st_remove_index: PreparedStatement,
}

impl Db {
    async fn new(session: Arc<Session>) -> anyhow::Result<Self> {
        Ok(Self {
            st_remove_index: session
                .prepare(Self::REMOVE_INDEX)
                .await
                .context("REMOVE_INDEX")?,
            session,
        })
    }

    const REMOVE_INDEX: &str = "DELETE FROM vector_benchmark.vector_indexes WHERE id = ?";
    async fn remove_index(&self, id: IndexId) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_remove_index, (id,))
            .await?;
        Ok(())
    }
}
