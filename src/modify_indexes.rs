/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Proprietary
 */

use {
    crate::{IndexId, IndexItemsCount, ScyllaDbUri},
    anyhow::Context,
    scylla::{
        client::{session::Session, session_builder::SessionBuilder},
        statement::prepared::PreparedStatement,
    },
    tokio::sync::mpsc::{self, Sender},
    tracing::warn,
};

pub(crate) enum ModifyIndexes {
    UpdateItemsCount {
        id: IndexId,
        items_count: IndexItemsCount,
    },
    Del {
        id: IndexId,
    },
}

pub(crate) trait ModifyIndexesExt {
    async fn update_items_count(&self, id: IndexId, items_count: IndexItemsCount);
    async fn del(&self, id: IndexId);
}

impl ModifyIndexesExt for Sender<ModifyIndexes> {
    async fn update_items_count(&self, id: IndexId, items_count: IndexItemsCount) {
        self.send(ModifyIndexes::UpdateItemsCount { id, items_count })
            .await
            .unwrap_or_else(|err| {
                warn!("ModifyIndexesExt::update_items_count: unable to send request: {err}")
            });
    }

    async fn del(&self, id: IndexId) {
        self.send(ModifyIndexes::Del { id })
            .await
            .unwrap_or_else(|err| warn!("ModifyIndexesExt::del: unable to send request: {err}"));
    }
}

pub(crate) async fn new(uri: ScyllaDbUri) -> anyhow::Result<Sender<ModifyIndexes>> {
    let db = Db::new(uri).await?;
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                ModifyIndexes::UpdateItemsCount { id, items_count } => db
                    .update_items_count(id, items_count)
                    .await
                    .unwrap_or_else(|err| {
                        warn!("modify_indexes: unable to update items count for index in db: {err}")
                    }),
                ModifyIndexes::Del { id } => db.remove_index(id).await.unwrap_or_else(|err| {
                    warn!("modify_indexes: unable to remove index from db: {err}")
                }),
            }
        }
    });
    Ok(tx)
}

struct Db {
    session: Session,
    st_update_items_count: PreparedStatement,
    st_remove_index: PreparedStatement,
}

impl Db {
    async fn new(uri: ScyllaDbUri) -> anyhow::Result<Self> {
        let session = SessionBuilder::new()
            .known_node(uri.0.as_str())
            .build()
            .await?;
        Ok(Self {
            st_update_items_count: session
                .prepare(Self::UPDATE_ITEMS_COUNT)
                .await
                .context("UPDATE_ITEMS_COUNT")?,
            st_remove_index: session
                .prepare(Self::REMOVE_INDEX)
                .await
                .context("REMOVE_INDEX")?,
            session,
        })
    }

    const UPDATE_ITEMS_COUNT: &str = "
        UPDATE vector_benchmark.vector_indexes
            SET indexed_elements_count = ?
            WHERE id = ?
        ";
    async fn update_items_count(
        &self,
        id: IndexId,
        items_count: IndexItemsCount,
    ) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_update_items_count, (items_count, id))
            .await?;
        Ok(())
    }

    const REMOVE_INDEX: &str = "DELETE FROM vector_benchmark.vector_indexes WHERE id = ?";
    async fn remove_index(&self, id: IndexId) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_remove_index, (id,))
            .await?;
        Ok(())
    }
}
