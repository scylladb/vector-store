/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::Embeddings;
use crate::IndexMetadata;
use crate::KeyspaceName;
use crate::PrimaryKey;
use crate::TableName;
use anyhow::Context;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug_span;
use tracing::warn;

type GetProcessedIdsR = anyhow::Result<BoxStream<'static, anyhow::Result<PrimaryKey>>>;
type GetItemsR = anyhow::Result<BoxStream<'static, anyhow::Result<(PrimaryKey, Embeddings)>>>;
type GetPrimaryKeyColumnsR = Vec<ColumnName>;

pub enum DbIndex {
    GetProcessedIds {
        tx: oneshot::Sender<GetProcessedIdsR>,
    },

    GetItems {
        tx: oneshot::Sender<GetItemsR>,
    },

    GetPrimaryKeyColumns {
        tx: oneshot::Sender<GetPrimaryKeyColumnsR>,
    },

    ResetItem {
        primary_key: PrimaryKey,
    },

    UpdateItem {
        primary_key: PrimaryKey,
    },
}

pub(crate) trait DbIndexExt {
    async fn get_processed_ids(&self) -> GetProcessedIdsR;

    async fn get_items(&self) -> GetItemsR;

    async fn get_primary_key_columns(&self) -> GetPrimaryKeyColumnsR;

    async fn reset_item(&self, primary_key: PrimaryKey) -> anyhow::Result<()>;

    async fn update_item(&self, primary_key: PrimaryKey) -> anyhow::Result<()>;
}

impl DbIndexExt for mpsc::Sender<DbIndex> {
    async fn get_processed_ids(&self) -> GetProcessedIdsR {
        let (tx, rx) = oneshot::channel();
        self.send(DbIndex::GetProcessedIds { tx }).await?;
        rx.await?
    }

    async fn get_items(&self) -> GetItemsR {
        let (tx, rx) = oneshot::channel();
        self.send(DbIndex::GetItems { tx }).await?;
        rx.await?
    }

    async fn get_primary_key_columns(&self) -> GetPrimaryKeyColumnsR {
        let (tx, rx) = oneshot::channel();
        if self
            .send(DbIndex::GetPrimaryKeyColumns { tx })
            .await
            .is_err()
        {
            warn!("db_index::get_primary_key_columns: unable to send internal message");
            return Vec::new();
        }
        rx.await.unwrap_or_else(|err| {
            warn!("db_index::get_primary_key_columns: unable to recv internal message: {err}");
            Vec::new()
        })
    }

    async fn reset_item(&self, primary_key: PrimaryKey) -> anyhow::Result<()> {
        self.send(DbIndex::ResetItem { primary_key }).await?;
        Ok(())
    }

    async fn update_item(&self, primary_key: PrimaryKey) -> anyhow::Result<()> {
        self.send(DbIndex::UpdateItem { primary_key }).await?;
        Ok(())
    }
}

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

async fn process(statements: Arc<Statements>, msg: DbIndex) {
    match msg {
        DbIndex::GetProcessedIds { tx } => tx
            .send(statements.get_processed_ids().await)
            .unwrap_or_else(|_| {
                warn!("db_index::process: Db::GetProcessedIds: unable to send response")
            }),

        DbIndex::GetItems { tx } => tx
            .send(statements.get_items().await)
            .unwrap_or_else(|_| warn!("db_index::process: Db::GetItems: unable to send response")),

        DbIndex::GetPrimaryKeyColumns { tx } => tx
            .send(statements.get_primary_key_columns())
            .unwrap_or_else(|_| {
                warn!("db_index::process: Db::GetPrimaryKeyColumns: unable to send response")
            }),

        DbIndex::ResetItem { primary_key } => statements
            .reset_item(primary_key)
            .await
            .unwrap_or_else(|err| warn!("db_index::process: Db::ResetItem: {err}")),

        DbIndex::UpdateItem { primary_key } => statements
            .update_item(primary_key)
            .await
            .unwrap_or_else(|err| warn!("db_index::process: Db::UpdateItem: {err}")),
    }
}

struct Statements {
    session: Arc<Session>,
    primary_key_columns: Vec<ColumnName>,
    st_get_processed_ids: PreparedStatement,
    st_get_items: PreparedStatement,
    st_reset_item: PreparedStatement,
    st_update_item: PreparedStatement,
}

impl Statements {
    async fn new(session: Arc<Session>, metadata: IndexMetadata) -> anyhow::Result<Self> {
        Ok(Self {
            st_get_processed_ids: session
                .prepare(Self::get_processed_ids_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                    &metadata.key_name,
                ))
                .await
                .context("get_processed_ids_query")?,

            st_get_items: session
                .prepare(Self::get_items_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                    &metadata.key_name,
                    &metadata.target_column,
                ))
                .await
                .context("get_items_query")?,

            st_reset_item: session
                .prepare(Self::reset_item_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                ))
                .await
                .context("reset_items_query")?,

            st_update_item: session
                .prepare(Self::update_item_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                ))
                .await
                .context("update_item_query")?,

            primary_key_columns: vec![metadata.key_name],

            session,
        })
    }

    fn get_primary_key_columns(&self) -> Vec<ColumnName> {
        self.primary_key_columns.clone()
    }

    fn get_processed_ids_query(
        keyspace: &KeyspaceName,
        table: &TableName,
        col_id: &ColumnName,
    ) -> String {
        format!(
            "
            SELECT {col_id}
            FROM {keyspace}.{table}
            WHERE processed = TRUE
            LIMIT 1000
            "
        )
    }

    async fn get_processed_ids(&self) -> GetProcessedIdsR {
        Ok(self
            .session
            .execute_iter(self.st_get_processed_ids.clone(), ())
            .await?
            .rows_stream::<(i64,)>()?
            .map_ok(|(key,)| vec![(key as u64).into()].into())
            .map_err(|err| err.into())
            .boxed())
    }

    fn get_items_query(
        keyspace: &KeyspaceName,
        table: &TableName,
        col_id: &ColumnName,
        col_emb: &ColumnName,
    ) -> String {
        format!(
            "
            SELECT {col_id}, {col_emb}
            FROM {keyspace}.{table}
            WHERE processed = FALSE
            "
        )
    }

    async fn get_items(&self) -> GetItemsR {
        Ok(self
            .session
            .execute_iter(self.st_get_items.clone(), ())
            .await?
            .rows_stream::<(i64, Vec<f32>)>()?
            .map_ok(|(key, embeddings)| (vec![(key as u64).into()].into(), embeddings.into()))
            .map_err(|err| err.into())
            .boxed())
    }

    fn reset_item_query(keyspace: &KeyspaceName, table: &TableName) -> String {
        format!(
            "
            UPDATE {keyspace}.{table}
                SET processed = False
                WHERE id = ?
            "
        )
    }

    async fn reset_item(&self, primary_key: PrimaryKey) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_reset_item, primary_key.0)
            .await?;
        Ok(())
    }

    fn update_item_query(keyspace: &KeyspaceName, table: &TableName) -> String {
        format!(
            "
            UPDATE {keyspace}.{table}
                SET processed = True
                WHERE id = ?
            "
        )
    }

    async fn update_item(&self, primary_key: PrimaryKey) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_update_item, primary_key.0)
            .await?;
        Ok(())
    }
}
