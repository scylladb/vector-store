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
use anyhow::anyhow;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::deserialize::row::ColumnIterator;
use scylla::deserialize::row::DeserializeRow;
use scylla::deserialize::value::DeserializeValue;
use scylla::errors::DeserializationError;
use scylla::errors::TypeCheckError;
use scylla::frame::response::result::ColumnSpec;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;
use scylla::value::Row;
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

#[derive(thiserror::Error, Debug)]
enum DeserializeError {
    #[error("Query for primary key & embeddings should contain at least two elements")]
    InvalidQuerySelectLength,
    #[error("Invalid embeddings type")]
    InvalidEmbeddingsType,
}

struct PrimaryKeyWithEmbeddings {
    primary_key: PrimaryKey,
    embeddings: Embeddings,
}

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for PrimaryKeyWithEmbeddings {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        if specs.len() < 2 {
            return Err(TypeCheckError::new(
                DeserializeError::InvalidQuerySelectLength,
            ));
        }
        let ColumnType::Vector { typ, .. } = specs.last().unwrap().typ() else {
            return Err(TypeCheckError::new(DeserializeError::InvalidEmbeddingsType));
        };
        let ColumnType::Native(NativeType::Float) = typ.as_ref() else {
            return Err(TypeCheckError::new(DeserializeError::InvalidEmbeddingsType));
        };
        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let columns = row.columns_remaining();
        let mut count = 0;
        let primary_key = row
            .take_while_ref(|_| {
                count += 1;
                count < columns
            })
            .map_ok(|column| CqlValue::deserialize(column.spec.typ(), column.slice))
            .flatten()
            .collect::<Result<Vec<_>, _>>()?
            .into();
        let embeddings = row
            .next()
            .unwrap()
            .and_then(|column| Vec::<f32>::deserialize(column.spec.typ(), column.slice))?
            .into();
        Ok(PrimaryKeyWithEmbeddings {
            primary_key,
            embeddings,
        })
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
        let cluster_state = session.get_cluster_state();
        let table = cluster_state
            .get_keyspace(metadata.keyspace_name.as_ref())
            .ok_or_else(|| anyhow!("keyspace {} does not exist", metadata.keyspace_name))?
            .tables
            .get(metadata.table_name.as_ref())
            .ok_or_else(|| anyhow!("table {} does not exist", metadata.table_name))?;

        let primary_key_columns = table
            .partition_key
            .iter()
            .chain(table.clustering_key.iter())
            .cloned()
            .map(ColumnName::from)
            .collect_vec();

        let st_primary_key_select = primary_key_columns.iter().join(", ");

        let st_primary_key_where = primary_key_columns
            .iter()
            .map(|column| format!("{column} = ?"))
            .join(" AND ");

        Ok(Self {
            primary_key_columns,

            st_get_processed_ids: session
                .prepare(Self::get_processed_ids_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                    &st_primary_key_select,
                ))
                .await
                .context("get_processed_ids_query")?,

            st_get_items: session
                .prepare(Self::get_items_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                    &st_primary_key_select,
                    &metadata.target_column,
                ))
                .await
                .context("get_items_query")?,

            st_reset_item: session
                .prepare(Self::reset_item_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                    &st_primary_key_where,
                ))
                .await
                .context("reset_items_query")?,

            st_update_item: session
                .prepare(Self::update_item_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                    &st_primary_key_where,
                ))
                .await
                .context("update_item_query")?,

            session,
        })
    }

    fn get_primary_key_columns(&self) -> Vec<ColumnName> {
        self.primary_key_columns.clone()
    }

    fn get_processed_ids_query(
        keyspace: &KeyspaceName,
        table: &TableName,
        st_primary_key_select: &str,
    ) -> String {
        format!(
            "
            SELECT {st_primary_key_select}
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
            .rows_stream::<Row>()?
            .map_err(|err| err.into())
            .and_then(|row| async move {
                row.columns
                    .into_iter()
                    .map(|col| col.ok_or_else(|| anyhow!("missing value for a primary key")))
                    .collect::<anyhow::Result<Vec<_>>>()
                    .map(PrimaryKey::from)
            })
            .boxed())
    }

    fn get_items_query(
        keyspace: &KeyspaceName,
        table: &TableName,
        st_primary_key_select: &str,
        embeddings: &ColumnName,
    ) -> String {
        format!(
            "
            SELECT {st_primary_key_select}, {embeddings}
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
            .rows_stream::<PrimaryKeyWithEmbeddings>()?
            .map_err(|err| err.into())
            .map_ok(|row| (row.primary_key, row.embeddings))
            .boxed())
    }

    fn reset_item_query(
        keyspace: &KeyspaceName,
        table: &TableName,
        st_primary_key_where: &str,
    ) -> String {
        format!(
            "
            UPDATE {keyspace}.{table}
                SET processed = False
                WHERE {st_primary_key_where}
            "
        )
    }

    async fn reset_item(&self, primary_key: PrimaryKey) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_reset_item, primary_key.0)
            .await?;
        Ok(())
    }

    fn update_item_query(
        keyspace: &KeyspaceName,
        table: &TableName,
        st_primary_key_where: &str,
    ) -> String {
        format!(
            "
            UPDATE {keyspace}.{table}
                SET processed = True
                WHERE {st_primary_key_where}
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
