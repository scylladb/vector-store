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
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;

type GetItemsR = anyhow::Result<BoxStream<'static, anyhow::Result<(PrimaryKey, Embeddings)>>>;
type GetPrimaryKeyColumnsR = Vec<ColumnName>;

pub enum DbIndex {
    GetItems {
        tx: oneshot::Sender<GetItemsR>,
    },

    GetPrimaryKeyColumns {
        tx: oneshot::Sender<GetPrimaryKeyColumnsR>,
    },
}

pub(crate) trait DbIndexExt {
    async fn get_items(&self) -> GetItemsR;

    async fn get_primary_key_columns(&self) -> GetPrimaryKeyColumnsR;
}

impl DbIndexExt for mpsc::Sender<DbIndex> {
    async fn get_items(&self) -> GetItemsR {
        let (tx, rx) = oneshot::channel();
        self.send(DbIndex::GetItems { tx }).await?;
        rx.await?
    }

    async fn get_primary_key_columns(&self) -> GetPrimaryKeyColumnsR {
        let (tx, rx) = oneshot::channel();
        self.send(DbIndex::GetPrimaryKeyColumns { tx })
            .await
            .expect("internal actor should receive request");
        rx.await.expect("internal actor should send response")
    }
}

pub(crate) async fn new(
    db_session: Arc<Session>,
    metadata: IndexMetadata,
) -> anyhow::Result<mpsc::Sender<DbIndex>> {
    let id = metadata.id();
    let statements = Arc::new(Statements::new(db_session, metadata).await?);
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(
        async move {
            while let Some(msg) = rx.recv().await {
                debug!("starting");
                tokio::spawn(process(Arc::clone(&statements), msg));
            }
            debug!("finished");
        }
        .instrument(debug_span!("db_index", "{}", id)),
    );
    Ok(tx)
}

async fn process(statements: Arc<Statements>, msg: DbIndex) {
    match msg {
        DbIndex::GetItems { tx } => tx
            .send(statements.get_items().await)
            .unwrap_or_else(|_| trace!("process: Db::GetItems: unable to send response")),

        DbIndex::GetPrimaryKeyColumns { tx } => tx
            .send(statements.get_primary_key_columns())
            .unwrap_or_else(|_| {
                trace!("process: Db::GetPrimaryKeyColumns: unable to send response")
            }),
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
    st_get_items: PreparedStatement,
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

        Ok(Self {
            primary_key_columns,

            st_get_items: session
                .prepare(Self::get_items_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                    &st_primary_key_select,
                    &metadata.target_column,
                ))
                .await
                .context("get_items_query")?,

            session,
        })
    }

    fn get_primary_key_columns(&self) -> Vec<ColumnName> {
        self.primary_key_columns.clone()
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
}
