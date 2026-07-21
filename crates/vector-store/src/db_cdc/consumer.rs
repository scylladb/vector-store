/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::ColumnName;
use crate::DbIndexedOperation;
use crate::DbIndexedRow;
use crate::IndexKey;
use crate::IndexKind;
use crate::IndexMetadata;
use crate::Metrics;
use crate::NonemptyArc;
use crate::NonemptyIteratorExt;
use crate::PrimaryKey;
use crate::Timestamp;
use crate::db_index;
use crate::db_index_backend;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;
use scylla::value::Row;
use scylla_cdc::consumer::CDCRow;
use scylla_cdc::consumer::Consumer;
use scylla_cdc::consumer::ConsumerFactory;
use scylla_cdc::consumer::OperationType;
use std::sync::Arc;
use tap::Pipe;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Upsert,
    Delete,
}

struct CdcConsumerData {
    session: Arc<Session>,
    st_select_values: PreparedStatement,
    index_key: IndexKey,
    primary_key_columns: NonemptyArc<ColumnName>,
    nonpk_partition_key_columns: Box<[ColumnName]>,
    target_columns: NonemptyArc<ColumnName>,
    filtering_columns: Arc<[ColumnName]>,
    kind: IndexKind,
    tx: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
    metrics: Arc<Metrics>,
    semaphore: Arc<Semaphore>,
}

impl CdcConsumerData {
    async fn process_upsert(
        &self,
        primary_key: Arc<Vec<CqlValue>>,
        timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        let rows_result = self
            .session
            .execute_unpaged(&self.st_select_values, primary_key.as_slice())
            .await?
            .into_rows_result()?;
        let primary_key = PrimaryKey::from(primary_key.iter().cloned());

        let async_in_progress = AsyncInProgress::cdc(
            self.metrics.indexing_lag.with_label_values(&[
                self.index_key.keyspace().as_ref(),
                self.index_key.index().as_ref(),
            ]),
            timestamp,
        );

        let Some(row) = rows_result.maybe_first_row::<Row>()? else {
            // If no row is found for the primary key, it is deleted
            _ = self
                .tx
                .send((
                    DbIndexedRow {
                        primary_key,
                        operation: DbIndexedOperation::Delete(timestamp),
                    },
                    async_in_progress,
                ))
                .await;
            return Ok(());
        };

        let target_columns_len = self.target_columns.len();
        let columns_len_expected = (target_columns_len.get()
            + self.nonpk_partition_key_columns.len()
            + self.filtering_columns.len())
            * 2;
        if row.columns.len() != columns_len_expected {
            let msg = format!(
                "Unexpected number of columns in row: expected {columns_len_expected}, got {received_len}",
                received_len = row.columns.len()
            );
            debug!("process_upsert: {msg}");
            bail!(msg);
        }

        let values =
            db_index::parse_values(row.columns, Some(timestamp), target_columns_len, &self.kind)?;
        _ = self
            .tx
            .send((
                DbIndexedRow {
                    primary_key,
                    operation: DbIndexedOperation::Upsert(values),
                },
                async_in_progress,
            ))
            .await;
        Ok(())
    }
}

struct CdcConsumer {
    consumer_data: Arc<CdcConsumerData>,
    primary_key: Arc<Vec<CqlValue>>,
    timestamp: Timestamp,
    operation: Operation,
}

impl CdcConsumer {
    async fn process_row(&self) {
        if matches!(self.operation, Operation::Upsert) {
            self.process_upsert().await;
        } else {
            self.process_delete().await;
        }
    }

    async fn process_upsert(&self) {
        let permit = Arc::clone(&self.consumer_data.semaphore)
            .acquire_owned()
            .await
            .unwrap();
        let primary_key = Arc::clone(&self.primary_key);
        let timestamp = self.timestamp;
        let consumer_data = Arc::clone(&self.consumer_data);
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = consumer_data.process_upsert(primary_key, timestamp).await {
                error!("Error processing upsert: {err}");
            }
        });
    }

    async fn process_delete(&self) {
        let _ = self
            .consumer_data
            .tx
            .send((
                DbIndexedRow {
                    primary_key: self.primary_key.iter().cloned().collect(),
                    operation: DbIndexedOperation::Delete(self.timestamp),
                },
                AsyncInProgress::cdc(
                    self.consumer_data.metrics.indexing_lag.with_label_values(&[
                        self.consumer_data.index_key.keyspace().as_ref(),
                        self.consumer_data.index_key.index().as_ref(),
                    ]),
                    self.timestamp,
                ),
            ))
            .await;
    }
}

#[async_trait]
impl Consumer for CdcConsumer {
    async fn consume_cdc(&mut self, mut row: CDCRow<'_>) -> anyhow::Result<()> {
        if self.consumer_data.tx.is_closed() {
            // a consumer should be closed now, some concurrent tasks could stay in a pipeline
            return Ok(());
        }

        let operation = match row.operation {
            OperationType::PartitionDelete | OperationType::RowDelete => Operation::Delete,

            OperationType::RowUpdate | OperationType::RowInsert | OperationType::PostImage => {
                Operation::Upsert
            }

            OperationType::PreImage
            | OperationType::RowRangeDelInclLeft
            | OperationType::RowRangeDelExclLeft
            | OperationType::RowRangeDelInclRight
            | OperationType::RowRangeDelExclRight => {
                // These operations are unsupported by our CDC reader, skip them.
                return Ok(());
            }
        };

        let Some(primary_key) = self
            .consumer_data
            .primary_key_columns
            .iter()
            .map(|column| row.take_value(column.as_ref()))
            .collect::<Option<Vec<_>>>()
        else {
            // If any primary key column is missing skip this row.
            return Ok(());
        };

        let timestamp = row
            .time
            .try_into()
            .map_err(|err| anyhow!("CDC error: converting row.time: {err}"))?;

        if timestamp == self.timestamp
            && &primary_key == self.primary_key.as_ref()
            && operation == self.operation
        {
            // Skip duplicate rows with the same primary key and timestamp.
            return Ok(());
        }
        self.primary_key = Arc::new(primary_key);
        self.timestamp = timestamp;
        self.operation = operation;

        self.process_row().await;

        Ok(())
    }
}

pub(super) struct CdcConsumerFactory(Arc<CdcConsumerData>);

#[async_trait]
impl ConsumerFactory for CdcConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(CdcConsumer {
            consumer_data: Arc::clone(&self.0),
            primary_key: Arc::new(vec![]),
            timestamp: Timestamp::MIN,
            operation: Operation::Upsert,
        })
    }
}

impl CdcConsumerFactory {
    pub(super) async fn new(
        session: Arc<Session>,
        metadata: &IndexMetadata,
        metrics: Arc<Metrics>,
        tx: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
        semaphore: Arc<Semaphore>,
    ) -> anyhow::Result<Self> {
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
            .collect_nonempty_arc()
            .ok_or_else(|| anyhow!("primary key must have at least one column"))?;

        let target_columns = metadata.target_columns.clone();
        let filtering_columns: Arc<[_]> = metadata
            .filtering_columns
            .iter()
            .filter(|column| !primary_key_columns.contains(column))
            .cloned()
            .collect();

        let nonpk_partition_key_columns: Box<[_]> = metadata
            .nonpk_partition_key_columns()
            .into_iter()
            .flatten()
            .cloned()
            .collect();

        let query = db_index_backend::request_query(
            &metadata.keyspace_name.as_ref().into(),
            &metadata.table_name.as_ref().into(),
            target_columns
                .iter()
                .chain(nonpk_partition_key_columns.iter())
                .chain(filtering_columns.iter()),
            primary_key_columns.iter(),
        );
        let st_select_values =
            session
                .prepare(query)
                .await
                .context("request_query")?
                .pipe(|mut stmt| {
                    stmt.set_is_idempotent(true);
                    stmt
                });

        Ok(Self(Arc::new(CdcConsumerData {
            session,
            st_select_values,
            index_key: metadata.key(),
            primary_key_columns,
            nonpk_partition_key_columns,
            target_columns,
            filtering_columns,
            kind: metadata.kind.clone(),
            tx,
            metrics,
            semaphore,
        })))
    }
}
