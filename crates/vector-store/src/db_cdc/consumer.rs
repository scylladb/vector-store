/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::AsyncInProgress;
use crate::ColumnName;
use crate::DbDriver;
use crate::DbIndexedOperation;
use crate::DbIndexedRow;
use crate::IndexKey;
use crate::IndexKind;
use crate::IndexMetadata;
use crate::Metrics;
use crate::NonemptyArc;
use crate::PrimaryKey;
use crate::Timestamp;
use crate::db_index;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use scylla_cdc::consumer::CDCRow;
use scylla_cdc::consumer::Consumer;
use scylla_cdc::consumer::ConsumerFactory;
use scylla_cdc::consumer::OperationType;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Upsert,
    Delete,
}

struct CdcConsumerData<T: DbDriver> {
    db_driver: T,
    session: Arc<Session>,
    st_select_values: T::Statement,
    index_key: IndexKey,
    primary_key_columns: NonemptyArc<ColumnName>,
    nonpk_partition_key_columns: Box<[ColumnName]>,
    target_columns: NonemptyArc<ColumnName>,
    filtering_columns: Arc<[ColumnName]>,
    /// See Statements::alternator_decode_types in db_index.rs.
    alternator_decode_types: Box<[Option<NativeType>]>,
    kind: IndexKind,
    tx: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
    metrics: Arc<Metrics>,
    semaphore: Arc<Semaphore>,
}

impl<T: DbDriver> CdcConsumerData<T> {
    async fn process_upsert(
        &self,
        primary_key: Arc<Vec<CqlValue>>,
        timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        let primary_key = PrimaryKey::from(primary_key.iter().cloned());
        let rows_result = self
            .db_driver
            .execute_fetch_row(&self.session, &self.st_select_values, &primary_key)
            .await?;

        let async_in_progress = AsyncInProgress::cdc(
            self.metrics.indexing_lag.with_label_values(&[
                self.index_key.keyspace().as_ref(),
                self.index_key.index().as_ref(),
            ]),
            timestamp,
        );

        let Some(row) = rows_result else {
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

        let values = db_index::parse_values(
            row.columns,
            Some(timestamp),
            target_columns_len,
            &self.kind,
            &self.alternator_decode_types,
        )?;
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

struct CdcConsumer<T: DbDriver> {
    consumer_data: Arc<CdcConsumerData<T>>,
    primary_key: Arc<Vec<CqlValue>>,
    timestamp: Timestamp,
    operation: Operation,
}

impl<T: DbDriver> CdcConsumer<T> {
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
impl<T: DbDriver> Consumer for CdcConsumer<T> {
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

pub(super) struct CdcConsumerFactory<T: DbDriver>(Arc<CdcConsumerData<T>>);

#[async_trait]
impl<T: DbDriver> ConsumerFactory for CdcConsumerFactory<T> {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(CdcConsumer {
            consumer_data: Arc::clone(&self.0),
            primary_key: Arc::new(vec![]),
            timestamp: Timestamp::MIN,
            operation: Operation::Upsert,
        })
    }
}

impl<T: DbDriver> CdcConsumerFactory<T> {
    pub(super) async fn new(
        db_driver: T,
        session: Arc<Session>,
        metadata: &IndexMetadata,
        metrics: Arc<Metrics>,
        tx: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
        semaphore: Arc<Semaphore>,
    ) -> anyhow::Result<Self> {
        let cluster = db_driver.cluster(&session);
        let table = db_driver
            .table(&cluster, &metadata.keyspace_name, &metadata.table_name)
            .ok_or_else(|| {
                anyhow!(
                    "table {}.{} does not exist",
                    metadata.keyspace_name,
                    metadata.table_name
                )
            })?;

        let primary_key_columns = metadata.primary_key_columns.clone();

        let target_columns = metadata.target_columns.clone();
        let filtering_columns: Arc<[_]> = metadata.nonpk_filtering_columns().cloned().collect();

        let nonpk_partition_key_columns: Box<[_]> = metadata
            .nonpk_partition_key_columns()
            .into_iter()
            .flatten()
            .cloned()
            .collect();

        let real_columns: HashMap<ColumnName, NativeType> = db_driver
            .columns(table)
            .filter_map(|(name, coltype)| {
                if let ColumnType::Native(typ) = &coltype.typ {
                    Some((name, typ.clone()))
                } else {
                    None
                }
            })
            .collect();
        let alternator_decode_types = db_index::alternator_decode_types(
            nonpk_partition_key_columns
                .iter()
                .chain(filtering_columns.iter()),
            metadata,
            &real_columns,
        );

        let st_select_values = db_driver.prepare_fetch_row(&session, metadata).await?;

        Ok(Self(Arc::new(CdcConsumerData {
            db_driver,
            session,
            st_select_values,
            index_key: metadata.key(),
            primary_key_columns,
            nonpk_partition_key_columns,
            target_columns,
            filtering_columns,
            alternator_decode_types,
            kind: metadata.kind.clone(),
            tx,
            metrics,
            semaphore,
        })))
    }
}
