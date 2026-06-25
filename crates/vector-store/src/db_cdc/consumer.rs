/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::ColumnName;
use crate::DbIndexedRow;
use crate::DbIndexedValue;
use crate::IndexKey;
use crate::IndexKind;
use crate::IndexMetadata;
use crate::Metrics;
use crate::NonemptyArc;
use crate::NonemptyIteratorExt;
use crate::db_index_backend::CdcValueStatus;
use crate::db_index_backend::DbIndexBackend;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::value::CqlValue;
use scylla_cdc::consumer::CDCRow;
use scylla_cdc::consumer::Consumer;
use scylla_cdc::consumer::ConsumerFactory;
use scylla_cdc::consumer::OperationType;
use std::sync::Arc;
use tokio::sync::mpsc;

fn extract_indexed_value(
    backend: &DbIndexBackend,
    value: CqlValue,
    kind: &IndexKind,
) -> anyhow::Result<Option<DbIndexedValue>> {
    match kind {
        IndexKind::Vs(_) => backend
            .extract_vector(value)
            .map(|opt| opt.map(DbIndexedValue::Vector)),
        IndexKind::Fts(_) => backend
            .extract_document(value)
            .map(|opt| opt.map(DbIndexedValue::Document)),
    }
}

struct CdcConsumerData {
    index_key: IndexKey,
    primary_key_columns: NonemptyArc<ColumnName>,
    backend: DbIndexBackend,
    kind: IndexKind,
    tx: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
    metrics: Arc<Metrics>,
}

struct CdcConsumer(Arc<CdcConsumerData>);

#[async_trait]
impl Consumer for CdcConsumer {
    async fn consume_cdc(&mut self, mut row: CDCRow<'_>) -> anyhow::Result<()> {
        if self.0.tx.is_closed() {
            // a consumer should be closed now, some concurrent tasks could stay in a pipeline
            return Ok(());
        }

        let source = &self.0.backend;
        // TODO: fix multi-target indexes
        let column = source.target_column_names().first().as_ref();
        if !row.column_deletable(column) {
            bail!("CDC error: column {column} should be deletable");
        }

        let value = match row.operation {
            OperationType::PartitionDelete | OperationType::RowDelete => None,

            OperationType::RowUpdate | OperationType::RowInsert | OperationType::PostImage => {
                match source.take_cdc_value(&mut row)? {
                    CdcValueStatus::Deleted => None,
                    CdcValueStatus::Skip => return Ok(()),
                    CdcValueStatus::NewValue(value) => {
                        extract_indexed_value(source, value, &self.0.kind)?
                    }
                }
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
            .0
            .primary_key_columns
            .iter()
            .map(|column| row.take_value(column.as_ref()))
            .collect()
        else {
            // If any primary key column is missing skip this row.
            return Ok(());
        };

        let timestamp = row
            .time
            .try_into()
            .map_err(|err| anyhow!("CDC error: converting row.time: {err}"))?;

        _ = self
            .0
            .tx
            .send((
                DbIndexedRow {
                    primary_key,
                    value,
                    timestamp,
                },
                AsyncInProgress::cdc(
                    self.0.metrics.indexing_lag.with_label_values(&[
                        self.0.index_key.keyspace().as_ref(),
                        self.0.index_key.index().as_ref(),
                    ]),
                    timestamp,
                ),
            ))
            .await;
        Ok(())
    }
}

pub(super) struct CdcConsumerFactory(Arc<CdcConsumerData>);

#[async_trait]
impl ConsumerFactory for CdcConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(CdcConsumer(Arc::clone(&self.0)))
    }
}

impl CdcConsumerFactory {
    pub(super) fn new(
        session: Arc<Session>,
        metadata: &IndexMetadata,
        metrics: Arc<Metrics>,
        tx: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
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

        let backend = DbIndexBackend::from(metadata);

        Ok(Self(Arc::new(CdcConsumerData {
            index_key: metadata.key(),
            primary_key_columns,
            backend,
            kind: metadata.kind.clone(),
            tx,
            metrics,
        })))
    }
}
