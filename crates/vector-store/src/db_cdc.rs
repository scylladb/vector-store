/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::ColumnName;
use crate::Config;
use crate::DbEmbedding;
use crate::IndexMetadata;
use crate::internals::Internals;
use crate::internals::InternalsExt;
use ::time::Date;
use ::time::Month;
use ::time::OffsetDateTime;
use ::time::PrimitiveDateTime;
use ::time::Time;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::value::CqlValue;
use scylla_cdc::consumer::CDCRow;
use scylla_cdc::consumer::Consumer;
use scylla_cdc::consumer::ConsumerFactory;
use scylla_cdc::log_reader::CDCLogReaderBuilder;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tap::Pipe;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::error;
use tracing::error_span;
use tracing::info;
use tracing::warn;

const CHECKPOINT_TIMESTAMP_OFFSET: Duration = Duration::from_mins(10);

/// Spawns a CDC actor that watches for session changes and manages the CDC reader lifecycle.
pub(crate) fn new(
    config_rx: watch::Receiver<Arc<Config>>,
    mut session_rx: watch::Receiver<Option<Arc<Session>>>,
    metadata: IndexMetadata,
    internals: Sender<Internals>,
    cdc_error_notify: Arc<Notify>,
    tx_embeddings: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
) {
    // Mark the receiver to ensure first session update is visible
    session_rx.mark_changed();

    let actor_key = metadata.key();
    tokio::spawn(
        async move {
            debug!("starting");

            let cdc_now = || SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
            let mut cdc_start = cdc_now();
            let mut cdc_reader: Option<scylla_cdc::log_reader::CDCLogReader> = None;
            let mut cdc_handler_task: Option<tokio::task::JoinHandle<Duration>> = None;
            let shutdown_notify = Arc::new(Notify::new());

            loop {
                // Wait for session changes
                if session_rx.changed().await.is_err() {
                    break;
                }

                let session_opt = session_rx.borrow_and_update().clone();

                match session_opt {
                    Some(session) => {
                        info!(
                            "Session available, creating CDC reader for {}",
                            metadata.key()
                        );

                        // Stop old CDC reader if exists
                        if let Some(mut reader) = cdc_reader.take() {
                            reader.stop();
                        }
                        if let Some(task) = cdc_handler_task.take() {
                            shutdown_notify.notify_one();
                            cdc_start = task.await.unwrap_or(cdc_now());
                        }

                        // Disable pending shutdown notifications
                        if pin!(shutdown_notify.notified()).enable() {
                            while pin!(shutdown_notify.notified()).enable() {
                                error!("Internal error: unable to cleanup CDC reader. Lets retry again.");
                                time::sleep(Duration::from_secs(1)).await;
                            }
                        }

                        // Create new CDC reader
                        let config = config_rx.borrow().clone();
                        match create_cdc_reader(
                            cdc_start,
                            config,
                            session,
                            metadata.clone(),
                            tx_embeddings.clone(),
                        )
                        .await
                        {
                            Ok((reader, handler)) => {
                                cdc_reader = Some(reader);

                                // Spawn CDC handler task
                                let shutdown_notify = Arc::clone(&shutdown_notify);
                                let cdc_error_notify = Arc::clone(&cdc_error_notify);
                                let handler_key = metadata.key();
                                let internals = internals.clone();
                                let cdc_key = metadata.key();
                                cdc_handler_task = Some(tokio::spawn(
                                    async move {
                                        tokio::select! {
                                            result = handler => {
                                                if let Err(err) = result {
                                                    warn!("CDC handler error: {err}");
                                                    internals
                                                        .increment_counter(format!("{handler_key}-cdc-handler-errors"))
                                                        .await;
                                                    cdc_error_notify.notify_one();
                                                }
                                            }
                                            _ = shutdown_notify.notified() => {
                                                debug!("CDC handler: shutdown requested");
                                            }
                                        }
                                        debug!("CDC handler finished");
                                        cdc_now()
                                    }
                                    .instrument(error_span!("cdc", "{cdc_key}")),
                                ));

                                info!("CDC reader created successfully for {}", metadata.key());
                            }
                            Err(e) => {
                                error!("Failed to create CDC reader: {}", e);
                            }
                        }
                    }
                    None => {
                        info!(
                            "Session became None, stopping CDC reader for {}",
                            metadata.key()
                        );

                        // Stop CDC reader
                        if let Some(mut reader) = cdc_reader.take() {
                            reader.stop();
                        }
                        if let Some(task) = cdc_handler_task.take() {
                            shutdown_notify.notify_one();
                            cdc_start = task.await.unwrap_or(cdc_now());
                        }
                    }
                }
            }

            // Cleanup
            if let Some(mut reader) = cdc_reader {
                reader.stop();
            }
            if let Some(task) = cdc_handler_task {
                shutdown_notify.notify_one();
                _ = task.await;
            }

            debug!("finished");
        }
        .instrument(error_span!("db_cdc", "{}", actor_key)),
    );
}

/// Creates a CDC log reader and its handler future.
async fn create_cdc_reader(
    cdc_start: Duration,
    config: Arc<Config>,
    session: Arc<Session>,
    metadata: IndexMetadata,
    tx_embeddings: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
) -> anyhow::Result<(
    scylla_cdc::log_reader::CDCLogReader,
    impl std::future::Future<Output = anyhow::Result<()>>,
)> {
    let consumer_factory = CdcConsumerFactory::new(Arc::clone(&session), &metadata, tx_embeddings)?;

    let cdc_start = cdc_start - CHECKPOINT_TIMESTAMP_OFFSET;
    info!(
        "Creating CDC log reader for {} starting from {:?}",
        metadata.key(),
        OffsetDateTime::UNIX_EPOCH + cdc_start
    );
    CDCLogReaderBuilder::new()
        .session(session)
        .keyspace(metadata.keyspace_name.as_ref())
        .table_name(metadata.table_name.as_ref())
        .consumer_factory(Arc::new(consumer_factory))
        .start_timestamp(chrono::Duration::from_std(cdc_start)?)
        .pipe(|builder| {
            if let Some(interval) = config.cdc_safety_interval {
                info!("Setting CDC safety interval to {interval:?}");
                builder.safety_interval(interval)
            } else {
                builder
            }
        })
        .pipe(|builder| {
            if let Some(interval) = config.cdc_sleep_interval {
                info!("Setting CDC sleep interval to {interval:?}");
                builder.sleep_interval(interval)
            } else {
                builder
            }
        })
        .build()
        .await
        .context("Failed to build CDC log reader")
}

struct CdcConsumerData {
    primary_key_columns: Vec<ColumnName>,
    target_column: ColumnName,
    tx: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
    gregorian_epoch: PrimitiveDateTime,
}

struct CdcConsumer(Arc<CdcConsumerData>);

#[async_trait]
impl Consumer for CdcConsumer {
    async fn consume_cdc(&mut self, mut row: CDCRow<'_>) -> anyhow::Result<()> {
        if self.0.tx.is_closed() {
            // a consumer should be closed now, some concurrent tasks could stay in a pipeline
            return Ok(());
        }

        let target_column = self.0.target_column.as_ref();
        if !row.column_deletable(target_column) {
            bail!("CDC error: target column {target_column} should be deletable");
        }

        let embedding = row
            .take_value(target_column)
            .map(|value| {
                let CqlValue::Vector(value) = value else {
                    bail!("CDC error: target column {target_column} should be VECTOR type");
                };
                value
                    .into_iter()
                    .map(|value| {
                        value.as_float().ok_or(anyhow!(
                            "CDC error: target column {target_column} should be VECTOR<float> type"
                        ))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .transpose()?
            .map(|embedding| embedding.into());

        let primary_key = self
            .0
            .primary_key_columns
            .iter()
            .map(|column| {
                if !row.column_exists(column.as_ref()) {
                    bail!("CDC error: primary key column {column} should exist");
                }
                if row.column_deletable(column.as_ref()) {
                    bail!("CDC error: primary key column {column} should not be deletable");
                }
                row.take_value(column.as_ref()).ok_or(anyhow!(
                    "CDC error: primary key column {column} value should exist"
                ))
            })
            .collect::<anyhow::Result<_>>()?;

        const HUNDREDS_NANOS_TO_MICROS: u64 = 10;
        let timestamp = (self.0.gregorian_epoch
            + Duration::from_micros(
                row.time
                    .get_timestamp()
                    .ok_or(anyhow!("CDC error: time has no timestamp"))?
                    .to_gregorian()
                    .0
                    / HUNDREDS_NANOS_TO_MICROS,
            ))
        .into();

        _ = self
            .0
            .tx
            .send((
                DbEmbedding {
                    primary_key,
                    embedding,
                    timestamp,
                },
                None,
            ))
            .await;
        Ok(())
    }
}

struct CdcConsumerFactory(Arc<CdcConsumerData>);

#[async_trait]
impl ConsumerFactory for CdcConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn Consumer> {
        Box::new(CdcConsumer(Arc::clone(&self.0)))
    }
}

impl CdcConsumerFactory {
    fn new(
        session: Arc<Session>,
        metadata: &IndexMetadata,
        tx: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
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
            .collect();

        let gregorian_epoch = PrimitiveDateTime::new(
            Date::from_calendar_date(1582, Month::October, 15)?,
            Time::MIDNIGHT,
        );

        Ok(Self(Arc::new(CdcConsumerData {
            primary_key_columns,
            target_column: metadata.target_column.clone(),
            tx,
            gregorian_epoch,
        })))
    }
}
