/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::ColumnName;
use crate::Config;
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
use crate::internals::Internals;
use crate::internals::InternalsExt;
use crate::perf;
use ::time::OffsetDateTime;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use futures::FutureExt;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;
use scylla::value::Row;
use scylla_cdc::consumer::CDCRow;
use scylla_cdc::consumer::Consumer;
use scylla_cdc::consumer::ConsumerFactory;
use scylla_cdc::consumer::OperationType;
use scylla_cdc::log_reader::CDCLogReaderBuilder;
use std::future;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tap::Pipe;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
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

// Default parameters for the wide-framed CDC reader (consistency-focused).
const DEFAULT_CONSISTENT_SAFETY_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_CONSISTENT_SLEEP_INTERVAL: Duration = Duration::from_secs(10);

// Default parameters for the fine-grained CDC reader (latency-focused).
const DEFAULT_REALTIME_SAFETY_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_REALTIME_SLEEP_INTERVAL: Duration = Duration::from_millis(500);

/// Default backoff duration for CDC reader restarts after errors.
const DEFAULT_BACKOFF_DURATION: Duration = Duration::from_secs(5);

/// Parameters for a CDC reader instance.
#[derive(Clone, Debug)]
struct CdcReaderParams {
    /// Safety interval: how far behind "now" to read CDC log.
    /// Higher values ensure data consistency but increase latency.
    safety_interval: Duration,
    /// Sleep interval: how often to poll for new CDC data.
    /// Lower values reduce latency but increase system load.
    sleep_interval: Duration,
}

impl CdcReaderParams {
    fn wide(config: &Config) -> Self {
        Self {
            safety_interval: config
                .cdc_safety_interval
                .unwrap_or(DEFAULT_CONSISTENT_SAFETY_INTERVAL),
            sleep_interval: config
                .cdc_sleep_interval
                .unwrap_or(DEFAULT_CONSISTENT_SLEEP_INTERVAL),
        }
    }

    fn fine(config: &Config) -> Self {
        Self {
            safety_interval: config
                .cdc_fine_safety_interval
                .unwrap_or(DEFAULT_REALTIME_SAFETY_INTERVAL),
            sleep_interval: config
                .cdc_fine_sleep_interval
                .unwrap_or(DEFAULT_REALTIME_SLEEP_INTERVAL),
        }
    }
}

pub(crate) enum DbCdc {}

/// Preset configurations for CDC reader actors.
pub(crate) enum CdcReaderConfig {
    Wide,
    Fine,
}

/// Spawns a CDC actor that watches for session changes and manages a CDC reader.
#[allow(clippy::too_many_arguments)]
pub(crate) fn new(
    config_rx: watch::Receiver<Arc<Config>>,
    mut session_rx: watch::Receiver<Option<Arc<Session>>>,
    metadata: IndexMetadata,
    metrics: Arc<Metrics>,
    internals: Sender<Internals>,
    tx_embeddings: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
    semaphore: Arc<Semaphore>,
    config: CdcReaderConfig,
) -> mpsc::Sender<DbCdc> {
    let (tx, mut rx) = mpsc::channel::<DbCdc>(perf::channel_size().into());

    // Mark the receiver to ensure first session update is visible
    session_rx.mark_changed();

    let mut reader = CdcReaderState::new(config, semaphore);
    let name = reader.name;
    let actor_key = metadata.key();
    let span_key = actor_key.clone();
    tokio::spawn(
        async move {
            debug!("starting");
            internals
                .increment_counter(format!("{actor_key}-{name}-cdc-actor-started"))
                .await;

            let mut backoff_duration = None;
            let mut err_counter = 0usize;

            loop {
                tokio::select! {
                    // Shut down when all senders are dropped
                    _ = rx.recv() => { break; }

                    // Wait for session changes
                    result = session_rx.changed() => {
                        if result.is_err() {
                            break;
                        }

                        let session_opt = session_rx.borrow_and_update().clone();
                        reader.handle_session_change(
                            session_opt, &config_rx, &metadata,
                            Arc::clone(&metrics),
                            &tx_embeddings, &internals,
                        ).await;
                        err_counter = 0;
                        backoff_duration = None;
                    }

                    _ = reader.error_notify.notified() => {
                        err_counter += 1;
                        backoff_duration = Some(DEFAULT_BACKOFF_DURATION);
                        warn!(
                            "{name} CDC reader error {err_counter}, restarting after {duration:?} backoff",
                            name = reader.name,
                            duration = backoff_duration.unwrap(),
                        );
                    }

                    _ = sleep_for_retry(backoff_duration.take()) => {
                        reader.restart_after_backoff(
                            &session_rx, &config_rx, &metadata,
                            Arc::clone(&metrics),
                            &tx_embeddings, &internals,
                        ).await;
                    }
                }
            }

            // Cleanup
            reader.stop().await;

            internals
                .increment_counter(format!("{actor_key}-{name}-cdc-actor-stopped"))
                .await;
            debug!("finished");
        }
        .instrument(error_span!("db_cdc", "{}-{}", span_key, name)),
    );

    tx
}

/// State for managing a CDC reader's lifecycle.
struct CdcReaderState {
    reader: Option<scylla_cdc::log_reader::CDCLogReader>,
    handler_task: Option<tokio::task::JoinHandle<Duration>>,
    shutdown_notify: Arc<Notify>,
    error_notify: Arc<Notify>,
    semaphore: Arc<Semaphore>,
    start: Duration,
    name: &'static str,
    params_fn: fn(&Config) -> CdcReaderParams,
}

impl CdcReaderState {
    fn new(config: CdcReaderConfig, semaphore: Arc<Semaphore>) -> Self {
        let ctor = |name: &'static str, params_fn: fn(&Config) -> CdcReaderParams| Self {
            reader: None,
            handler_task: None,
            shutdown_notify: Arc::new(Notify::new()),
            error_notify: Arc::new(Notify::new()),
            semaphore,
            start: cdc_now(),
            name,
            params_fn,
        };
        match config {
            CdcReaderConfig::Wide => ctor("wide", CdcReaderParams::wide),
            CdcReaderConfig::Fine => ctor("fine", CdcReaderParams::fine),
        }
    }

    /// Stops the current CDC reader and handler task, preserving the last checkpoint.
    async fn stop(&mut self) {
        if let Some(mut reader) = self.reader.take() {
            reader.stop();
        }
        if let Some(task) = self.handler_task.take() {
            self.shutdown_notify.notify_one();
            self.start = task.await.unwrap_or(cdc_now());
        }
    }

    /// Stops the current reader, drains stale notifications, and starts a new reader with the given parameters.
    async fn restart(
        &mut self,
        params: CdcReaderParams,
        session: &Arc<Session>,
        metadata: &IndexMetadata,
        metrics: Arc<Metrics>,
        tx_embeddings: &mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
        internals: &Sender<Internals>,
    ) {
        self.stop().await;
        drain_pending_notifications(&self.shutdown_notify);

        match create_cdc_reader(
            self.start,
            params.clone(),
            Arc::clone(session),
            metadata.clone(),
            metrics,
            tx_embeddings.clone(),
            Arc::clone(&self.semaphore),
            self.name,
        )
        .await
        {
            Ok((reader, handler)) => {
                self.reader = Some(reader);
                self.handler_task = Some(spawn_handler_task(
                    handler,
                    Arc::clone(&self.shutdown_notify),
                    Arc::clone(&self.error_notify),
                    internals.clone(),
                    metadata,
                    self.name,
                ));

                info!(
                    "{} CDC reader created successfully for {} (safety: {:?}, sleep: {:?})",
                    self.name,
                    metadata.key(),
                    params.safety_interval,
                    params.sleep_interval
                );
            }
            Err(e) => {
                error!("Failed to create {} CDC reader: {e}", self.name);
                self.error_notify.notify_one();
            }
        }
    }

    /// Handles a session change by restarting or stopping the CDC reader.
    async fn handle_session_change(
        &mut self,
        session: Option<Arc<Session>>,
        config_rx: &watch::Receiver<Arc<Config>>,
        metadata: &IndexMetadata,
        metrics: Arc<Metrics>,
        tx_embeddings: &mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
        internals: &Sender<Internals>,
    ) {
        match session {
            Some(session) => {
                info!(
                    "Session available, creating {} CDC reader for {}",
                    self.name,
                    metadata.key()
                );

                let config = config_rx.borrow().clone();

                drain_pending_notifications(&self.error_notify);
                let params = (self.params_fn)(&config);
                self.restart(
                    params,
                    &session,
                    metadata,
                    metrics,
                    tx_embeddings,
                    internals,
                )
                .await;
            }
            None => {
                info!(
                    "Session became None, stopping {} CDC reader for {}",
                    self.name,
                    metadata.key()
                );

                self.stop().await;
            }
        }
    }

    /// Completes a pending backoff by restarting the CDC reader.
    async fn restart_after_backoff(
        &mut self,
        session_rx: &watch::Receiver<Option<Arc<Session>>>,
        config_rx: &watch::Receiver<Arc<Config>>,
        metadata: &IndexMetadata,
        metrics: Arc<Metrics>,
        tx_embeddings: &mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
        internals: &Sender<Internals>,
    ) {
        let session = session_rx.borrow().clone();
        if let Some(session) = session {
            let config = config_rx.borrow().clone();
            let params = (self.params_fn)(&config);
            self.restart(
                params,
                &session,
                metadata,
                metrics,
                tx_embeddings,
                internals,
            )
            .await;
        }
    }
}

fn cdc_now() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

/// Sleep for the specified duration, or indefinitely if None, to implement backoff.
async fn sleep_for_retry(duration: Option<Duration>) {
    if let Some(duration) = duration {
        time::sleep(duration).await;
    } else {
        future::pending::<()>().await;
    }
}

/// Drains any pending shutdown notifications to avoid stale wakeups.
fn drain_pending_notifications(notify: &Notify) {
    while notify.notified().now_or_never().is_some() {}
}

/// Creates a CDC log reader with the given parameters.
#[allow(clippy::too_many_arguments)]
async fn create_cdc_reader(
    start: Duration,
    params: CdcReaderParams,
    session: Arc<Session>,
    metadata: IndexMetadata,
    metrics: Arc<Metrics>,
    tx_embeddings: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
    semaphore: Arc<Semaphore>,
    reader_name: &str,
) -> anyhow::Result<(
    scylla_cdc::log_reader::CDCLogReader,
    impl std::future::Future<Output = anyhow::Result<()>>,
)> {
    let consumer_factory = CdcConsumerFactory::new(
        Arc::clone(&session),
        &metadata,
        metrics,
        tx_embeddings,
        semaphore,
    )
    .await?;

    let cdc_start = start - CHECKPOINT_TIMESTAMP_OFFSET;
    info!(
        "Creating {reader_name} CDC log reader for {} starting from {:?}",
        metadata.key(),
        OffsetDateTime::UNIX_EPOCH + cdc_start
    );

    CDCLogReaderBuilder::new()
        .session(session)
        .keyspace(metadata.keyspace_name.as_ref())
        .table_name(metadata.table_name.as_ref())
        .consumer_factory(Arc::new(consumer_factory))
        .start_timestamp(chrono::Duration::from_std(cdc_start)?)
        .safety_interval(params.safety_interval)
        .sleep_interval(params.sleep_interval)
        .build()
        .await
        .context(format!("Failed to build {reader_name} CDC log reader"))
}

/// Spawns a task that runs the CDC handler future until completion or shutdown.
fn spawn_handler_task(
    handler: impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    shutdown_notify: Arc<Notify>,
    cdc_error_notify: Arc<Notify>,
    internals: Sender<Internals>,
    metadata: &IndexMetadata,
    reader_name: &str,
) -> tokio::task::JoinHandle<Duration> {
    let handler_key = metadata.key();
    let span_name = format!("{reader_name}_cdc_handler");
    let started_counter_name = format!("{handler_key}-{reader_name}-cdc-handler-started");
    let stopped_counter_name = format!("{handler_key}-{reader_name}-cdc-handler-stopped");
    let errors_counter_name = format!("{handler_key}-{reader_name}-cdc-handler-errors");

    tokio::spawn(
        async move {
            internals.increment_counter(started_counter_name).await;
            tokio::select! {
                result = handler => {
                    if let Err(err) = result {
                        warn!("CDC handler error: {err}");
                        internals.increment_counter(errors_counter_name).await;
                        cdc_error_notify.notify_one();
                    }
                }
                _ = shutdown_notify.notified() => {
                    debug!("CDC handler: shutdown requested");
                }
            }
            internals.increment_counter(stopped_counter_name).await;
            debug!("CDC handler finished");
            cdc_now()
        }
        .instrument(error_span!("cdc_handler", "{}", span_name)),
    )
}

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

        let target_columns_len = self.target_columns.len().get();
        let columns_len_expected = (target_columns_len + self.filtering_columns.len()) * 2;
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
                // These operations are unspported by our CDC reader, skip them.
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

struct CdcConsumerFactory(Arc<CdcConsumerData>);

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
    async fn new(
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

        let query = db_index_backend::request_query(
            &metadata.keyspace_name.as_ref().into(),
            &metadata.table_name.as_ref().into(),
            target_columns.iter().chain(filtering_columns.iter()),
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
            target_columns,
            filtering_columns,
            kind: metadata.kind.clone(),
            tx,
            metrics,
            semaphore,
        })))
    }
}
