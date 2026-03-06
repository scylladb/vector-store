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
use futures::future::select_all;
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

// Default parameters for the wide-framed CDC reader (consistency-focused).
const DEFAULT_CONSISTENT_SAFETY_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_CONSISTENT_SLEEP_INTERVAL: Duration = Duration::from_secs(10);

// Default parameters for the fine-grained CDC reader (latency-focused).
const DEFAULT_REALTIME_SAFETY_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_REALTIME_SLEEP_INTERVAL: Duration = Duration::from_millis(500);

/// Maximum consecutive errors before a CDC reader with Backoff policy escalates to session teardown.
const DEFAULT_MAX_CONSECUTIVE_ERRORS: u32 = 3;

/// Base backoff duration for CDC reader restarts after errors.
/// The actual backoff is `DEFAULT_BACKOFF_BASE * consecutive_error_count`.
const DEFAULT_BACKOFF_BASE: Duration = Duration::from_secs(5);

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
                .cdc_wide_safety_interval
                .unwrap_or(DEFAULT_CONSISTENT_SAFETY_INTERVAL),
            sleep_interval: config
                .cdc_wide_sleep_interval
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

/// Spawns a CDC actor that watches for session changes and manages the CDC readers lifecycle.
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

            let session_error_notify = Arc::clone(&cdc_error_notify);

            let mut readers = vec![
                CdcReaderState::new(
                    "wide",
                    CdcErrorPolicy::propagate(cdc_error_notify),
                    CdcReaderParams::wide,
                ),
                CdcReaderState::new(
                    "fine",
                    CdcErrorPolicy::backoff_and_retry(session_error_notify),
                    CdcReaderParams::fine,
                ),
            ];

            loop {
                tokio::select! {
                    // Wait for session changes
                    result = session_rx.changed() => {
                        if result.is_err() {
                            break;
                        }

                        let session_opt = session_rx.borrow_and_update().clone();
                        handle_session_change(
                            &mut readers, session_opt, &config_rx,
                            &metadata, &tx_embeddings, &internals,
                        ).await;
                    }

                    (index, _, _) = select_all(
                        readers.iter().enumerate().map(|(i, r)| {
                            let n = Arc::clone(&r.error_notify);
                            Box::pin(async move { n.notified().await; i })
                        })
                    ) => {
                        readers[index].handle_error(
                            &session_rx, &config_rx, &metadata,
                            &tx_embeddings, &internals,
                        ).await;
                    }
                }
            }

            // Cleanup
            stop_all(&mut readers).await;

            debug!("finished");
        }
        .instrument(error_span!("db_cdc", "{}", actor_key)),
    );
}

/// Handles a session change by restarting or stopping all CDC readers.
async fn handle_session_change(
    readers: &mut [CdcReaderState],
    session_opt: Option<Arc<Session>>,
    config_rx: &watch::Receiver<Arc<Config>>,
    metadata: &IndexMetadata,
    tx_embeddings: &mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
    internals: &Sender<Internals>,
) {
    match session_opt {
        Some(session) => {
            info!(
                "Session available, creating CDC readers for {}",
                metadata.key()
            );

            let config = config_rx.borrow().clone();

            for reader in readers.iter_mut() {
                reader.reset_on_session_change();
                let params = (reader.params_fn)(&config);
                reader
                    .restart(params, &session, metadata, tx_embeddings, internals)
                    .await;
            }
        }
        None => {
            info!(
                "Session became None, stopping CDC readers for {}",
                metadata.key()
            );

            stop_all(readers).await;
        }
    }
}

/// Stops all CDC readers.
async fn stop_all(readers: &mut [CdcReaderState]) {
    for reader in readers.iter_mut() {
        reader.stop().await;
    }
}

/// Defines how a CDC reader handles errors from its handler task.
enum CdcErrorPolicy {
    /// Propagate errors to the session level via the provided notify,
    /// causing a session teardown and reconnect.
    Propagate(Arc<Notify>),
    /// Handle errors locally with backoff and retry.
    /// After `max_consecutive_errors` consecutive failures, escalate to
    /// session teardown via `session_notify`.
    BackoffAndRetry {
        max_consecutive_errors: u32,
        backoff_base: Duration,
        consecutive_errors: u32,
        session_notify: Arc<Notify>,
    },
}

impl CdcErrorPolicy {
    fn propagate(session_notify: Arc<Notify>) -> Self {
        Self::Propagate(session_notify)
    }

    fn backoff_and_retry(session_notify: Arc<Notify>) -> Self {
        Self::BackoffAndRetry {
            max_consecutive_errors: DEFAULT_MAX_CONSECUTIVE_ERRORS,
            backoff_base: DEFAULT_BACKOFF_BASE,
            consecutive_errors: 0,
            session_notify,
        }
    }
}

/// Per-reader state for managing a single CDC reader's lifecycle.
struct CdcReaderState {
    reader: Option<scylla_cdc::log_reader::CDCLogReader>,
    handler_task: Option<tokio::task::JoinHandle<Duration>>,
    shutdown_notify: Arc<Notify>,
    error_notify: Arc<Notify>,
    start: Duration,
    name: &'static str,
    error_policy: CdcErrorPolicy,
    params_fn: fn(&Config) -> CdcReaderParams,
}

impl CdcReaderState {
    fn new(
        name: &'static str,
        error_policy: CdcErrorPolicy,
        params_fn: fn(&Config) -> CdcReaderParams,
    ) -> Self {
        Self {
            reader: None,
            handler_task: None,
            shutdown_notify: Arc::new(Notify::new()),
            error_notify: Arc::new(Notify::new()),
            start: cdc_now(),
            name,
            error_policy,
            params_fn,
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

    /// Drains stale error notifications and resets the consecutive error counter.
    fn reset_on_session_change(&mut self) {
        let _ = pin!(self.error_notify.notified()).enable();
        if let CdcErrorPolicy::BackoffAndRetry {
            consecutive_errors, ..
        } = &mut self.error_policy
        {
            *consecutive_errors = 0;
        }
    }

    /// Stops the current reader, drains stale notifications, and starts a new reader with the given parameters.
    async fn restart(
        &mut self,
        params: CdcReaderParams,
        session: &Arc<Session>,
        metadata: &IndexMetadata,
        tx_embeddings: &mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
        internals: &Sender<Internals>,
    ) {
        self.stop().await;
        drain_pending_notifications(&self.shutdown_notify).await;

        match create_cdc_reader(
            self.start,
            params.clone(),
            Arc::clone(session),
            metadata.clone(),
            tx_embeddings.clone(),
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
                    "{} CDC reader created for {} (safety: {:?}, sleep: {:?})",
                    self.name,
                    metadata.key(),
                    params.safety_interval,
                    params.sleep_interval
                );
            }
            Err(e) => {
                error!("Failed to create {} CDC reader: {e}", self.name);
            }
        }
    }

    /// Handles an error notification according to the reader's [`CdcErrorPolicy`].
    async fn handle_error(
        &mut self,
        session_rx: &watch::Receiver<Option<Arc<Session>>>,
        config_rx: &watch::Receiver<Arc<Config>>,
        metadata: &IndexMetadata,
        tx_embeddings: &mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
        internals: &Sender<Internals>,
    ) {
        match &mut self.error_policy {
            CdcErrorPolicy::Propagate(session_notify) => {
                session_notify.notify_one();
            }
            CdcErrorPolicy::BackoffAndRetry {
                max_consecutive_errors,
                backoff_base,
                consecutive_errors,
                session_notify,
            } => {
                *consecutive_errors += 1;
                let count = *consecutive_errors;
                let limit = *max_consecutive_errors;

                if count >= limit {
                    let session_notify = Arc::clone(session_notify);
                    warn!(
                        "{} CDC reader failed {count} consecutive times, escalating to session teardown",
                        self.name
                    );
                    session_notify.notify_one();
                } else {
                    let backoff = *backoff_base * count;
                    warn!(
                        "{} CDC reader error ({count}/{limit}), restarting after {backoff:?} backoff",
                        self.name,
                    );
                    time::sleep(backoff).await;

                    let session = session_rx.borrow().clone();
                    if let Some(session) = session {
                        let config = config_rx.borrow().clone();
                        let params = (self.params_fn)(&config);
                        self.restart(params, &session, metadata, tx_embeddings, internals)
                            .await;
                    }
                }
            }
        }
    }
}

fn cdc_now() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

/// Drains any pending shutdown notifications to avoid stale wakeups.
async fn drain_pending_notifications(notify: &Notify) {
    if pin!(notify.notified()).enable() {
        while pin!(notify.notified()).enable() {
            error!("Internal error: unable to cleanup CDC reader. Retrying.");
            time::sleep(Duration::from_secs(1)).await;
        }
    }
}

/// Creates a CDC log reader with the given parameters.
async fn create_cdc_reader(
    start: Duration,
    params: CdcReaderParams,
    session: Arc<Session>,
    metadata: IndexMetadata,
    tx_embeddings: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
    reader_name: &str,
) -> anyhow::Result<(
    scylla_cdc::log_reader::CDCLogReader,
    impl std::future::Future<Output = anyhow::Result<()>>,
)> {
    let consumer_factory = CdcConsumerFactory::new(Arc::clone(&session), &metadata, tx_embeddings)?;

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
    let counter_name = format!("{handler_key}-{reader_name}-cdc-handler-errors");

    tokio::spawn(
        async move {
            tokio::select! {
                result = handler => {
                    if let Err(err) = result {
                        warn!("CDC handler error: {err}");
                        internals.increment_counter(counter_name).await;
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
        .instrument(error_span!("cdc_handler", "{}", span_name)),
    )
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
