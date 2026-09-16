/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::AsyncInProgress;
use crate::Config;
use crate::DbDriver;
use crate::DbIndexedRow;
use crate::IndexMetadata;
use crate::IndexName;
use crate::KeyspaceName;
use crate::Metrics;
use crate::db_cdc::READER_FINE;
use crate::db_cdc::READER_WIDE;
use crate::db_cdc::checkpoint_saver::MetricsCheckpointSaver;
use crate::db_cdc::consumer::CdcConsumerFactory;
use crate::db_driver::CdcLogReaderConfig;
use crate::db_index::DbIndexSession;
use crate::internals::Internals;
use crate::internals::InternalsExt;
use crate::perf;
use ::time::OffsetDateTime;
use anyhow::Context;
use futures::FutureExt;
use scylla::client::session::Session;
use std::future;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
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

impl CdcReaderConfig {
    fn name_and_params_fn(self) -> (&'static str, fn(&Config) -> CdcReaderParams) {
        match self {
            CdcReaderConfig::Wide => (READER_WIDE, CdcReaderParams::wide),
            CdcReaderConfig::Fine => (READER_FINE, CdcReaderParams::fine),
        }
    }
}

/// Spawns a CDC actor that watches for session changes and manages a CDC reader.
#[allow(clippy::too_many_arguments)]
pub(crate) fn new<T: DbDriver>(
    config_rx: watch::Receiver<Arc<Config>>,
    db_driver: T,
    mut db_session: DbIndexSession,
    metadata: IndexMetadata,
    internals: mpsc::Sender<Internals>,
    metrics: Arc<Metrics>,
    tx_embeddings: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
    semaphore: Arc<Semaphore>,
    config: CdcReaderConfig,
) -> mpsc::Sender<DbCdc> {
    let (tx, mut rx) = mpsc::channel::<DbCdc>(perf::channel_size().into());

    // Mark the db_session to ensure first session update is visible
    db_session.mark_changed();

    let (name, params_fn) = config.name_and_params_fn();
    let mut reader = CdcReaderState::new(
        db_driver.clone(),
        name,
        params_fn,
        Arc::clone(&metrics),
        metadata.keyspace_name.clone(),
        metadata.index_name.clone(),
        semaphore,
    );
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
                    session = db_session.wait_for_changed() => {
                        let Some(session) = session else {
                            // Db index dropped
                            break;
                        };

                        reader.handle_session_change(
                            session, &config_rx, &metadata,
                            &tx_embeddings, &internals,
                        ).await;
                        err_counter = 0;
                        backoff_duration = None;
                    }

                    _ = reader.error_notify.notified() => {
                        err_counter += 1;
                        backoff_duration = Some(DEFAULT_BACKOFF_DURATION);
                        reader.set_reader_metric_down();
                        warn!(
                            "{name} CDC reader error {err_counter}, restarting after {duration:?} backoff",
                            name = reader.name,
                            duration = backoff_duration.unwrap(),
                        );
                    }

                    _ = sleep_for_retry(backoff_duration.take()) => {
                        let Some(session) = db_session.current() else {
                            // Db index dropped
                            break;
                        };
                        reader.restart_after_backoff(
                            session, &config_rx, &metadata,
                            &tx_embeddings, &internals,
                        ).await;
                    }
                }
            }

            // Cleanup
            reader.stop().await;
            reader.remove_metrics();

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
struct CdcReaderState<T: DbDriver> {
    db_driver: T,
    reader: Option<T::CdcLogReader>,
    handler_task: Option<tokio::task::JoinHandle<Duration>>,
    shutdown_notify: Arc<Notify>,
    error_notify: Arc<Notify>,
    semaphore: Arc<Semaphore>,
    start: Duration,
    name: &'static str,
    params_fn: fn(&Config) -> CdcReaderParams,
    metrics: Arc<Metrics>,
    keyspace: KeyspaceName,
    index_name: IndexName,
}

impl<T: DbDriver> CdcReaderState<T> {
    fn new(
        db_driver: T,
        name: &'static str,
        params_fn: fn(&Config) -> CdcReaderParams,
        metrics: Arc<Metrics>,
        keyspace: KeyspaceName,
        index_name: IndexName,
        semaphore: Arc<Semaphore>,
    ) -> Self {
        let state = Self {
            db_driver,
            reader: None,
            handler_task: None,
            shutdown_notify: Arc::new(Notify::new()),
            error_notify: Arc::new(Notify::new()),
            semaphore,
            start: cdc_now(),
            name,
            params_fn,
            metrics,
            keyspace,
            index_name,
        };
        state.set_reader_metric_down();
        state
    }

    /// Sets the `cdc_reader_up` gauge to 1 (running) for this reader.
    fn set_reader_metric_up(&self) {
        self.metrics
            .cdc_reader_up
            .with_label_values(&[self.keyspace.as_ref(), self.index_name.as_ref(), self.name])
            .set(1.0);
    }

    /// Sets the `cdc_reader_up` gauge to 0 (stopped) for this reader.
    fn set_reader_metric_down(&self) {
        self.metrics
            .cdc_reader_up
            .with_label_values(&[self.keyspace.as_ref(), self.index_name.as_ref(), self.name])
            .set(0.0);
    }

    /// Removes this reader's metric series.
    /// Must only be called after the actor loop has exited.
    /// Calling it earlier races with updates that would recreate the series.
    fn remove_metrics(&self) {
        self.metrics.remove_reader_labels(
            self.keyspace.as_ref(),
            self.index_name.as_ref(),
            self.name,
        );
    }

    /// Stops the current CDC reader and handler task, preserving the last checkpoint.
    async fn stop(&mut self) {
        if let Some(reader) = self.reader.take() {
            self.db_driver.stop_cdc_log_reader(reader);
        }
        if let Some(task) = self.handler_task.take() {
            self.shutdown_notify.notify_one();
            self.start = task.await.unwrap_or(cdc_now());
        }
        self.set_reader_metric_down();
    }

    /// Stops the current reader, drains stale notifications, and starts a new reader with the given parameters.
    async fn restart(
        &mut self,
        params: CdcReaderParams,
        session: &Arc<Session>,
        metadata: &IndexMetadata,
        tx_embeddings: &mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
        internals: &mpsc::Sender<Internals>,
    ) {
        self.stop().await;
        drain_pending_notifications(&self.shutdown_notify);

        match create_cdc_reader(
            self.start,
            params.clone(),
            self.db_driver.clone(),
            Arc::clone(session),
            metadata.clone(),
            tx_embeddings.clone(),
            Arc::clone(&self.semaphore),
            Arc::clone(&self.metrics),
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
                    Arc::clone(&self.metrics),
                    metadata,
                    self.name,
                ));

                self.set_reader_metric_up();

                info!(
                    "{} CDC reader created successfully for {} (safety: {:?}, sleep: {:?})",
                    self.name,
                    metadata.key(),
                    params.safety_interval,
                    params.sleep_interval
                );
            }
            Err(e) => {
                error!("Failed to create {} CDC reader: {e:#}", self.name);
                self.set_reader_metric_down();
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
        tx_embeddings: &mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
        internals: &mpsc::Sender<Internals>,
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
                self.restart(params, &session, metadata, tx_embeddings, internals)
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
        session: Option<Arc<Session>>,
        config_rx: &watch::Receiver<Arc<Config>>,
        metadata: &IndexMetadata,
        tx_embeddings: &mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
        internals: &mpsc::Sender<Internals>,
    ) {
        if let Some(session) = session {
            record_reader_restart(&self.metrics, &self.keyspace, &self.index_name, self.name);
            let config = config_rx.borrow().clone();
            let params = (self.params_fn)(&config);
            self.restart(params, &session, metadata, tx_embeddings, internals)
                .await;
        }
    }
}

/// Increments the `cdc_handler_errors_total` counter for the given index and reader.
fn record_handler_error(
    metrics: &Metrics,
    keyspace: &KeyspaceName,
    index_name: &IndexName,
    reader: &str,
) {
    metrics
        .cdc_handler_errors_total
        .with_label_values(&[keyspace.as_ref(), index_name.as_ref(), reader])
        .inc();
}

/// Increments the `cdc_reader_restarts_total` counter for the given index and reader.
fn record_reader_restart(
    metrics: &Metrics,
    keyspace: &KeyspaceName,
    index_name: &IndexName,
    reader: &str,
) {
    metrics
        .cdc_reader_restarts_total
        .with_label_values(&[keyspace.as_ref(), index_name.as_ref(), reader])
        .inc();
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
async fn create_cdc_reader<T: DbDriver>(
    start: Duration,
    params: CdcReaderParams,
    db_driver: T,
    session: Arc<Session>,
    metadata: IndexMetadata,
    tx_embeddings: mpsc::Sender<(DbIndexedRow, AsyncInProgress)>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    reader_name: &str,
) -> anyhow::Result<(
    T::CdcLogReader,
    impl std::future::Future<Output = anyhow::Result<()>>,
)> {
    let consumer_factory = CdcConsumerFactory::new(
        db_driver.clone(),
        Arc::clone(&session),
        &metadata,
        Arc::clone(&metrics),
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

    let checkpoint_saver = Arc::new(MetricsCheckpointSaver::new(
        metrics,
        metadata.keyspace_name.as_ref().to_string(),
        metadata.index_name.as_ref().to_string(),
        reader_name.to_string(),
    ));

    db_driver
        .cdc_log_reader(
            session,
            CdcLogReaderConfig::default()
                .keyspace(metadata.keyspace_name)
                .table_name(metadata.table_name)
                .consumer_factory(Arc::new(consumer_factory))
                .start_timestamp(cdc_start)
                .safety_interval(params.safety_interval)
                .sleep_interval(params.sleep_interval)
                .should_save_progress(true)
                .checkpoint_saver(checkpoint_saver),
        )
        .await
        .context(format!("Failed to build {reader_name} CDC log reader"))
}

/// Spawns a task that runs the CDC handler future until completion or shutdown.
fn spawn_handler_task(
    handler: impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    shutdown_notify: Arc<Notify>,
    cdc_error_notify: Arc<Notify>,
    internals: mpsc::Sender<Internals>,
    metrics: Arc<Metrics>,
    metadata: &IndexMetadata,
    reader_name: &str,
) -> tokio::task::JoinHandle<Duration> {
    let handler_key = metadata.key();
    let span_name = format!("{reader_name}_cdc_handler");
    let started_counter_name = format!("{handler_key}-{reader_name}-cdc-handler-started");
    let stopped_counter_name = format!("{handler_key}-{reader_name}-cdc-handler-stopped");
    let errors_counter_name = format!("{handler_key}-{reader_name}-cdc-handler-errors");
    let keyspace = metadata.keyspace_name.clone();
    let index_name = metadata.index_name.clone();
    let reader_name = reader_name.to_string();

    tokio::spawn(
        async move {
            internals.increment_counter(started_counter_name).await;
            tokio::select! {
                result = handler => {
                    if let Err(err) = result {
                        warn!("CDC handler error: {err}");
                        internals.increment_counter(errors_counter_name).await;
                        record_handler_error(&metrics, &keyspace, &index_name, &reader_name);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_driver::tests::UnimplementedDbDriver;
    use prometheus::Encoder;
    use prometheus::TextEncoder;

    fn metric_families_text(metrics: &Metrics) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&metrics.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn new_state<T: DbDriver>(db_driver: T, metrics: Arc<Metrics>) -> CdcReaderState<T> {
        CdcReaderState::new(
            db_driver,
            READER_WIDE,
            CdcReaderParams::wide,
            metrics,
            "ks".into(),
            "idx".into(),
            Semaphore::new(1).into(),
        )
    }

    #[test]
    fn new_state_initializes_reader_up_gauge_to_zero() {
        let metrics = Arc::new(Metrics::new());

        // The gauge must be present immediately, even for a reader that never starts.
        let _state = new_state(UnimplementedDbDriver, Arc::clone(&metrics));

        let output = metric_families_text(&metrics);
        assert!(
            output.contains(r#"cdc_reader_up{index_name="idx",keyspace="ks",reader="wide"} 0"#),
            "expected cdc_reader_up=0 immediately after creation, got:\n{output}"
        );
    }

    #[tokio::test]
    async fn stop_on_freshly_created_state_sets_reader_up_to_zero() {
        let metrics = Arc::new(Metrics::new());
        let mut state = new_state(UnimplementedDbDriver, Arc::clone(&metrics));

        // Never started: `stop()` should be a no-op but must still report the reader as down.
        state.stop().await;

        let output = metric_families_text(&metrics);
        assert!(
            output.contains(r#"cdc_reader_up{index_name="idx",keyspace="ks",reader="wide"} 0"#)
        );
    }
}
