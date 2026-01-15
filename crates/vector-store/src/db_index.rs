/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::ColumnName;
use crate::Config;
use crate::DbEmbedding;
use crate::IndexMetadata;
use crate::KeyspaceName;
use crate::Percentage;
use crate::Progress;
use crate::TableName;
use crate::node_state::Event;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::errors::PagerExecutionError;
use scylla::routing::Token;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;
use scylla::value::Row;
use scylla_cdc::consumer::CDCRow;
use scylla_cdc::consumer::Consumer;
use scylla_cdc::consumer::ConsumerFactory;
use scylla_cdc::log_reader::CDCLogReaderBuilder;
use std::collections::HashMap;
use std::iter;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tap::Pipe;
use time::Date;
use time::OffsetDateTime;
use time::Time;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

type GetPrimaryKeyColumnsR = Arc<Vec<ColumnName>>;
type GetTableColumnsR = Arc<HashMap<ColumnName, NativeType>>;
type RangeScanResult =
    anyhow::Result<Pin<Box<dyn Stream<Item = DbEmbedding> + std::marker::Send>>, anyhow::Error>;
const START_RETRY_TIMEOUT: Duration = Duration::from_millis(100);
const RETRY_TIMEOUT_LIMIT: Duration = Duration::from_secs(16);
const INCREASE_RATE: u32 = 2;

impl From<u64> for Percentage {
    fn from(value: u64) -> Self {
        Percentage::try_from((value as f64 / u64::MAX as f64) * 100.0).unwrap()
    }
}

impl From<u64> for Progress {
    fn from(value: u64) -> Self {
        if value == u64::MAX {
            Progress::Done
        } else {
            Progress::InProgress(Percentage::from(value))
        }
    }
}

pub enum DbIndex {
    GetPrimaryKeyColumns {
        tx: oneshot::Sender<GetPrimaryKeyColumnsR>,
    },
    GetTableColumns {
        tx: oneshot::Sender<GetTableColumnsR>,
    },
    FullScanProgress {
        tx: oneshot::Sender<Progress>,
    },
}

pub(crate) trait DbIndexExt {
    async fn get_primary_key_columns(&self) -> GetPrimaryKeyColumnsR;
    async fn get_table_columns(&self) -> GetTableColumnsR;
    async fn full_scan_progress(&self) -> Progress;
}

impl DbIndexExt for mpsc::Sender<DbIndex> {
    async fn get_primary_key_columns(&self) -> GetPrimaryKeyColumnsR {
        let (tx, rx) = oneshot::channel();
        self.send(DbIndex::GetPrimaryKeyColumns { tx })
            .await
            .expect("internal actor should receive request");
        rx.await.expect("internal actor should send response")
    }

    async fn get_table_columns(&self) -> GetTableColumnsR {
        let (tx, rx) = oneshot::channel();
        self.send(DbIndex::GetTableColumns { tx })
            .await
            .expect("internal actor should receive request");
        rx.await.expect("internal actor should send response")
    }

    async fn full_scan_progress(&self) -> Progress {
        let (tx, rx) = oneshot::channel();
        self.send(DbIndex::FullScanProgress { tx })
            .await
            .expect("internal actor should receive request");
        rx.await.expect("internal actor should send response")
    }
}

pub(crate) async fn new(
    mut config_rx: watch::Receiver<Arc<Config>>,
    mut session_rx: watch::Receiver<Option<Arc<Session>>>,
    metadata: IndexMetadata,
    node_state: Sender<NodeState>,
) -> anyhow::Result<(
    mpsc::Sender<DbIndex>,
    mpsc::Receiver<(DbEmbedding, Option<AsyncInProgress>)>,
)> {
    let id = metadata.id();

    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx_index, mut rx_index) = mpsc::channel(CHANNEL_SIZE);
    let (tx_embeddings, rx_embeddings) = mpsc::channel(CHANNEL_SIZE);

    // Mark the receiver to ensure first session update is visible
    session_rx.mark_changed();

    let mut statements_session_rx = session_rx.clone();
    let cdc_metadata = metadata.clone();
    let cdc_tx_embeddings = tx_embeddings.clone();
    let cdc_id = id.clone();
    let cdc_manager_id = id.clone();

    // Spawn CDC management task - handles session changes and CDC reader lifecycle
    tokio::spawn(
        async move {
            debug!("CDC manager starting");

            let mut cdc_reader: Option<scylla_cdc::log_reader::CDCLogReader> = None;
            let mut cdc_handler_task: Option<tokio::task::JoinHandle<()>> = None;
            let shutdown_notify = Arc::new(Notify::new());

            loop {
                // Wait for session changes
                if session_rx.changed().await.is_err() {
                    // Session sender dropped, cleanup and exit
                    if let Some(mut reader) = cdc_reader {
                        reader.stop();
                    }
                    if let Some(task) = cdc_handler_task {
                        shutdown_notify.notify_one();
                        task.abort();
                    }
                    break;
                }

                let session_opt = session_rx.borrow_and_update().clone();

                match session_opt {
                    Some(session) => {
                        info!(
                            "Session available, creating CDC reader for {}",
                            cdc_metadata.id()
                        );

                        // Stop old CDC reader if exists
                        if let Some(mut reader) = cdc_reader.take() {
                            reader.stop();
                        }
                        if let Some(task) = cdc_handler_task.take() {
                            shutdown_notify.notify_one();
                            task.abort();
                        }

                        // Create new CDC reader
                        let config = config_rx.borrow_and_update().clone();
                        match create_cdc_reader(
                            config,
                            session,
                            cdc_metadata.clone(),
                            cdc_tx_embeddings.clone(),
                        )
                        .await
                        {
                            Ok((reader, handler)) => {
                                cdc_reader = Some(reader);

                                // Spawn CDC handler task
                                let shutdown_notify_clone = Arc::clone(&shutdown_notify);
                                let handler_id = cdc_id.clone();
                                cdc_handler_task = Some(tokio::spawn(
                                    async move {
                                        tokio::select! {
                                            result = handler => {
                                                if let Err(err) = result {
                                                    debug!("CDC handler error: {err}");
                                                }
                                            }
                                            _ = shutdown_notify_clone.notified() => {
                                                debug!("CDC handler: shutdown requested");
                                            }
                                        }
                                        debug!("CDC handler finished");
                                    }
                                    .instrument(debug_span!("cdc", "{}", handler_id)),
                                ));

                                info!("CDC reader created successfully for {}", cdc_metadata.id());
                            }
                            Err(e) => {
                                error!("Failed to create CDC reader: {}", e);
                            }
                        }
                    }
                    None => {
                        info!(
                            "Session became None, stopping CDC reader for {}",
                            cdc_metadata.id()
                        );

                        // Stop CDC reader
                        if let Some(mut reader) = cdc_reader.take() {
                            reader.stop();
                        }
                        if let Some(task) = cdc_handler_task.take() {
                            shutdown_notify.notify_one();
                            task.abort();
                        }
                    }
                }
            }

            debug!("CDC manager finished");
        }
        .instrument(debug_span!("cdc_manager", "{}", cdc_manager_id)),
    );

    // Wait for initial session to create statements
    while statements_session_rx.borrow().is_none() {
        if statements_session_rx.changed().await.is_err() {
            return Err(anyhow::anyhow!(
                "Session sender dropped before initialization"
            ));
        }
    }

    let statements = Arc::new(Statements::new(statements_session_rx, metadata.clone()).await?);

    // Spawn main task for full scan and message processing
    tokio::spawn(
        async move {
            debug!("starting");
            let completed_scan_length = Arc::new(AtomicU64::new(0));
            node_state
                .send_event(Event::FullScanStarted(metadata.clone()))
                .await;

            info!("starting full scan on {}", metadata.id());

            let mut initial_scan = Box::pin(statements.initial_scan(
                tx_embeddings.clone(),
                completed_scan_length.clone(),
            ));

            // Initial scan and message processing loop
            loop {
                tokio::select! {
                    _ = &mut initial_scan => {
                        node_state
                            .send_event(Event::FullScanFinished(metadata.clone()))
                            .await;
                        break;
                    }

                    Some(msg) = rx_index.recv() => {
                        tokio::spawn(process(Arc::clone(&statements), msg, completed_scan_length.clone()));
                    }

                    else => {
                        break;
                    }
                }
            }

            info!("finished full scan on {}", metadata.id());

            // Continue processing messages after scan completes
            while let Some(msg) = rx_index.recv().await {
                tokio::spawn(process(Arc::clone(&statements), msg, completed_scan_length.clone()));
            }

            debug!("finished");
        }
        .instrument(debug_span!("db_index", "{}", id)),
    );

    Ok((tx_index, rx_embeddings))
}

// Helper function to create CDC reader - extracted to avoid duplication
async fn create_cdc_reader(
    config: Arc<Config>,
    session: Arc<Session>,
    metadata: IndexMetadata,
    tx_embeddings: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
) -> anyhow::Result<(
    scylla_cdc::log_reader::CDCLogReader,
    impl std::future::Future<Output = anyhow::Result<()>>,
)> {
    let consumer_factory = CdcConsumerFactory::new(Arc::clone(&session), &metadata, tx_embeddings)?;

    CDCLogReaderBuilder::new()
        .session(session)
        .keyspace(metadata.keyspace_name.as_ref())
        .table_name(metadata.table_name.as_ref())
        .consumer_factory(Arc::new(consumer_factory))
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

async fn process(statements: Arc<Statements>, msg: DbIndex, completed_scan_length: Arc<AtomicU64>) {
    match msg {
        DbIndex::GetPrimaryKeyColumns { tx } => tx
            .send(statements.get_primary_key_columns())
            .unwrap_or_else(|_| {
                trace!("process: Db::GetPrimaryKeyColumns: unable to send response")
            }),
        DbIndex::GetTableColumns { tx } => tx
            .send(statements.get_table_columns())
            .unwrap_or_else(|_| trace!("process: Db::GetTableColumns: unable to send response")),
        DbIndex::FullScanProgress { tx } => {
            let completed_scan_length =
                completed_scan_length.load(std::sync::atomic::Ordering::Relaxed);

            if tx.send(Progress::from(completed_scan_length)).is_err() {
                trace!("process: Db::FullScanProgress: unable to send response");
            }
        }
    }
}

struct Statements {
    session_rx: tokio::sync::watch::Receiver<Option<Arc<Session>>>,
    primary_key_columns: Arc<Vec<ColumnName>>,
    table_columns: GetTableColumnsR,
    st_range_scan: PreparedStatement,
}

impl Statements {
    async fn new(
        session_rx: tokio::sync::watch::Receiver<Option<Arc<Session>>>,
        metadata: IndexMetadata,
    ) -> anyhow::Result<Self> {
        let session = session_rx
            .borrow()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No session available for Statements initialization"))?;
        session.await_schema_agreement().await?;

        let cluster_state = session.get_cluster_state();
        let table = cluster_state
            .get_keyspace(metadata.keyspace_name.as_ref())
            .ok_or_else(|| anyhow!("keyspace {} does not exist", metadata.keyspace_name))?
            .tables
            .get(metadata.table_name.as_ref())
            .ok_or_else(|| anyhow!("table {} does not exist", metadata.table_name))?;

        let primary_key_columns = Arc::new(
            table
                .partition_key
                .iter()
                .chain(table.clustering_key.iter())
                .cloned()
                .map(ColumnName::from)
                .collect_vec(),
        );

        let table_columns = Arc::new(
            table
                .columns
                .iter()
                .filter_map(|(name, coltype)| {
                    if let ColumnType::Native(typ) = &coltype.typ {
                        Some((ColumnName::from(name.clone()), typ.clone()))
                    } else {
                        None
                    }
                })
                .collect(),
        );

        let st_partition_key_list = table.partition_key.iter().join(", ");
        let st_primary_key_list = primary_key_columns.iter().join(", ");

        Ok(Self {
            primary_key_columns,

            table_columns,

            st_range_scan: session
                .prepare(Self::range_scan_query(
                    &metadata.keyspace_name,
                    &metadata.table_name,
                    &st_primary_key_list,
                    &st_partition_key_list,
                    &metadata.target_column,
                ))
                .await
                .context("range_scan_query")?,

            session_rx,
        })
    }

    fn get_primary_key_columns(&self) -> GetPrimaryKeyColumnsR {
        self.primary_key_columns.clone()
    }

    fn get_table_columns(&self) -> GetTableColumnsR {
        self.table_columns.clone()
    }

    fn range_scan_query(
        keyspace: &KeyspaceName,
        table: &TableName,
        st_primary_key_list: &str,
        st_partition_key_list: &str,
        embedding: &ColumnName,
    ) -> String {
        format!(
            "
            SELECT {st_primary_key_list}, {embedding}, writetime({embedding})
            FROM {keyspace}.{table}
            WHERE
                token({st_partition_key_list}) >= ?
                AND token({st_partition_key_list}) <= ?
            BYPASS CACHE
            "
        )
    }

    async fn preform_range_scan(&self, begin: Token, end: Token) -> RangeScanResult {
        let mut range_scan = self.range_scan_stream(begin, end).await;
        let mut retry_timeout = START_RETRY_TIMEOUT;
        while let Err(err) = &range_scan {
            let connection_error = err.downcast_ref::<PagerExecutionError>();
            if connection_error.is_none()
                || !matches!(
                    connection_error.unwrap(),
                    PagerExecutionError::NextPageError(_)
                )
            {
                error!("Fatal error during scan of the range ({begin:?}, {end:?}): {err}");
                break;
            }
            warn!("Lost connection during scan of the range ({begin:?}, {end:?}), retrying");
            tokio::time::sleep(retry_timeout).await;
            range_scan = self.range_scan_stream(begin, end).await;

            // We exponentially increase the timeout in case of repeating errors
            // to decrease the amount of failed connection retries until a limit
            // is reached
            if retry_timeout < RETRY_TIMEOUT_LIMIT {
                retry_timeout *= INCREASE_RATE;
            }
        }
        range_scan
    }

    /// The initial full scan of embeddings stored in a ScyllaDB table. It scans concurrently using
    /// token ranges read from a rust driver. At first it prepares ranges, limits concurrent scans
    /// using semaphore, and runs each scan in separate concurrent task using cloned mpsc channel
    /// to send read embeddings into the pipeline.
    async fn initial_scan(
        &self,
        tx: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
        completed_scan_length: Arc<AtomicU64>,
    ) {
        let semaphore_capacity = self.nr_parallel_queries().get();
        let semaphore = Arc::new(Semaphore::new(semaphore_capacity));

        for (begin, end) in self.fullscan_ranges() {
            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();

            let range_scan = self.preform_range_scan(begin, end).await;
            if let Ok(embeddings) = range_scan {
                let tx = tx.clone();
                let scan_length = completed_scan_length.clone();
                tokio::spawn(async move {
                    let (tx_in_progress, mut rx_in_progress) = mpsc::channel(1);
                    embeddings
                        .for_each(move |embedding| {
                            let tx = tx.clone();
                            let tx_in_progress = tx_in_progress.clone();
                            async move {
                                _ = tx
                                    .send((embedding, Some(AsyncInProgress(tx_in_progress))))
                                    .await;
                            }
                        })
                        .await;

                    // wait until all in-progress markers are dropped
                    while rx_in_progress.recv().await.is_some() {
                        rx_in_progress.len();
                    }

                    //Safety: end > begin, and the range fits into u64
                    scan_length.fetch_add(
                        end.value().abs_diff(begin.value() - 1),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    drop(permit);
                });
            } else {
                drop(permit);
            }
        }

        // Acquire all permits to wait until all spawned tasks have finished and released their permits.
        let _permits = semaphore
            .acquire_many(semaphore_capacity as u32)
            .await
            .unwrap();
    }

    fn nr_shards_in_cluster(&self) -> NonZeroUsize {
        self.session_rx
            .borrow()
            .as_ref()
            .and_then(|session| {
                NonZeroUsize::try_from(
                    session
                        .get_cluster_state()
                        .get_nodes_info()
                        .iter()
                        .filter_map(|node| node.sharder())
                        .map(|sharder| sharder.nr_shards.get() as usize)
                        .sum::<usize>(),
                )
                .ok()
            })
            .unwrap_or(NonZeroUsize::new(1).unwrap())
    }

    // Parallel queries = (cores in cluster) * (smuge factor)
    fn nr_parallel_queries(&self) -> NonZeroUsize {
        const SMUGE_FACTOR: NonZeroUsize = NonZeroUsize::new(3).unwrap();
        self.nr_shards_in_cluster()
            .checked_mul(SMUGE_FACTOR)
            .unwrap()
    }

    /// Creates an iterator over all tokens ranges available in a cluster. A token ring is taken
    /// from the rust driver as a list of tokens. A token range is build from a token pair
    /// (tokens[idx], tokens[idx+1]-1) to be able to use WHERE token >= ? AND token <= ? in CQL
    /// queries - the next token starts the new range. Additionally to the list of tokens taken
    /// from the token ring, the first item is the lowest possible token, and the last item is the
    /// highest possible token - for support the specific token range after the highest token to
    /// the lowest token. The highest possible token value is not decremented, because it doesn't
    /// start a new range.
    fn fullscan_ranges(&self) -> impl Iterator<Item = (Token, Token)> {
        const TOKEN_MAX: i64 = i64::MAX; // the highest possible token value in the ScyllaDB
        const TOKEN_MIN: i64 = -TOKEN_MAX; // the lowest possible token value in the ScyllaDB

        let tokens = iter::once(Token::new(TOKEN_MIN))
            .chain(
                self.session_rx
                    .borrow()
                    .as_ref()
                    .map(|session| {
                        session
                            .get_cluster_state()
                            .replica_locator()
                            .ring()
                            .iter()
                            .map(|(token, _)| token)
                            .copied()
                            .collect_vec()
                    })
                    .unwrap_or_default(),
            )
            .collect_vec();
        tokens
            .into_iter()
            .circular_tuple_windows()
            .map(|(begin, end)| {
                if begin > end {
                    // this is the last token range
                    (begin, Token::new(TOKEN_MAX))
                } else {
                    // prepare a range without the last token
                    (begin, Token::new(end.value() - 1))
                }
            })
    }

    async fn range_scan_stream(
        &self,
        begin: Token,
        end: Token,
    ) -> anyhow::Result<BoxStream<'static, DbEmbedding>> {
        // last two columns are embedding and writetime
        let columns_len_expected = self.primary_key_columns.len() + 2;
        let session = self
            .session_rx
            .borrow()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No active session for range scan"))?;
        Ok(session
            .execute_iter(self.st_range_scan.clone(), (begin.value(), end.value()))
            .await?
            .rows_stream::<Row>()?
            .map_err(anyhow::Error::from)
            .map_ok(move |mut row| {
                if row.columns.len() != columns_len_expected {
                    debug!(
                        "range_scan_stream: bad length of columns: {} != {}",
                        row.columns.len(),
                        columns_len_expected
                    );
                    return None;
                }

                let Some(CqlValue::BigInt(timestamp)) = row.columns.pop().unwrap() else {
                    debug!("range_scan_stream: bad type of a writetime");
                    return None;
                };
                let timestamp =
                    (OffsetDateTime::UNIX_EPOCH + Duration::from_micros(timestamp as u64)).into();

                let Some(CqlValue::Vector(embedding)) = row.columns.pop().unwrap() else {
                    debug!("range_scan_stream: bad type of an embedding");
                    return None;
                };
                let Ok(embedding) = embedding
                    .into_iter()
                    .map(|value| {
                        let CqlValue::Float(value) = value else {
                            bail!("range_scan_stream: bad type of an embedding element");
                        };
                        Ok(value)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()
                    .inspect_err(|err| debug!("range_scan_stream: {err}"))
                else {
                    return None;
                };
                let embedding = Some(embedding.into());

                let Ok(primary_key) = row
                    .columns
                    .into_iter()
                    .map(|value| {
                        let Some(value) = value else {
                            bail!("range_scan_stream: missing a primary key column");
                        };
                        Ok(value)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()
                    .inspect_err(|err| debug!("range_scan_stream: {err}"))
                else {
                    return None;
                };
                let primary_key = primary_key.into();

                Some(DbEmbedding {
                    primary_key,
                    embedding,
                    timestamp,
                })
            })
            .filter_map(|value| async move {
                value
                    .inspect_err(|err| debug!("range_scan_stream: problem with parsing row: {err}"))
                    .ok()
                    .flatten()
            })
            .boxed())
    }
}

struct CdcConsumerData {
    primary_key_columns: Vec<ColumnName>,
    target_column: ColumnName,
    tx: mpsc::Sender<(DbEmbedding, Option<AsyncInProgress>)>,
    gregorian_epoch: OffsetDateTime,
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
            .collect::<anyhow::Result<Vec<_>>>()?
            .into();

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

        let gregorian_epoch = OffsetDateTime::new_utc(
            Date::from_calendar_date(1582, time::Month::October, 15)?,
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_percentage_from_u64() {
        let percentage = Percentage::from(0);
        assert_eq!(percentage.get(), 0.0);
        let percentage = Percentage::from(u64::MAX / 2);
        assert_eq!(percentage.get(), 50.0);
        let percentage = Percentage::from(u64::MAX);
        assert_eq!(percentage.get(), 100.0);
    }

    #[test]
    fn test_progress_from_u64() {
        let progress = Progress::from(0);
        assert!(matches!(progress, Progress::InProgress(p) if p.get() == 0.0));
        let progress = Progress::from(u64::MAX / 2);
        assert!(matches!(progress, Progress::InProgress(p) if p.get() == 50.0));
        let progress = Progress::from(u64::MAX);
        assert!(matches!(progress, Progress::Done));
    }
}
