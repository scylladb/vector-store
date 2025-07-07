/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::DbEmbedding;
use crate::IndexMetadata;
use crate::KeyspaceName;
use crate::TableName;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use async_trait::async_trait;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::routing::Token;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;
use scylla::value::Row;
use scylla_cdc::consumer::CDCRow;
use scylla_cdc::consumer::Consumer;
use scylla_cdc::consumer::ConsumerFactory;
use scylla_cdc::log_reader::CDCLogReaderBuilder;
use std::iter;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use time::Date;
use time::OffsetDateTime;
use time::Time;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;
use tracing::warn;

type GetPrimaryKeyColumnsR = Vec<ColumnName>;

pub enum DbIndex {
    GetPrimaryKeyColumns {
        tx: oneshot::Sender<GetPrimaryKeyColumnsR>,
    },
}

pub(crate) trait DbIndexExt {
    async fn get_primary_key_columns(&self) -> GetPrimaryKeyColumnsR;
}

impl DbIndexExt for mpsc::Sender<DbIndex> {
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
) -> anyhow::Result<(mpsc::Sender<DbIndex>, mpsc::Receiver<DbEmbedding>)> {
    let id = metadata.id();

    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx_index, mut rx_index) = mpsc::channel(CHANNEL_SIZE);
    let (tx_embeddings, rx_embeddings) = mpsc::channel(CHANNEL_SIZE);

    let (mut cdc_reader, cdc_handler) = CDCLogReaderBuilder::new()
        .session(Arc::clone(&db_session))
        .keyspace(metadata.keyspace_name.as_ref())
        .table_name(metadata.table_name.as_ref())
        .consumer_factory(Arc::new(CdcConsumerFactory::new(
            Arc::clone(&db_session),
            &metadata,
            tx_embeddings.clone(),
        )?))
        .build()
        .await?;

    let statements = Arc::new(Statements::new(db_session, metadata).await?);

    tokio::spawn(
        async move {
            debug!("starting");

            if let Err(err) = cdc_handler.await {
                debug!("handler: {err}");
            }

            debug!("finished");
        }
        .instrument(debug_span!("cdc", "{}", id)),
    );

    tokio::spawn(
        async move {
            debug!("starting");

            while !rx_index.is_closed() {
                tokio::select! {
                    _ = statements.initial_scan(tx_embeddings.clone()) => {
                        break;
                    }
                    Some(msg) = rx_index.recv() => {
                        tokio::spawn(process(Arc::clone(&statements), msg));
                    }
                }
            }

            debug!("finished initial load");

            while let Some(msg) = rx_index.recv().await {
                tokio::spawn(process(Arc::clone(&statements), msg));
            }

            cdc_reader.stop();

            debug!("finished");
        }
        .instrument(debug_span!("db_index", "{}", id)),
    );
    Ok((tx_index, rx_embeddings))
}

async fn process(statements: Arc<Statements>, msg: DbIndex) {
    match msg {
        DbIndex::GetPrimaryKeyColumns { tx } => tx
            .send(statements.get_primary_key_columns())
            .unwrap_or_else(|_| {
                trace!("process: Db::GetPrimaryKeyColumns: unable to send response")
            }),
    }
}

struct Statements {
    session: Arc<Session>,
    primary_key_columns: Vec<ColumnName>,
    st_range_scan: PreparedStatement,
}

impl Statements {
    async fn new(session: Arc<Session>, metadata: IndexMetadata) -> anyhow::Result<Self> {
        session.await_schema_agreement().await?;

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

        let st_partition_key_list = table.partition_key.iter().join(", ");
        let st_primary_key_list = primary_key_columns.iter().join(", ");

        Ok(Self {
            primary_key_columns,

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

            session,
        })
    }

    fn get_primary_key_columns(&self) -> Vec<ColumnName> {
        self.primary_key_columns.clone()
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
            "
        )
    }

    /// The initial full scan of embeddings stored in a ScyllaDB table. It scans concurrently using
    /// token ranges read from a rust driver. At first it prepares ranges, limits concurrent scans
    /// using semaphore, and runs each scan in separate concurrent task using cloned mpsc channel
    /// to send read embeddings into the pipeline.
    async fn initial_scan(&self, tx: mpsc::Sender<DbEmbedding>) {
        let semaphore = Arc::new(Semaphore::new(self.nr_parallel_queries().get()));

        for (begin, end) in self.fullscan_ranges() {
            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();

            if let Ok(embeddings) = self.range_scan_stream(begin, end).await.inspect_err(|err| {
                warn!("unable to do initial scan for range ({begin:?}, {end:?}): {err}")
            }) {
                let tx = tx.clone();
                tokio::spawn(async move {
                    embeddings
                        .for_each(|embedding| async {
                            _ = tx.send(embedding).await;
                        })
                        .await;
                    drop(permit);
                });
            }
        }
    }

    fn nr_shards_in_cluster(&self) -> NonZeroUsize {
        NonZeroUsize::try_from(
            self.session
                .get_cluster_state()
                .get_nodes_info()
                .iter()
                .filter_map(|node| node.sharder())
                .map(|sharder| sharder.nr_shards.get() as usize)
                .sum::<usize>(),
        )
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
                self.session
                    .get_cluster_state()
                    .replica_locator()
                    .ring()
                    .iter()
                    .map(|(token, _)| token)
                    .copied(),
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
        Ok(self
            .session
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
    tx: mpsc::Sender<DbEmbedding>,
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
            .send(DbEmbedding {
                primary_key,
                embedding,
                timestamp,
            })
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
        tx: mpsc::Sender<DbEmbedding>,
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
