/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::DbEmbeddings;
use crate::Embeddings;
use crate::IndexMetadata;
use crate::KeyspaceName;
use crate::PrimaryKey;
use crate::TableName;
use anyhow::Context;
use anyhow::anyhow;
use futures::StreamExt;
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
use scylla::routing::Token;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;
use std::iter;
use std::num::NonZeroUsize;
use std::sync::Arc;
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
) -> anyhow::Result<(mpsc::Sender<DbIndex>, mpsc::Receiver<DbEmbeddings>)> {
    let id = metadata.id();
    let statements = Arc::new(Statements::new(db_session, metadata).await?);

    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx_index, mut rx_index) = mpsc::channel(CHANNEL_SIZE);
    let (tx_embeddings, rx_embeddings) = mpsc::channel(CHANNEL_SIZE);

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
        embeddings: &ColumnName,
    ) -> String {
        format!(
            "
            SELECT {st_primary_key_list}, {embeddings}
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
    async fn initial_scan(&self, tx: mpsc::Sender<DbEmbeddings>) {
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
    ) -> anyhow::Result<BoxStream<'static, DbEmbeddings>> {
        Ok(self
            .session
            .execute_iter(self.st_range_scan.clone(), (begin.value(), end.value()))
            .await?
            .rows_stream::<PrimaryKeyWithEmbeddings>()?
            .filter_map(|embeddings| async move {
                embeddings
                    .inspect_err(|err| debug!("range_scan_stream: problem with parsing row: {err}"))
                    .ok()
            })
            .map(|row| DbEmbeddings {
                primary_key: row.primary_key,
                embeddings: row.embeddings,
            })
            .boxed())
    }
}
