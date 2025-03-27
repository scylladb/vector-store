/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Proprietary
 */

use crate::index::Index;
use crate::index::IndexExt;
use crate::ColumnName;
use crate::Embeddings;
use crate::Key;
use crate::TableName;
use anyhow::Context;
use futures::Stream;
use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::errors::NextRowError;
use scylla::statement::prepared::PreparedStatement;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time;
use tracing::debug;
use tracing::info;
use tracing::info_span;
use tracing::warn;
use tracing::Instrument;

pub(crate) enum MonitorItems {}

pub(crate) async fn new(
    db_session: Arc<Session>,
    table: TableName,
    col_id: ColumnName,
    col_emb: ColumnName,
    index: Sender<Index>,
) -> anyhow::Result<Sender<MonitorItems>> {
    let db = Arc::new(Db::new(db_session, table, col_id, col_emb).await?);

    // The value was taken from initial benchmarks
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(async move {
        info!("resetting items");
        let mut state = State::Reset;

        const INTERVAL: Duration = Duration::from_secs(1);
        let mut interval = time::interval(INTERVAL);

        while !rx.is_closed() {
            tokio::select! {
                _ = interval.tick() => {
                    match state {
                        State::Reset => {
                            if !reset_items(&db)
                                .await
                                .unwrap_or_else(|err| {
                                    warn!("monitor_items: unable to reset items in table: {err}");
                                    false
                                })
                            {
                                info!("copying items");
                                state = State::Copy;
                            } else {
                                interval.reset_immediately();
                            }
                        }
                        State::Copy => {
                            if table_to_index(&db, &index)
                                .await
                                .unwrap_or_else(|err| {
                                    warn!("monitor_items: unable to copy data from table to index: {err}");
                                    false
                                })
                            {
                                interval.reset_immediately();
                            }
                        }
                    }
                }

                _ = rx.recv() => { }
            }
        }
    }.instrument(info_span!("monitor items")));
    Ok(tx)
}

enum State {
    Reset,
    Copy,
}

struct Db {
    session: Arc<Session>,
    st_get_processed_ids: PreparedStatement,
    st_get_items: PreparedStatement,
    st_reset_items: PreparedStatement,
    st_update_items: PreparedStatement,
}

impl Db {
    async fn new(
        session: Arc<Session>,
        table: TableName,
        col_id: ColumnName,
        col_emb: ColumnName,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            st_get_processed_ids: session
                .prepare(Self::get_processed_ids_query(&table, &col_id))
                .await
                .context("get_processed_ids_query")?,
            st_get_items: session
                .prepare(Self::get_items_query(&table, &col_id, &col_emb))
                .await
                .context("get_items_query")?,
            st_reset_items: session
                .prepare(Self::reset_items_query(&table))
                .await
                .context("reset_items_query")?,
            st_update_items: session
                .prepare(Self::update_items_query(&table))
                .await
                .context("update_items_query")?,
            session,
        })
    }

    fn get_processed_ids_query(table: &TableName, col_id: &ColumnName) -> String {
        format!(
            "
            SELECT {col_id}
            FROM {table}
            WHERE processed = TRUE
            LIMIT 1000
            "
        )
    }
    async fn get_processed_ids(
        &self,
    ) -> anyhow::Result<impl Stream<Item = Result<Key, NextRowError>>> {
        Ok(self
            .session
            .execute_iter(self.st_get_processed_ids.clone(), ())
            .await?
            .rows_stream::<(i64,)>()?
            .map_ok(|(key,)| (key as u64).into()))
    }

    fn get_items_query(table: &TableName, col_id: &ColumnName, col_emb: &ColumnName) -> String {
        format!(
            "
            SELECT {col_id}, {col_emb}
            FROM {table}
            WHERE processed = FALSE
            "
        )
    }
    async fn get_items(
        &self,
    ) -> anyhow::Result<impl Stream<Item = Result<(Key, Embeddings), NextRowError>>> {
        Ok(self
            .session
            .execute_iter(self.st_get_items.clone(), ())
            .await?
            .rows_stream::<(i64, Vec<f32>)>()?
            .map_ok(|(key, embeddings)| ((key as u64).into(), embeddings.into())))
    }

    fn reset_items_query(table: &TableName) -> String {
        format!(
            "
            UPDATE {table}
                SET processed = False
                WHERE id IN ?
            "
        )
    }
    async fn reset_items(&self, keys: &[Key]) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_reset_items, (keys,))
            .await?;
        Ok(())
    }

    fn update_items_query(table: &TableName) -> String {
        format!(
            "
            UPDATE {table}
                SET processed = True
                WHERE id IN ?
            "
        )
    }
    async fn update_items(&self, keys: &[Key]) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_update_items, (keys,))
            .await?;
        Ok(())
    }
}

async fn reset_items(db: &Arc<Db>) -> anyhow::Result<bool> {
    // The value was taken from initial benchmarks
    const CHUNK_SIZE: usize = 100;
    let mut keys_chunks = db.get_processed_ids().await?.try_chunks(CHUNK_SIZE);

    let mut resetting = Vec::new();
    while let Some(keys) = keys_chunks.try_next().await? {
        let db = Arc::clone(db);
        resetting.push(tokio::spawn(async move {
            db.reset_items(&keys).await.map(|_| keys.len())
        }));
    }
    let mut count = 0;
    for processed in resetting.into_iter() {
        count += processed.await??;
    }
    debug!("processed new items: {count}");
    Ok(count > 0)
}

/// Get new embeddings from db and add to the index. Then mark embeddings in db as processed
async fn table_to_index(db: &Arc<Db>, index: &Sender<Index>) -> anyhow::Result<bool> {
    let mut rows = db.get_items().await?;

    // The value was taken from initial benchmarks
    const PROCESSED_CHUNK_SIZE: usize = 100;

    // The container for processed keys
    let mut processed = Vec::new();
    // The accumulator for number of processed embeddings
    let mut count = 0;

    while let Some((key, embeddings)) = rows.try_next().await? {
        processed.push(key);
        count += 1;

        if processed.len() == PROCESSED_CHUNK_SIZE {
            // A new chunk of processed keys is prepared. Mark them as processed in db in the
            // background.
            let processed: Vec<_> = mem::take(&mut processed);
            let db = Arc::clone(db);
            tokio::spawn(async move {
                db.update_items(&processed).await.unwrap_or_else(|err| {
                    warn!("monitor_items::table_to_index: unable to update items: {err}")
                });
            });
        }

        // Add the embeddings in the background
        tokio::spawn({
            let index = index.clone();
            async move {
                index.add(key, embeddings).await;
            }
        });
    }

    if !processed.is_empty() {
        db.update_items(&processed).await?;
    }

    if count > 0 {
        debug!("table_to_index: processed {count} items",);
    }

    Ok(count > 0)
}
