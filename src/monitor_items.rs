/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Proprietary
 */

use crate::index::Index;
use crate::index::IndexExt;
use crate::ColumnName;
use crate::Embeddings;
use crate::Key;
use crate::ScyllaDbUri;
use crate::TableName;
use anyhow::Context;
use futures::Stream;
use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::NextRowError;
use scylla::statement::prepared::PreparedStatement;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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
    uri: ScyllaDbUri,
    table: TableName,
    col_id: ColumnName,
    col_emb: ColumnName,
    index: Sender<Index>,
) -> anyhow::Result<Sender<MonitorItems>> {
    let db = Arc::new(Db::new(uri, table, col_id, col_emb).await?);
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        info!("resetting items");
        let mut state = State::Reset;
        let mut interval = time::interval(time::Duration::from_secs(1));
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
    session: Session,
    st_get_processed_ids: PreparedStatement,
    st_get_items: PreparedStatement,
    st_reset_items: PreparedStatement,
    st_update_items: PreparedStatement,
}

impl Db {
    async fn new(
        uri: ScyllaDbUri,
        table: TableName,
        col_id: ColumnName,
        col_emb: ColumnName,
    ) -> anyhow::Result<Self> {
        let session = SessionBuilder::new()
            .known_node(uri.0.as_str())
            .build()
            .await?;
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
    let mut keys_chunks = db.get_processed_ids().await?.try_chunks(100);
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

async fn table_to_index(db: &Arc<Db>, index: &Sender<Index>) -> anyhow::Result<bool> {
    let mut rows = db.get_items().await?;
    let counter_before = Arc::new(AtomicUsize::new(0));
    let counter_after = Arc::new(AtomicUsize::new(0));
    let mut processed = Vec::new();
    let mut count = 0;
    while let Some((key, embeddings)) = rows.try_next().await? {
        counter_before.fetch_add(1, Ordering::Relaxed);
        processed.push(key);
        count += 1;
        if processed.len() == 100 {
            let processed: Vec<_> = mem::take(&mut processed);
            let db = Arc::clone(db);
            tokio::spawn(async move {
                db.update_items(&processed).await.unwrap_or_else(|err| {
                    warn!("monitor_items::table_to_index: unable to update items: {err}")
                });
            });
        }
        tokio::spawn({
            let index = index.clone();
            let counter_after = Arc::clone(&counter_after);
            async move {
                index.add(key, embeddings).await;
                counter_after.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
    if !processed.is_empty() {
        db.update_items(&processed).await?;
    }
    {
        let before = counter_before.load(Ordering::Relaxed);
        let after = counter_after.load(Ordering::Relaxed);
        if before != 0 || after != 0 {
            debug!("table_to_index: before = {before}, after = {after}",);
        }
    }
    if count > 0 {
        debug!("table_to_index: processed {count} items",);
    }
    Ok(count > 0)
}
