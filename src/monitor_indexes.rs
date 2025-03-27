/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Proprietary
 */

use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::Connectivity;
use crate::Dimensions;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexId;
use crate::ScyllaDbUri;
use anyhow::Context;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time;
use tracing::warn;

pub(crate) enum MonitorIndexes {}

pub(crate) async fn new(
    uri: ScyllaDbUri,
    engine: Sender<Engine>,
) -> anyhow::Result<Sender<MonitorIndexes>> {
    let db = Db::new(uri).await?;
    let mut known = HashSet::new();
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_secs(1));
        while !rx.is_closed() {
            tokio::select! {
                _ = interval.tick() => {
                    let indexes = db.not_cancelled_indexes().await.unwrap_or_else(|err| {
                        warn!("monitor_indexes: unable to get not cancelled indexes in db: {err}");
                        HashSet::new()
                    });
                    let cancelled = db.cancelled_indexes().await.unwrap_or_else(|err| {
                        warn!("monitor_indexes: unable to get cancelled indexes in db: {err}");
                        HashSet::new()
                    });
                    del_indexes(&engine, known.difference(&indexes).chain(cancelled.iter())).await;
                    add_indexes(
                        &db,
                        &engine,
                        indexes
                            .difference(&known)
                            .filter(|id| !cancelled.contains(id)),
                    )
                    .await;
                    known = indexes
                        .into_iter()
                        .filter(|id| !cancelled.contains(id))
                        .collect();
                }
                _ = rx.recv() => { }
            }
        }
    });
    Ok(tx)
}

struct Db {
    session: Session,
    st_not_cancelled_indexes: PreparedStatement,
    st_cancelled_indexes: PreparedStatement,
    st_get_index_params: PreparedStatement,
}

impl Db {
    async fn new(uri: ScyllaDbUri) -> anyhow::Result<Self> {
        let session = SessionBuilder::new()
            .known_node(uri.0.as_str())
            .build()
            .await?;
        Ok(Self {
            st_not_cancelled_indexes: session
                .prepare(Self::NOT_CANCELLED_INDEXES)
                .await
                .context("NOT_CANCELLED_INDEXES")?,
            st_cancelled_indexes: session
                .prepare(Self::CANCELLED_INDEXES)
                .await
                .context("CANCELLED_INDEXES")?,
            st_get_index_params: session
                .prepare(Self::GET_INDEX_PARAMS)
                .await
                .context("GET_INDEX_PARAMS")?,
            session,
        })
    }

    const NOT_CANCELLED_INDEXES: &str = "
        SELECT id
        FROM vector_benchmark.vector_indexes
        WHERE canceled = FALSE
        ALLOW FILTERING
        ";
    async fn not_cancelled_indexes(&self) -> anyhow::Result<HashSet<IndexId>> {
        Ok(self
            .session
            .execute_unpaged(&self.st_not_cancelled_indexes, &[])
            .await?
            .into_rows_result()?
            .rows::<(String,)>()?
            .map_ok(|(id,)| id.into())
            .try_collect()?)
    }

    const CANCELLED_INDEXES: &str = "
        SELECT id
        FROM vector_benchmark.vector_indexes
        WHERE canceled = TRUE
        ALLOW FILTERING
        ";
    async fn cancelled_indexes(&self) -> anyhow::Result<HashSet<IndexId>> {
        Ok(self
            .session
            .execute_unpaged(&self.st_cancelled_indexes, &[])
            .await?
            .into_rows_result()?
            .rows::<(String,)>()?
            .map_ok(|(id,)| id.into())
            .try_collect()?)
    }

    const GET_INDEX_PARAMS: &str =
        "SELECT dimension, param_m, param_ef_construct, param_ef_search FROM vector_benchmark.vector_indexes WHERE id = ?";
    async fn get_index_params(
        &self,
        id: IndexId,
    ) -> anyhow::Result<Option<(Dimensions, Connectivity, ExpansionAdd, ExpansionSearch)>> {
        Ok(self
            .session
            .execute_unpaged(&self.st_get_index_params, (id,))
            .await?
            .into_rows_result()?
            .rows::<(i32, i32, i32, i32)>()?
            .filter_ok(
                |(dimensions, connectivity, expansion_add, expansion_search)| {
                    *dimensions > 0
                        && *connectivity >= 0
                        && *expansion_add >= 0
                        && *expansion_search >= 0
                },
            )
            .map_ok(
                |(dimensions, connectivity, expansion_add, expansion_search)| {
                    (
                        (dimensions as usize).into(),
                        (connectivity as usize).into(),
                        (expansion_add as usize).into(),
                        (expansion_search as usize).into(),
                    )
                },
            )
            .next()
            .transpose()?)
    }
}

async fn add_indexes(db: &Db, engine: &Sender<Engine>, ids: impl Iterator<Item = &IndexId>) {
    for id in ids {
        let Ok(Some((dimensions, connectivity, expansion_add, expansion_search))) =
            db.get_index_params(id.clone()).await.inspect_err(|err| {
                warn!("monitor_indexes::add_indexes: unable to get index params: {err}")
            })
        else {
            continue;
        };
        engine
            .add_index(
                id.clone(),
                "id".to_string().into(),
                "embedding".to_string().into(),
                dimensions,
                connectivity,
                expansion_add,
                expansion_search,
            )
            .await;
    }
}

async fn del_indexes(engine: &Sender<Engine>, ids: impl Iterator<Item = &IndexId>) {
    for id in ids {
        engine.del_index(id.clone()).await;
    }
}
