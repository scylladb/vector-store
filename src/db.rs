/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::db_index;
use crate::db_index::DbIndex;
use crate::Connectivity;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexId;
use crate::IndexMetadata;
use anyhow::Context;
use futures::TryStreamExt;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimeuuid;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug_span;
use tracing::warn;
use tracing::Instrument;

pub(crate) enum Db {
    #[allow(clippy::enum_variant_names)]
    GetIndexDb {
        metadata: IndexMetadata,
        tx: oneshot::Sender<anyhow::Result<mpsc::Sender<DbIndex>>>,
    },

    LatestSchemaVersion {
        tx: oneshot::Sender<anyhow::Result<Option<CqlTimeuuid>>>,
    },

    GetIndexParams {
        id: IndexId,
        #[allow(clippy::type_complexity)]
        tx: oneshot::Sender<anyhow::Result<Option<(Connectivity, ExpansionAdd, ExpansionSearch)>>>,
    },
}

pub(crate) trait DbExt {
    async fn get_index_db(&self, metadata: IndexMetadata) -> anyhow::Result<mpsc::Sender<DbIndex>>;

    async fn latest_schema_version(&self) -> anyhow::Result<Option<CqlTimeuuid>>;

    async fn get_index_params(
        &self,
        id: IndexId,
    ) -> anyhow::Result<Option<(Connectivity, ExpansionAdd, ExpansionSearch)>>;
}

impl DbExt for mpsc::Sender<Db> {
    async fn get_index_db(&self, metadata: IndexMetadata) -> anyhow::Result<mpsc::Sender<DbIndex>> {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexDb { metadata, tx }).await?;
        rx.await?
    }

    async fn latest_schema_version(&self) -> anyhow::Result<Option<CqlTimeuuid>> {
        let (tx, rx) = oneshot::channel();
        self.send(Db::LatestSchemaVersion { tx }).await?;
        rx.await?
    }

    async fn get_index_params(
        &self,
        id: IndexId,
    ) -> anyhow::Result<Option<(Connectivity, ExpansionAdd, ExpansionSearch)>> {
        let (tx, rx) = oneshot::channel();
        self.send(Db::GetIndexParams { id, tx }).await?;
        rx.await?
    }
}

pub(crate) async fn new(db_session: Arc<Session>) -> anyhow::Result<mpsc::Sender<Db>> {
    let statements = Arc::new(Statements::new(db_session).await?);
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(
        async move {
            while let Some(msg) = rx.recv().await {
                tokio::spawn(process(Arc::clone(&statements), msg));
            }
        }
        .instrument(debug_span!("db")),
    );
    Ok(tx)
}

async fn process(statements: Arc<Statements>, msg: Db) {
    match msg {
        Db::GetIndexDb { metadata, tx } => tx
            .send(statements.get_index_db(metadata).await)
            .unwrap_or_else(|_| warn!("db::process: Db::GetIndexDb: unable to send response")),

        Db::LatestSchemaVersion { tx } => tx
            .send(statements.latest_schema_version().await)
            .unwrap_or_else(|_| {
                warn!("db::process: Db::LatestSchemaVersion: unable to send response")
            }),

        Db::GetIndexParams { id, tx } => tx
            .send(statements.get_index_params(id).await)
            .unwrap_or_else(|_| warn!("db::process: Db::GetIndexParams: unable to send response")),
    }
}

struct Statements {
    session: Arc<Session>,
    st_latest_schema_version: PreparedStatement,
    st_get_index_params: PreparedStatement,
}

impl Statements {
    async fn new(session: Arc<Session>) -> anyhow::Result<Self> {
        Ok(Self {
            st_latest_schema_version: session
                .prepare(Self::ST_LATEST_SCHEMA_VERSION)
                .await
                .context("ST_LATEST_SCHEMA_VERSION")?,

            st_get_index_params: session
                .prepare(Self::ST_GET_INDEX_PARAMS)
                .await
                .context("ST_GET_INDEX_PARAMS")?,

            session,
        })
    }

    async fn get_index_db(&self, metadata: IndexMetadata) -> anyhow::Result<mpsc::Sender<DbIndex>> {
        db_index::new(Arc::clone(&self.session), metadata).await
    }

    const ST_LATEST_SCHEMA_VERSION: &str = "
        SELECT state_id
        FROM system.group0_history
        WHERE key = 'history'
        ORDER BY state_id DESC
        LIMIT 1
        ";

    async fn latest_schema_version(&self) -> anyhow::Result<Option<CqlTimeuuid>> {
        Ok(self
            .session
            .execute_iter(self.st_latest_schema_version.clone(), &[])
            .await?
            .rows_stream::<(CqlTimeuuid,)>()?
            .try_next()
            .await?
            .map(|(timeuuid,)| timeuuid))
    }

    const ST_GET_INDEX_PARAMS: &str = "
        SELECT param_m, param_ef_construct, param_ef_search
        FROM vector_benchmark.vector_indexes
        WHERE id = ?
        ";

    async fn get_index_params(
        &self,
        id: IndexId,
    ) -> anyhow::Result<Option<(Connectivity, ExpansionAdd, ExpansionSearch)>> {
        Ok(self
            .session
            .execute_iter(self.st_get_index_params.clone(), (id,))
            .await?
            .rows_stream::<(Option<i32>, Option<i32>, Option<i32>)>()?
            .try_filter_map(|(connectivity, expansion_add, expansion_search)| {
                Box::pin(async move {
                    Ok(connectivity
                        .zip(expansion_add)
                        .zip(expansion_search)
                        .and_then(|((connectivity, expansion_add), expansion_search)| {
                            (connectivity >= 0 && expansion_add >= 0 && expansion_search >= 0)
                                .then_some((connectivity, expansion_add, expansion_search))
                        }))
                })
            })
            .map_ok(|(connectivity, expansion_add, expansion_search)| {
                (
                    (connectivity as usize).into(),
                    (expansion_add as usize).into(),
                    (expansion_search as usize).into(),
                )
            })
            .try_next()
            .await?)
    }
}
