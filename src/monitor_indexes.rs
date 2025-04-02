/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db::Db;
use crate::db::DbExt;
use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::IndexMetadata;
use scylla::value::CqlTimeuuid;
use std::collections::HashSet;
use std::mem;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time;
use tracing::warn;

pub(crate) enum MonitorIndexes {}

pub(crate) async fn new(
    db: Sender<Db>,
    engine: Sender<Engine>,
) -> anyhow::Result<Sender<MonitorIndexes>> {
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        const INTERVAL: Duration = Duration::from_secs(1);
        let mut interval = time::interval(INTERVAL);

        let mut schema_version = SchemaVersion::new();
        let mut indexes = HashSet::new();
        while !rx.is_closed() {
            tokio::select! {
                _ = interval.tick() => {
                    if !schema_version.has_changed(&db).await {
                        continue;
                    }
                    let Ok(mut new_indexes) = get_indexes(&db).await.inspect_err(|err| {
                        warn!("monitor_indexes: unable to get the list of indexes: {err}");
                    }) else {
                        schema_version.reset();
                        continue;
                    };
                    del_indexes(&engine, indexes.difference(&new_indexes)).await;
                    add_indexes(&engine, new_indexes.difference(&indexes)).await;
                    mem::swap(&mut indexes, &mut new_indexes);
                }
                _ = rx.recv() => { }
            }
        }
    });
    Ok(tx)
}

#[derive(PartialEq)]
struct SchemaVersion(Option<CqlTimeuuid>);

impl SchemaVersion {
    fn new() -> Self {
        Self(None)
    }

    async fn has_changed(&mut self, db: &Sender<Db>) -> bool {
        let schema_version = db.latest_schema_version().await.unwrap_or_else(|err| {
            warn!("monitor_indexes: unable to get latest schema change: {err}");
            None
        });
        if self.0 == schema_version {
            return false;
        };
        self.0 = schema_version;
        true
    }

    fn reset(&mut self) {
        self.0 = None;
    }
}

async fn get_indexes(db: &Sender<Db>) -> anyhow::Result<HashSet<IndexMetadata>> {
    let mut indexes = HashSet::new();
    for idx in db.get_indexes().await?.into_iter() {
        let Some(version) = db
            .get_index_version(idx.keyspace.clone(), idx.index.clone())
            .await
            .inspect_err(|err| {
                warn!("monitor_indexes::get_indexes: unable to get index version: {err}")
            })?
        else {
            warn!("monitor_indexes::get_indexes: no version for index {idx:?}");
            continue;
        };

        let Some(dimensions) = db
            .get_index_target_type(idx.keyspace.clone(), idx.table.clone(), idx.target.clone())
            .await
            .inspect_err(|err| {
                warn!("monitor_indexes::get_indexes: unable to get index target type: {err}")
            })?
        else {
            warn!("monitor_indexes::get_indexes: missing or unsupported type for index {idx:?}");
            continue;
        };

        let (connectivity, expansion_add, expansion_search) = if let Some(params) =
            db.get_index_params(idx.id()).await.inspect_err(|err| {
                warn!("monitor_indexes::get_indexes: unable to get index params: {err}")
            })? {
            params
        } else {
            warn!("monitor_indexes::get_indexes: no params for index {idx:?}");
            (0.into(), 0.into(), 0.into())
        };

        let metadata = IndexMetadata {
            keyspace_name: idx.keyspace,
            index_name: idx.index,
            table_name: idx.table,
            target_name: idx.target,
            key_name: "id".to_string().into(),
            dimensions,
            connectivity,
            expansion_add,
            expansion_search,
            version,
        };
        indexes.insert(metadata);
    }
    Ok(indexes)
}

async fn add_indexes(engine: &Sender<Engine>, idxs: impl Iterator<Item = &IndexMetadata>) {
    for idx in idxs {
        engine.add_index(idx.clone()).await;
    }
}

async fn del_indexes(engine: &Sender<Engine>, idxs: impl Iterator<Item = &IndexMetadata>) {
    for idx in idxs {
        engine.del_index(idx.id()).await;
    }
}
