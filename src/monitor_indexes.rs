/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexMetadata;
use crate::SpaceType;
use crate::db::Db;
use crate::db::DbExt;
use crate::engine::Engine;
use crate::engine::EngineExt;
use futures::StreamExt;
use futures::stream;
use scylla::value::CqlTimeuuid;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::warn;

pub(crate) enum MonitorIndexes {}

pub(crate) async fn new(
    db: Sender<Db>,
    engine: Sender<Engine>,
) -> anyhow::Result<Sender<MonitorIndexes>> {
    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(
        async move {
            const INTERVAL: Duration = Duration::from_secs(1);
            let mut interval = time::interval(INTERVAL);

            let mut schema_version = SchemaVersion::new();
            let mut indexes = HashSet::new();
            while !rx.is_closed() {
                tokio::select! {
                    _ = interval.tick() => {
                        // check if schema has changed from the last time
                        if !schema_version.has_changed(&db).await {
                            continue;
                        }
                        let Ok(new_indexes) = get_indexes(&db).await.inspect_err(|err| {
                            debug!("monitor_indexes: unable to get the list of indexes: {err}");
                        }) else {
                            // there was an error during retrieving indexes, reset schema version
                            // and retry next time
                            schema_version.reset();
                            continue;
                        };
                        del_indexes(&engine, indexes.difference(&new_indexes)).await;
                        let AddIndexesR {added, has_failures} = add_indexes(
                            &engine,
                            new_indexes.into_iter().filter(|idx| !indexes.contains(idx))
                        ).await;
                        indexes.extend(added);
                        if has_failures {
                            // if a process has failures we will need to repeat the operation
                            // so let's reset schema version here
                            schema_version.reset();
                        }
                    }
                    _ = rx.recv() => { }
                }
            }
        }
        .instrument(debug_span!("monitor_indexes")),
    );
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
            warn!("unable to get latest schema change from db: {err}");
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
            .inspect_err(|err| warn!("unable to get index version: {err}"))?
        else {
            debug!("get_indexes: no version for index {idx:?}");
            continue;
        };

        let Some(dimensions) = db
            .get_index_target_type(
                idx.keyspace.clone(),
                idx.table.clone(),
                idx.target_column.clone(),
            )
            .await
            .inspect_err(|err| warn!("unable to get index target dimensions: {err}"))?
        else {
            debug!("get_indexes: missing or unsupported type for index {idx:?}");
            continue;
        };

        let (connectivity, expansion_add, expansion_search, space_type) = if let Some(params) = db
            .get_index_params(idx.keyspace.clone(), idx.table.clone(), idx.index.clone())
            .await
            .inspect_err(|err| warn!("unable to get index params: {err}"))?
        {
            params
        } else {
            debug!("get_indexes: no params for index {idx:?}");
            (0.into(), 0.into(), 0.into(), SpaceType::default())
        };

        let metadata = IndexMetadata {
            keyspace_name: idx.keyspace,
            index_name: idx.index,
            table_name: idx.table,
            target_column: idx.target_column,
            dimensions,
            connectivity,
            expansion_add,
            expansion_search,
            space_type,
            version,
        };

        if !db.is_valid_index(metadata.clone()).await {
            debug!("get_indexes: not valid index {}", metadata.id());
            continue;
        }

        indexes.insert(metadata);
    }
    Ok(indexes)
}

struct AddIndexesR {
    added: HashSet<IndexMetadata>,
    has_failures: bool,
}

async fn add_indexes(
    engine: &Sender<Engine>,
    idxs: impl Iterator<Item = IndexMetadata>,
) -> AddIndexesR {
    let has_failures = AtomicBool::new(false);
    let added = stream::iter(idxs)
        .filter_map(|idx| async {
            engine
                .add_index(idx.clone())
                .await
                .inspect_err(|_| {
                    has_failures.store(true, Ordering::Relaxed);
                })
                .ok()
                .map(|_| idx)
        })
        .collect()
        .await;
    AddIndexesR {
        added,
        has_failures: has_failures.load(Ordering::Relaxed),
    }
}

async fn del_indexes(engine: &Sender<Engine>, idxs: impl Iterator<Item = &IndexMetadata>) {
    for idx in idxs {
        engine.del_index(idx.id()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[tokio::test]
    async fn schema_version_changed() {
        let (tx_db, mut rx_db) = mpsc::channel(10);

        let task_db = tokio::spawn(async move {
            let version1 = CqlTimeuuid::from_bytes([1; 16]);
            let version2 = CqlTimeuuid::from_bytes([2; 16]);

            // step 1
            let Some(Db::LatestSchemaVersion { tx }) = rx_db.recv().await else {
                unreachable!();
            };
            tx.send(Err(anyhow!("test issue"))).unwrap();

            // step 2
            let Some(Db::LatestSchemaVersion { tx }) = rx_db.recv().await else {
                unreachable!();
            };
            tx.send(Ok(None)).unwrap();

            // step 3
            let Some(Db::LatestSchemaVersion { tx }) = rx_db.recv().await else {
                unreachable!();
            };
            tx.send(Ok(Some(version1))).unwrap();

            // step 4
            let Some(Db::LatestSchemaVersion { tx }) = rx_db.recv().await else {
                unreachable!();
            };
            tx.send(Err(anyhow!("test issue"))).unwrap();

            // step 5
            let Some(Db::LatestSchemaVersion { tx }) = rx_db.recv().await else {
                unreachable!();
            };
            tx.send(Ok(Some(version1))).unwrap();

            // step 6
            let Some(Db::LatestSchemaVersion { tx }) = rx_db.recv().await else {
                unreachable!();
            };
            tx.send(Ok(None)).unwrap();

            // step 7
            let Some(Db::LatestSchemaVersion { tx }) = rx_db.recv().await else {
                unreachable!();
            };
            tx.send(Ok(Some(version1))).unwrap();

            // step 8
            let Some(Db::LatestSchemaVersion { tx }) = rx_db.recv().await else {
                unreachable!();
            };
            tx.send(Ok(Some(version1))).unwrap();

            // step 9
            let Some(Db::LatestSchemaVersion { tx }) = rx_db.recv().await else {
                unreachable!();
            };
            tx.send(Ok(Some(version2))).unwrap();
        });

        let mut sv = SchemaVersion::new();

        // step 1: Err should not change the schema version
        assert!(!sv.has_changed(&tx_db).await);

        // step 2: None should not change the schema version
        assert!(!sv.has_changed(&tx_db).await);

        // step 3: value1 should change the schema version
        assert!(sv.has_changed(&tx_db).await);

        // step 4: Err should change the schema version
        assert!(sv.has_changed(&tx_db).await);

        // step 5: value1 should change the schema version
        assert!(sv.has_changed(&tx_db).await);

        // step 6: None should change the schema version
        assert!(sv.has_changed(&tx_db).await);

        // step 7: value1 should change the schema version
        assert!(sv.has_changed(&tx_db).await);

        // step 8: value1 should not change the schema version
        assert!(!sv.has_changed(&tx_db).await);

        // step 9: value2 should change the schema version
        assert!(sv.has_changed(&tx_db).await);

        task_db.await.unwrap();
    }
}
