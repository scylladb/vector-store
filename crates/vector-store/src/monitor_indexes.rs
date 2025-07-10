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
use crate::node_state::Event;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
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
    node_state: Sender<NodeState>,
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
                        node_state.send_event(
                            Event::DiscoveringIndexes,
                        ).await;
                        let Ok(new_indexes) = get_indexes(&db).await.inspect_err(|err| {
                            debug!("monitor_indexes: unable to get the list of indexes: {err}");
                        }) else {
                            // there was an error during retrieving indexes, reset schema version
                            // and retry next time
                            schema_version.reset();
                            continue;
                        };
                        node_state.send_event(
                            Event::IndexesDiscovered(new_indexes.clone()),
                        ).await;
                        del_indexes(&engine, indexes.extract_if(|idx| !new_indexes.contains(idx))).await;
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

async fn del_indexes(engine: &Sender<Engine>, idxs: impl Iterator<Item = IndexMetadata>) {
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

    #[tokio::test]
    #[ntest::timeout(5_000)]
    async fn index_metadata_are_removed_once() {
        use crate::DbCustomIndex;
        use crate::IndexId;
        use crate::IndexName;
        use std::collections::HashMap;
        use std::collections::HashSet;
        use std::num::NonZeroUsize;
        use std::sync::{Arc, Mutex};
        use tokio::sync::Notify;
        use uuid::Uuid;

        type IndexesT = HashSet<IndexId>;

        // Dummy db index for testing
        fn sample_db_index(name: &str) -> DbCustomIndex {
            DbCustomIndex {
                keyspace: "ks".to_string().into(),
                index: name.to_string().into(),
                table: "tbl".to_string().into(),
                target_column: "embedding".to_string().into(),
            }
        }

        // Shared state for the test
        #[derive(Debug, Clone)]
        struct TestState {
            // The current set of indexes in the "database"
            db_indexes: Arc<Mutex<Vec<DbCustomIndex>>>,
            // The indexes that the engine currently has
            engine_indexes: Arc<Mutex<IndexesT>>,
            // Schema version counter
            schema_version: Arc<Mutex<u16>>,
            // Indexes version map
            index_versions: Arc<Mutex<HashMap<IndexName, Uuid>>>,
            // Notify to signal changes
            notify: Arc<Notify>,
            // Count of delete calls for each index
            del_calls: Arc<Mutex<HashMap<IndexId, usize>>>,
        }

        impl TestState {
            fn new() -> Self {
                Self {
                    db_indexes: Arc::new(Mutex::new(Vec::new())),
                    engine_indexes: Arc::new(Mutex::new(HashSet::new())),
                    schema_version: Arc::new(Mutex::new(0)),
                    index_versions: Arc::new(Mutex::new(HashMap::new())),
                    notify: Arc::new(Notify::new()),
                    del_calls: Arc::new(Mutex::new(HashMap::new())),
                }
            }

            async fn add_index(&self, index: DbCustomIndex) {
                self.db_indexes.lock().unwrap().push(index);
                *self.schema_version.lock().unwrap() += 1;
                self.notify.notified().await;
            }

            async fn del_index(&self, index_name: IndexName) {
                self.db_indexes
                    .lock()
                    .unwrap()
                    .retain(|idx| idx.index != index_name);
                *self.schema_version.lock().unwrap() += 1;
                self.notify.notified().await;
            }

            fn get_db_indexes(&self) -> Vec<DbCustomIndex> {
                let guard = self.db_indexes.lock().unwrap();
                guard
                    .iter()
                    .map(|idx| DbCustomIndex {
                        keyspace: idx.keyspace.clone(),
                        index: idx.index.clone(),
                        table: idx.table.clone(),
                        target_column: idx.target_column.clone(),
                    })
                    .collect()
            }
        }

        // Engine mock
        async fn new_engine(state: TestState) -> anyhow::Result<mpsc::Sender<Engine>> {
            let (tx_eng, mut rx_eng) = mpsc::channel(10);

            tokio::spawn(async move {
                while let Some(msg) = rx_eng.recv().await {
                    match msg {
                        Engine::GetIndexIds { .. } => {}
                        Engine::AddIndex { metadata, tx } => {
                            state.engine_indexes.lock().unwrap().insert(metadata.id());
                            tx.send(Ok(())).unwrap();
                            state.notify.notify_waiters();
                        }
                        Engine::DelIndex { id } => {
                            let mut calls = state.del_calls.lock().unwrap();
                            *calls.entry(id.clone()).or_insert(0) += 1;
                            state.engine_indexes.lock().unwrap().remove(&id);
                            state.notify.notify_waiters();
                        }
                        Engine::GetIndex { .. } => {}
                    }
                }
            });

            Ok(tx_eng)
        }

        // DB mock
        async fn new_db(state: TestState) -> anyhow::Result<mpsc::Sender<Db>> {
            let (tx_db, mut rx_db) = mpsc::channel(10);
            tokio::spawn(async move {
                while let Some(msg) = rx_db.recv().await {
                    match msg {
                        Db::GetDbIndex { tx, .. } => {
                            // Not needed for this test
                            let _ = tx.send(Err(anyhow::anyhow!("Not implemented")));
                        }
                        Db::LatestSchemaVersion { tx } => {
                            let version = *state.schema_version.lock().unwrap();
                            let version_bytes = [version as u8; 16];
                            tx.send(Ok(Some(CqlTimeuuid::from_bytes(version_bytes))))
                                .unwrap();
                        }
                        Db::GetIndexes { tx } => {
                            let indexes = state.get_db_indexes();
                            tx.send(Ok(indexes)).unwrap();
                        }
                        Db::GetIndexVersion { index, tx, .. } => {
                            // Return a version for all indexes
                            let mut guard = state.index_versions.lock().unwrap();
                            let version = guard.entry(index).or_insert_with(Uuid::new_v4);
                            tx.send(Ok(Some((*version).into()))).unwrap();
                        }
                        Db::GetIndexTargetType { tx, .. } => {
                            // Return dimensions for all indexes
                            tx.send(Ok(Some(NonZeroUsize::new(3).unwrap().into())))
                                .unwrap();
                        }
                        Db::GetIndexParams { tx, .. } => {
                            // Return default params for all indexes
                            tx.send(Ok(Some((
                                Default::default(), // connectivity
                                Default::default(), // expansion_add
                                Default::default(), // expansion_search
                                Default::default(), // space_type
                            ))))
                            .unwrap();
                        }
                        Db::IsValidIndex { tx, .. } => {
                            // All indexes are valid for this test
                            tx.send(true).unwrap();
                        }
                    }
                }
            });
            Ok(tx_db)
        }

        let state = TestState::new();

        let tx_db = new_db(state.clone()).await.unwrap();
        let tx_eng = new_engine(state.clone()).await.unwrap();
        let (tx_ns, _rx_ns) = mpsc::channel(10);

        // Start the monitor
        let _monitor = new(tx_db.clone(), tx_eng.clone(), tx_ns.clone())
            .await
            .unwrap();

        // Add two indexes
        let index1 = sample_db_index("index1");
        let index2 = sample_db_index("index2");
        let index1_id = index1.id();
        let index2_id = index2.id();

        state.add_index(index1).await;
        state.add_index(index2).await;

        let engine_indexes = state.engine_indexes.lock().unwrap().clone();
        assert!(
            engine_indexes.contains(&index1_id) && engine_indexes.contains(&index2_id),
            "Both indexes should be present"
        );

        // Remove index2 from the list
        state.del_index(index2_id.index()).await;

        let engine_indexes = state.engine_indexes.lock().unwrap().clone();
        assert!(
            engine_indexes.contains(&index1_id) && !engine_indexes.contains(&index2_id),
            "Only index1 should remain"
        );

        // Remove index1 from the list
        state.del_index(index1_id.index()).await;

        let engine_indexes = state.engine_indexes.lock().unwrap().clone();
        assert!(
            !engine_indexes.contains(&index1_id) && !engine_indexes.contains(&index2_id),
            "Both indexes should be removed"
        );

        // Assert del_index called only once per index
        let calls = state.del_calls.lock().unwrap();
        assert_eq!(
            calls.get(&index1_id).copied().unwrap_or(0),
            1,
            "index1 should be removed once"
        );
        assert_eq!(
            calls.get(&index2_id).copied().unwrap_or(0),
            1,
            "index2 should be removed once"
        );
    }
}
