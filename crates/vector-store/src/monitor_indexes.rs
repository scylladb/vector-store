/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Connectivity;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexMetadata;
use crate::Quantization;
use crate::SpaceType;
use crate::db::Db;
use crate::db::DbExt;
use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::node_state::Event;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use anyhow::bail;
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
use tracing::error_span;
use tracing::info;
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
                            info!("monitor_indexes: unable to get the list of indexes: {err}");
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
        .instrument(error_span!("monitor_indexes")),
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
            .get_index_version(idx.keyspace.clone(), idx.table.clone(), idx.index.clone())
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

        let (connectivity, expansion_add, expansion_search, space_type, quantization) =
            if let Some(params) = db
                .get_index_params(idx.keyspace.clone(), idx.table.clone(), idx.index.clone())
                .await
                .inspect_err(|err| warn!("unable to get index params: {err}"))?
            {
                params
            } else {
                debug!("get_indexes: no params for index {idx:?}");
                (
                    Connectivity::default(),
                    ExpansionAdd::default(),
                    ExpansionSearch::default(),
                    SpaceType::default(),
                    Quantization::default(),
                )
            };

        let metadata = IndexMetadata {
            keyspace_name: idx.keyspace,
            index_name: idx.index,
            table_name: idx.table,
            target_column: idx.target_column,
            index_type: idx.index_type,
            filtering_columns: idx.filtering_columns,
            dimensions,
            connectivity,
            expansion_add,
            expansion_search,
            space_type,
            version,
            quantization,
        };

        if !db.is_valid_index(metadata.clone()).await {
            let msg = format!("get_indexes: not valid index {}", metadata.id());
            debug!(msg);
            bail!(msg);
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
    use crate::DbCustomIndex;
    use crate::DbIndexType;
    use crate::IndexId;
    use crate::IndexName;
    use crate::db;
    use crate::db::LatestSchemaVersionR;
    use crate::db::tests::MockSimDb;
    use crate::engine;
    use crate::engine::tests::MockSimEngine;
    use anyhow::anyhow;
    use futures::FutureExt;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::sync::Notify;
    use uuid::Uuid;

    #[tokio::test]
    async fn schema_version_changed() {
        let version1 = CqlTimeuuid::from_bytes([1; 16]);
        let version2 = CqlTimeuuid::from_bytes([2; 16]);
        let latest_schema_version: Arc<Mutex<Option<LatestSchemaVersionR>>> =
            Arc::new(Mutex::new(None));
        let set_latest_schema_version = |v| {
            latest_schema_version.lock().unwrap().replace(v);
        };

        let mut mock_db = MockSimDb::new();
        mock_db.expect_latest_schema_version().returning({
            let latest_schema_version = Arc::clone(&latest_schema_version);
            move |tx| {
                let latest_schema_version = Arc::clone(&latest_schema_version);
                async move {
                    tx.send(
                        latest_schema_version
                            .lock()
                            .unwrap()
                            .take()
                            .expect("latest_schema_version should have a value"),
                    )
                    .unwrap();
                }
                .boxed()
            }
        });

        let mut sv = SchemaVersion::new();
        let tx_db = db::tests::new(mock_db);

        // step 1: Err should not change the schema version
        set_latest_schema_version(Err(anyhow!("test issue")));
        assert!(!sv.has_changed(&tx_db).await);

        // step 2: None should not change the schema version
        set_latest_schema_version(Ok(None));
        assert!(!sv.has_changed(&tx_db).await);

        // step 3: value1 should change the schema version
        set_latest_schema_version(Ok(Some(version1)));
        assert!(sv.has_changed(&tx_db).await);

        // step 4: Err should change the schema version
        set_latest_schema_version(Err(anyhow!("test issue")));
        assert!(sv.has_changed(&tx_db).await);

        // step 5: value1 should change the schema version
        set_latest_schema_version(Ok(Some(version1)));
        assert!(sv.has_changed(&tx_db).await);

        // step 6: None should change the schema version
        set_latest_schema_version(Ok(None));
        assert!(sv.has_changed(&tx_db).await);

        // step 7: value1 should change the schema version
        set_latest_schema_version(Ok(Some(version1)));
        assert!(sv.has_changed(&tx_db).await);

        // step 8: value1 should not change the schema version
        set_latest_schema_version(Ok(Some(version1)));
        assert!(!sv.has_changed(&tx_db).await);

        // step 9: value2 should change the schema version
        set_latest_schema_version(Ok(Some(version2)));
        assert!(sv.has_changed(&tx_db).await);
    }

    #[tokio::test]
    #[ntest::timeout(5_000)]
    async fn index_metadata_are_removed_once() {
        type IndexesT = HashSet<IndexId>;

        // Dummy db index for testing
        fn sample_db_index(name: &str) -> DbCustomIndex {
            DbCustomIndex {
                keyspace: "ks".to_string().into(),
                index: name.to_string().into(),
                table: "tbl".to_string().into(),
                target_column: "embedding".to_string().into(),
                index_type: DbIndexType::Global,
                filtering_columns: Arc::new(Vec::new()),
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
                        index_type: DbIndexType::Global,
                        filtering_columns: Arc::new(Vec::new()),
                    })
                    .collect()
            }
        }

        let state = TestState::new();

        // Engine mock
        let mut mock_engine = MockSimEngine::new();

        mock_engine.expect_add_index().returning({
            let state = state.clone();
            move |metadata, tx| {
                let state = state.clone();
                async move {
                    state.engine_indexes.lock().unwrap().insert(metadata.id());
                    tx.send(Ok(())).unwrap();
                    state.notify.notify_waiters();
                }
                .boxed()
            }
        });

        mock_engine.expect_del_index().returning({
            let state = state.clone();
            move |id| {
                let state = state.clone();
                async move {
                    let mut calls = state.del_calls.lock().unwrap();
                    *calls.entry(id.clone()).or_insert(0) += 1;
                    state.engine_indexes.lock().unwrap().remove(&id);
                    state.notify.notify_waiters();
                }
                .boxed()
            }
        });

        // DB mock
        let mut mock_db = MockSimDb::new();

        mock_db.expect_latest_schema_version().returning({
            let state = state.clone();
            move |tx| {
                let state = state.clone();
                async move {
                    let version = *state.schema_version.lock().unwrap();
                    let version_bytes = [version as u8; 16];
                    tx.send(Ok(Some(CqlTimeuuid::from_bytes(version_bytes))))
                        .unwrap();
                }
                .boxed()
            }
        });

        mock_db.expect_get_indexes().returning({
            let state = state.clone();
            move |tx| {
                let state = state.clone();
                async move {
                    let indexes = state.get_db_indexes();
                    tx.send(Ok(indexes)).unwrap();
                }
                .boxed()
            }
        });

        mock_db.expect_get_index_version().returning({
            let state = state.clone();
            move |_, _, index, tx| {
                let state = state.clone();
                async move {
                    // Return a version for all indexes
                    let mut guard = state.index_versions.lock().unwrap();
                    let version = guard.entry(index).or_insert_with(Uuid::new_v4);
                    tx.send(Ok(Some((*version).into()))).unwrap();
                }
                .boxed()
            }
        });

        mock_db
            .expect_get_index_target_type()
            .returning(move |_, _, _, tx| {
                async move {
                    // Return dimensions for all indexes
                    tx.send(Ok(Some(NonZeroUsize::new(3).unwrap().into())))
                        .unwrap();
                }
                .boxed()
            });

        mock_db
            .expect_get_index_params()
            .returning(move |_, _, _, tx| {
                async move {
                    // Return default params for all indexes
                    tx.send(Ok(Some((
                        Default::default(), // connectivity
                        Default::default(), // expansion_add
                        Default::default(), // expansion_search
                        Default::default(), // space_type
                        Default::default(), // quantization
                    ))))
                    .unwrap();
                }
                .boxed()
            });

        mock_db.expect_is_valid_index().returning(move |_, tx| {
            async move {
                // All indexes are valid for this test
                tx.send(true).unwrap();
            }
            .boxed()
        });

        let tx_db = db::tests::new(mock_db);
        let tx_eng = engine::tests::new(mock_engine);
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

    #[tokio::test]
    #[ntest::timeout(5_000)]
    async fn get_indexes_failed_while_index_is_invalid() {
        let valid_indexes: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(vec![]));
        let set_valid_indexes = |v| {
            *valid_indexes.lock().unwrap() = v;
        };

        let mut mock_db = MockSimDb::new();

        mock_db.expect_get_indexes().returning({
            move |tx| {
                async move {
                    let index = || DbCustomIndex {
                        keyspace: "ks".to_string().into(),
                        index: "idx".to_string().into(),
                        table: "tbl".to_string().into(),
                        target_column: "embedding".to_string().into(),
                        index_type: DbIndexType::Global,
                        filtering_columns: Arc::new(Vec::new()),
                    };
                    tx.send(Ok(vec![index(), index(), index()])).unwrap();
                }
                .boxed()
            }
        });

        mock_db.expect_get_index_version().returning({
            move |_, _, _, tx| {
                async move {
                    tx.send(Ok(Some(Uuid::new_v4().into()))).unwrap();
                }
                .boxed()
            }
        });

        mock_db
            .expect_get_index_target_type()
            .returning(move |_, _, _, tx| {
                async move {
                    tx.send(Ok(Some(NonZeroUsize::new(3).unwrap().into())))
                        .unwrap();
                }
                .boxed()
            });

        mock_db
            .expect_get_index_params()
            .returning(move |_, _, _, tx| {
                async move {
                    tx.send(Ok(Some((
                        Default::default(),
                        Default::default(),
                        Default::default(),
                        Default::default(),
                        Default::default(),
                    ))))
                    .unwrap();
                }
                .boxed()
            });

        mock_db.expect_is_valid_index().returning({
            let valid_indexes = Arc::clone(&valid_indexes);
            move |_, tx| {
                let valid_indexes = Arc::clone(&valid_indexes);
                async move {
                    tx.send(valid_indexes.lock().unwrap().remove(0)).unwrap();
                }
                .boxed()
            }
        });

        let db = db::tests::new(mock_db);

        // all indexes are valid
        set_valid_indexes(vec![true, true, true]);
        assert!(get_indexes(&db).await.is_ok());

        // second index is invalid
        set_valid_indexes(vec![true, false, true]);
        assert!(get_indexes(&db).await.is_err());
    }
}
