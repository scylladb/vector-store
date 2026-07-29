/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexKey;
use crate::IndexMetadata;
use crate::perf;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry::Vacant;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::info;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    Initializing,
    ConnectingToDb,
    DiscoveringIndexes,
    IndexingEmbeddings,
    Serving,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexStatus {
    Initializing,
    FullScanning,
    Serving,
}

pub enum Event {
    ConnectingToDb,
    ConnectedToDb,
    DiscoveringIndexes,
    IndexesDiscovered(HashSet<IndexMetadata>),
    FullScanStarted(IndexMetadata),
    FullScanFinished(IndexMetadata),
}

pub enum NodeState {
    SendEvent(Event),
    GetStatus(oneshot::Sender<NodeStatus>),
    GetIndexStatus(oneshot::Sender<Option<IndexStatus>>, String, String),
}

pub(crate) trait NodeStateExt {
    async fn send_event(&self, event: Event);
    async fn get_status(&self) -> NodeStatus;
    async fn get_index_status(&self, keyspace: &str, index: &str) -> Option<IndexStatus>;
}

impl NodeStateExt for mpsc::Sender<NodeState> {
    async fn send_event(&self, event: Event) {
        let msg = NodeState::SendEvent(event);
        self.send(msg)
            .await
            .expect("NodeStateExt::send_event: internal actor should receive event");
    }

    async fn get_status(&self) -> NodeStatus {
        let (tx, rx) = oneshot::channel();
        self.send(NodeState::GetStatus(tx))
            .await
            .expect("NodeStateExt::get_status: internal actor should receive request");
        rx.await
            .expect("NodeStateExt::get_status: failed to receive status")
    }

    async fn get_index_status(&self, keyspace: &str, index: &str) -> Option<IndexStatus> {
        let (tx, rx) = oneshot::channel();
        self.send(NodeState::GetIndexStatus(
            tx,
            keyspace.to_string(),
            index.to_string(),
        ))
        .await
        .expect("NodeStateExt::get_index_status: internal actor should receive request");
        rx.await
            .expect("NodeStateExt::get_index_status: failed to receive index status")
    }
}

fn update_indexes(idxs: &mut HashMap<IndexKey, IndexStatus>, keys: HashSet<IndexKey>) {
    // Remove indexes that are no longer present
    idxs.retain(|idx, _| keys.contains(idx));

    for key in keys.into_iter() {
        // Add index only if not already present
        if let Vacant(e) = idxs.entry(key) {
            e.insert(IndexStatus::Initializing);
        }
    }
}

pub(crate) async fn new() -> mpsc::Sender<NodeState> {
    let (tx, mut rx) = mpsc::channel(perf::channel_size().into());

    tokio::spawn(
        async move {
            debug!("starting");

            let mut status = NodeStatus::Initializing;
            let mut initial_idxs: Option<HashSet<_>> = None;
            let mut idxs = HashMap::<IndexKey, IndexStatus>::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    NodeState::SendEvent(event) => match event {
                        Event::ConnectingToDb => {
                            status = NodeStatus::ConnectingToDb;
                        }

                        Event::ConnectedToDb => {}

                        Event::DiscoveringIndexes => match status {
                            NodeStatus::Initializing | NodeStatus::ConnectingToDb => {
                                if let Some(initial_idxs) = &initial_idxs {
                                    if initial_idxs.is_empty() {
                                        // Initial indexes already built
                                        status = NodeStatus::Serving;
                                    } else {
                                        // Still in the process of building initial indexes
                                        status = NodeStatus::IndexingEmbeddings
                                    }
                                } else {
                                    // Need to discover indexes first
                                    status = NodeStatus::DiscoveringIndexes;
                                }
                            }
                            _ => {}
                        },

                        Event::IndexesDiscovered(indexes) => {
                            let mut initial = false;
                            if initial_idxs.is_none() {
                                initial = true;
                                initial_idxs = Some(indexes.clone());
                            }

                            update_indexes(
                                &mut idxs,
                                indexes.iter().map(|meta| meta.key()).collect(),
                            );

                            let initial_idxs = initial_idxs
                                .as_mut()
                                .expect("initial_idxs should be Some here");

                            // remove any initial indexes that are no longer present
                            initial_idxs.retain(|idx| {
                                idxs.contains_key(&idx.key()) && indexes.contains(idx)
                            });

                            if initial_idxs.is_empty() {
                                if status != NodeStatus::Serving {
                                    status = NodeStatus::Serving;
                                    info!(
                                        "Service is running, no {more}initial indexes to build",
                                        more = if initial { "" } else { "more " }
                                    );
                                }
                                continue;
                            }

                            // we have initial indexes to build, so we need to be in the
                            // IndexingEmbeddings state
                            status = NodeStatus::IndexingEmbeddings;
                        }

                        Event::FullScanStarted(metadata) => {
                            if let Some(index_status) = idxs.get_mut(&metadata.key()) {
                                *index_status = IndexStatus::FullScanning;
                            }
                        }

                        Event::FullScanFinished(metadata) => {
                            if let Some(index_status) = idxs.get_mut(&metadata.key()) {
                                *index_status = IndexStatus::Serving;
                            }

                            let Some(initial_idxs) = &mut initial_idxs else {
                                error!(
                                    "Received FullScanFinished for index {index:?} \
                                    but initial_idxs is None",
                                    index = metadata.key()
                                );
                                continue;
                            };
                            initial_idxs.remove(&metadata);
                            if initial_idxs.is_empty() && status != NodeStatus::Serving {
                                status = NodeStatus::Serving;
                                info!("Service is running, finished building initial indexes");
                            }
                        }
                    },
                    NodeState::GetStatus(tx) => {
                        tx.send(status).unwrap_or_else(|_| {
                            tracing::debug!("Failed to send current state");
                        });
                    }
                    NodeState::GetIndexStatus(tx, keyspace, index) => {
                        if let Some(index_status) = idxs.get(&IndexKey::new(
                            &crate::KeyspaceName(keyspace.clone()),
                            &crate::IndexName(index.clone()),
                        )) {
                            tx.send(Some(*index_status)).unwrap_or_else(|_| {
                                tracing::debug!("Failed to send index status");
                            });
                        } else {
                            tx.send(None).unwrap_or_else(|_| {
                                tracing::debug!("Failed to send index status for missing index");
                            });
                        }
                    }
                }
            }
            debug!("finished");
        }
        .instrument(debug_span!("node_state")),
    );

    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbIndexPartitioning;
    use crate::Dimensions;
    use crate::IndexKind;
    use crate::IndexName;
    use crate::IndexOptionsVs;
    use crate::KeyspaceName;
    use crate::NonemptyArc;
    use crate::TableName;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use uuid::Uuid;

    fn index_metadata(name: &str) -> IndexMetadata {
        IndexMetadata {
            keyspace_name: KeyspaceName("test_keyspace".to_string()),
            index_name: IndexName(name.to_string()),
            table_name: TableName("test_table".to_string()),
            primary_key_columns: NonemptyArc::new(["pk"]).unwrap(),
            partition_key_count: NonZeroUsize::new(1).unwrap(),
            target_columns: NonemptyArc::new(["test_column"]).unwrap(),
            partitioning: DbIndexPartitioning::Global,
            filtering_columns: Arc::new([]),
            version: Uuid::new_v4().into(),
            kind: IndexKind::Vs(IndexOptionsVs {
                dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
                connectivity: Default::default(),
                expansion_add: Default::default(),
                expansion_search: Default::default(),
                space_type: Default::default(),
                quantization: Default::default(),
            }),
        }
    }

    #[tokio::test]
    async fn node_state_events_one_by_one() {
        let node_state = new().await;
        let idx1 = index_metadata("idx1");
        let idx2 = index_metadata("idx2");

        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Initializing);

        node_state.send_event(Event::ConnectingToDb).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::ConnectedToDb).await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::DiscoveringIndexes).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::DiscoveringIndexes);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state.send_event(Event::FullScanFinished(idx1)).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state.send_event(Event::FullScanFinished(idx2)).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);
    }

    #[tokio::test]
    async fn node_state_discovering_indexes_while_bootstrapping() {
        let node_state = new().await;
        let idx1 = index_metadata("idx1");
        let idx2 = index_metadata("idx2");

        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Initializing);

        node_state.send_event(Event::ConnectingToDb).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::ConnectedToDb).await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::DiscoveringIndexes).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::DiscoveringIndexes);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::FullScanFinished(idx1.clone()))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state.send_event(Event::DiscoveringIndexes).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state.send_event(Event::FullScanFinished(idx2)).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);
    }

    #[tokio::test]
    async fn node_state_db_reconnect_while_bootstrapping() {
        let node_state = new().await;
        let idx1 = index_metadata("idx1");
        let idx2 = index_metadata("idx2");

        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Initializing);

        node_state.send_event(Event::ConnectingToDb).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::ConnectedToDb).await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::DiscoveringIndexes).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::DiscoveringIndexes);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::FullScanFinished(idx1.clone()))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state.send_event(Event::ConnectingToDb).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::ConnectedToDb).await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::DiscoveringIndexes).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state.send_event(Event::FullScanFinished(idx2)).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);
    }

    #[tokio::test]
    async fn node_state_removing_index_while_bootstrapping() {
        let node_state = new().await;
        let idx1 = index_metadata("idx1");
        let idx2 = index_metadata("idx2");

        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Initializing);

        node_state.send_event(Event::ConnectingToDb).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::ConnectedToDb).await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::DiscoveringIndexes).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::DiscoveringIndexes);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::FullScanFinished(idx1.clone()))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([idx1.clone()])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);
    }

    #[tokio::test]
    async fn node_state_update_index_with_changed_metadata_while_bootstrapping() {
        let node_state = new().await;
        let idx1 = index_metadata("idx1");
        let idx2 = index_metadata("idx2");
        let idx2_updated = IndexMetadata {
            version: Uuid::new_v4().into(),
            ..idx2.clone()
        };

        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Initializing);

        node_state.send_event(Event::ConnectingToDb).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::ConnectedToDb).await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::DiscoveringIndexes).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::DiscoveringIndexes);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::FullScanFinished(idx1.clone()))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2_updated.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);
    }

    #[tokio::test]
    async fn node_state_adding_index_while_bootstrapping() {
        let node_state = new().await;
        let idx1 = index_metadata("idx1");
        let idx2 = index_metadata("idx2");
        let idx3 = index_metadata("idx3");

        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Initializing);

        node_state.send_event(Event::ConnectingToDb).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::ConnectedToDb).await;
        assert_eq!(status, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::DiscoveringIndexes).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::DiscoveringIndexes);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::FullScanFinished(idx1.clone()))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([
                idx1.clone(),
                idx2.clone(),
                idx3.clone(),
            ])))
            .await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);

        node_state.send_event(Event::FullScanFinished(idx2)).await;
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);
    }

    #[tokio::test]
    async fn test_index_state_changes_as_expected() {
        let node_state = new().await;
        let mut status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Initializing);
        node_state.send_event(Event::ConnectingToDb).await;
        status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::ConnectingToDb);
        node_state.send_event(Event::ConnectedToDb).await;
        node_state.send_event(Event::DiscoveringIndexes).await;
        status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::DiscoveringIndexes);
        let idx = IndexMetadata {
            keyspace_name: KeyspaceName("test_keyspace".to_string()),
            index_name: IndexName("test_index".to_string()),
            table_name: TableName("test_table".to_string()),
            primary_key_columns: NonemptyArc::new(["pk"]).unwrap(),
            partition_key_count: NonZeroUsize::new(1).unwrap(),
            target_columns: NonemptyArc::new(["test_column"]).unwrap(),
            partitioning: DbIndexPartitioning::Global,
            filtering_columns: Arc::new([]),
            version: Uuid::new_v4().into(),
            kind: IndexKind::Vs(IndexOptionsVs {
                dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
                connectivity: Default::default(),
                expansion_add: Default::default(),
                expansion_search: Default::default(),
                space_type: Default::default(),
                quantization: Default::default(),
            }),
        };
        let idxs = HashSet::from([idx.clone()]);
        node_state.send_event(Event::IndexesDiscovered(idxs)).await;

        // Check index state after discovery
        let idx_status = node_state
            .get_index_status(&idx.keyspace_name.0, &idx.index_name.0)
            .await;
        assert_eq!(idx_status, Some(IndexStatus::Initializing));

        // Simulate full scan started and finished for idx
        node_state
            .send_event(Event::FullScanStarted(idx.clone()))
            .await;
        let idx_status = node_state
            .get_index_status(&idx.keyspace_name.0, &idx.index_name.0)
            .await;
        assert_eq!(idx_status, Some(IndexStatus::FullScanning));

        node_state
            .send_event(Event::FullScanFinished(idx.clone()))
            .await;
        let idx_status = node_state
            .get_index_status(&idx.keyspace_name.0, &idx.index_name.0)
            .await;
        assert_eq!(idx_status, Some(IndexStatus::Serving));

        // Simulate removal of the index (empty set)
        node_state
            .send_event(Event::IndexesDiscovered(HashSet::new()))
            .await;
        let idx_status = node_state
            .get_index_status(&idx.keyspace_name.0, &idx.index_name.0)
            .await;
        assert_eq!(idx_status, None); // Index should be missing
    }

    #[tokio::test]
    async fn no_indexes_discovered() {
        let node_state = new().await;

        assert_eq!(node_state.get_status().await, NodeStatus::Initializing);

        node_state.send_event(Event::ConnectingToDb).await;
        assert_eq!(node_state.get_status().await, NodeStatus::ConnectingToDb);

        node_state.send_event(Event::DiscoveringIndexes).await;
        assert_eq!(
            node_state.get_status().await,
            NodeStatus::DiscoveringIndexes
        );

        node_state
            .send_event(Event::IndexesDiscovered(HashSet::new()))
            .await;
        assert_eq!(node_state.get_status().await, NodeStatus::Serving);
    }

    #[tokio::test]
    async fn status_remains_serving_when_discovering_indexes() {
        let node_state = new().await;
        // Move to Serving status
        node_state.send_event(Event::ConnectingToDb).await;
        node_state.send_event(Event::DiscoveringIndexes).await;
        node_state
            .send_event(Event::IndexesDiscovered(HashSet::new()))
            .await;
        assert_eq!(node_state.get_status().await, NodeStatus::Serving);

        // Try to trigger DiscoveringIndexes again
        node_state.send_event(Event::DiscoveringIndexes).await;
        // Status should remain Serving
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);

        let idx = IndexMetadata {
            keyspace_name: KeyspaceName("test_keyspace".to_string()),
            index_name: IndexName("test_index".to_string()),
            table_name: TableName("test_table".to_string()),
            primary_key_columns: NonemptyArc::new(["pk"]).unwrap(),
            partition_key_count: NonZeroUsize::new(1).unwrap(),
            target_columns: NonemptyArc::new(["test_column"]).unwrap(),
            partitioning: DbIndexPartitioning::Global,
            filtering_columns: Arc::new([]),
            version: Uuid::new_v4().into(),
            kind: IndexKind::Vs(IndexOptionsVs {
                dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
                connectivity: Default::default(),
                expansion_add: Default::default(),
                expansion_search: Default::default(),
                space_type: Default::default(),
                quantization: Default::default(),
            }),
        };

        // Simulate discovering an index
        node_state
            .send_event(Event::IndexesDiscovered(HashSet::from([idx.clone()])))
            .await;
        // Status should remain Serving
        let status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::Serving);

        // Index state should be present and in Initializing state
        let idx_status = node_state
            .get_index_status(&idx.keyspace_name.0, &idx.index_name.0)
            .await;
        assert_eq!(idx_status, Some(IndexStatus::Initializing));
    }
}
