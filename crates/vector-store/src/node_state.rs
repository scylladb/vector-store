/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexId;
use crate::IndexMetadata;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry::Vacant;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
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

fn update_indexes(idxs: &mut HashMap<IndexId, IndexStatus>, ids: HashSet<IndexId>) {
    // Remove indexes that are no longer present
    idxs.retain(|idx, _| ids.contains(idx));

    for id in ids.into_iter() {
        // Add index only if not already present
        if let Vacant(e) = idxs.entry(id) {
            e.insert(IndexStatus::Initializing);
        }
    }
}

pub(crate) async fn new() -> mpsc::Sender<NodeState> {
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        async move {
            debug!("starting");

            let mut status = NodeStatus::Initializing;
            let mut initial_idxs = HashSet::new();
            let mut idxs = HashMap::<IndexId, IndexStatus>::new();
            while let Some(msg) = rx.recv().await {
                match msg {
                    NodeState::SendEvent(event) => match event {
                        Event::ConnectingToDb => {
                            status = NodeStatus::ConnectingToDb;
                        }
                        Event::ConnectedToDb => {}
                        Event::DiscoveringIndexes => {
                            if status != NodeStatus::Serving {
                                status = NodeStatus::DiscoveringIndexes;
                            }
                        }
                        Event::IndexesDiscovered(indexes) => {
                            if indexes.is_empty() && status != NodeStatus::Serving {
                                status = NodeStatus::Serving;
                                info!("Service is running, no indexes to build");
                                continue;
                            }

                            update_indexes(
                                &mut idxs,
                                indexes.iter().map(|meta| meta.id()).collect(),
                            );

                            if status == NodeStatus::DiscoveringIndexes {
                                status = NodeStatus::IndexingEmbeddings;
                                initial_idxs = indexes;
                            }
                        }
                        Event::FullScanStarted(metadata) => {
                            if let Some(index_status) = idxs.get_mut(&metadata.id()) {
                                *index_status = IndexStatus::FullScanning;
                            }
                        }
                        Event::FullScanFinished(metadata) => {
                            if let Some(index_status) = idxs.get_mut(&metadata.id()) {
                                *index_status = IndexStatus::Serving;
                            }

                            initial_idxs.remove(&metadata);
                            if initial_idxs.is_empty() && status != NodeStatus::Serving {
                                status = NodeStatus::Serving;
                                info!("Service is running, finished building indexes");
                            }
                        }
                    },
                    NodeState::GetStatus(tx) => {
                        tx.send(status).unwrap_or_else(|_| {
                            tracing::debug!("Failed to send current state");
                        });
                    }
                    NodeState::GetIndexStatus(tx, keyspace, index) => {
                        if let Some(index_status) = idxs.get(&IndexId::new(
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
    use crate::ColumnName;
    use crate::DbIndexType;
    use crate::Dimensions;
    use crate::IndexName;
    use crate::KeyspaceName;
    use crate::TableName;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_node_state_changes_as_expected() {
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
        let idx1 = IndexMetadata {
            keyspace_name: KeyspaceName("test_keyspace".to_string()),
            index_name: IndexName("test_index".to_string()),
            table_name: TableName("test_table".to_string()),
            target_column: ColumnName("test_column".to_string()),
            index_type: DbIndexType::Global,
            filtering_columns: Arc::new(Vec::new()),
            dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
            connectivity: Default::default(),
            expansion_add: Default::default(),
            expansion_search: Default::default(),
            space_type: Default::default(),
            version: Uuid::new_v4().into(),
            quantization: Default::default(),
        };
        let idx2 = IndexMetadata {
            keyspace_name: KeyspaceName("test_keyspace".to_string()),
            index_name: IndexName("test_index1".to_string()),
            table_name: TableName("test_table".to_string()),
            target_column: ColumnName("test_column".to_string()),
            index_type: DbIndexType::Global,
            filtering_columns: Arc::new(Vec::new()),
            dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
            connectivity: Default::default(),
            expansion_add: Default::default(),
            expansion_search: Default::default(),
            space_type: Default::default(),
            version: Uuid::new_v4().into(),
            quantization: Default::default(),
        };
        let idxs = HashSet::from([idx1.clone(), idx2.clone()]);
        node_state.send_event(Event::IndexesDiscovered(idxs)).await;
        status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);
        node_state.send_event(Event::FullScanFinished(idx1)).await;
        status = node_state.get_status().await;
        assert_eq!(status, NodeStatus::IndexingEmbeddings);
        node_state.send_event(Event::FullScanFinished(idx2)).await;
        status = node_state.get_status().await;
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
            target_column: ColumnName("test_column".to_string()),
            index_type: DbIndexType::Global,
            filtering_columns: Arc::new(Vec::new()),
            dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
            connectivity: Default::default(),
            expansion_add: Default::default(),
            expansion_search: Default::default(),
            space_type: Default::default(),
            version: Uuid::new_v4().into(),
            quantization: Default::default(),
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
            target_column: ColumnName("test_column".to_string()),
            index_type: DbIndexType::Global,
            filtering_columns: Arc::new(Vec::new()),
            dimensions: Dimensions(NonZeroUsize::new(3).unwrap()),
            connectivity: Default::default(),
            expansion_add: Default::default(),
            expansion_search: Default::default(),
            space_type: Default::default(),
            version: Uuid::new_v4().into(),
            quantization: Default::default(),
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
