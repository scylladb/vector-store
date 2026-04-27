/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::DbIndexType;
use crate::IndexKey;
use crate::IndexMetadata;
use crate::IndexVersion;
use crate::KeyspaceName;
use crate::Quantization;
use crate::TableName;
use crate::db_index::DbIndex;
use crate::index::Index;
use crate::monitor_items::MonitorItems;
use crate::node_state::IndexStatus;
use crate::node_state::NodeState;
use crate::node_state::NodeStateExt;
use futures::future::join_all;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;

/// Indicates whether an index requires server-side filtering for a given query.
///
/// Used by routing to rank candidate indexes: an index that needs no filtering
/// (all restriction columns are covered by the partition key or filtering columns)
/// is preferred over one that still needs filtering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NeedsFiltering {
    /// All restriction columns are covered by the index partition key
    /// or filtering columns - no extra filtering needed.
    No,
    /// This many restriction columns are not covered by the partition key
    /// or filtering columns and require server-side filtering.
    Yes(usize),
}

impl PartialOrd for NeedsFiltering {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NeedsFiltering {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (NeedsFiltering::No, NeedsFiltering::No) => std::cmp::Ordering::Equal,
            (NeedsFiltering::No, NeedsFiltering::Yes(_)) => std::cmp::Ordering::Greater,
            (NeedsFiltering::Yes(_), NeedsFiltering::No) => std::cmp::Ordering::Less,
            (NeedsFiltering::Yes(a), NeedsFiltering::Yes(b)) => b.cmp(a),
        }
    }
}

/// Key for grouping indexes that can be routed between each other
/// (i.e., indexes over the same keyspace, table, and target column).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RoutingGroupKey {
    keyspace: KeyspaceName,
    table: TableName,
    column: ColumnName,
}

impl From<&IndexMetadata> for RoutingGroupKey {
    fn from(metadata: &IndexMetadata) -> Self {
        Self {
            keyspace: metadata.keyspace_name.clone(),
            table: metadata.table_name.clone(),
            column: metadata.target_column.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct IndexEntry {
    pub(crate) index: mpsc::Sender<Index>,
    pub(crate) _monitor: mpsc::Sender<MonitorItems>,
    pub(crate) db_index: mpsc::Sender<DbIndex>,
    pub(crate) routing_group: RoutingGroupKey,
    pub(crate) index_type: DbIndexType,
    pub(crate) filtering_columns: Arc<Vec<ColumnName>>,
    pub(crate) version: IndexVersion,
    pub(crate) quantization: Quantization,
}

impl IndexEntry {
    pub(crate) fn new(
        index: mpsc::Sender<Index>,
        monitor: mpsc::Sender<MonitorItems>,
        db_index: mpsc::Sender<DbIndex>,
        primary_key_columns: Arc<Vec<ColumnName>>,
        metadata: IndexMetadata,
    ) -> Self {
        let routing_group = RoutingGroupKey::from(&metadata);
        let filtering_columns: Arc<Vec<ColumnName>> = Arc::new(
            metadata
                .filtering_columns
                .iter()
                .chain(primary_key_columns.iter())
                .cloned()
                .collect(),
        );
        Self {
            index,
            _monitor: monitor,
            db_index,
            routing_group,
            index_type: metadata.index_type,
            filtering_columns,
            version: metadata.version,
            quantization: metadata.quantization,
        }
    }
}

/// Storage for all active indexes.
#[derive(Debug)]
pub(crate) struct Indexes(HashMap<IndexKey, IndexEntry>);

impl Indexes {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn get(&self, key: &IndexKey) -> Option<&IndexEntry> {
        self.0.get(key)
    }

    pub(crate) fn contains_key(&self, key: &IndexKey) -> bool {
        self.0.contains_key(key)
    }

    pub(crate) fn insert(&mut self, key: IndexKey, entry: IndexEntry) {
        self.0.insert(key, entry);
    }

    pub(crate) fn remove(&mut self, key: &IndexKey) -> Option<IndexEntry> {
        self.0.remove(key)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&IndexKey, &IndexEntry)> {
        self.0.iter()
    }
}

/// Maps each routing group to the set of index keys that belong to it.
#[derive(Debug)]
pub(crate) struct RoutingMap(HashMap<RoutingGroupKey, Vec<IndexKey>>);

impl RoutingMap {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn get(&self, key: &RoutingGroupKey) -> Option<&Vec<IndexKey>> {
        self.0.get(key)
    }

    pub(crate) fn add(&mut self, group: RoutingGroupKey, key: IndexKey) {
        self.0.entry(group).or_default().push(key);
    }

    pub(crate) fn remove(&mut self, group: RoutingGroupKey, key: &IndexKey) {
        if let Entry::Occupied(mut e) = self.0.entry(group) {
            e.get_mut().retain(|k| k != key);
            if e.get().is_empty() {
                e.remove();
            }
        }
    }
}

/// Computes a routing score for an index given the query's restriction columns.
///
/// Returns `None` when the index cannot serve the query at all. This happens
/// when a local index's partition key columns are not all present in the
/// equality restrictions, or when any non-partition-key restriction column is
/// not in the index's filtering columns.
///
/// Returns `Some(NeedsFiltering)` otherwise, indicating how many restriction
/// columns still require index-level filtering (via filtering columns).
pub(crate) fn score_index(
    index_type: &DbIndexType,
    filtering_columns: &[ColumnName],
    equality_columns: &[ColumnName],
    range_columns: &[ColumnName],
) -> Option<NeedsFiltering> {
    if !equality_columns
        .iter()
        .chain(range_columns.iter())
        .all(|col| filtering_columns.contains(col))
    {
        return None;
    }

    match index_type {
        DbIndexType::Global => {
            let uncovered = equality_columns.len() + range_columns.len();
            Some(if uncovered == 0 {
                NeedsFiltering::No
            } else {
                NeedsFiltering::Yes(uncovered)
            })
        }
        DbIndexType::Local(pk_columns) => {
            if !pk_columns.iter().all(|col| equality_columns.contains(col)) {
                return None;
            }
            let uncovered = equality_columns.len() - pk_columns.len() + range_columns.len();
            Some(if uncovered == 0 {
                NeedsFiltering::No
            } else {
                NeedsFiltering::Yes(uncovered)
            })
        }
    }
}

/// Result of routing an ANN query to the best matching index.
pub(crate) enum BestIndexState {
    /// The requested index does not exist at all.
    NotFound,
    /// The requested index exists but no serving candidate was found.
    NotServing(mpsc::Sender<DbIndex>),
    /// A serving candidate was found.
    Serving(IndexKey, mpsc::Sender<Index>, mpsc::Sender<DbIndex>),
}

/// Determines the index to route a query to, given a requested `IndexKey`.
///
/// To ensure queries are routed to the most up-to-date and best-matching index,
/// this function applies the following routing logic:
///
/// 1. Returns `NotFound` if the requested index key does not exist.
/// 2. Identifies all candidate indexes within the same routing group
///    (i.e., sharing the same keyspace, table, and target column).
/// 3. Filters out candidates whose `score_index` returns `None` (invalid).
/// 4. Narrows down the remaining candidates to those that are actively serving.
/// 5. Picks the candidate with the highest score, breaking ties by
///    the newest `IndexVersion`.
/// 6. Returns `NotServing` if no candidate meets the criteria.
pub(crate) async fn route_index(
    key: &IndexKey,
    equality_columns: &[ColumnName],
    range_columns: &[ColumnName],
    indexes: &Indexes,
    routing_map: &RoutingMap,
    node_state: &mpsc::Sender<NodeState>,
) -> BestIndexState {
    let Some(requested_entry) = indexes.get(key) else {
        return BestIndexState::NotFound;
    };
    let candidates = routing_map
        .get(&requested_entry.routing_group)
        .expect("routing_map must contain group for every index in indexes");

    let routed_key = join_all(candidates.iter().filter_map(|k| {
        let entry = indexes.get(k)?;
        let score = score_index(
            &entry.index_type,
            &entry.filtering_columns,
            equality_columns,
            range_columns,
        )?;
        Some(async move {
            let is_serving = node_state
                .get_index_status(k.keyspace().as_ref(), k.index().as_ref())
                .await
                .is_some_and(|status| status == IndexStatus::Serving);
            is_serving.then_some((k, score, &entry.version))
        })
    }))
    .await
    .into_iter()
    .flatten()
    .max_by(|(_, score_a, version_a), (_, score_b, version_b)| {
        score_a.cmp(score_b).then_with(|| version_a.cmp(version_b))
    })
    .map(|(k, _, _)| k);

    match routed_key {
        Some(routed_key) => {
            let routed_entry = indexes.get(routed_key).expect("routed key must exist");
            if routed_key != key {
                debug!("routing index request from {key} to {routed_key}");
            }
            BestIndexState::Serving(
                routed_key.clone(),
                routed_entry.index.clone(),
                routed_entry.db_index.clone(),
            )
        }
        None => BestIndexState::NotServing(requested_entry.db_index.clone()),
    }
}

/// Direct index lookup without routing.
///
/// Returns the index and db_index actors for the exact key requested,
/// regardless of routing groups or serving status. Used by status and
/// metrics endpoints that need to query a specific index.
pub(crate) fn get_index(
    key: &IndexKey,
    indexes: &Indexes,
) -> Option<(mpsc::Sender<Index>, mpsc::Sender<DbIndex>)> {
    let entry = indexes.get(key)?;
    Some((entry.index.clone(), entry.db_index.clone()))
}
