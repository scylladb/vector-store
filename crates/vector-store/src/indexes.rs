/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::DbIndexPartitioning;
use crate::IndexKey;
use crate::IndexMetadata;
use crate::IndexVersion;
use crate::KeyspaceName;
use crate::NonemptyArc;
use crate::Progress;
use crate::TableName;
use crate::db_index::DbIndex;
use crate::db_index::DbIndexExt;
use crate::fts_index::FtsIndex;
use crate::monitor_items::MonitorItems;
use crate::node_state::IndexStatus;
use crate::vs_index::VsIndex;
use scylla::cluster::metadata::NativeType;
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
    columns: NonemptyArc<ColumnName>,
}

impl From<&IndexMetadata> for RoutingGroupKey {
    fn from(metadata: &IndexMetadata) -> Self {
        Self {
            keyspace: metadata.keyspace_name.clone(),
            table: metadata.table_name.clone(),
            columns: metadata.target_columns.clone(),
        }
    }
}

pub(crate) struct IndexEntry<I, D = ()> {
    index: mpsc::Sender<I>,
    _monitor: mpsc::Sender<MonitorItems>,
    db_index: mpsc::Sender<DbIndex>,
    status: IndexStatus,
    progress: Progress,
    primary_key_columns: NonemptyArc<ColumnName>,
    data: D,
}

impl<I, D: std::fmt::Debug> std::fmt::Debug for IndexEntry<I, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexEntry")
            .field("status", &self.status)
            .field("progress", &self.progress)
            .field("data", &self.data)
            .finish_non_exhaustive()
    }
}

pub(crate) type VsIndexEntry = IndexEntry<VsIndex, VsIndexData>;
pub(crate) type FtsIndexEntry = IndexEntry<FtsIndex>;

#[derive(Debug)]
pub(crate) struct VsIndexData {
    routing_group: RoutingGroupKey,
    partitioning: DbIndexPartitioning,
    filtering_columns: Arc<[ColumnName]>,
    table_columns: Arc<HashMap<ColumnName, NativeType>>,
    version: IndexVersion,
    options: crate::IndexOptionsVs,
}

impl<I, D> IndexEntry<I, D> {
    pub(crate) fn index(&self) -> &mpsc::Sender<I> {
        &self.index
    }

    pub(crate) fn db_index(&self) -> mpsc::Sender<DbIndex> {
        self.db_index.clone()
    }

    pub(crate) fn progress(&self) -> Progress {
        self.progress
    }

    pub(crate) fn set_progress(&mut self, progress: Progress) {
        self.progress = progress;
    }

    pub(crate) fn status(&self) -> IndexStatus {
        self.status
    }

    pub(crate) fn set_status(&mut self, status: IndexStatus) {
        self.status = status;
    }

    pub(crate) fn primary_key_columns(&self) -> &NonemptyArc<ColumnName> {
        &self.primary_key_columns
    }
}

impl VsIndexEntry {
    pub(crate) async fn new(
        index: mpsc::Sender<VsIndex>,
        monitor: mpsc::Sender<MonitorItems>,
        db_index: mpsc::Sender<DbIndex>,
        metadata: IndexMetadata,
    ) -> anyhow::Result<Self> {
        let routing_group = RoutingGroupKey::from(&metadata);
        let options = metadata
            .vs()
            .ok_or_else(|| {
                anyhow::anyhow!("add_index_vs must be called with a vector-search index")
            })?
            .clone();
        let primary_key_columns = db_index.get_primary_key_columns().await;
        let filtering_columns = metadata
            .filtering_columns
            .iter()
            .chain(primary_key_columns.iter())
            .cloned()
            .collect();
        let table_columns = db_index.get_table_columns().await;
        let progress = db_index.full_scan_progress().await;
        Ok(Self {
            index,
            _monitor: monitor,
            db_index,
            status: IndexStatus::Initializing,
            progress,
            primary_key_columns,
            data: VsIndexData {
                routing_group,
                partitioning: metadata.partitioning,
                filtering_columns,
                table_columns,
                version: metadata.version,
                options,
            },
        })
    }

    pub(crate) fn options(&self) -> &crate::IndexOptionsVs {
        &self.data.options
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
    fn score_index(
        &self,
        equality_columns: &[ColumnName],
        range_columns: &[ColumnName],
    ) -> Option<NeedsFiltering> {
        if !equality_columns
            .iter()
            .chain(range_columns.iter())
            .all(|col| self.data.filtering_columns.contains(col))
        {
            return None;
        }

        match &self.data.partitioning {
            DbIndexPartitioning::Global => {
                let uncovered = equality_columns.len() + range_columns.len();
                Some(if uncovered == 0 {
                    NeedsFiltering::No
                } else {
                    NeedsFiltering::Yes(uncovered)
                })
            }
            DbIndexPartitioning::Local(pk_columns) => {
                if !pk_columns.iter().all(|col| equality_columns.contains(col)) {
                    return None;
                }
                let uncovered =
                    equality_columns.len() - pk_columns.len().get() + range_columns.len();
                Some(if uncovered == 0 {
                    NeedsFiltering::No
                } else {
                    NeedsFiltering::Yes(uncovered)
                })
            }
        }
    }
}

impl FtsIndexEntry {
    pub(crate) async fn new(
        index: mpsc::Sender<FtsIndex>,
        monitor: mpsc::Sender<MonitorItems>,
        db_index: mpsc::Sender<DbIndex>,
    ) -> Self {
        let primary_key_columns = db_index.get_primary_key_columns().await;
        let progress = db_index.full_scan_progress().await;
        Self {
            index,
            _monitor: monitor,
            db_index,
            status: IndexStatus::Initializing,
            progress,
            primary_key_columns,
            data: (),
        }
    }
}

/// Result of routing an ANN query to the best matching VS index.
pub(crate) enum BestIndexState {
    /// The requested index does not exist at all.
    NotFound,
    /// The requested index exists but no serving candidate was found.
    NotServing(Progress),
    /// Serving candidates exist but none can handle a global query
    /// (all are local-only and the query lacks partition key restrictions).
    NoGlobalIndex,
    /// A serving candidate was found.
    Serving {
        key: IndexKey,
        index: mpsc::Sender<VsIndex>,
        primary_key_columns: NonemptyArc<ColumnName>,
        table_columns: Arc<HashMap<ColumnName, NativeType>>,
        needs_filtering: NeedsFiltering,
    },
}

/// Storage for all active indexes and map of routing group to the set of index keys that belong to it.
#[derive(Debug)]
pub(crate) struct Indexes {
    vs_entries: HashMap<IndexKey, VsIndexEntry>,
    vs_routing: HashMap<RoutingGroupKey, Vec<IndexKey>>,
    fts_entries: HashMap<IndexKey, FtsIndexEntry>,
}

impl Indexes {
    pub(crate) fn new() -> Self {
        Self {
            vs_entries: HashMap::new(),
            vs_routing: HashMap::new(),
            fts_entries: HashMap::new(),
        }
    }

    pub(crate) fn get_vs(&self, key: &IndexKey) -> Option<&VsIndexEntry> {
        self.vs_entries.get(key)
    }

    pub(crate) fn get_vs_mut(&mut self, key: &IndexKey) -> Option<&mut VsIndexEntry> {
        self.vs_entries.get_mut(key)
    }

    pub(crate) fn get_fts(&self, key: &IndexKey) -> Option<&FtsIndexEntry> {
        self.fts_entries.get(key)
    }

    pub(crate) fn get_fts_mut(&mut self, key: &IndexKey) -> Option<&mut FtsIndexEntry> {
        self.fts_entries.get_mut(key)
    }

    pub(crate) fn contains_key(&self, key: &IndexKey) -> bool {
        self.vs_entries.contains_key(key) || self.fts_entries.contains_key(key)
    }

    pub(crate) fn insert_vs(&mut self, key: IndexKey, entry: VsIndexEntry) {
        let routing_group = entry.data.routing_group.clone();
        self.vs_entries.insert(key.clone(), entry);
        self.vs_routing.entry(routing_group).or_default().push(key);
    }

    pub(crate) fn insert_fts(&mut self, key: IndexKey, entry: FtsIndexEntry) {
        self.fts_entries.insert(key, entry);
    }

    pub(crate) fn remove(&mut self, key: &IndexKey) -> bool {
        if let Some(entry) = self.vs_entries.remove(key) {
            if let Entry::Occupied(mut e) = self.vs_routing.entry(entry.data.routing_group) {
                e.get_mut().retain(|k| k != key);
                if e.get().is_empty() {
                    e.remove();
                }
            }
            true
        } else {
            self.fts_entries.remove(key).is_some()
        }
    }

    pub(crate) fn iter_vs(&self) -> impl Iterator<Item = (&IndexKey, &VsIndexEntry)> {
        self.vs_entries.iter()
    }

    pub(crate) fn iter_fts(&self) -> impl Iterator<Item = (&IndexKey, &FtsIndexEntry)> {
        self.fts_entries.iter()
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
    pub(crate) fn best_index(
        &self,
        key: &IndexKey,
        equality_columns: &[ColumnName],
        range_columns: &[ColumnName],
    ) -> BestIndexState {
        let Some(requested_entry) = self.vs_entries.get(key) else {
            return BestIndexState::NotFound;
        };
        let candidates = self
            .vs_routing
            .get(&requested_entry.data.routing_group)
            .expect("routing_map must contain group for every index in indexes");

        let routed_key = candidates
            .iter()
            .filter_map(|key| self.vs_entries.get(key).map(|entry| (key, entry)))
            .filter(|(_, entry)| entry.status == IndexStatus::Serving)
            .filter_map(|(key, entry)| {
                entry
                    .score_index(equality_columns, range_columns)
                    .map(|score| (key, score, &entry.data.version))
            })
            .max_by(|(_, score_a, version_a), (_, score_b, version_b)| {
                score_a.cmp(score_b).then_with(|| version_a.cmp(version_b))
            })
            .map(|(k, score, _)| (k, score));

        match routed_key {
            Some((routed_key, needs_filtering)) => {
                let routed_entry = self
                    .vs_entries
                    .get(routed_key)
                    .expect("routed key must exist");
                if routed_key != key {
                    debug!("routing index request from {key} to {routed_key}");
                }
                BestIndexState::Serving {
                    key: routed_key.clone(),
                    index: routed_entry.index.clone(),
                    primary_key_columns: routed_entry.primary_key_columns.clone(),
                    table_columns: Arc::clone(&routed_entry.data.table_columns),
                    needs_filtering: needs_filtering.clone(),
                }
            }
            None => {
                let has_serving = candidates
                    .iter()
                    .filter_map(|key| self.vs_entries.get(key))
                    .any(|entry| entry.status == IndexStatus::Serving);
                if has_serving {
                    BestIndexState::NoGlobalIndex
                } else {
                    BestIndexState::NotServing(requested_entry.progress)
                }
            }
        }
    }
}
