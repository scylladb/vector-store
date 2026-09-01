/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::create_config_channels;
use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::db_basic::ScanFn;
use crate::db_basic::Table;
use crate::vs_index::usearch_test_config;
use futures::FutureExt;
use httpclient::HttpClient;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use std::sync::Arc;
use uuid::Uuid;
use vector_store::ColumnName;
use vector_store::DbIndexPartitioning;
use vector_store::HttpServerExt;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsFts;
use vector_store::IndexOptionsVs;
use vector_store::NonemptyArc;
use vector_store::NonemptyIteratorExt;
use vector_store::Timestamp;

pub(crate) fn ordered_timeuuid(time: u32) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[0..4].copy_from_slice(&time.to_be_bytes());
    bytes[6] = 0x10;
    bytes[8] = 0x80;
    Uuid::from_bytes(bytes)
}

/// A scan function that never completes, keeping the index bootstrapping.
pub(crate) fn blocking_scan_fn() -> ScanFn {
    Box::new(|_tx| std::future::pending::<()>().boxed())
}

pub(crate) fn single_row_scan(
    pks: impl IntoIterator<Item = CqlValue> + Send + Sync + 'static,
) -> ScanFn {
    db_basic::scan_fn_vectors([(
        pks.into_iter().collect::<Vec<_>>().into(),
        Some(vec![1.0, 2.0, 3.0].into()),
        [].into(),
        Timestamp::from_millis(10),
    )])
}

pub(crate) fn make_vs_index(
    name: &str,
    primary_key_columns: &[&str],
    partition_key_count: usize,
    target_column: &str,
    partitioning: DbIndexPartitioning,
    filtering_columns: &[&str],
    version: Uuid,
) -> IndexMetadata {
    make_index_with_kind(
        name,
        primary_key_columns,
        partition_key_count,
        target_column,
        partitioning,
        filtering_columns,
        version,
        IndexKind::Vs(IndexOptionsVs {
            dimensions: NonZeroUsize::new(3).unwrap().into(),
            connectivity: Default::default(),
            expansion_add: Default::default(),
            expansion_search: Default::default(),
            space_type: Default::default(),
            quantization: Default::default(),
        }),
    )
}

pub(crate) fn make_fts_index(
    name: &str,
    primary_key_columns: &[&str],
    partition_key_count: usize,
    target_column: &str,
    version: Uuid,
) -> IndexMetadata {
    make_fts_index_with_options(
        name,
        primary_key_columns,
        partition_key_count,
        target_column,
        version,
        IndexOptionsFts::default(),
    )
}

pub(crate) fn make_fts_index_with_options(
    name: &str,
    primary_key_columns: &[&str],
    partition_key_count: usize,
    target_column: &str,
    version: Uuid,
    options: IndexOptionsFts,
) -> IndexMetadata {
    make_index_with_kind(
        name,
        primary_key_columns,
        partition_key_count,
        target_column,
        DbIndexPartitioning::Global,
        &[],
        version,
        IndexKind::Fts(options),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn make_index_with_kind(
    name: &str,
    primary_key_columns: &[&str],
    partition_key_count: usize,
    target_column: &str,
    partitioning: DbIndexPartitioning,
    filtering_columns: &[&str],
    version: Uuid,
    kind: IndexKind,
) -> IndexMetadata {
    IndexMetadata {
        keyspace_name: "vector".into(),
        table_name: "items".into(),
        index_name: name.into(),
        primary_key_columns: NonemptyArc::new(
            primary_key_columns.iter().map(|v| ColumnName::from(*v)),
        )
        .unwrap(),
        partition_key_count: NonZeroUsize::new(partition_key_count).unwrap(),
        target_columns: NonemptyArc::new([target_column]).unwrap(),
        partitioning,
        filtering_columns: filtering_columns
            .iter()
            .map(|s| ColumnName::from(*s))
            .collect(),
        alternator_attribute_types: Default::default(),
        version: version.into(),
        kind,
    }
}

pub(crate) async fn setup() -> (HttpClient, DbBasic, impl Sized) {
    let node_state = vector_store::new_node_state().await;
    let (db_actor, db) = db_basic::new(node_state.clone());
    let (receivers, senders) = create_config_channels(usearch_test_config()).await;
    let (server, _mtls) = vector_store::run(Some(node_state), Some(db_actor), receivers)
        .await
        .unwrap();
    let addr = (*server.address().await.borrow()).unwrap();
    (HttpClient::new(addr), db, (server, senders))
}

pub(crate) fn add_table(
    db: &DbBasic,
    primary_keys: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    vector_columns: impl IntoIterator<Item = ColumnName>,
) {
    db.add_table(
        "vector".into(),
        "items".into(),
        Table {
            primary_keys: primary_keys.into_iter().collect_nonempty_arc().unwrap(),
            partition_key_count,
            columns: Arc::new(columns.into_iter().collect()),
            dimensions: vector_columns
                .into_iter()
                .map(|c| (c, NonZeroUsize::new(3).unwrap().into()))
                .collect(),
        },
    )
    .unwrap();
}
