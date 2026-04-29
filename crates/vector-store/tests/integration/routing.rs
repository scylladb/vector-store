/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::create_config_channels;
use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::db_basic::ScanFn;
use crate::db_basic::Table;
use crate::usearch::test_config;
use crate::wait_for;
use futures::FutureExt;
use httpapi::IndexStatus;
use httpapi::PostIndexAnnFilter;
use httpapi::PostIndexAnnRestriction;
use httpclient::HttpClient;
use reqwest::StatusCode;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use std::sync::Arc;
use uuid::Uuid;
use vector_store::ColumnName;
use vector_store::DbIndexType;
use vector_store::IndexMetadata;
use vector_store::Timestamp;

const ANN_LIMIT: usize = 5;

fn ordered_timeuuid(time: u32) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[0..4].copy_from_slice(&time.to_be_bytes());
    bytes[6] = 0x10;
    bytes[8] = 0x80;
    Uuid::from_bytes(bytes)
}

/// A scan function that never completes, keeping the index bootstrapping.
fn blocking_scan_fn() -> ScanFn {
    Box::new(|_tx| std::future::pending::<()>().boxed())
}

fn single_row_scan(pks: impl IntoIterator<Item = CqlValue> + Send + Sync + 'static) -> ScanFn {
    db_basic::scan_fn([(
        pks.into_iter().collect::<Vec<_>>().into(),
        Some(vec![1.0, 2.0, 3.0].into()),
        Timestamp::from_unix_timestamp(10),
    )])
}

fn make_index(
    name: &str,
    column: &str,
    index_type: DbIndexType,
    filtering_columns: &[&str],
    version: Uuid,
) -> IndexMetadata {
    IndexMetadata {
        keyspace_name: "vector".into(),
        table_name: "items".into(),
        index_name: name.into(),
        target_column: column.into(),
        index_type,
        filtering_columns: Arc::new(
            filtering_columns
                .iter()
                .map(|s| ColumnName::from(*s))
                .collect(),
        ),
        dimensions: NonZeroUsize::new(3).unwrap().into(),
        connectivity: Default::default(),
        expansion_add: Default::default(),
        expansion_search: Default::default(),
        space_type: Default::default(),
        version: version.into(),
        quantization: Default::default(),
    }
}

async fn setup() -> (HttpClient, DbBasic, impl Sized) {
    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();
    let (db_actor, db) = db_basic::new(node_state.clone());
    let (receivers, senders) = create_config_channels(test_config()).await;
    let index_factory = vector_store::new_index_factory_usearch(receivers.config.clone()).unwrap();
    let (server, addr) =
        vector_store::run(node_state, db_actor, internals, index_factory, receivers)
            .await
            .unwrap();
    (HttpClient::new(addr), db, (server, senders))
}

fn add_table(
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
            primary_keys: Arc::new(primary_keys.into_iter().collect()),
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

async fn wait_for_serving(client: &HttpClient, index: &IndexMetadata) {
    let ks = index.keyspace_name.as_ref().into();
    let idx = index.index_name.as_ref().into();
    wait_for(
        || async {
            client
                .index_status(&ks, &idx)
                .await
                .is_ok_and(|s| s.status == IndexStatus::Serving)
        },
        &format!("index {} to be serving", index.index_name),
    )
    .await;
}

async fn wait_for_bootstrapping(client: &HttpClient, index: &IndexMetadata) {
    let ks = index.keyspace_name.as_ref().into();
    let idx = index.index_name.as_ref().into();
    wait_for(
        || async {
            client
                .index_status(&ks, &idx)
                .await
                .is_ok_and(|s| s.status == IndexStatus::Bootstrapping)
        },
        &format!("index {} to be bootstrapping", index.index_name),
    )
    .await;
}

async fn post_ann(client: &HttpClient, index: &IndexMetadata) -> reqwest::Response {
    let keyspace_name = index.keyspace_name.as_ref().into();
    let index_name = index.index_name.as_ref().into();
    client
        .post_ann(
            &keyspace_name,
            &index_name,
            vec![0.0_f32, 0.0, 0.0].into(),
            None,
            NonZeroUsize::new(ANN_LIMIT).unwrap().into(),
        )
        .await
}

async fn post_ann_with_filter(
    client: &HttpClient,
    index: &IndexMetadata,
    filter: PostIndexAnnFilter,
) -> reqwest::Response {
    let keyspace_name = index.keyspace_name.as_ref().into();
    let index_name = index.index_name.as_ref().into();
    client
        .post_ann(
            &keyspace_name,
            &index_name,
            vec![0.0_f32, 0.0, 0.0].into(),
            Some(filter),
            NonZeroUsize::new(ANN_LIMIT).unwrap().into(),
        )
        .await
}

async fn assert_ann_served_by(
    client: &HttpClient,
    expected: &IndexMetadata,
    request: impl std::future::Future<Output = reqwest::Response>,
) -> reqwest::Response {
    client
        .internals_clear_counters()
        .await
        .expect("internals counters must be cleared");
    let counter_name = format!(
        "ann-served-request--{}--{}",
        expected.keyspace_name, expected.index_name
    );
    client
        .internals_start_counter(counter_name.clone())
        .await
        .expect("internals served counter must be registered");

    let before = client
        .internals_counters()
        .await
        .unwrap()
        .get(&counter_name)
        .copied()
        .unwrap_or(0);
    let response = request.await;
    let after = client
        .internals_counters()
        .await
        .unwrap()
        .get(&counter_name)
        .copied()
        .unwrap_or(0);
    assert_eq!(
        after - before,
        1,
        "expected ANN request to be served by {}",
        expected.index_name,
    );
    response
}

#[tokio::test]
#[ntest::timeout(10_000)]
#[cfg_attr(not(feature = "slow-test-hooks"), ignore = "requires slow-test-hooks")]
async fn ann_routes_to_serving_index_while_replacement_is_bootstrapping() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into()],
        1,
        [("pk".into(), NativeType::Int)],
        ["embedding".into()],
    );

    let oldest = make_index(
        "oldest",
        "embedding",
        DbIndexType::Global,
        &[],
        ordered_timeuuid(1),
    );
    db.add_index(
        oldest.clone(),
        Some(single_row_scan([CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &oldest).await;

    let replacement = make_index(
        "replacement",
        "embedding",
        DbIndexType::Global,
        &[],
        ordered_timeuuid(2),
    );
    db.add_index(replacement.clone(), Some(blocking_scan_fn()), None)
        .unwrap();
    wait_for_bootstrapping(&client, &replacement).await;

    let response = assert_ann_served_by(&client, &oldest, post_ann(&client, &replacement)).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ntest::timeout(10_000)]
#[cfg_attr(not(feature = "slow-test-hooks"), ignore = "requires slow-test-hooks")]
async fn ann_routes_to_newest_serving_index() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into()],
        1,
        [("pk".into(), NativeType::Int)],
        ["embedding".into()],
    );

    let oldest = make_index(
        "oldest",
        "embedding",
        DbIndexType::Global,
        &[],
        ordered_timeuuid(1),
    );
    db.add_index(
        oldest.clone(),
        Some(single_row_scan([CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &oldest).await;

    let replacement = make_index(
        "replacement",
        "embedding",
        DbIndexType::Global,
        &[],
        ordered_timeuuid(2),
    );
    db.add_index(
        replacement.clone(),
        Some(single_row_scan([CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &replacement).await;

    let response = assert_ann_served_by(&client, &replacement, post_ann(&client, &oldest)).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ntest::timeout(10_000)]
#[cfg_attr(not(feature = "slow-test-hooks"), ignore = "requires slow-test-hooks")]
async fn ann_routes_to_newest_local_index_with_same_score() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".into(), NativeType::Int),
            ("ck".into(), NativeType::Int),
        ],
        ["embedding".into()],
    );

    let older_local = make_index(
        "older",
        "embedding",
        DbIndexType::Local(Arc::new(vec!["pk".into()])),
        &[],
        ordered_timeuuid(1),
    );
    db.add_index(
        older_local.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &older_local).await;

    let newer_local = make_index(
        "newer",
        "embedding",
        DbIndexType::Local(Arc::new(vec!["pk".into()])),
        &[],
        ordered_timeuuid(2),
    );
    db.add_index(
        newer_local.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &newer_local).await;

    let filter = PostIndexAnnFilter {
        restrictions: vec![PostIndexAnnRestriction::Eq {
            lhs: "pk".into(),
            rhs: 1.into(),
        }],
        allow_filtering: false,
    };

    let response = assert_ann_served_by(
        &client,
        &newer_local,
        post_ann_with_filter(&client, &older_local, filter),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ntest::timeout(10_000)]
#[cfg_attr(not(feature = "slow-test-hooks"), ignore = "requires slow-test-hooks")]
async fn ann_routes_to_local_index_with_more_matching_partition_key_columns() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk1".into(), "pk2".into(), "ck".into()],
        2,
        [
            ("pk1".into(), NativeType::Int),
            ("pk2".into(), NativeType::Int),
            ("ck".into(), NativeType::Int),
        ],
        ["embedding".into()],
    );

    let less_precise = make_index(
        "less_precise",
        "embedding",
        DbIndexType::Local(Arc::new(vec!["pk1".into()])),
        &[],
        ordered_timeuuid(1),
    );
    db.add_index(
        less_precise.clone(),
        Some(single_row_scan([
            CqlValue::Int(1),
            CqlValue::Int(1),
            CqlValue::Int(1),
        ])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &less_precise).await;

    let more_precise = make_index(
        "more_precise",
        "embedding",
        DbIndexType::Local(Arc::new(vec!["pk1".into(), "pk2".into()])),
        &[],
        ordered_timeuuid(2),
    );
    db.add_index(
        more_precise.clone(),
        Some(single_row_scan([
            CqlValue::Int(1),
            CqlValue::Int(1),
            CqlValue::Int(1),
        ])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &more_precise).await;

    let filter = PostIndexAnnFilter {
        restrictions: vec![
            PostIndexAnnRestriction::Eq {
                lhs: "pk1".into(),
                rhs: 1.into(),
            },
            PostIndexAnnRestriction::Eq {
                lhs: "pk2".into(),
                rhs: 1.into(),
            },
        ],
        allow_filtering: false,
    };

    let response = assert_ann_served_by(
        &client,
        &more_precise,
        post_ann_with_filter(&client, &less_precise, filter),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ntest::timeout(10_000)]
#[cfg_attr(not(feature = "slow-test-hooks"), ignore = "requires slow-test-hooks")]
async fn ann_routes_to_local_index_with_filter_columns_covering_restriction() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".into(), NativeType::Int),
            ("ck".into(), NativeType::Int),
            ("f".into(), NativeType::Int),
        ],
        ["embedding".into()],
    );

    let covering = make_index(
        "covering",
        "embedding",
        DbIndexType::Local(Arc::new(vec!["pk".into()])),
        &["f"],
        ordered_timeuuid(1),
    );
    db.add_index(
        covering.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &covering).await;

    let non_covering = make_index(
        "non_covering",
        "embedding",
        DbIndexType::Local(Arc::new(vec!["pk".into()])),
        &[],
        ordered_timeuuid(2),
    );
    db.add_index(
        non_covering.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &non_covering).await;

    let filter = PostIndexAnnFilter {
        restrictions: vec![
            PostIndexAnnRestriction::Eq {
                lhs: "pk".into(),
                rhs: 1.into(),
            },
            PostIndexAnnRestriction::Eq {
                lhs: "f".into(),
                rhs: 1.into(),
            },
        ],
        allow_filtering: true,
    };

    // TODO: update this assertion to expect StatusCode::OK once filtering on
    // non-primary-key columns is supported end-to-end. Currently the request
    // is routed correctly but fails at the filter validation layer.
    let response = assert_ann_served_by(
        &client,
        &covering,
        post_ann_with_filter(&client, &non_covering, filter),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[ntest::timeout(10_000)]
#[cfg_attr(not(feature = "slow-test-hooks"), ignore = "requires slow-test-hooks")]
async fn ann_routes_to_global_index_with_filter_columns_covering_restriction() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".into(), NativeType::Int),
            ("ck".into(), NativeType::Int),
            ("f".into(), NativeType::Int),
        ],
        ["embedding".into()],
    );

    let covering = make_index(
        "covering",
        "embedding",
        DbIndexType::Global,
        &["f"],
        ordered_timeuuid(1),
    );
    db.add_index(
        covering.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &covering).await;

    let non_covering = make_index(
        "non_covering",
        "embedding",
        DbIndexType::Global,
        &[],
        ordered_timeuuid(2),
    );
    db.add_index(
        non_covering.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &non_covering).await;

    let filter = PostIndexAnnFilter {
        restrictions: vec![
            PostIndexAnnRestriction::Eq {
                lhs: "pk".into(),
                rhs: 1.into(),
            },
            PostIndexAnnRestriction::Eq {
                lhs: "f".into(),
                rhs: 1.into(),
            },
        ],
        allow_filtering: true,
    };

    // TODO: update this assertion to expect StatusCode::OK once filtering on
    // non-primary-key columns is supported end-to-end. Currently the request
    // is routed correctly but fails at the filter validation layer.
    let response = assert_ann_served_by(
        &client,
        &covering,
        post_ann_with_filter(&client, &non_covering, filter),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[ntest::timeout(10_000)]
#[cfg_attr(not(feature = "slow-test-hooks"), ignore = "requires slow-test-hooks")]
async fn ann_routes_to_local_index_when_pk_restrictions_match() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".into(), NativeType::Int),
            ("ck".into(), NativeType::Int),
        ],
        ["embedding".into()],
    );

    let local_index = make_index(
        "local_idx",
        "embedding",
        DbIndexType::Local(Arc::new(vec!["pk".into()])),
        &[],
        ordered_timeuuid(1),
    );
    db.add_index(
        local_index.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &local_index).await;

    let global_index = make_index(
        "global_idx",
        "embedding",
        DbIndexType::Global,
        &[],
        ordered_timeuuid(2),
    );
    db.add_index(
        global_index.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &global_index).await;

    let filter = PostIndexAnnFilter {
        restrictions: vec![PostIndexAnnRestriction::Eq {
            lhs: "pk".into(),
            rhs: 1.into(),
        }],
        allow_filtering: false,
    };

    let response = assert_ann_served_by(
        &client,
        &local_index,
        post_ann_with_filter(&client, &global_index, filter),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[ntest::timeout(10_000)]
#[cfg_attr(not(feature = "slow-test-hooks"), ignore = "requires slow-test-hooks")]
async fn ann_routes_to_global_index_without_pk_restrictions() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".into(), NativeType::Int),
            ("ck".into(), NativeType::Int),
        ],
        ["embedding".into()],
    );

    let local_index = make_index(
        "local_idx",
        "embedding",
        DbIndexType::Local(Arc::new(vec!["pk".into()])),
        &[],
        ordered_timeuuid(1),
    );
    db.add_index(
        local_index.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &local_index).await;

    let global_index = make_index(
        "global_idx",
        "embedding",
        DbIndexType::Global,
        &[],
        ordered_timeuuid(2),
    );
    db.add_index(
        global_index.clone(),
        Some(single_row_scan([CqlValue::Int(1), CqlValue::Int(1)])),
        None,
    )
    .unwrap();
    wait_for_serving(&client, &global_index).await;

    let response =
        assert_ann_served_by(&client, &global_index, post_ann(&client, &local_index)).await;
    assert_eq!(response.status(), StatusCode::OK);
}
