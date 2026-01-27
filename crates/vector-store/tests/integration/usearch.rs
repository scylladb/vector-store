/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Duration;
use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::db_basic::Table;
use crate::wait_for;
use crate::wait_for_value;
use ::time::OffsetDateTime;
use httpclient::HttpClient;
use reqwest::StatusCode;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use uuid::Uuid;
use vector_store::ColumnName;
use vector_store::Config;
use vector_store::Connectivity;
use vector_store::DbIndexType;
use vector_store::Dimensions;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::IndexMetadata;
use vector_store::Percentage;
use vector_store::PrimaryKey;
use vector_store::Quantization;
use vector_store::SpaceType;
use vector_store::Timestamp;
use vector_store::Vector;
use vector_store::httproutes::PostIndexAnnFilter;
use vector_store::httproutes::PostIndexAnnResponse;
use vector_store::httproutes::PostIndexAnnRestriction;
use vector_store::node_state::NodeState;

pub(crate) fn test_config() -> Config {
    Config {
        vector_store_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    }
}

pub(crate) async fn setup_store(
    config: Config,
    index_type: DbIndexType,
    primary_keys: impl IntoIterator<Item = ColumnName>,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    values: impl IntoIterator<Item = (PrimaryKey, Option<Vector>, Timestamp)>,
) -> (
    impl std::future::Future<Output = (HttpClient, impl Sized, impl Sized)>,
    IndexMetadata,
    DbBasic,
    Sender<NodeState>,
) {
    setup_store_with_quantization(
        config,
        index_type,
        primary_keys,
        columns,
        values,
        Quantization::default(),
        NonZeroUsize::new(3).unwrap().into(),
    )
    .await
}

pub(crate) async fn setup_store_with_quantization(
    config: Config,
    index_type: DbIndexType,
    primary_keys: impl IntoIterator<Item = ColumnName>,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    values: impl IntoIterator<Item = (PrimaryKey, Option<Vector>, Timestamp)>,
    quantization: Quantization,
    dimension: Dimensions,
) -> (
    impl std::future::Future<Output = (HttpClient, impl Sized, impl Sized)>,
    IndexMetadata,
    DbBasic,
    Sender<NodeState>,
) {
    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();

    let (db_actor, db) = db_basic::new(node_state.clone());

    let columns: Arc<HashMap<_, _>> = Arc::new(columns.into_iter().collect());
    let index = IndexMetadata {
        keyspace_name: "vector".into(),
        table_name: "items".into(),
        index_name: "ann".into(),
        target_column: "embedding".into(),
        index_type,
        filtering_columns: Arc::new(columns.keys().cloned().collect()),
        dimensions: dimension,
        connectivity: Connectivity::default(),
        expansion_add: ExpansionAdd::default(),
        expansion_search: ExpansionSearch::default(),
        space_type: SpaceType::Euclidean,
        version: Uuid::new_v4().into(),
        quantization,
    };

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: Arc::new(primary_keys.into_iter().collect()),
            columns,
            dimensions: [(index.target_column.clone(), index.dimensions)]
                .into_iter()
                .collect(),
        },
    )
    .unwrap();

    db.insert_values(
        &index.keyspace_name,
        &index.table_name,
        &index.target_column,
        values,
    )
    .unwrap();

    db.add_index(
        &index.keyspace_name,
        index.index_name.clone(),
        index.clone().into(),
    )
    .unwrap();

    let (config_tx, config_rx) = watch::channel(Arc::new(config));
    let index_factory = vector_store::new_index_factory_usearch(config_rx.clone()).unwrap();

    let run = {
        let node_state = node_state.clone();
        async move {
            let (server, addr) =
                vector_store::run(node_state, db_actor, internals, index_factory, config_rx)
                    .await
                    .unwrap();

            (HttpClient::new(addr), server, config_tx)
        }
    };

    (run, index, db, node_state)
}

pub(crate) async fn setup_store_and_wait_for_index(
    index_type: DbIndexType,
    primary_keys: impl IntoIterator<Item = ColumnName>,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    values: impl IntoIterator<Item = (PrimaryKey, Option<Vector>, Timestamp)>,
) -> (
    IndexMetadata,
    HttpClient,
    DbBasic,
    impl Sized,
    Sender<NodeState>,
) {
    let (run, index, db, node_state) =
        setup_store(test_config(), index_type, primary_keys, columns, values).await;
    let (client, server, _config_tx) = run.await;

    wait_for(
        || async { !client.indexes().await.is_empty() },
        "Waiting for index to be added to the store",
    )
    .await;

    (index, client, db, (server, _config_tx), node_state)
}

#[tokio::test]
async fn simple_create_search_delete_index() {
    crate::enable_tracing();

    let (run, index, db, _node_state) = setup_store(
        test_config(),
        DbIndexType::Global,
        ["pk".into(), "ck".into()],
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        [
            (
                vec![CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
                Some(vec![1., 1., 1.].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            ),
            (
                vec![CqlValue::Int(2), CqlValue::Text("two".to_string())].into(),
                Some(vec![2., -2., 2.].into()),
                OffsetDateTime::from_unix_timestamp(20).unwrap().into(),
            ),
            (
                vec![CqlValue::Int(3), CqlValue::Text("three".to_string())].into(),
                Some(vec![3., 3., 3.].into()),
                OffsetDateTime::from_unix_timestamp(30).unwrap().into(),
            ),
        ],
    )
    .await;
    let (client, _server, _config_tx) = run.await;

    wait_for(
        || async {
            client
                .index_status(&index.keyspace_name, &index.index_name)
                .await
                .expect("failed to get index status")
                .count
                == 3
        },
        "Waiting for 3 vectors to be indexed",
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0], vector_store::IndexInfo::new("vector", "ann"));

    let (primary_keys, distances) = client
        .ann(
            &index.keyspace_name,
            &index.index_name,
            vec![2.1, -2., 2.].into(),
            None,
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;
    assert_eq!(distances.len(), 1);
    let primary_keys_pk = primary_keys.get(&"pk".into()).unwrap();
    let primary_keys_ck = primary_keys.get(&"ck".into()).unwrap();
    assert_eq!(distances.len(), primary_keys_pk.len());
    assert_eq!(distances.len(), primary_keys_ck.len());
    assert_eq!(primary_keys_pk.first().unwrap().as_i64().unwrap(), 2);
    assert_eq!(primary_keys_ck.first().unwrap().as_str().unwrap(), "two");

    db.del_index(&index.keyspace_name, &index.index_name)
        .unwrap();

    wait_for(
        || async { client.indexes().await.is_empty() },
        "Waiting for all indexes to be removed from the store",
    )
    .await;
}

#[tokio::test]
async fn failed_db_index_create() {
    crate::enable_tracing();

    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();
    let (db_actor, db) = db_basic::new(node_state.clone());

    let index = IndexMetadata {
        keyspace_name: "vector".into(),
        table_name: "items".into(),
        index_name: "ann".into(),
        target_column: "embedding".into(),
        index_type: DbIndexType::Global,
        filtering_columns: Arc::new(Vec::new()),
        dimensions: NonZeroUsize::new(3).unwrap().into(),
        connectivity: Default::default(),
        expansion_add: Default::default(),
        expansion_search: Default::default(),
        space_type: Default::default(),
        version: Uuid::new_v4().into(),
        quantization: Default::default(),
    };

    let (_, rx) = watch::channel(Arc::new(Config::default()));
    let index_factory = vector_store::new_index_factory_usearch(rx).unwrap();

    let (_config_tx, config_rx) = watch::channel(Arc::new(test_config()));

    let (_server_actor, addr) =
        vector_store::run(node_state, db_actor, internals, index_factory, config_rx)
            .await
            .unwrap();

    let client = HttpClient::new(addr);

    db.set_next_get_db_index_failed();

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: Arc::new(vec!["pk".into(), "ck".into()]),
            columns: Arc::new(
                [
                    ("pk".into(), NativeType::Int),
                    ("ck".into(), NativeType::Text),
                ]
                .into_iter()
                .collect(),
            ),
            dimensions: [(index.target_column.clone(), index.dimensions)]
                .into_iter()
                .collect(),
        },
    )
    .unwrap();
    db.add_index(
        &index.keyspace_name,
        index.index_name.clone(),
        index.clone().into(),
    )
    .unwrap();

    wait_for(
        || async { !client.indexes().await.is_empty() },
        "Waiting for index to be added to the store",
    )
    .await;

    db.add_index(&index.keyspace_name, "ann2".into(), index.clone().into())
        .unwrap();

    wait_for(
        || async { client.indexes().await.len() == 2 },
        "Waiting for 2nd index to be added to the store",
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 2);
    assert!(indexes.contains(&vector_store::IndexInfo::new("vector", "ann")));
    assert!(indexes.contains(&vector_store::IndexInfo::new("vector", "ann2")));

    db.add_index(&index.keyspace_name, "ann3".into(), index.clone().into())
        .unwrap();

    wait_for(
        || async { client.indexes().await.len() == 3 },
        "Waiting for 3rd index to be added to the store",
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 3);
    assert!(indexes.contains(&vector_store::IndexInfo::new("vector", "ann")));
    assert!(indexes.contains(&vector_store::IndexInfo::new("vector", "ann2")));
    assert!(indexes.contains(&vector_store::IndexInfo::new("vector", "ann3")));

    db.del_index(&index.keyspace_name, &"ann2".into()).unwrap();

    wait_for(
        || async { client.indexes().await.len() == 2 },
        "Waiting for index to be removed from the store",
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 2);
    assert!(indexes.contains(&vector_store::IndexInfo::new("vector", "ann")));
    assert!(indexes.contains(&vector_store::IndexInfo::new("vector", "ann3")));
}

#[tokio::test]
async fn ann_returns_bad_request_when_provided_vector_size_is_not_eq_index_dimensions() {
    crate::enable_tracing();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        ["pk".into(), "ck".into()],
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        [],
    )
    .await;

    let result = client
        .post_ann(
            &index.keyspace_name,
            &index.index_name,
            vec![1.0, 2.0].into(), // Only 2 dimensions, should be 3 (index.dimensions)
            None,
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;

    assert_eq!(result.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn ann_fail_while_building() {
    crate::enable_tracing();
    let (run, index, db, _node_state) = setup_store(
        test_config(),
        DbIndexType::Global,
        ["pk".into(), "ck".into()],
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        [],
    )
    .await;
    db.set_next_full_scan_progress(vector_store::Progress::InProgress(
        Percentage::try_from(33.333).unwrap(),
    ));
    let (client, _server, _config_tx) = run.await;

    let result = client
        .post_ann(
            &index.keyspace_name,
            &index.index_name,
            vec![1.0, 2.0, 3.0].into(),
            None,
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;

    assert_eq!(result.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        result.text().await.unwrap(),
        "Index vector.ann is not available yet as it is still being constructed, progress: 33.333%"
    );
}

#[tokio::test]
async fn ann_failed_when_wrong_number_of_primary_keys() {
    crate::enable_tracing();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        vec!["pk".into()],
        [("pk".into(), NativeType::Int)],
        [(
            vec![CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
            Some(vec![1., 1., 1.].into()),
            OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
        )],
    )
    .await;

    wait_for(
        || async {
            let response = client
                .post_ann(
                    &index.keyspace_name,
                    &index.index_name,
                    vec![1.0, 2.0, 3.0].into(),
                    None,
                    NonZeroUsize::new(1).unwrap().into(),
                )
                .await;

            if response.status() == StatusCode::INTERNAL_SERVER_ERROR {
                true
            } else {
                let response = response.json::<PostIndexAnnResponse>().await.unwrap();
                assert_eq!(response.distances.len(), 0);
                false
            }
        },
        "Waiting for index to be return internal server error on ANN",
    )
    .await;
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_partition_key_int_eq() {
    crate::enable_tracing();

    let pk_column: ColumnName = "pk".into();
    let ck_column: ColumnName = "ck".into();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        [pk_column.clone(), ck_column.clone()],
        [
            (pk_column.clone(), NativeType::Int),
            (ck_column.clone(), NativeType::Int),
        ],
        (0..30).map(|i| {
            (
                vec![CqlValue::Int(i / 10), CqlValue::Int(i % 10)].into(),
                Some(vec![i as f32, i as f32, i as f32].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            )
        }),
    )
    .await;

    // Search for nearest neighbors with a filter on primary key "pk" = 1
    let pk_ck_values = wait_for_value(
        || async {
            let (primary_keys, _) = client
                .ann(
                    &index.keyspace_name,
                    &index.index_name,
                    vec![1.0, 2.0, 3.0].into(),
                    Some(PostIndexAnnFilter {
                        restrictions: vec![PostIndexAnnRestriction::Eq {
                            lhs: pk_column.clone(),
                            rhs: 1.into(),
                        }],
                        allow_filtering: false,
                    }),
                    NonZeroUsize::new(100).unwrap().into(),
                )
                .await;
            let pk_ck_values: HashSet<_> = primary_keys
                .get(&pk_column)
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as usize)
                .zip(
                    primary_keys
                        .get(&ck_column)
                        .unwrap()
                        .iter()
                        .map(|v| v.as_i64().unwrap() as usize),
                )
                .collect();
            (pk_ck_values.len() == 10).then_some(pk_ck_values)
        },
        "Waiting for ANN to return 10 results",
    )
    .await;
    (0..10).for_each(|i| {
        assert!(
            pk_ck_values.contains(&(1, i)),
            "Expected ck_values to contain value (1, {i})"
        );
    });
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_clustering_key_int_eq() {
    crate::enable_tracing();

    let pk_column: ColumnName = "pk".into();
    let ck_column: ColumnName = "ck".into();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        [pk_column.clone(), ck_column.clone()],
        [
            (pk_column.clone(), NativeType::Int),
            (ck_column.clone(), NativeType::Int),
        ],
        (0..30).map(|i| {
            (
                vec![CqlValue::Int(i / 10), CqlValue::Int(i % 10)].into(),
                Some(vec![i as f32, i as f32, i as f32].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            )
        }),
    )
    .await;

    // Search for nearest neighbors with a filter on primary key "ck" = 1
    let pk_ck_values = wait_for_value(
        || async {
            let (primary_keys, _) = client
                .ann(
                    &index.keyspace_name,
                    &index.index_name,
                    vec![1.0, 2.0, 3.0].into(),
                    Some(PostIndexAnnFilter {
                        restrictions: vec![PostIndexAnnRestriction::Eq {
                            lhs: ck_column.clone(),
                            rhs: 1.into(),
                        }],
                        allow_filtering: false,
                    }),
                    NonZeroUsize::new(100).unwrap().into(),
                )
                .await;
            let pk_ck_values: HashSet<_> = primary_keys
                .get(&pk_column)
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as usize)
                .zip(
                    primary_keys
                        .get(&ck_column)
                        .unwrap()
                        .iter()
                        .map(|v| v.as_i64().unwrap() as usize),
                )
                .collect();
            (pk_ck_values.len() == 3).then_some(pk_ck_values)
        },
        "Waiting for ANN to return 3 results",
    )
    .await;
    (0..3).for_each(|i| {
        assert!(
            pk_ck_values.contains(&(i, 1)),
            "Expected pk_values to contain value ({i}, 1)"
        );
    });
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_partition_key_int_in() {
    crate::enable_tracing();

    let pk_column: ColumnName = "pk".into();
    let ck_column: ColumnName = "ck".into();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        [pk_column.clone(), ck_column.clone()],
        [
            (pk_column.clone(), NativeType::Int),
            (ck_column.clone(), NativeType::Int),
        ],
        (0..30).map(|i| {
            (
                vec![CqlValue::Int(i / 10), CqlValue::Int(i % 10)].into(),
                Some(vec![i as f32, i as f32, i as f32].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            )
        }),
    )
    .await;

    // Search for nearest neighbors with a filter on primary key "pk" IN (1, 2)
    let pk_ck_values = wait_for_value(
        || async {
            let (primary_keys, _) = client
                .ann(
                    &index.keyspace_name,
                    &index.index_name,
                    vec![1.0, 2.0, 3.0].into(),
                    Some(PostIndexAnnFilter {
                        restrictions: vec![PostIndexAnnRestriction::In {
                            lhs: pk_column.clone(),
                            rhs: vec![1.into(), 2.into()],
                        }],
                        allow_filtering: false,
                    }),
                    NonZeroUsize::new(100).unwrap().into(),
                )
                .await;
            let pk_ck_values: HashSet<_> = primary_keys
                .get(&pk_column)
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as usize)
                .zip(
                    primary_keys
                        .get(&ck_column)
                        .unwrap()
                        .iter()
                        .map(|v| v.as_i64().unwrap() as usize),
                )
                .collect();
            (pk_ck_values.len() == 20).then_some(pk_ck_values)
        },
        "Waiting for ANN to return 20 results",
    )
    .await;
    (0..10).for_each(|i| {
        assert!(
            pk_ck_values.contains(&(1, i)),
            "Expected pk_ck_values to contain value (1, {i})"
        );
        assert!(
            pk_ck_values.contains(&(2, i)),
            "Expected pk_ck_values to contain value (2, {i})"
        );
    });
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_clustering_key_int_in() {
    crate::enable_tracing();

    let pk_column: ColumnName = "pk".into();
    let ck_column: ColumnName = "ck".into();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        [pk_column.clone(), ck_column.clone()],
        [
            (pk_column.clone(), NativeType::Int),
            (ck_column.clone(), NativeType::Int),
        ],
        (0..30).map(|i| {
            (
                vec![CqlValue::Int(i / 10), CqlValue::Int(i % 10)].into(),
                Some(vec![i as f32, i as f32, i as f32].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            )
        }),
    )
    .await;

    // Search for nearest neighbors with a filter on primary key "ck" IN (1, 3)
    let pk_ck_values = wait_for_value(
        || async {
            let (primary_keys, _) = client
                .ann(
                    &index.keyspace_name,
                    &index.index_name,
                    vec![1.0, 2.0, 3.0].into(),
                    Some(PostIndexAnnFilter {
                        restrictions: vec![PostIndexAnnRestriction::In {
                            lhs: ck_column.clone(),
                            rhs: vec![1.into(), 3.into()],
                        }],
                        allow_filtering: false,
                    }),
                    NonZeroUsize::new(100).unwrap().into(),
                )
                .await;
            let pk_ck_values: HashSet<_> = primary_keys
                .get(&pk_column)
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as usize)
                .zip(
                    primary_keys
                        .get(&ck_column)
                        .unwrap()
                        .iter()
                        .map(|v| v.as_i64().unwrap() as usize),
                )
                .collect();
            (pk_ck_values.len() == 6).then_some(pk_ck_values)
        },
        "Waiting for ANN to return 6 results",
    )
    .await;
    (0..3).for_each(|i| {
        assert!(
            pk_ck_values.contains(&(i, 1)),
            "Expected pk_ck_values to contain value ({i}, 1)"
        );
        assert!(
            pk_ck_values.contains(&(i, 3)),
            "Expected pk_ck_values to contain value ({i}, 3)"
        );
    });
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_primary_key_int_eq_tuple() {
    crate::enable_tracing();

    let pk_column: ColumnName = "pk".into();
    let ck_column: ColumnName = "ck".into();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        [pk_column.clone(), ck_column.clone()],
        [
            (pk_column.clone(), NativeType::Int),
            (ck_column.clone(), NativeType::Int),
        ],
        (0..30).map(|i| {
            (
                vec![CqlValue::Int(i / 10), CqlValue::Int(i % 10)].into(),
                Some(vec![i as f32, i as f32, i as f32].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            )
        }),
    )
    .await;

    // Search for nearest neighbors with a filter on primary key ("pk", "ck") = (1, 5)
    let (primary_keys, _) = client
        .ann(
            &index.keyspace_name,
            &index.index_name,
            vec![1.0, 2.0, 3.0].into(),
            Some(PostIndexAnnFilter {
                restrictions: vec![PostIndexAnnRestriction::EqTuple {
                    lhs: vec![pk_column.clone(), ck_column.clone()],
                    rhs: vec![1.into(), 5.into()],
                }],
                allow_filtering: false,
            }),
            NonZeroUsize::new(100).unwrap().into(),
        )
        .await;
    let pk_values: Vec<_> = primary_keys
        .get(&pk_column)
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap() as usize)
        .collect();
    let ck_values: Vec<_> = primary_keys
        .get(&ck_column)
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap() as usize)
        .collect();
    assert_eq!(pk_values.len(), 1);
    assert_eq!(ck_values.len(), 1);
    assert_eq!(pk_values.first(), Some(&1));
    assert_eq!(ck_values.first(), Some(&5));
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_primary_key_int_in_tuple() {
    crate::enable_tracing();

    let pk_column: ColumnName = "pk".into();
    let ck_column: ColumnName = "ck".into();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        [pk_column.clone(), ck_column.clone()],
        [
            (pk_column.clone(), NativeType::Int),
            (ck_column.clone(), NativeType::Int),
        ],
        (0..30).map(|i| {
            (
                vec![CqlValue::Int(i / 10), CqlValue::Int(i % 10)].into(),
                Some(vec![i as f32, i as f32, i as f32].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            )
        }),
    )
    .await;

    // Search for nearest neighbors with a filter on primary key ("pk", "ck") IN ((0,7), (1, 5))
    let (primary_keys, _) = client
        .ann(
            &index.keyspace_name,
            &index.index_name,
            vec![1.0, 2.0, 3.0].into(),
            Some(PostIndexAnnFilter {
                restrictions: vec![PostIndexAnnRestriction::InTuple {
                    lhs: vec![pk_column.clone(), ck_column.clone()],
                    rhs: vec![vec![0.into(), 7.into()], vec![1.into(), 5.into()]],
                }],
                allow_filtering: false,
            }),
            NonZeroUsize::new(100).unwrap().into(),
        )
        .await;
    let pk_ck_values: HashSet<_> = primary_keys
        .get(&pk_column)
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap() as usize)
        .zip(
            primary_keys
                .get(&ck_column)
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as usize),
        )
        .collect();
    assert_eq!(pk_ck_values.len(), 2);
    assert!(
        pk_ck_values.contains(&(0, 7)),
        "Expected pk_ck_values to contain value (0, 7)"
    );
    assert!(
        pk_ck_values.contains(&(1, 5)),
        "Expected pk_ck_values to contain value (1, 5)"
    );
}

/// Sets up a store with pk (Int) and ck (Int) columns with 30 vectors.
/// Vectors are created with pk = i/10 and ck = i%10 for i in 0..30.
async fn setup_int_int_store() -> (
    IndexMetadata,
    HttpClient,
    ColumnName,
    ColumnName,
    DbBasic,
    impl Sized,
    Sender<NodeState>,
) {
    let pk_column: ColumnName = "pk".into();
    let ck_column: ColumnName = "ck".into();
    let (index, client, db, server, node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        [pk_column.clone(), ck_column.clone()],
        [
            (pk_column.clone(), NativeType::Int),
            (ck_column.clone(), NativeType::Int),
        ],
        (0..30).map(|i| {
            (
                vec![CqlValue::Int(i / 10), CqlValue::Int(i % 10)].into(),
                Some(vec![i as f32, i as f32, i as f32].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            )
        }),
    )
    .await;
    (index, client, pk_column, ck_column, db, server, node_state)
}

/// Runs an ANN query with the given restrictions and returns a HashSet of (pk, ck) values.
async fn run_ann_filter_int_int(
    client: &HttpClient,
    index: &IndexMetadata,
    pk_column: &ColumnName,
    ck_column: &ColumnName,
    restrictions: Vec<PostIndexAnnRestriction>,
    expected_count: usize,
) -> HashSet<(usize, usize)> {
    wait_for_value(
        || async {
            let (primary_keys, _) = client
                .ann(
                    &index.keyspace_name,
                    &index.index_name,
                    vec![1.0, 2.0, 3.0].into(),
                    Some(PostIndexAnnFilter {
                        restrictions: restrictions.clone(),
                        allow_filtering: false,
                    }),
                    NonZeroUsize::new(100).unwrap().into(),
                )
                .await;
            let pk_ck_values: HashSet<_> = primary_keys
                .get(pk_column)
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as usize)
                .zip(
                    primary_keys
                        .get(ck_column)
                        .unwrap()
                        .iter()
                        .map(|v| v.as_i64().unwrap() as usize),
                )
                .collect();
            (pk_ck_values.len() == expected_count).then_some(pk_ck_values)
        },
        &format!("Waiting for ANN to return {expected_count} results"),
    )
    .await
}

/// Asserts that all expected (pk, ck) pairs are present in the results,
/// and that no other values are present.
fn assert_pk_ck_combinations(
    pk_ck_values: &HashSet<(usize, usize)>,
    expected_pk_ck: impl IntoIterator<Item = (usize, usize)>,
) {
    let expected: HashSet<_> = expected_pk_ck.into_iter().collect();
    let diff: HashSet<_> = pk_ck_values.symmetric_difference(&expected).collect();
    assert!(
        diff.is_empty(),
        "Sets differ. Symmetric difference: {:?}",
        diff
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_clustering_key_int_lt() {
    crate::enable_tracing();

    let (index, client, pk_column, ck_column, _db, _server, _node_state) =
        setup_int_int_store().await;

    // Search for nearest neighbors with a filter on clustering key "ck" < 3
    // Should return rows where ck is 0, 1, or 2 (3 values per pk, 3 pks = 9 total)
    let pk_ck_values = run_ann_filter_int_int(
        &client,
        &index,
        &pk_column,
        &ck_column,
        vec![PostIndexAnnRestriction::Lt {
            lhs: ck_column.clone(),
            rhs: 3.into(),
        }],
        9,
    )
    .await;
    assert_pk_ck_combinations(
        &pk_ck_values,
        [
            (0, 0),
            (0, 1),
            (0, 2),
            (1, 0),
            (1, 1),
            (1, 2),
            (2, 0),
            (2, 1),
            (2, 2),
        ],
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_clustering_key_int_lte() {
    crate::enable_tracing();

    let (index, client, pk_column, ck_column, _db, _server, _node_state) =
        setup_int_int_store().await;

    // Search for nearest neighbors with a filter on clustering key "ck" <= 2
    // Should return rows where ck is 0, 1, or 2 (3 values per pk, 3 pks = 9 total)
    let pk_ck_values = run_ann_filter_int_int(
        &client,
        &index,
        &pk_column,
        &ck_column,
        vec![PostIndexAnnRestriction::Lte {
            lhs: ck_column.clone(),
            rhs: 2.into(),
        }],
        9,
    )
    .await;
    assert_pk_ck_combinations(
        &pk_ck_values,
        [
            (0, 0),
            (0, 1),
            (0, 2),
            (1, 0),
            (1, 1),
            (1, 2),
            (2, 0),
            (2, 1),
            (2, 2),
        ],
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_clustering_key_int_gt() {
    crate::enable_tracing();

    let (index, client, pk_column, ck_column, _db, _server, _node_state) =
        setup_int_int_store().await;

    // Search for nearest neighbors with a filter on clustering key "ck" > 6
    // Should return rows where ck is 7, 8, or 9 (3 values per pk, 3 pks = 9 total)
    let pk_ck_values = run_ann_filter_int_int(
        &client,
        &index,
        &pk_column,
        &ck_column,
        vec![PostIndexAnnRestriction::Gt {
            lhs: ck_column.clone(),
            rhs: 6.into(),
        }],
        9,
    )
    .await;
    assert_pk_ck_combinations(
        &pk_ck_values,
        [
            (0, 7),
            (0, 8),
            (0, 9),
            (1, 7),
            (1, 8),
            (1, 9),
            (2, 7),
            (2, 8),
            (2, 9),
        ],
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_clustering_key_int_gte() {
    crate::enable_tracing();

    let (index, client, pk_column, ck_column, _db, _server, _node_state) =
        setup_int_int_store().await;

    // Search for nearest neighbors with a filter on clustering key "ck" >= 7
    // Should return rows where ck is 7, 8, or 9 (3 values per pk, 3 pks = 9 total)
    let pk_ck_values = run_ann_filter_int_int(
        &client,
        &index,
        &pk_column,
        &ck_column,
        vec![PostIndexAnnRestriction::Gte {
            lhs: ck_column.clone(),
            rhs: 7.into(),
        }],
        9,
    )
    .await;
    assert_pk_ck_combinations(
        &pk_ck_values,
        [
            (0, 7),
            (0, 8),
            (0, 9),
            (1, 7),
            (1, 8),
            (1, 9),
            (2, 7),
            (2, 8),
            (2, 9),
        ],
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_clustering_key_int_range() {
    crate::enable_tracing();

    let (index, client, pk_column, ck_column, _db, _server, _node_state) =
        setup_int_int_store().await;

    // Search for nearest neighbors with a filter on clustering key 3 <= "ck" < 6
    // Should return rows where ck is 3, 4, or 5 (3 values per pk, 3 pks = 9 total)
    let pk_ck_values = run_ann_filter_int_int(
        &client,
        &index,
        &pk_column,
        &ck_column,
        vec![
            PostIndexAnnRestriction::Gte {
                lhs: ck_column.clone(),
                rhs: 3.into(),
            },
            PostIndexAnnRestriction::Lt {
                lhs: ck_column.clone(),
                rhs: 6.into(),
            },
        ],
        9,
    )
    .await;
    assert_pk_ck_combinations(
        &pk_ck_values,
        [
            (0, 3),
            (0, 4),
            (0, 5),
            (1, 3),
            (1, 4),
            (1, 5),
            (2, 3),
            (2, 4),
            (2, 5),
        ],
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_primary_key_int_lt_tuple() {
    crate::enable_tracing();

    let (index, client, pk_column, ck_column, _db, _server, _node_state) =
        setup_int_int_store().await;

    // Search for nearest neighbors with a filter on primary key ("pk", "ck") < (1, 5)
    // Should return rows where (pk, ck) < (1, 5), i.e., pk=0 (all ck) and pk=1 with ck < 5
    // That's 10 (pk=0) + 5 (pk=1, ck=0..4) = 15 rows
    let pk_ck_values = run_ann_filter_int_int(
        &client,
        &index,
        &pk_column,
        &ck_column,
        vec![PostIndexAnnRestriction::LtTuple {
            lhs: vec![pk_column.clone(), ck_column.clone()],
            rhs: vec![1.into(), 5.into()],
        }],
        15,
    )
    .await;
    // pk=0, all ck values
    assert_pk_ck_combinations(
        &pk_ck_values,
        [
            (0, 0),
            (0, 1),
            (0, 2),
            (0, 3),
            (0, 4),
            (0, 5),
            (0, 6),
            (0, 7),
            (0, 8),
            (0, 9),
            (1, 0),
            (1, 1),
            (1, 2),
            (1, 3),
            (1, 4),
        ],
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_primary_key_int_lte_tuple() {
    crate::enable_tracing();

    let (index, client, pk_column, ck_column, _db, _server, _node_state) =
        setup_int_int_store().await;

    // Search for nearest neighbors with a filter on primary key ("pk", "ck") <= (1, 5)
    // Should return rows where (pk, ck) <= (1, 5), i.e., pk=0 (all ck) and pk=1 with ck <= 5
    // That's 10 (pk=0) + 6 (pk=1, ck=0..5) = 16 rows
    let pk_ck_values = run_ann_filter_int_int(
        &client,
        &index,
        &pk_column,
        &ck_column,
        vec![PostIndexAnnRestriction::LteTuple {
            lhs: vec![pk_column.clone(), ck_column.clone()],
            rhs: vec![1.into(), 5.into()],
        }],
        16,
    )
    .await;
    // pk=0, all ck values
    assert_pk_ck_combinations(
        &pk_ck_values,
        [
            (0, 0),
            (0, 1),
            (0, 2),
            (0, 3),
            (0, 4),
            (0, 5),
            (0, 6),
            (0, 7),
            (0, 8),
            (0, 9),
            (1, 0),
            (1, 1),
            (1, 2),
            (1, 3),
            (1, 4),
            (1, 5),
        ],
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_primary_key_int_gt_tuple() {
    crate::enable_tracing();

    let (index, client, pk_column, ck_column, _db, _server, _node_state) =
        setup_int_int_store().await;

    // Search for nearest neighbors with a filter on primary key ("pk", "ck") > (1, 5)
    // Should return rows where (pk, ck) > (1, 5), i.e., pk=1 with ck > 5 and pk=2 (all ck)
    // That's 4 (pk=1, ck=6..9) + 10 (pk=2) = 14 rows
    let pk_ck_values = run_ann_filter_int_int(
        &client,
        &index,
        &pk_column,
        &ck_column,
        vec![PostIndexAnnRestriction::GtTuple {
            lhs: vec![pk_column.clone(), ck_column.clone()],
            rhs: vec![1.into(), 5.into()],
        }],
        14,
    )
    .await;
    // pk=1, ck > 5 and pk=2, all ck values
    assert_pk_ck_combinations(
        &pk_ck_values,
        [
            (1, 6),
            (1, 7),
            (1, 8),
            (1, 9),
            (2, 0),
            (2, 1),
            (2, 2),
            (2, 3),
            (2, 4),
            (2, 5),
            (2, 6),
            (2, 7),
            (2, 8),
            (2, 9),
        ],
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_primary_key_int_gte_tuple() {
    crate::enable_tracing();

    let (index, client, pk_column, ck_column, _db, _server, _node_state) =
        setup_int_int_store().await;

    // Search for nearest neighbors with a filter on primary key ("pk", "ck") >= (1, 5)
    // Should return rows where (pk, ck) >= (1, 5), i.e., pk=1 with ck >= 5 and pk=2 (all ck)
    // That's 5 (pk=1, ck=5..9) + 10 (pk=2) = 15 rows
    let pk_ck_values = run_ann_filter_int_int(
        &client,
        &index,
        &pk_column,
        &ck_column,
        vec![PostIndexAnnRestriction::GteTuple {
            lhs: vec![pk_column.clone(), ck_column.clone()],
            rhs: vec![1.into(), 5.into()],
        }],
        15,
    )
    .await;
    // pk=1, ck >= 5 and pk=2, all ck values
    assert_pk_ck_combinations(
        &pk_ck_values,
        [
            (1, 5),
            (1, 6),
            (1, 7),
            (1, 8),
            (1, 9),
            (2, 0),
            (2, 1),
            (2, 2),
            (2, 3),
            (2, 4),
            (2, 5),
            (2, 6),
            (2, 7),
            (2, 8),
            (2, 9),
        ],
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_filter_partition_key_text_gt() {
    crate::enable_tracing();

    let pk_column: ColumnName = "pk".into();
    let ck_column: ColumnName = "ck".into();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexType::Global,
        [pk_column.clone(), ck_column.clone()],
        [
            (pk_column.clone(), NativeType::Text),
            (ck_column.clone(), NativeType::Int),
        ],
        ["a", "b", "c", "d", "e"].iter().enumerate().map(|(i, pk)| {
            (
                vec![CqlValue::Text(pk.to_string()), CqlValue::Int(i as i32)].into(),
                Some(vec![i as f32, i as f32, i as f32].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            )
        }),
    )
    .await;

    // Search for nearest neighbors with a filter on partition key "pk" > "b"
    // Should return rows where pk is "c", "d", or "e"
    let pk_ck_values = wait_for_value(
        || async {
            let (primary_keys, _) = client
                .ann(
                    &index.keyspace_name,
                    &index.index_name,
                    vec![1.0, 2.0, 3.0].into(),
                    Some(PostIndexAnnFilter {
                        restrictions: vec![PostIndexAnnRestriction::Gt {
                            lhs: pk_column.clone(),
                            rhs: "b".into(),
                        }],
                        allow_filtering: false,
                    }),
                    NonZeroUsize::new(100).unwrap().into(),
                )
                .await;
            let pk_ck_values: HashSet<_> = primary_keys
                .get(&pk_column)
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .zip(
                    primary_keys
                        .get(&ck_column)
                        .unwrap()
                        .iter()
                        .map(|v| v.as_i64().unwrap() as usize),
                )
                .collect();
            (pk_ck_values.len() == 3).then_some(pk_ck_values)
        },
        "Waiting for ANN to return 3 results",
    )
    .await;
    assert!(
        pk_ck_values.contains(&("c".to_string(), 2)),
        "Expected pk_ck_values to contain value (c, 2)"
    );
    assert!(
        pk_ck_values.contains(&("d".to_string(), 3)),
        "Expected pk_ck_values to contain value (d, 3)"
    );
    assert!(
        pk_ck_values.contains(&("e".to_string(), 4)),
        "Expected pk_ck_values to contain value (e, 4)"
    );
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn http_server_is_responsive_when_index_add_hangs() {
    crate::enable_tracing();
    let config = Config {
        usearch_simulator: Some(vec![
            Duration::from_secs(0),
            Duration::from_secs(20), // Simulate long add operation (longer than test timeout).
            Duration::from_secs(0),
        ]),
        ..test_config()
    };
    let (run, _index, _db, _node_state) = setup_store(
        config,
        DbIndexType::Global,
        ["pk".into(), "ck".into()],
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        [(
            vec![CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
            Some(vec![1., 1., 1.].into()),
            OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
        )],
    )
    .await;

    let (client, _server, _config_tx) = run.await;

    // Ensure the HTTP server stays responsive while the (simulated) embedding add is long-running.
    let status = client.status().await.unwrap();

    assert_eq!(status, vector_store::httproutes::NodeStatus::Serving);
}
