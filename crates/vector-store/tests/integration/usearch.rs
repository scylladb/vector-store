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
        primary_keys,
        columns,
        values,
        Quantization::default(),
    )
    .await
}

pub(crate) async fn setup_store_with_quantization(
    config: Config,
    primary_keys: impl IntoIterator<Item = ColumnName>,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    values: impl IntoIterator<Item = (PrimaryKey, Option<Vector>, Timestamp)>,
    quantization: Quantization,
) -> (
    impl std::future::Future<Output = (HttpClient, impl Sized, impl Sized)>,
    IndexMetadata,
    DbBasic,
    Sender<NodeState>,
) {
    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();

    let (db_actor, db) = db_basic::new(node_state.clone());

    let index = IndexMetadata {
        keyspace_name: "vector".into(),
        table_name: "items".into(),
        index_name: "ann".into(),
        target_column: "embedding".into(),
        dimensions: NonZeroUsize::new(3).unwrap().into(),
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
            columns: Arc::new(columns.into_iter().collect()),
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
        setup_store(test_config(), primary_keys, columns, values).await;
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
async fn ann_works_with_embedding_field_name() {
    // Ensure backward compatibility with the old field name "embedding".
    crate::enable_tracing();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        ["pk".into(), "ck".into()],
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        [],
    )
    .await;
    #[derive(serde::Serialize)]
    struct EmbeddingRequest {
        embedding: Vector,
    }

    let response = client
        .post_ann_data(
            &index.keyspace_name,
            &index.index_name,
            &EmbeddingRequest {
                embedding: vec![1.0, 2.0, 3.0].into(),
            },
        )
        .await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn ann_failed_when_wrong_number_of_primary_keys() {
    crate::enable_tracing();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
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
