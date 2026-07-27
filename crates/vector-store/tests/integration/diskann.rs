/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::create_config_channels;
use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::db_basic::ScanFn;
use crate::db_basic::Table;
use crate::wait_for;
use httpapi::IndexNotReadyReason;
use httpapi::IndexStatus;
use httpapi::PostIndexAnnFilter;
use httpapi::PostIndexAnnResponse;
use httpapi::PostIndexAnnRestriction;
use httpclient::HttpClient;
use reqwest::StatusCode;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use uuid::Uuid;
use vector_store::ColumnName;
use vector_store::Config;
use vector_store::Connectivity;
use vector_store::DbIndexPartitioning;
use vector_store::Dimensions;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::HttpServerExt;
use vector_store::IndexKind;
use vector_store::IndexMetadata;
use vector_store::IndexOptionsVs;
use vector_store::NonemptyArc;
use vector_store::NonemptyIteratorExt;
use vector_store::Percentage;
use vector_store::Quantization;
use vector_store::SpaceType;
use vector_store::Timestamp;
use vector_store::node_state::NodeState;

pub(crate) fn test_config() -> Config {
    Config {
        vector_store_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    }
}

pub(crate) async fn setup_store(
    config: Config,
    partitioning: DbIndexPartitioning,
    primary_keys: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    fullscan_fn: Option<ScanFn>,
    cdc_fn: Option<ScanFn>,
) -> (
    impl std::future::Future<Output = (HttpClient, impl Sized, impl Sized)>,
    IndexMetadata,
    DbBasic,
    Sender<NodeState>,
) {
    setup_store_with_quantization(
        config,
        partitioning,
        primary_keys,
        partition_key_count,
        columns,
        fullscan_fn,
        cdc_fn,
        Quantization::default(),
        NonZeroUsize::new(3).unwrap().into(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn setup_store_with_quantization(
    config: Config,
    partitioning: DbIndexPartitioning,
    primary_keys: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    fullscan_fn: Option<ScanFn>,
    cdc_fn: Option<ScanFn>,
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

    let primary_keys = primary_keys.into_iter().collect_nonempty_arc().unwrap();
    let columns: Arc<HashMap<_, _>> = Arc::new(columns.into_iter().collect());
    let filtering_columns = columns
        .keys()
        .filter(|c| !primary_keys.contains(c))
        .cloned()
        .collect();
    let index = IndexMetadata {
        keyspace_name: "vector".into(),
        table_name: "items".into(),
        index_name: "ann".into(),
        primary_key_columns: primary_keys.clone(),
        partition_key_count: NonZeroUsize::new(partition_key_count).unwrap(),
        target_columns: NonemptyArc::new(["embedding"]).unwrap(),
        partitioning,
        filtering_columns,
        version: Uuid::new_v4().into(),
        kind: IndexKind::Vs(IndexOptionsVs {
            dimensions: dimension,
            connectivity: Connectivity::default(),
            expansion_add: ExpansionAdd::default(),
            expansion_search: ExpansionSearch::default(),
            space_type: SpaceType::Euclidean,
            quantization,
        }),
    };

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys,
            partition_key_count,
            columns,
            dimensions: [(
                index.target_columns.first().clone(),
                index.vs().unwrap().dimensions,
            )]
            .into_iter()
            .collect(),
        },
    )
    .unwrap();

    db.add_index(index.clone(), fullscan_fn, cdc_fn).unwrap();

    let (receivers, senders) = create_config_channels(config).await;
    let index_factory = vector_store::new_index_factory_diskann(receivers.config.clone()).unwrap();

    let run = {
        let node_state = node_state.clone();
        async move {
            let (server, _mtls) = vector_store::run(
                node_state,
                db_actor,
                internals,
                index_factory,
                receivers,
                vector_store::new_metrics(),
            )
            .await
            .unwrap();
            let addr = (*server.address().await.borrow()).unwrap();

            (HttpClient::new(addr), server, senders)
        }
    };

    (run, index, db, node_state)
}

pub(crate) async fn setup_store_and_wait_for_index(
    partitioning: DbIndexPartitioning,
    primary_keys: impl IntoIterator<Item = ColumnName>,
    partition_key_count: usize,
    columns: impl IntoIterator<Item = (ColumnName, NativeType)>,
    fullscan_fn: Option<ScanFn>,
    cdc_fn: Option<ScanFn>,
    expected_count: Option<usize>,
) -> (
    IndexMetadata,
    HttpClient,
    DbBasic,
    impl Sized,
    Sender<NodeState>,
) {
    let (run, index, db, node_state) = setup_store(
        test_config(),
        partitioning,
        primary_keys,
        partition_key_count,
        columns,
        fullscan_fn,
        cdc_fn,
    )
    .await;
    let (client, server, _config_tx) = run.await;

    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| {
                    status.status == IndexStatus::Serving
                        && expected_count.is_none_or(|count| status.count == count)
                })
        },
        "Waiting for index to be serving",
    )
    .await;

    (index, client, db, (server, _config_tx), node_state)
}

#[tokio::test]
async fn simple_create_search_delete_index() {
    crate::enable_tracing();

    let (run, index, db, _node_state) = setup_store(
        test_config(),
        DbIndexPartitioning::Global,
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        Some(db_basic::scan_fn_vectors([
            (
                [CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
                Some(vec![1., 1., 1.].into()),
                [].into(),
                Timestamp::from_millis(10),
            ),
            (
                [CqlValue::Int(2), CqlValue::Text("two".to_string())].into(),
                Some(vec![2., -2., 2.].into()),
                [].into(),
                Timestamp::from_millis(20),
            ),
            (
                [CqlValue::Int(3), CqlValue::Text("three".to_string())].into(),
                Some(vec![3., 3., 3.].into()),
                [].into(),
                Timestamp::from_millis(30),
            ),
        ])),
        None,
    )
    .await;
    let (client, _server, _config_tx) = run.await;

    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();
    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| status.status == IndexStatus::Serving && status.count == 3)
        },
        "Waiting for 3 vectors to be indexed",
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0], httpapi::IndexInfo::new("vector", "ann"));

    let (primary_keys, distances, similarity_scores) = client
        .ann(
            &keyspace_name,
            &index_name,
            vec![2.1, -2., 2.].into(),
            None,
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;
    assert_eq!(distances.len(), 1);
    assert_eq!(similarity_scores.len(), 1);
    let primary_keys_pk = primary_keys.get(&"pk".into()).unwrap();
    let primary_keys_ck = primary_keys.get(&"ck".into()).unwrap();
    assert_eq!(distances.len(), primary_keys_pk.len());
    assert_eq!(distances.len(), primary_keys_ck.len());
    assert_eq!(similarity_scores.len(), distances.len());
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
        primary_key_columns: NonemptyArc::new(["pk", "ck"]).unwrap(),
        partition_key_count: NonZeroUsize::new(1).unwrap(),
        target_columns: NonemptyArc::new(["embedding"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new([]),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Vs(IndexOptionsVs {
            dimensions: NonZeroUsize::new(3).unwrap().into(),
            connectivity: Default::default(),
            expansion_add: Default::default(),
            expansion_search: Default::default(),
            space_type: Default::default(),
            quantization: Default::default(),
        }),
    };

    let (_, rx) = watch::channel(Arc::new(Config::default()));
    let index_factory = vector_store::new_index_factory_diskann(rx).unwrap();

    let (receivers, _senders) = create_config_channels(test_config()).await;
    let (server, _mtls) = vector_store::run(
        node_state,
        db_actor,
        internals,
        index_factory,
        receivers,
        vector_store::new_metrics(),
    )
    .await
    .unwrap();
    let addr = (*server.address().await.borrow()).unwrap();

    let client = HttpClient::new(addr);

    db.set_next_get_db_index_failed();

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: NonemptyArc::new(["pk", "ck"]).unwrap(),
            partition_key_count: 1,
            columns: Arc::new(
                [
                    ("pk".into(), NativeType::Int),
                    ("ck".into(), NativeType::Text),
                ]
                .into_iter()
                .collect(),
            ),
            dimensions: [(
                index.target_columns.first().clone(),
                index.vs().unwrap().dimensions,
            )]
            .into_iter()
            .collect(),
        },
    )
    .unwrap();
    db.add_index(index.clone(), None, None).unwrap();

    wait_for(
        || async { !client.indexes().await.is_empty() },
        "Waiting for index to be added to the store",
    )
    .await;

    db.add_index(
        IndexMetadata {
            index_name: "ann2".into(),
            ..index.clone()
        },
        None,
        None,
    )
    .unwrap();

    wait_for(
        || async { client.indexes().await.len() == 2 },
        "Waiting for 2nd index to be added to the store",
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 2);
    assert!(indexes.contains(&httpapi::IndexInfo::new("vector", "ann")));
    assert!(indexes.contains(&httpapi::IndexInfo::new("vector", "ann2")));

    db.add_index(
        IndexMetadata {
            index_name: "ann3".into(),
            ..index.clone()
        },
        None,
        None,
    )
    .unwrap();

    wait_for(
        || async { client.indexes().await.len() == 3 },
        "Waiting for 3rd index to be added to the store",
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 3);
    assert!(indexes.contains(&httpapi::IndexInfo::new("vector", "ann")));
    assert!(indexes.contains(&httpapi::IndexInfo::new("vector", "ann2")));
    assert!(indexes.contains(&httpapi::IndexInfo::new("vector", "ann3")));

    db.del_index(&index.keyspace_name, &"ann2".into()).unwrap();

    wait_for(
        || async { client.indexes().await.len() == 2 },
        "Waiting for index to be removed from the store",
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 2);
    assert!(indexes.contains(&httpapi::IndexInfo::new("vector", "ann")));
    assert!(indexes.contains(&httpapi::IndexInfo::new("vector", "ann3")));
}

#[tokio::test]
async fn ann_returns_bad_request_when_provided_vector_size_is_not_eq_index_dimensions() {
    crate::enable_tracing();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexPartitioning::Global,
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        Some(db_basic::scan_fn_vectors([(
            [CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
            Some(vec![1., 1., 1.].into()),
            [].into(),
            Timestamp::from_millis(10),
        )])),
        None,
        Some(1),
    )
    .await;

    let result = client
        .post_ann(
            &index.keyspace_name.into(),
            &index.index_name.into(),
            vec![1.0, 2.0].into(), // Only 2 dimensions, should be 3 (index.vs().unwrap().dimensions)
            None,
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;

    assert_eq!(result.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[ntest::timeout(10_000)]
async fn ann_returns_bad_request_when_filtering_required_but_not_allowed() {
    crate::enable_tracing();

    let pk_column: ColumnName = "pk".into();
    let ck_column: ColumnName = "ck".into();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexPartitioning::Global,
        [pk_column.clone(), ck_column.clone()],
        1,
        [
            (pk_column.clone(), NativeType::Int),
            (ck_column.clone(), NativeType::Int),
        ],
        Some(db_basic::scan_fn_vectors([(
            [CqlValue::Int(1), CqlValue::Int(1)].into(),
            Some(vec![1., 1., 1.].into()),
            [].into(),
            Timestamp::from_millis(10),
        )])),
        None,
        Some(1),
    )
    .await;

    let result = client
        .post_ann(
            &index.keyspace_name.into(),
            &index.index_name.into(),
            vec![1.0, 2.0, 3.0].into(),
            Some(PostIndexAnnFilter {
                restrictions: vec![PostIndexAnnRestriction::Eq {
                    lhs: pk_column.into(),
                    rhs: 1.into(),
                }],
                allow_filtering: false,
            }),
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;

    assert_eq!(result.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn ann_fail_while_building_when_node_is_bootstrapping() {
    crate::enable_tracing();
    let (run, index, db, _node_state) = setup_store(
        test_config(),
        DbIndexPartitioning::Global,
        ["pk".into(), "ck".into()],
        1,
        [
            ("pk".to_string().into(), NativeType::Int),
            ("ck".to_string().into(), NativeType::Text),
        ],
        Some(db_basic::pending_scan_fn()),
        None,
    )
    .await;
    db.set_next_full_scan_progress(vector_store::Progress::InProgress(
        Percentage::try_from(33.333).unwrap(),
    ));
    let (client, _server, _config_tx) = run.await;

    let keyspace_name = index.keyspace_name.into();
    let index_name = index.index_name.into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| status.status == IndexStatus::Bootstrapping)
        },
        "Waiting for index to be bootstrapping",
    )
    .await;

    let result = client
        .post_ann(
            &keyspace_name,
            &index_name,
            vec![1.0, 2.0, 3.0].into(),
            None,
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;

    assert_eq!(result.status(), StatusCode::SERVICE_UNAVAILABLE);
    let reason: IndexNotReadyReason = result.json().await.unwrap();
    assert_eq!(reason, IndexNotReadyReason::NodeBootstrapping);
}

#[tokio::test]
async fn ann_fail_while_building_when_node_is_serving() {
    crate::enable_tracing();

    let (serving_index, client, db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexPartitioning::Global,
        ["pk".into()],
        1,
        [("pk".to_string().into(), NativeType::Int)],
        Some(db_basic::scan_fn_vectors([(
            [CqlValue::Int(1)].into(),
            Some(vec![1., 1., 1.].into()),
            [].into(),
            Timestamp::from_millis(10),
        )])),
        None,
        Some(1),
    )
    .await;

    let index: IndexMetadata = IndexMetadata {
        index_name: "ann_building".into(),
        target_columns: NonemptyArc::new(["embedding2"]).unwrap(),
        version: uuid::Uuid::new_v4().into(),
        ..serving_index
    };
    // Add new column to the table so that the new index will be created on a new column.
    // This allows the routing mechanism to always route to this index.
    // Otherwise, when both indexes are on the same column, the routing mechanism will always route to the serving index.
    db.add_vector_column(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        index.target_columns.first().clone(),
        index.vs().unwrap().dimensions,
    )
    .unwrap();
    // Add second index that is Bootstrapping (pending_scan_fn). So now we have node Serving and this index Bootstrapping - a good state to test desired scenario.
    db.add_index(index.clone(), Some(db_basic::pending_scan_fn()), None)
        .unwrap();
    db.set_next_full_scan_progress(vector_store::Progress::InProgress(
        Percentage::try_from(75.0).unwrap(),
    ));

    let keyspace_name: httpapi::KeyspaceName = index.keyspace_name.into();
    let index_name: httpapi::IndexName = index.index_name.into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|status| status.status == IndexStatus::Bootstrapping)
        },
        "Waiting for index to be bootstrapping",
    )
    .await;

    let result = client
        .post_ann(
            &keyspace_name,
            &index_name,
            vec![1.0, 2.0, 3.0].into(),
            None,
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;

    assert_eq!(result.status(), StatusCode::SERVICE_UNAVAILABLE);
    let reason: IndexNotReadyReason = result.json().await.unwrap();
    let IndexNotReadyReason::IndexBuilding { message } = reason else {
        panic!("expected IndexBuilding, got {reason:?}");
    };
    assert!(
        message.contains(&format!("{keyspace_name}.{index_name}")),
        "unexpected message: {message}"
    );
    assert!(
        message.contains("progress: 75.000%"),
        "unexpected message: {message}"
    );
}

#[tokio::test]
async fn ann_failed_when_wrong_number_of_primary_keys() {
    crate::enable_tracing();
    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexPartitioning::Global,
        vec!["pk".into()],
        1,
        [("pk".into(), NativeType::Int)],
        Some(db_basic::scan_fn_vectors([(
            [CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
            Some(vec![1., 1., 1.].into()),
            [].into(),
            Timestamp::from_millis(10),
        )])),
        None,
        Some(1),
    )
    .await;

    let keyspace_name = index.keyspace_name.into();
    let index_name = index.index_name.into();
    wait_for(
        || async {
            let response = client
                .post_ann(
                    &keyspace_name,
                    &index_name,
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
async fn null_vector_is_not_indexed() {
    crate::enable_tracing();

    let (run, index, _db, _node_state) = setup_store(
        test_config(),
        DbIndexPartitioning::Global,
        ["pk".into()],
        1,
        [("pk".to_string().into(), NativeType::Int)],
        Some(db_basic::scan_fn_vectors([
            (
                [CqlValue::Int(1)].into(),
                Some(vec![1., 1., 1.].into()),
                [].into(),
                Timestamp::from_millis(10),
            ),
            (
                [CqlValue::Int(2)].into(),
                None,
                [].into(),
                Timestamp::from_millis(20),
            ),
        ])),
        None,
    )
    .await;
    let (client, _server, _config_tx) = run.await;

    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();
    wait_for(
        || async {
            let status = client
                .index_status(&keyspace_name, &index_name)
                .await
                .expect("failed to get index status");
            status.status == httpapi::IndexStatus::Serving && status.count == 1
        },
        "Waiting for exactly 1 vector to be indexed (null vector must be skipped)",
    )
    .await;
}

// Regression test for the similarity_scores returned by ann(). These need
// to be similarity scores (decreasing), not distance scores (increasing).
//
// This test inserts 3 vectors at different distances from the query and
// checks that:
//  1. similarity_scores are in strictly decreasing order (nearest = highest score).
//  2. For Euclidean distance, the formula similarity = 1/(1+d) is applied
//     correctly.
#[tokio::test]
#[ntest::timeout(10_000)]
async fn similarity_scores_are_decreasing_and_correctly_converted() {
    crate::enable_tracing();

    let node_state = vector_store::new_node_state().await;
    let internals = vector_store::new_internals();
    let (db_actor, db) = db_basic::new(node_state.clone());

    // Use a 1-D Euclidean index so distances are easy to predict.
    let index = IndexMetadata {
        keyspace_name: "vector".into(),
        table_name: "items".into(),
        index_name: "ann".into(),
        primary_key_columns: NonemptyArc::new(["pk"]).unwrap(),
        partition_key_count: NonZeroUsize::new(1).unwrap(),
        target_columns: NonemptyArc::new(["embedding"]).unwrap(),
        partitioning: DbIndexPartitioning::Global,
        filtering_columns: Arc::new([]),
        version: Uuid::new_v4().into(),
        kind: IndexKind::Vs(IndexOptionsVs {
            dimensions: NonZeroUsize::new(1).unwrap().into(),
            connectivity: Connectivity::default(),
            expansion_add: ExpansionAdd::default(),
            expansion_search: ExpansionSearch::default(),
            space_type: SpaceType::Euclidean,
            quantization: Quantization::default(),
        }),
    };

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: NonemptyArc::new(["pk"]).unwrap(),
            partition_key_count: 1,
            columns: Arc::new([("pk".into(), NativeType::Int)].into_iter().collect()),
            dimensions: [(
                index.target_columns.first().clone(),
                index.vs().unwrap().dimensions,
            )]
            .into_iter()
            .collect(),
        },
    )
    .unwrap();

    // Three items with 1-D vectors [0.0], [1.0], [3.0].
    // Query vector is [0.0]. USearch computes squared L2 distance, so the
    // squared distances are 0, 1, 9 respectively, and expected similarity
    // scores (using 1/(1+d)) are 1/(1+0)=1.0, 1/(1+1)=0.5, 1/(1+9)=0.1.
    db.add_index(
        index.clone(),
        Some(db_basic::scan_fn_vectors([
            (
                [CqlValue::Int(1)].into(),
                Some(vec![0.0_f32].into()),
                [].into(),
                Timestamp::from_millis(10),
            ),
            (
                [CqlValue::Int(2)].into(),
                Some(vec![1.0_f32].into()),
                [].into(),
                Timestamp::from_millis(20),
            ),
            (
                [CqlValue::Int(3)].into(),
                Some(vec![3.0_f32].into()),
                [].into(),
                Timestamp::from_millis(30),
            ),
        ])),
        None,
    )
    .unwrap();

    let (_, rx) = watch::channel(Arc::new(Config::default()));
    let index_factory = vector_store::new_index_factory_diskann(rx).unwrap();
    let (receivers, _senders) = create_config_channels(test_config()).await;
    let (server, _mtls) = vector_store::run(
        node_state,
        db_actor,
        internals,
        index_factory,
        receivers,
        vector_store::new_metrics(),
    )
    .await
    .unwrap();
    let addr = (*server.address().await.borrow()).unwrap();
    let client = HttpClient::new(addr);

    let keyspace_name = index.keyspace_name.clone().into();
    let index_name = index.index_name.clone().into();

    wait_for(
        || async {
            client
                .index_status(&keyspace_name, &index_name)
                .await
                .is_ok_and(|s| s.status == IndexStatus::Serving && s.count == 3)
        },
        "Waiting for 3 vectors to be indexed",
    )
    .await;

    let (_primary_keys, distances, similarity_scores) = client
        .ann(
            &keyspace_name,
            &index_name,
            vec![0.0_f32].into(),
            None,
            NonZeroUsize::new(3).unwrap().into(),
        )
        .await;

    assert_eq!(similarity_scores.len(), 3);
    assert_eq!(distances.len(), 3);

    let scores: Vec<f32> = similarity_scores
        .iter()
        .map(|s| serde_json::to_value(s).unwrap().as_f64().unwrap() as f32)
        .collect();

    // Scores must be in strictly decreasing order (nearest item has highest similarity).
    assert!(
        scores[0] > scores[1] && scores[1] > scores[2],
        "similarity_scores must be in decreasing order, got: {scores:?}"
    );

    // Verify the Euclidean similarity formula 1/(1+d) is correctly applied.
    let epsilon = 1e-5_f32;
    assert!(
        (scores[0] - 1.0).abs() < epsilon,
        "nearest item (distance=0) should have similarity 1.0, got {}",
        scores[0]
    );
    assert!(
        (scores[1] - 0.5).abs() < epsilon,
        "second item (distance=1) should have similarity 0.5, got {}",
        scores[1]
    );
    assert!(
        (scores[2] - 0.1).abs() < epsilon,
        "farthest item (squared distance=9) should have similarity 0.1, got {}",
        scores[2]
    );
}

#[tokio::test]
async fn empty_index_has_zero_count() {
    crate::enable_tracing();

    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexPartitioning::Global,
        ["pk".into()],
        1,
        [("pk".to_string().into(), NativeType::Int)],
        Some(db_basic::scan_fn_vectors(std::iter::empty())),
        None,
        Some(0),
    )
    .await;

    let status = client
        .index_status(
            &index.keyspace_name.clone().into(),
            &index.index_name.clone().into(),
        )
        .await
        .unwrap();
    assert_eq!(status.status, IndexStatus::Serving);
    assert_eq!(status.count, 0);
}

#[tokio::test]
async fn empty_index_returns_empty_ann_results() {
    crate::enable_tracing();

    let (index, client, _db, _server, _node_state) = setup_store_and_wait_for_index(
        DbIndexPartitioning::Global,
        ["pk".into()],
        1,
        [("pk".to_string().into(), NativeType::Int)],
        Some(db_basic::scan_fn_vectors(std::iter::empty())),
        None,
        Some(0),
    )
    .await;

    let (primary_keys, distances, similarity_scores) = client
        .ann(
            &index.keyspace_name.clone().into(),
            &index.index_name.clone().into(),
            vec![1.0, 1.0, 1.0].into(),
            None,
            NonZeroUsize::new(10).unwrap().into(),
        )
        .await;

    assert!(primary_keys.get(&"pk".into()).unwrap().is_empty());
    assert!(distances.is_empty());
    assert!(similarity_scores.is_empty());
}
