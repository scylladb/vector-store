/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::common::add_table;
use crate::common::blocking_scan_fn;
use crate::common::make_vs_index;
use crate::common::ordered_timeuuid;
use crate::common::setup;
use crate::common::single_row_scan;
use crate::wait_for;
use httpapi::IndexStatus;
use httpapi::PostIndexAnnFilter;
use httpapi::PostIndexAnnRestriction;
use httpclient::HttpClient;
use reqwest::StatusCode;
use rstest::rstest;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use std::time::Duration;
use vector_store::DbIndexPartitioning;
use vector_store::IndexMetadata;
use vector_store::NonemptyArc;

const ANN_LIMIT: usize = 5;

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

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
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

    let oldest = make_vs_index(
        "oldest",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
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

    let replacement = make_vs_index(
        "replacement",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
        &[],
        ordered_timeuuid(2),
    );
    db.add_index(replacement.clone(), Some(blocking_scan_fn()), None)
        .unwrap();
    wait_for_bootstrapping(&client, &replacement).await;

    let response = assert_ann_served_by(&client, &oldest, post_ann(&client, &replacement)).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
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

    let oldest = make_vs_index(
        "oldest",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
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

    let replacement = make_vs_index(
        "replacement",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
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

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
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

    let older_local = make_vs_index(
        "older",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Local(NonemptyArc::new(["pk"]).unwrap()),
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

    let newer_local = make_vs_index(
        "newer",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Local(NonemptyArc::new(["pk"]).unwrap()),
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

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
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

    let less_precise = make_vs_index(
        "less_precise",
        &["pk1", "pk2", "ck"],
        2,
        "embedding",
        DbIndexPartitioning::Local(NonemptyArc::new(["pk1"]).unwrap()),
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

    let more_precise = make_vs_index(
        "more_precise",
        &["pk1", "pk2", "ck"],
        2,
        "embedding",
        DbIndexPartitioning::Local(NonemptyArc::new(["pk1", "pk2"]).unwrap()),
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

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
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

    let covering = make_vs_index(
        "covering",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Local(NonemptyArc::new(["pk"]).unwrap()),
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

    let non_covering = make_vs_index(
        "non_covering",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Local(NonemptyArc::new(["pk"]).unwrap()),
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

    let response = assert_ann_served_by(
        &client,
        &covering,
        post_ann_with_filter(&client, &non_covering, filter),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
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

    let covering = make_vs_index(
        "covering",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
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

    let non_covering = make_vs_index(
        "non_covering",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
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

    let response = assert_ann_served_by(
        &client,
        &covering,
        post_ann_with_filter(&client, &non_covering, filter),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
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

    let local_index = make_vs_index(
        "local_idx",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Local(NonemptyArc::new(["pk"]).unwrap()),
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

    let global_index = make_vs_index(
        "global_idx",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
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

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
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

    let local_index = make_vs_index(
        "local_idx",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Local(NonemptyArc::new(["pk"]).unwrap()),
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

    let global_index = make_vs_index(
        "global_idx",
        &["pk", "ck"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
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
