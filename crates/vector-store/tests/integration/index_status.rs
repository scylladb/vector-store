/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::common;
use crate::common::add_table;
use crate::common::make_vs_index;
use crate::common::ordered_timeuuid;
use crate::common::setup;
use crate::db_basic;
use crate::db_basic::Index;
use crate::db_basic::TOKEN_MAX;
use crate::db_basic::TOKEN_MIN;
use crate::wait_for;
use httpapi::IndexStatus;
use rstest::rstest;
use scylla::cluster::metadata::NativeType;
use scylla::routing::Token;
use scylla::value::CqlValue;
use std::time::Duration;
use vector_store::DbIndexPartitioning;

// While the initial full table scan that backfills an index is paused (held
// open via blocking_scan_fn, simulating a paused/slow scan), the index status
// endpoint must report it as BOOTSTRAPPING and surface the build progress
// percentage reported by the database. Once the scan finishes the index
// becomes SERVING with build_progress == 100.
#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
#[cfg_attr(not(feature = "slow-test-hooks"), ignore = "requires slow-test-hooks")]
async fn index_status_reports_build_progress_while_bootstrapping() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into()],
        1,
        [("pk".into(), NativeType::Int)],
        ["embedding".into()],
    );

    let index = make_vs_index(
        "building",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
        &[],
        ordered_timeuuid(1),
    );
    db.set_token_ring(vec![
        Token::new(TOKEN_MIN / 2),
        Token::new(0),
        Token::new(TOKEN_MAX / 2),
    ]);
    db.add_index(
        Index::new(index.clone())
            .with_range_scan(Token::new(TOKEN_MIN), db_basic::pending_range_scan_fn()),
    )
    .unwrap();

    let ks = index.keyspace_name.as_ref().into();
    let idx = index.index_name.as_ref().into();
    wait_for(
        || async {
            client
                .index_status(&ks, &idx)
                .await
                .is_ok_and(|s| s.status == IndexStatus::Bootstrapping && s.build_progress == 75.0)
        },
        "index to be bootstrapping with build_progress == 75%",
    )
    .await;
}

// A fully built (serving) index reports build_progress == 100.
#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn index_status_reports_full_progress_when_serving() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into()],
        1,
        [("pk".into(), NativeType::Int)],
        ["embedding".into()],
    );

    let index = make_vs_index(
        "ready",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
        &[],
        ordered_timeuuid(1),
    );
    db.add_index(Index::new(index.clone()).with_range_scan(
        Token::new(TOKEN_MIN),
        common::single_row_range_scan([CqlValue::Int(1)]),
    ))
    .unwrap();

    let ks = index.keyspace_name.as_ref().into();
    let idx = index.index_name.as_ref().into();
    wait_for(
        || async {
            client
                .index_status(&ks, &idx)
                .await
                .is_ok_and(|s| s.status == IndexStatus::Serving && s.build_progress == 100.0)
        },
        "index to be serving with build_progress == 100%",
    )
    .await;
}
