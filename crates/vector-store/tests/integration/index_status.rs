/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::routing::add_table;
use crate::routing::blocking_scan_fn;
use crate::routing::make_index;
use crate::routing::ordered_timeuuid;
use crate::routing::setup;
use crate::routing::single_row_scan;
use crate::wait_for;
use httpapi::IndexStatus;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use vector_store::DbIndexPartitioning;
use vector_store::Percentage;
use vector_store::Progress;

// While the initial full table scan that backfills an index is paused (held
// open via blocking_scan_fn, simulating a paused/slow scan), the index status
// endpoint must report it as BOOTSTRAPPING and surface the build progress
// percentage reported by the database. Once the scan finishes the index
// becomes SERVING with build_progress == 100.
#[tokio::test]
#[ntest::timeout(10_000)]
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

    // Pin the reported full-scan progress to a partial value, then add an index
    // whose scan never completes, keeping it bootstrapping at that progress.
    db.set_next_full_scan_progress(Progress::InProgress(Percentage::try_from(37.0).unwrap()));

    let index = make_index(
        "building",
        "embedding",
        DbIndexPartitioning::Global,
        &[],
        ordered_timeuuid(1),
    );
    db.add_index(index.clone(), Some(blocking_scan_fn()), None)
        .unwrap();

    let ks = index.keyspace_name.as_ref().into();
    let idx = index.index_name.as_ref().into();
    wait_for(
        || async {
            client
                .index_status(&ks, &idx)
                .await
                .is_ok_and(|s| s.status == IndexStatus::Bootstrapping && s.build_progress == 37.0)
        },
        "index to be bootstrapping with build_progress == 37%",
    )
    .await;
}

// A fully built (serving) index reports build_progress == 100.
#[tokio::test]
#[ntest::timeout(10_000)]
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

    let index = make_index(
        "ready",
        "embedding",
        DbIndexPartitioning::Global,
        &[],
        ordered_timeuuid(1),
    );
    db.add_index(
        index.clone(),
        Some(single_row_scan([CqlValue::Int(1)])),
        None,
    )
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
