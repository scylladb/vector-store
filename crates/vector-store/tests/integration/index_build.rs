/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::common;
use crate::db_basic;
use crate::db_basic::DbBasic;
use crate::db_basic::Index;
use crate::db_basic::TOKEN_MAX;
use crate::db_basic::TOKEN_MIN;
use crate::vs_index;
use crate::wait_for;
use httpapi::IndexStatus;
use httpclient::HttpClient;
use rstest::rstest;
use scylla::cluster::metadata::NativeType;
use scylla::routing::Token;
use scylla::value::CqlValue;
use scylla_cdc::consumer::OperationType;
use std::num::NonZeroUsize;
use std::time::Duration;
use vector_store::Config;
use vector_store::DbIndexPartitioning;
use vector_store::DbRow;
use vector_store::DbValue;
use vector_store::HttpServerExt;

const CDC_WIDE_SAFETY_INTERVAL: Duration = Duration::from_millis(10);
const CDC_FINE_SAFETY_INTERVAL: Duration = Duration::from_millis(1);

fn test_config() -> Config {
    Config {
        cdc_safety_interval: Some(CDC_WIDE_SAFETY_INTERVAL),
        cdc_fine_safety_interval: Some(CDC_FINE_SAFETY_INTERVAL),
        ..vs_index::usearch_test_config()
    }
}

async fn setup() -> (HttpClient, DbBasic, impl Sized) {
    let db = db_basic::new();
    let (receivers, senders) = crate::create_config_channels(test_config()).await;
    let (server, _mtls) = vector_store::run(db.clone(), receivers).await.unwrap();
    let addr = (*server.address().await.borrow()).unwrap();
    (HttpClient::new(addr), db, (server, senders))
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn fullscan_and_cdc() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    db.set_nr_shards(NonZeroUsize::new(4).unwrap());
    db.set_token_ring(vec![
        Token::new(TOKEN_MIN / 2),
        Token::new(0),
        Token::new(TOKEN_MAX / 2),
    ]);

    common::add_table(
        &db,
        ["pk".into()],
        1,
        [("pk".into(), NativeType::Int)],
        ["embedding".into()],
    );

    let index = common::make_vs_index(
        "building",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
        &[],
        common::ordered_timeuuid(1),
    );
    let column_specs = db_basic::cdc_column_specs_vector(
        &index.keyspace_name,
        &index.table_name,
        [("pk", NativeType::Int)],
        "embedding",
        NonZeroUsize::new(3).unwrap(),
    );
    db.add_index(
        Index::new(index.clone())
            .with_range_scan(
                Token::new(TOKEN_MIN),
                db_basic::range_scan_fn_vectors([
                    ([CqlValue::Int(1)], Some(vec![1., 1., 1.].into()), [], 1),
                    ([CqlValue::Int(2)], Some(vec![2., 2., 2.].into()), [], 1),
                ]),
            )
            .with_range_scan(
                Token::new(TOKEN_MIN / 2),
                db_basic::range_scan_fn_vectors([
                    ([CqlValue::Int(3)], Some(vec![3., 3., 3.].into()), [], 1),
                    ([CqlValue::Int(4)], Some(vec![4., 4., 4.].into()), [], 1),
                ]),
            )
            .with_range_scan(
                Token::new(0),
                db_basic::range_scan_fn_vectors([
                    ([CqlValue::Int(5)], Some(vec![5., 5., 5.].into()), [], 1),
                    ([CqlValue::Int(6)], Some(vec![6., 6., 6.].into()), [], 1),
                ]),
            )
            .with_range_scan(
                Token::new(TOKEN_MAX / 2),
                db_basic::range_scan_fn_vectors([
                    ([CqlValue::Int(7)], Some(vec![7., 7., 7.].into()), [], 1),
                    ([CqlValue::Int(8)], Some(vec![8., 8., 8.].into()), [], 1),
                ]),
            )
            .with_cdc_scan(
                CDC_WIDE_SAFETY_INTERVAL,
                db_basic::cdc_scan_fn([
                    ([CqlValue::Int(9)], OperationType::RowUpdate, 1),
                    ([CqlValue::Int(10)], OperationType::RowUpdate, 1),
                ]),
                column_specs.clone(),
            )
            .with_cdc_scan(
                CDC_FINE_SAFETY_INTERVAL,
                db_basic::cdc_scan_fn([
                    ([CqlValue::Int(11)], OperationType::RowUpdate, 1),
                    ([CqlValue::Int(12)], OperationType::RowUpdate, 1),
                ]),
                column_specs,
            )
            .with_fetch_row(db_basic::fetch_row(|pk| match pk.get(0).unwrap() {
                CqlValue::Int(9) => Ok(Some(DbRow {
                    columns: vec![
                        Some(DbValue::Vector(vec![9., 9., 9.].into())),
                        Some(DbValue::Value(CqlValue::BigInt(1))),
                    ],
                })),
                CqlValue::Int(10) => Ok(Some(DbRow {
                    columns: vec![
                        Some(DbValue::Vector(vec![10., 10., 10.].into())),
                        Some(DbValue::Value(CqlValue::BigInt(1))),
                    ],
                })),
                CqlValue::Int(11) => Ok(Some(DbRow {
                    columns: vec![
                        Some(DbValue::Vector(vec![11., 11., 11.].into())),
                        Some(DbValue::Value(CqlValue::BigInt(1))),
                    ],
                })),
                CqlValue::Int(12) => Ok(Some(DbRow {
                    columns: vec![
                        Some(DbValue::Vector(vec![12., 12., 12.].into())),
                        Some(DbValue::Value(CqlValue::BigInt(1))),
                    ],
                })),
                _ => panic!("Unexpected primary key: {:?}", pk),
            })),
    )
    .unwrap();

    let ks = index.keyspace_name.as_ref().into();
    let idx = index.index_name.as_ref().into();
    wait_for(
        || async {
            client
                .index_status(&ks, &idx)
                .await
                .is_ok_and(|s| s.status == IndexStatus::Serving && s.count == 12)
        },
        "index to be serving with count == 12",
    )
    .await;
}
