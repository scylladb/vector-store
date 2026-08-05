/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
use crate::common::add_table;
use crate::common::make_fts_index;
use crate::common::make_index_with_kind;
use crate::common::ordered_timeuuid;
use crate::common::setup;
use crate::common::single_row_scan;
use crate::db_basic;
use crate::wait_for;
use httpapi::DataType;
use httpapi::FulltextIndexOptions;
use httpapi::IndexOptions;
use httpapi::SimilarityFunction;
use httpapi::VectorIndexOptions;
use rstest::rstest;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;
use vector_store::DbIndexPartitioning;
use vector_store::IndexKind;
use vector_store::IndexOptionsFts;
use vector_store::IndexOptionsVs;
use vector_store::Timestamp;

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn index_info_reports_vector_index_options() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into()],
        1,
        [("pk".into(), NativeType::Int)],
        ["embedding".into()],
    );

    let index = make_index_with_kind(
        "custom",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
        &[],
        ordered_timeuuid(1),
        IndexKind::Vs(IndexOptionsVs {
            dimensions: NonZeroUsize::new(3).unwrap().into(),
            connectivity: 32.into(),
            expansion_add: 200.into(),
            expansion_search: 100.into(),
            space_type: vector_store::SpaceType::Euclidean,
            quantization: vector_store::Quantization::F16,
        }),
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
        || async { client.index_info(&ks, &idx).await.is_ok() },
        "index to be discovered",
    )
    .await;

    let info = client.index_info(&ks, &idx).await.unwrap();
    assert_eq!(
        info.options,
        IndexOptions::Vector(VectorIndexOptions {
            dimensions: 3,
            maximum_node_connections: 32,
            construction_beam_width: 200,
            search_beam_width: 100,
            similarity_function: SimilarityFunction::Euclidean,
            quantization: DataType::F16,
        })
    );
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn index_info_reports_fulltext_index_options() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into()],
        1,
        [
            ("pk".into(), NativeType::Int),
            ("content".into(), NativeType::Text),
        ],
        [],
    );

    let index = make_fts_index("fulltext", &["pk"], 1, "content", ordered_timeuuid(1));
    db.add_index(
        index.clone(),
        Some(db_basic::scan_fn_documents([(
            [CqlValue::Int(1)].into(),
            Some("hello world".to_string()),
            Timestamp::from_millis(10),
        )])),
        None,
    )
    .unwrap();

    let ks = index.keyspace_name.as_ref().into();
    let idx = index.index_name.as_ref().into();
    wait_for(
        || async { client.index_info(&ks, &idx).await.is_ok() },
        "index to be discovered",
    )
    .await;

    let info = client.index_info(&ks, &idx).await.unwrap();
    assert_eq!(
        info.options,
        IndexOptions::Fulltext(FulltextIndexOptions {
            analyzer: "standard".to_string(),
            positions: true,
        })
    );
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn indexes_lists_all_indexes_with_options() {
    crate::enable_tracing();
    let (client, db, _keep) = setup().await;

    add_table(
        &db,
        ["pk".into()],
        1,
        [
            ("pk".into(), NativeType::Int),
            ("content".into(), NativeType::Text),
        ],
        ["embedding".into()],
    );

    let first_options = IndexOptionsVs {
        dimensions: NonZeroUsize::new(3).unwrap().into(),
        connectivity: 32.into(),
        expansion_add: 200.into(),
        expansion_search: 100.into(),
        space_type: vector_store::SpaceType::Euclidean,
        quantization: vector_store::Quantization::F16,
    };
    let first_index = make_index_with_kind(
        "first",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
        &[],
        ordered_timeuuid(1),
        IndexKind::Vs(first_options.clone()),
    );
    db.add_index(
        first_index.clone(),
        Some(single_row_scan([CqlValue::Int(1)])),
        None,
    )
    .unwrap();

    let second_options = IndexOptionsVs {
        dimensions: NonZeroUsize::new(3).unwrap().into(),
        connectivity: 64.into(),
        expansion_add: 150.into(),
        expansion_search: 50.into(),
        space_type: vector_store::SpaceType::DotProduct,
        quantization: vector_store::Quantization::BF16,
    };
    let second_index = make_index_with_kind(
        "second",
        &["pk"],
        1,
        "embedding",
        DbIndexPartitioning::Global,
        &[],
        ordered_timeuuid(2),
        IndexKind::Vs(second_options.clone()),
    );
    db.add_index(
        second_index.clone(),
        Some(single_row_scan([CqlValue::Int(1)])),
        None,
    )
    .unwrap();

    let fts_options = IndexOptionsFts {};
    let fts_index = make_fts_index("third", &["pk"], 1, "content", ordered_timeuuid(3));
    db.add_index(
        fts_index.clone(),
        Some(db_basic::scan_fn_documents([(
            [CqlValue::Int(1)].into(),
            Some("hello world".to_string()),
            Timestamp::from_millis(10),
        )])),
        None,
    )
    .unwrap();

    wait_for(
        || async { client.indexes().await.len() == 3 },
        "all indexes to be listed",
    )
    .await;

    let entries = client.indexes().await;
    let expected_options = HashMap::from([
        (
            "first",
            IndexOptions::Vector(VectorIndexOptions::from(&first_options)),
        ),
        (
            "second",
            IndexOptions::Vector(VectorIndexOptions::from(&second_options)),
        ),
        (
            "third",
            IndexOptions::Fulltext(FulltextIndexOptions::from(&fts_options)),
        ),
    ]);
    for entry in &entries {
        assert_eq!(
            &entry.options,
            expected_options.get(entry.index.as_ref()).unwrap()
        );
    }
}
