/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
use crate::common;
use crate::common::add_table;
use crate::common::make_fts_index;
use crate::common::make_fts_index_with_options;
use crate::common::make_index_with_kind;
use crate::common::make_vs_index;
use crate::common::ordered_timeuuid;
use crate::common::setup;
use crate::db_basic;
use crate::db_basic::Index;
use crate::db_basic::TOKEN_MIN;
use crate::db_basic::Table;
use crate::wait_for;
use crate::wait_for_value;
use httpapi::DataType;
use httpapi::FulltextIndexOptions;
use httpapi::IndexOptions;
use httpapi::IndexStatus;
use httpapi::SimilarityFunction;
use httpapi::VectorIndexOptions;
use httpclient::HttpClient;
use reqwest::StatusCode;
use rstest::rstest;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::routing::Token;
use scylla::value::CqlValue;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use vector_store::Analyzer;
use vector_store::Connectivity;
use vector_store::DbIndexPartitioning;
use vector_store::Dimensions;
use vector_store::ExpansionAdd;
use vector_store::ExpansionSearch;
use vector_store::IndexKind;
use vector_store::IndexOptionsFts;
use vector_store::IndexOptionsPattern;
use vector_store::IndexOptionsVs;
use vector_store::NonemptyIteratorExt;
use vector_store::Quantization;
use vector_store::SpaceType;

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
    db.add_index(Index::new(index.clone()).with_range_scan(
        Token::new(db_basic::TOKEN_MIN),
        common::single_row_range_scan([CqlValue::Int(1)]),
    ))
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
#[case::defaults(IndexOptionsFts::default(), "standard", true)]
#[case::non_defaults(
    IndexOptionsFts {
        analyzer: Analyzer::German,
        positions: false.into(),
    },
    "german",
    false
)]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn index_info_reports_fulltext_index_options(
    #[case] options: IndexOptionsFts,
    #[case] analyzer: &str,
    #[case] positions: bool,
) {
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

    let index = make_fts_index_with_options(
        "fulltext",
        &["pk"],
        1,
        "content",
        ordered_timeuuid(1),
        options,
    );
    db.add_index(Index::new(index.clone()).with_range_scan(
        Token::new(TOKEN_MIN),
        db_basic::range_scan_fn_documents([(
            [CqlValue::Int(1)],
            Some("hello world".to_string()),
            10,
        )]),
    ))
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
            analyzer: analyzer.to_string(),
            positions,
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
    db.add_index(Index::new(first_index.clone()).with_range_scan(
        Token::new(TOKEN_MIN),
        common::single_row_range_scan([CqlValue::Int(1)]),
    ))
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
    db.add_index(Index::new(second_index.clone()).with_range_scan(
        Token::new(TOKEN_MIN),
        common::single_row_range_scan([CqlValue::Int(1)]),
    ))
    .unwrap();

    let fts_options = IndexOptionsFts::default();
    let fts_index = make_fts_index("third", &["pk"], 1, "content", ordered_timeuuid(3));
    db.add_index(Index::new(fts_index.clone()).with_range_scan(
        Token::new(TOKEN_MIN),
        db_basic::range_scan_fn_documents([(
            [CqlValue::Int(1)],
            Some("hello world".to_string()),
            10,
        )]),
    ))
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

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn indexes_listing_reports_status_and_progress_when_serving() {
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
        Token::new(db_basic::TOKEN_MIN),
        common::single_row_range_scan([CqlValue::Int(1)]),
    ))
    .unwrap();

    let entries = wait_for_value(
        async || {
            let entries = client.indexes().await;
            entries
                .first()
                .is_some_and(|entry| entry.status == IndexStatus::Serving && entry.count == 1)
                .then_some(entries)
        },
        "the listed index to be serving with a single item indexed",
    )
    .await;

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].index.as_ref(), index.index_name.as_ref());
    assert_eq!(entries[0].build_progress, 100.0);
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn indexes_listing_reports_progress_of_bootstrapping_index() {
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
        Token::new(db_basic::TOKEN_MIN / 2),
        Token::new(0),
        Token::new(db_basic::TOKEN_MAX / 2),
    ]);
    db.add_index(Index::new(index.clone()).with_range_scan(
        Token::new(db_basic::TOKEN_MIN),
        db_basic::pending_range_scan_fn(),
    ))
    .unwrap();

    // We have four token ranges, single range is blocking, so the progress should be 75%
    wait_for(
        || async {
            client.indexes().await.first().is_some_and(|entry| {
                entry.status == IndexStatus::Bootstrapping && entry.build_progress == 75.0
            })
        },
        "the listed index to be bootstrapping with build_progress == 75%",
    )
    .await;
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn indexes_listing_reports_status_of_a_fulltext_index() {
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
    db.add_index(Index::new(index.clone()).with_range_scan(
        Token::new(db_basic::TOKEN_MIN),
        db_basic::range_scan_fn_documents([(
            [CqlValue::Int(1)],
            Some("hello world".to_string()),
            10,
        )]),
    ))
    .unwrap();

    let entries = wait_for_value(
        async || {
            let entries = client.indexes().await;
            entries
                .first()
                .is_some_and(|entry| entry.status == IndexStatus::Serving && entry.count == 1)
                .then_some(entries)
        },
        "the listed full-text index to be serving with a single document indexed",
    )
    .await;

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].index.as_ref(), index.index_name.as_ref());
    assert!(matches!(entries[0].options, IndexOptions::Fulltext(_)));
    assert_eq!(entries[0].build_progress, 100.0);
}

const UNSUPPORTED_RESPONSE: &str = "index keyspace.idx cannot be served: \
    unsupported primary key column type: column pk has type frozen<tuple<int>>, \
    which cannot be represented in an index primary key";

async fn setup_unsupported_index(
    kind: IndexKind,
    target_column: &'static str,
) -> (HttpClient, impl Sized) {
    let (client, db, keep) = setup().await;
    db.add_table(
        "keyspace".into(),
        "items".into(),
        Table {
            primary_keys: ["pk".into()].into_iter().collect_nonempty_arc().unwrap(),
            partition_key_count: 1,
            columns: Arc::new(
                [
                    (
                        "pk".into(),
                        ColumnType::Tuple(vec![ColumnType::Native(NativeType::Int)]),
                    ),
                    ("content".into(), ColumnType::Native(NativeType::Text)),
                ]
                .into_iter()
                .collect(),
            ),
            dimensions: match &kind {
                IndexKind::Vs(IndexOptionsVs { dimensions, .. }) => {
                    [(target_column.into(), *dimensions)].into_iter().collect()
                }
                _ => Default::default(),
            },
        },
    )
    .unwrap();
    db.add_index(Index::new(make_index_with_kind(
        "idx",
        &["pk"],
        1,
        target_column,
        DbIndexPartitioning::Global,
        &[],
        ordered_timeuuid(1),
        kind,
    )))
    .unwrap();

    wait_for(
        || async {
            client
                .index_info(&"keyspace".into(), &"idx".into())
                .await
                .is_ok_and(|info| info.status == IndexStatus::Unsupported)
        },
        "the unsupported index to be discovered",
    )
    .await;
    (client, (db, keep))
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn unsupported_index_is_listed_as_unsupported() {
    crate::enable_tracing();
    let (client, _keep) = setup_unsupported_index(
        IndexKind::Vs(IndexOptionsVs {
            dimensions: Dimensions::from(NonZeroUsize::new(3).unwrap()),
            connectivity: Connectivity::default(),
            expansion_add: ExpansionAdd::default(),
            expansion_search: ExpansionSearch::default(),
            space_type: SpaceType::default(),
            quantization: Quantization::default(),
        }),
        "embedding",
    )
    .await;

    let info = client
        .index_info(&"keyspace".into(), &"idx".into())
        .await
        .unwrap();
    assert_eq!(info.status, IndexStatus::Unsupported);
    assert_eq!(info.count, 0);
    assert_eq!(info.build_progress, 0.0);
    assert!(matches!(
        info.options,
        IndexOptions::Vector(VectorIndexOptions { dimensions: 3, .. })
    ));
    assert_eq!(client.indexes().await, vec![info]);
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn unsupported_vector_index_reports_why_it_cannot_be_served() {
    crate::enable_tracing();
    let (client, _keep) = setup_unsupported_index(
        IndexKind::Vs(IndexOptionsVs {
            dimensions: Dimensions::from(NonZeroUsize::new(3).unwrap()),
            connectivity: Connectivity::default(),
            expansion_add: ExpansionAdd::default(),
            expansion_search: ExpansionSearch::default(),
            space_type: SpaceType::default(),
            quantization: Quantization::default(),
        }),
        "embedding",
    )
    .await;
    let (ks, idx) = ("keyspace".into(), "idx".into());

    let limit = NonZeroUsize::new(1).unwrap().into();
    let response = client
        .post_ann(&ks, &idx, vec![1.0, 2.0, 3.0].into(), None, limit)
        .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.text().await.unwrap(), UNSUPPORTED_RESPONSE);
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn unsupported_fulltext_index_reports_why_it_cannot_be_served_from_bm25() {
    crate::enable_tracing();
    let (client, _keep) =
        setup_unsupported_index(IndexKind::Fts(IndexOptionsFts::default()), "content").await;

    let limit = NonZeroUsize::new(1).unwrap().into();
    let response = client
        .post_bm25(&"keyspace".into(), &"idx".into(), "hello".into(), limit)
        .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.text().await.unwrap(), UNSUPPORTED_RESPONSE);
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn unsupported_fulltext_index_reports_why_it_cannot_be_served_from_highlight() {
    crate::enable_tracing();
    let (client, _keep) =
        setup_unsupported_index(IndexKind::Fts(IndexOptionsFts::default()), "content").await;

    let response = client
        .post_highlight(&"keyspace".into(), &"idx".into(), "hello".into(), vec![])
        .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.text().await.unwrap(), UNSUPPORTED_RESPONSE);
}

#[rstest]
#[timeout(Duration::from_secs(10))]
#[tokio::test]
async fn unsupported_pattern_index_reports_why_it_cannot_be_served() {
    crate::enable_tracing();
    let (client, _keep) = setup_unsupported_index(
        IndexKind::Pattern(IndexOptionsPattern::default()),
        "content",
    )
    .await;

    let limit = NonZeroUsize::new(1).unwrap().into();
    let response = client
        .post_like(&"keyspace".into(), &"idx".into(), "%hello%".into(), limit)
        .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(response.text().await.unwrap(), UNSUPPORTED_RESPONSE);
}
