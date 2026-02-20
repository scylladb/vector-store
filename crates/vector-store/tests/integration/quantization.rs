/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::usearch::setup_store_with_quantization;
use crate::usearch::test_config;
use crate::wait_for;
use crate::wait_for_value;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use time::OffsetDateTime;
use vector_store::ColumnName;
use vector_store::DataType;
use vector_store::DbIndexType;
use vector_store::Distance;
use vector_store::Quantization;
use vector_store::httproutes;
use vector_store::httproutes::PostIndexAnnFilter;
use vector_store::httproutes::PostIndexAnnRestriction;

#[tokio::test]
// This test verifies that quantization is applied correctly by observing its effect on search results.
// We use specific vectors that, when quantized from F32 to I8, produce a significantly different
// squared Euclidean distance (L2^2). This difference is caused by the loss of precision during
// quantization, which alters the distance between the vectors.
//
// Vectors used in the test:
// - Search vector: [1.0, 0.0, 0.0]
// - Stored vector: [0.9, 0.1, 0.1]
//
// With F32 (full precision), the squared Euclidean distance is small (~0.03).
//
// With I8 quantization:
// - The search vector becomes [127, 0, 0].
// - The stored vector becomes [114, 13, 13] (since 0.9*127~114 and 0.1*127~13).
// The rounding introduces a quantization error that increases the distance between the vectors,
// resulting in a much larger squared Euclidean distance (~507).
async fn quantization_is_effectively_applied() {
    crate::enable_tracing();
    async fn get_distance_by_performing_search(
        quantization: Quantization,
        add_vector: Vec<f32>,
        search_vector: Vec<f32>,
    ) -> httproutes::Distance {
        let values = [(
            vec![CqlValue::Int(1)].into(),
            Some(add_vector.into()),
            OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
        )];
        let (run, index, _db, _node_state) = setup_store_with_quantization(
            test_config(),
            DbIndexType::Global,
            ["pk".into()],
            [(
                "pk".to_string().into(),
                scylla::cluster::metadata::NativeType::Int,
            )],
            values.to_vec(),
            quantization,
            NonZeroUsize::new(3).unwrap().into(),
        )
        .await;
        let (client, _server, _config_tx) = run.await;
        wait_for(
            || async {
                client
                    .index_status(&index.keyspace_name, &index.index_name)
                    .await
                    .is_ok_and(|s| s.count == values.len())
            },
            &format!("Waiting for 1 vector to be indexed ({:?})", quantization),
        )
        .await;

        let (_, distances, _) = client
            .ann(
                &index.keyspace_name,
                &index.index_name,
                search_vector.into(),
                None,
                NonZeroUsize::new(1).unwrap().into(),
            )
            .await;
        distances[0]
    }

    let add_vector: Vec<f32> = vec![0.9, 0.1, 0.1];
    let search_vector: Vec<f32> = vec![1.0, 0.0, 0.0];

    let f32_distance = get_distance_by_performing_search(
        Quantization::F32,
        add_vector.clone(),
        search_vector.clone(),
    )
    .await;

    let i8_distance = get_distance_by_performing_search(
        Quantization::I8,
        add_vector.clone(),
        search_vector.clone(),
    )
    .await;

    // Assert F32 distance is small (high precision)
    assert!(
        f32_distance < Distance::new_euclidean(0.1).unwrap().into(),
        "F32 distance should be small due to high precision. Got: {:?}",
        f32_distance
    );
    // Assert I8 distance is larger (quantization error)
    assert!(
        i8_distance > Distance::new_euclidean(300.0).unwrap().into(),
        "I8 distance should be larger due to quantization. Got: {:?}",
        i8_distance
    );
}

#[tokio::test]
async fn quantization_is_returned_as_index_data_type() {
    crate::enable_tracing();

    for (quantization, expected_data_type) in [
        (Quantization::default(), DataType::F32),
        (Quantization::F32, DataType::F32),
        (Quantization::F16, DataType::F16),
        (Quantization::BF16, DataType::BF16),
        (Quantization::I8, DataType::I8),
        (Quantization::B1, DataType::B1),
    ] {
        let (run, index, _db, _node_state) = setup_store_with_quantization(
            test_config(),
            DbIndexType::Global,
            [],
            [],
            [],
            quantization,
            NonZeroUsize::new(3).unwrap().into(),
        )
        .await;

        let (client, _server, _config) = run.await;

        let index_info = wait_for_value(
            || async {
                let indexes = client.indexes().await;
                indexes.into_iter().find(|idx| {
                    idx.keyspace == index.keyspace_name && idx.index == index.index_name
                })
            },
            "Waiting for index to be added to the store",
        )
        .await;

        assert_eq!(index_info.data_type, expected_data_type);
    }
}

async fn search_with_quantization(quantization: Quantization, filter: Option<PostIndexAnnFilter>) {
    const DIMENSIONS: usize = 1536;
    let vector = vec![0.5; DIMENSIONS];
    let pk_value = 1;
    let pk_column: ColumnName = "pk".into();
    let vectors = vec![(
        vec![CqlValue::Int(pk_value)].into(),
        Some(vector.clone().into()),
        OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
    )];

    let (run, index, _db, _node_state) = setup_store_with_quantization(
        test_config(),
        DbIndexType::Global,
        [pk_column.clone()],
        [(
            pk_column.clone(),
            scylla::cluster::metadata::NativeType::Int,
        )],
        vectors,
        quantization,
        NonZeroUsize::new(DIMENSIONS).unwrap().into(),
    )
    .await;

    let (client, _server, _config_tx) = run.await;

    // expect to find the inserted vector as the nearest neighbor
    // with distance 0.0 as we are searching for the same vector
    wait_for(
        || async {
            client
                .ann(
                    &index.keyspace_name,
                    &index.index_name,
                    vector.clone().into(),
                    filter.clone(),
                    NonZeroUsize::new(1).unwrap().into(),
                )
                .await
                .1[0]
                == Distance::new_euclidean(0.0).unwrap().into()
        },
        &format!(
            "Waiting for ANN search to return 1 distance ({:?})",
            quantization
        ),
    )
    .await;
}

#[tokio::test]
// Sanity check to ensure that searches work with all supported quantization types.
async fn can_search_when_quantization_is_enabled() {
    crate::enable_tracing();

    let quantizations = vec![
        Quantization::F32,
        Quantization::F16,
        Quantization::BF16,
        Quantization::I8,
        Quantization::B1,
    ];

    for quantization in quantizations {
        search_with_quantization(quantization, None).await;
    }
}

#[tokio::test]
// Sanity check to ensure that filtered searches work with all supported quantization types.
async fn can_search_with_filter_when_quantization_is_enabled() {
    crate::enable_tracing();

    let quantizations = vec![
        Quantization::F32,
        Quantization::F16,
        Quantization::BF16,
        Quantization::I8,
        Quantization::B1,
    ];

    let pk_value = 1;
    let pk_column: ColumnName = "pk".into();

    for quantization in quantizations {
        search_with_quantization(
            quantization,
            Some(PostIndexAnnFilter {
                restrictions: vec![PostIndexAnnRestriction::In {
                    lhs: pk_column.clone(),
                    rhs: vec![pk_value.into()],
                }],
                allow_filtering: false,
            }),
        )
        .await;
    }
}

#[tokio::test]
async fn binary_quantization_with_non_divisible_by_8_dimensions() {
    crate::enable_tracing();

    const DIMENSIONS: usize = 100; // Not divisible by 8
    let vector = vec![0.5; DIMENSIONS];
    let pk_value = 1;
    let pk_column: ColumnName = "pk".into();
    let vectors = vec![(
        vec![CqlValue::Int(pk_value)].into(),
        Some(vector.clone().into()),
        OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
    )];

    let (run, index, _db, _node_state) = setup_store_with_quantization(
        test_config(),
        DbIndexType::Global,
        [pk_column.clone()],
        [(
            pk_column.clone(),
            scylla::cluster::metadata::NativeType::Int,
        )],
        vectors,
        Quantization::B1,
        NonZeroUsize::new(DIMENSIONS).unwrap().into(),
    )
    .await;

    let (client, _server, _config_tx) = run.await;

    // expect to find the inserted vector as the nearest neighbor
    // with distance 0.0 as we are searching for the same vector
    wait_for(
        || async {
            client
                .ann(
                    &index.keyspace_name,
                    &index.index_name,
                    vector.clone().into(),
                    None,
                    NonZeroUsize::new(1).unwrap().into(),
                )
                .await
                .1[0]
                == Distance::new_euclidean(0.0).unwrap().into()
        },
        "Waiting for ANN search to return 1 distance",
    )
    .await;
}
