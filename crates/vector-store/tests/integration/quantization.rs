/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::usearch::{setup_store_with_quantization, test_config};
use crate::wait_for;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;
use time::OffsetDateTime;
use vector_store::Distance;
use vector_store::Quantization;

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
    ) -> Distance {
        let values = [(
            vec![CqlValue::Int(1)].into(),
            Some(add_vector.into()),
            OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
        )];
        let (run, index, _db, _node_state) = setup_store_with_quantization(
            test_config(),
            ["pk".into()],
            [(
                "pk".to_string().into(),
                scylla::cluster::metadata::NativeType::Int,
            )],
            values.to_vec(),
            quantization,
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

        let (_, distances) = client
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
        f32_distance < Distance::from(0.1),
        "F32 distance should be small due to high precision. Got: {:?}",
        f32_distance
    );
    // Assert I8 distance is larger (quantization error)
    assert!(
        i8_distance > Distance::from(300.0),
        "I8 distance should be larger due to quantization. Got: {:?}",
        i8_distance
    );
}
