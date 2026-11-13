/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! `install` on real usearch indexes: which indexes get a kernel, that usearch computes every
//! distance with it (bit for bit what the kernel returns for the vectors as the index stores
//! them: f16 for an f16 index, although they are added and searched as f32), and that searches
//! return NumKong's neighbours.

use cosine_kernels::F16Kernel;
use cosine_kernels::F32Kernel;
use cosine_kernels::install;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use usearch::Index;
use usearch::IndexOptions;
use usearch::MetricKind;
use usearch::ScalarKind;
use usearch::f16;

/// As in the unit tests: the kernels against an f64 reference.
const TOLERANCE: f64 = 2e-6;

const VECTORS: u64 = 600;
const QUERIES: u64 = 30;
const NEIGHBOURS: usize = 10;

fn new_index(metric: MetricKind, quantization: ScalarKind, dimensions: usize) -> Index {
    let options = IndexOptions {
        dimensions,
        metric,
        quantization,
        ..Default::default()
    };
    let index = Index::new(&options).expect("the index is created");
    index
        .reserve(VECTORS as usize)
        .expect("the index reserves memory");
    index
}

/// Deterministic vectors in [-1, 1) (SplitMix64).
fn vector(seed: u64, dimensions: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x2545_F491_4F6C_DD1D);
    (0..dimensions)
        .map(|_| {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            ((z >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
        })
        .collect()
}

fn reference(a: &[f32], b: &[f32]) -> f64 {
    let (mut ab, mut a2, mut b2) = (0f64, 0f64, 0f64);
    for (&x, &y) in a.iter().zip(b) {
        let (x, y) = (f64::from(x), f64::from(y));
        ab += x * y;
        a2 += x * x;
        b2 += y * y;
    }
    if a2 == 0.0 && b2 == 0.0 {
        return 0.0;
    }
    if ab == 0.0 {
        return 1.0;
    }
    (1.0 - ab / (a2 * b2).sqrt()).max(0.0)
}

#[test]
fn install_replaces_the_kernel_of_cosine_f32_and_f16_indexes_only() {
    let metrics = [MetricKind::Cos, MetricKind::L2sq, MetricKind::IP];
    let quantizations = [
        ScalarKind::F32,
        ScalarKind::F16,
        ScalarKind::BF16,
        ScalarKind::I8,
    ];
    for metric in metrics {
        for quantization in quantizations {
            let mut index = new_index(metric, quantization, 8);
            let expected = match (metric, quantization) {
                (MetricKind::Cos, ScalarKind::F32) => F32Kernel::best().map(F32Kernel::name),
                (MetricKind::Cos, ScalarKind::F16) => F16Kernel::best().map(F16Kernel::name),
                _ => None,
            };
            assert_eq!(install(&mut index), expected, "{metric:?} {quantization:?}");
        }
    }
}

/// A custom metric replaces usearch's for adds and for exact and approximate searches, of f32 and
/// f16 indexes alike: what `install` relies on. A metric that answers 0.25 and counts its calls
/// shows it, where comparing distances with the reference could not tell a kernel of ours from
/// NumKong's.
#[test]
fn usearch_computes_every_distance_with_a_custom_metric() {
    const DISTANCE: f32 = 0.25;
    for quantization in [ScalarKind::F32, ScalarKind::F16] {
        let mut index = new_index(MetricKind::Cos, quantization, 16);
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        match quantization {
            ScalarKind::F32 => index.change_metric::<f32>(Box::new(move |_, _| {
                counted.fetch_add(1, Ordering::Relaxed);
                DISTANCE
            })),
            _ => index.change_metric::<f16>(Box::new(move |_, _| {
                counted.fetch_add(1, Ordering::Relaxed);
                DISTANCE
            })),
        }
        for key in 0..100 {
            index
                .add(key, &vector(key, 16))
                .expect("the vector is added");
        }
        let after_adds = calls.load(Ordering::Relaxed);
        assert!(
            after_adds > 0,
            "{quantization:?}: adds computed no distance"
        );
        let query = vector(1000, 16);
        let exact = index.exact_search(&query, 5).expect("the search runs");
        let approximate = index.search(&query, 5).expect("the search runs");
        assert!(
            calls.load(Ordering::Relaxed) > after_adds,
            "{quantization:?}"
        );
        for distance in exact.distances.iter().chain(&approximate.distances) {
            assert_eq!(*distance, DISTANCE, "{quantization:?}");
        }
    }
}

/// Adds the same vectors to an index with NumKong's kernel and one with ours, and checks every
/// distance an exact and an approximate search of ours returns against the kernel itself (bit for
/// bit) and the reference over the vectors as the index stores them, and the exact neighbours
/// against NumKong's.
fn check_searches(quantization: ScalarKind, dimensions: usize) {
    let numkong = new_index(MetricKind::Cos, quantization, dimensions);
    let mut ours = new_index(MetricKind::Cos, quantization, dimensions);
    let Some(kernel) = install(&mut ours) else {
        eprintln!("no {quantization:?} kernel on this CPU: NumKong's stays");
        return;
    };
    eprintln!("{quantization:?} {dimensions}: {kernel}");
    let mut stored = Vec::new();
    let mut stored_halves = Vec::new();
    for key in 0..VECTORS {
        let vector = vector(key, dimensions);
        numkong.add(key, &vector).expect("the vector is added");
        ours.add(key, &vector).expect("the vector is added");
        // An f16 index stores the vector rounded to f16; reading it back gives those values.
        let mut kept = vec![0f32; dimensions];
        ours.get(key, &mut kept).expect("the vector is read");
        stored.push(kept);
        if quantization == ScalarKind::F16 {
            let mut halves = f16::from_i16s(&vec![0i16; dimensions]).to_vec();
            ours.get(key, &mut halves).expect("the vector is read");
            stored_halves.push(halves);
        }
    }
    // The kernel's own distance between two stored vectors.
    let direct = |a: usize, b: usize| match quantization {
        ScalarKind::F32 => F32Kernel::best()
            .and_then(|kernel| kernel.distance(&stored[a], &stored[b]))
            .expect("install found this kernel"),
        _ => F16Kernel::best()
            .and_then(|kernel| kernel.distance(&stored_halves[a], &stored_halves[b]))
            .expect("install found this kernel"),
    };

    for query_key in 0..QUERIES {
        // A stored vector, so that the search's conversion of the query to f16 is exact.
        let query = &stored[query_key as usize];
        let exact = ours
            .exact_search(query, NEIGHBOURS)
            .expect("the search runs");
        let approximate = ours.search(query, NEIGHBOURS).expect("the search runs");
        for (key, distance) in exact
            .keys
            .iter()
            .zip(&exact.distances)
            .chain(approximate.keys.iter().zip(&approximate.distances))
        {
            let kernels = direct(query_key as usize, *key as usize);
            assert_eq!(
                distance.to_bits(),
                kernels.to_bits(),
                "{quantization:?} {dimensions}, query {query_key}, key {key}: {distance} where the \
                 kernel gives {kernels}"
            );
            let expected = reference(query, &stored[*key as usize]);
            assert!(
                (f64::from(*distance) - expected).abs() <= TOLERANCE,
                "{quantization:?} {dimensions}, query {query_key}, key {key}: {distance} instead of {expected}"
            );
        }

        let theirs = numkong
            .exact_search(query, NEIGHBOURS)
            .expect("the search runs");
        assert_eq!(exact.keys.len(), theirs.keys.len());
        for (ours, theirs) in exact.distances.iter().zip(&theirs.distances) {
            assert!(
                (f64::from(*ours) - f64::from(*theirs)).abs() <= TOLERANCE,
                "{quantization:?} {dimensions}, query {query_key}: {ours} where NumKong has {theirs}"
            );
        }
        // The neighbours are the same, except where two candidates for the last places are
        // closer to each other than the kernels' difference.
        let farthest = f64::from(*theirs.distances.last().expect("neighbours found"));
        let theirs: HashSet<_> = theirs.keys.iter().collect();
        for (key, distance) in exact.keys.iter().zip(&exact.distances) {
            assert!(
                theirs.contains(key) || (f64::from(*distance) - farthest).abs() <= TOLERANCE,
                "{quantization:?} {dimensions}, query {query_key}: key {key} at {distance} is not \
                 among NumKong's neighbours, the farthest at {farthest}"
            );
        }
    }
}

#[test]
fn f32_searches_return_numkongs_neighbours() {
    for dimensions in [3, 100, 768] {
        check_searches(ScalarKind::F32, dimensions);
    }
}

#[test]
fn f16_searches_return_numkongs_neighbours() {
    for dimensions in [3, 100, 768] {
        check_searches(ScalarKind::F16, dimensions);
    }
}
