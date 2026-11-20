/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Vector Store's own cosine distance kernels for usearch indexes.
//!
//! usearch computes distances with NumKong's kernels. For cosine, NumKong accumulates f32
//! vectors in f64 and converts f16 vectors lane by lane before multiplying, which keeps its
//! results within a few ULP of an exact reference. An HNSW search only orders distances and
//! cannot use that precision, and it costs about a third of the search throughput on Graviton4
//! (VECTOR-720) and a tenth on Sapphire Rapids (VECTOR-1054). Upstream keeps the precision as
//! NumKong's numerical contract (ashvardanian/NumKong#393), so the faster kernels live here and
//! [`install`], the crate's only public item, plugs them into an index through usearch's
//! custom-metric API.
//!
//! The kernels (`csrc/cosine.c`) are the ones Vector Store used from a patched NumKong:
//! f32 accumulation for f32 vectors on NEON, SVE, AVX2 (Haswell) and AVX-512 (Skylake), and
//! widening f16 multiply-adds on FEAT_FHM and SVE2. A kernel is picked at run time from the
//! CPU's features, in the order NumKong's dispatch used for those kernels. An index or CPU
//! with no kernel here keeps NumKong's.
//!
//! The crate builds for x86-64 and aarch64 with GCC or Clang only (`build.rs` refuses other
//! targets), the ones Vector Store supports.

mod kernel;

#[cfg(target_arch = "aarch64")]
use kernel::F16Kernel;
use kernel::F32Kernel;
use usearch::Index;
use usearch::MetricKind;
use usearch::ScalarKind;

/// Replaces the distance function of `index` with Vector Store's cosine kernel for this CPU,
/// and returns the kernel's name. Only a `MetricKind::Cos` index of `ScalarKind::F32`, or of
/// `ScalarKind::F16` on aarch64, on a CPU with one of the kernels gets one; any other index
/// keeps its NumKong kernel, and the function returns `None`.
///
/// The kernel applies to everything that computes a distance in the index: adds, approximate
/// and exact searches. usearch does not save a custom metric with the index, so an index that
/// is saved and loaded again needs `install` again.
///
/// Call it once per index, before adding vectors: usearch keeps only the last metric it was
/// given, and a second call leaks the first. Like usearch's `Index::change_metric`, it panics if
/// usearch cannot allocate the buffer the new metric needs.
///
/// This is usearch 2's API. usearch 3 (3.0.0-rc.1 on its main-dev branch, 2026-11) has no custom
/// metric in its Rust binding, and its angular metric precomputes each vector's norm at insertion:
/// an upgrade to it replaces this crate, and its cosine cost needs measuring anew.
pub fn install(index: &mut Index) -> Option<&'static str> {
    if index.metric_kind() != MetricKind::Cos {
        return None;
    }
    let dimensions = index.dimensions();
    match index.scalar_kind() {
        ScalarKind::F32 => {
            let kernel = F32Kernel::best()?;
            index.change_metric::<f32>(kernel.metric(dimensions)?);
            Some(kernel.name())
        }
        #[cfg(target_arch = "aarch64")]
        ScalarKind::F16 => {
            let kernel = F16Kernel::best()?;
            index.change_metric::<usearch::f16>(kernel.metric(dimensions)?);
            Some(kernel.name())
        }
        _ => None,
    }
}

/// `install` on real usearch indexes: which indexes get a kernel, that usearch computes every
/// distance with it (bit for bit what the kernel returns for the vectors as the index stores
/// them: f16 for an f16 index, although they are added and searched as f32), and that searches
/// return NumKong's neighbours.
#[cfg(test)]
mod tests {
    #[cfg(target_arch = "aarch64")]
    use super::F16Kernel;
    use super::F32Kernel;
    use super::install;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use usearch::Index;
    use usearch::IndexOptions;
    use usearch::MetricKind;
    use usearch::ScalarKind;
    use usearch::f16;

    /// As in the kernels' tests: the kernels against an f64 reference.
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
                    #[cfg(target_arch = "aarch64")]
                    (MetricKind::Cos, ScalarKind::F16) => F16Kernel::best().map(F16Kernel::name),
                    _ => None,
                };
                assert_eq!(install(&mut index), expected, "{metric:?} {quantization:?}");
            }
        }
    }

    /// A custom metric replaces usearch's for adds and for exact and approximate searches, of f32
    /// and f16 indexes alike: what `install` relies on. A metric that answers 0.25 and counts its
    /// calls shows it, where comparing distances with the reference could not tell a kernel of
    /// ours from NumKong's.
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

    /// The vectors an index of ours keeps, read back as f32 and, for an f16 index, as f16.
    struct Stored {
        floats: Vec<Vec<f32>>,
        halves: Vec<Vec<f16>>,
    }

    impl Stored {
        /// The distance the index's kernel computes between stored vectors `a` and `b`.
        fn kernels_distance(&self, quantization: ScalarKind, a: usize, b: usize) -> f32 {
            let distance = match quantization {
                ScalarKind::F32 => F32Kernel::best()
                    .and_then(|kernel| kernel.distance(&self.floats[a], &self.floats[b])),
                #[cfg(target_arch = "aarch64")]
                ScalarKind::F16 => F16Kernel::best()
                    .and_then(|kernel| kernel.distance(&self.halves[a], &self.halves[b])),
                _ => None,
            };
            distance.expect("install found this kernel")
        }
    }

    /// Adds the same vectors to an index with NumKong's kernel and one with ours, and checks every
    /// distance an exact and an approximate search of ours returns against the kernel itself (bit
    /// for bit) and the reference over the vectors as the index stores them, and the exact
    /// neighbours against NumKong's.
    fn check_searches(quantization: ScalarKind, dimensions: usize) {
        let numkong = new_index(MetricKind::Cos, quantization, dimensions);
        let mut ours = new_index(MetricKind::Cos, quantization, dimensions);
        let Some(kernel) = install(&mut ours) else {
            eprintln!("no {quantization:?} kernel on this CPU: NumKong's stays");
            return;
        };
        eprintln!("{quantization:?} {dimensions}: {kernel}");
        let mut stored = Stored {
            floats: Vec::new(),
            halves: Vec::new(),
        };
        for key in 0..VECTORS {
            let vector = vector(key, dimensions);
            numkong.add(key, &vector).expect("the vector is added");
            ours.add(key, &vector).expect("the vector is added");
            // An f16 index stores the vector rounded to f16; reading it back gives those values.
            let mut kept = vec![0f32; dimensions];
            ours.get(key, &mut kept).expect("the vector is read");
            stored.floats.push(kept);
            if quantization == ScalarKind::F16 {
                let mut halves = f16::from_i16s(&vec![0i16; dimensions]).to_vec();
                ours.get(key, &mut halves).expect("the vector is read");
                stored.halves.push(halves);
            }
        }

        for query_key in 0..QUERIES {
            // A stored vector, so that the search's conversion of the query to f16 is exact.
            let query = &stored.floats[query_key as usize];
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
                let kernels =
                    stored.kernels_distance(quantization, query_key as usize, *key as usize);
                assert_eq!(
                    distance.to_bits(),
                    kernels.to_bits(),
                    "{quantization:?} {dimensions}, query {query_key}, key {key}: {distance} where \
                     the kernel gives {kernels}"
                );
                let expected = reference(query, &stored.floats[*key as usize]);
                assert!(
                    (f64::from(*distance) - expected).abs() <= TOLERANCE,
                    "{quantization:?} {dimensions}, query {query_key}, key {key}: {distance} \
                     instead of {expected}"
                );
            }

            let theirs = numkong
                .exact_search(query, NEIGHBOURS)
                .expect("the search runs");
            assert_eq!(exact.keys.len(), theirs.keys.len());
            for (ours, theirs) in exact.distances.iter().zip(&theirs.distances) {
                assert!(
                    (f64::from(*ours) - f64::from(*theirs)).abs() <= TOLERANCE,
                    "{quantization:?} {dimensions}, query {query_key}: {ours} where NumKong has \
                     {theirs}"
                );
            }
            // The neighbours are the same, except where two candidates for the last places are
            // closer to each other than the kernels' difference.
            let farthest = f64::from(*theirs.distances.last().expect("neighbours found"));
            let theirs: HashSet<_> = theirs.keys.iter().collect();
            for (key, distance) in exact.keys.iter().zip(&exact.distances) {
                assert!(
                    theirs.contains(key) || (f64::from(*distance) - farthest).abs() <= TOLERANCE,
                    "{quantization:?} {dimensions}, query {query_key}: key {key} at {distance} is \
                     not among NumKong's neighbours, the farthest at {farthest}"
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
}
