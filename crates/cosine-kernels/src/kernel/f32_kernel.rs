/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! The f32 cosine kernels of this target, and which one this CPU runs.

use crate::kernel::Features;
use crate::kernel::ffi;
use crate::kernel::ffi::Metric;

/// A cosine kernel for f32 vectors. Only the kernels of the target exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum F32Kernel {
    /// aarch64 Advanced SIMD.
    #[cfg(target_arch = "aarch64")]
    Neon,
    /// aarch64 SVE, of any vector length.
    #[cfg(target_arch = "aarch64")]
    Sve,
    /// x86-64 AVX2 and FMA.
    #[cfg(target_arch = "x86_64")]
    Haswell,
    /// x86-64 AVX-512.
    #[cfg(target_arch = "x86_64")]
    Skylake,
}

impl F32Kernel {
    /// Every kernel of the target, in the order of preference of NumKong's dispatch.
    #[cfg(target_arch = "aarch64")]
    const ALL: [Self; 2] = [Self::Sve, Self::Neon];
    #[cfg(target_arch = "x86_64")]
    const ALL: [Self; 2] = [Self::Skylake, Self::Haswell];

    /// The kernel's name, as logged.
    pub(crate) fn name(self) -> &'static str {
        match self {
            #[cfg(target_arch = "aarch64")]
            Self::Neon => "neon",
            #[cfg(target_arch = "aarch64")]
            Self::Sve => "sve",
            #[cfg(target_arch = "x86_64")]
            Self::Haswell => "haswell",
            #[cfg(target_arch = "x86_64")]
            Self::Skylake => "skylake",
        }
    }

    /// The kernel for this CPU, or `None` if NumKong's kernel should stay.
    pub(crate) fn best() -> Option<Self> {
        Self::best_with(Features::detect())
    }

    /// The kernel as a usearch metric over vectors of `dimensions` elements, or `None` if it
    /// cannot run on this CPU. See [`ffi::f32_metric`] for how it may be called.
    pub(crate) fn metric(self, dimensions: usize) -> Option<Metric<f32>> {
        ffi::f32_metric(self, dimensions)
    }

    /// The cosine distance between `a` and `b`, or `None` if the kernel cannot run on this CPU
    /// or the slices differ in length.
    #[cfg(test)]
    pub(crate) fn distance(self, a: &[f32], b: &[f32]) -> Option<f32> {
        ffi::f32_distance(self, a, b)
    }

    /// The kernels this CPU runs, in the order of preference.
    #[cfg(test)]
    pub(crate) fn supported() -> impl Iterator<Item = Self> {
        let features = Features::detect();
        Self::ALL
            .into_iter()
            .filter(move |kernel| kernel.runs_with(features))
    }

    /// Whether a CPU with `features` can run the kernel. `ffi` asks it with the detected
    /// features before it calls the kernel; the tests ask it with made-up ones.
    pub(super) fn runs_with(self, features: Features) -> bool {
        match self {
            #[cfg(target_arch = "aarch64")]
            Self::Neon => features.neon,
            #[cfg(target_arch = "aarch64")]
            Self::Sve => features.sve,
            #[cfg(target_arch = "x86_64")]
            Self::Haswell => features.haswell,
            #[cfg(target_arch = "x86_64")]
            Self::Skylake => features.skylake,
        }
    }

    fn best_with(features: Features) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kernel| kernel.runs_with(features))
    }
}

#[cfg(test)]
mod tests {
    use super::F32Kernel;
    use crate::kernel::Features;
    use crate::kernel::tests::MAX_LENGTH;
    use crate::kernel::tests::Rng;
    use crate::kernel::tests::SPECIAL_LENGTHS;
    use crate::kernel::tests::TOLERANCE;
    use crate::kernel::tests::reference;

    fn supported() -> Vec<F32Kernel> {
        let kernels: Vec<_> = F32Kernel::supported().collect();
        eprintln!("f32 cosine kernels exercised: {kernels:?}");
        kernels
    }

    /// Pairs of vectors of `length` elements: signed, positive (distances near 0, where the
    /// subtraction cancels) and near-duplicates.
    fn pairs(rng: &mut Rng, length: usize) -> Vec<(Vec<f32>, Vec<f32>)> {
        let signed = |rng: &mut Rng| {
            (0..length)
                .map(|_| rng.uniform(-1.0, 1.0))
                .collect::<Vec<_>>()
        };
        let positive = |rng: &mut Rng| {
            (0..length)
                .map(|_| rng.uniform(0.0, 1.0))
                .collect::<Vec<_>>()
        };
        let a = signed(rng);
        let near = a.iter().map(|&x| x + rng.uniform(-1e-3, 1e-3)).collect();
        vec![
            (signed(rng), signed(rng)),
            (positive(rng), positive(rng)),
            (a, near),
        ]
    }

    /// Checks one distance against the reference and returns the difference.
    fn check(kernel: F32Kernel, a: &[f32], b: &[f32]) -> f64 {
        let distance = kernel.distance(a, b).expect("the kernel is supported");
        let expected = reference(a, b);
        let error = (f64::from(distance) - expected).abs();
        assert!(
            error <= TOLERANCE && distance >= 0.0,
            "{kernel:?}, length {}: {distance} instead of {expected}",
            a.len()
        );
        error
    }

    #[test]
    fn kernels_match_the_reference_at_every_length() {
        for kernel in supported() {
            let mut rng = Rng(1086);
            let mut largest = 0f64;
            for length in 1..=MAX_LENGTH {
                for (a, b) in pairs(&mut rng, length) {
                    largest = largest.max(check(kernel, &a, &b));
                }
            }
            eprintln!("{kernel:?}: largest difference from the reference {largest:.2e}");
        }
    }

    /// The special cases of NumKong's angular kernel: two zero vectors (also two empty ones) are
    /// at distance 0, a zero and a non-zero vector (or any zero dot product) at 1, a vector from
    /// itself at about 0 (never below), from its opposite at 2, and a NaN makes the distance NaN.
    #[test]
    fn kernels_keep_the_special_cases_of_numkong() {
        let kernels = supported();
        for &kernel in &kernels {
            assert_eq!(kernel.distance(&[], &[]), Some(0.0), "{kernel:?}");
        }
        for length in SPECIAL_LENGTHS {
            let zeros = vec![0f32; length];
            let mut unit = vec![0f32; length];
            unit[0] = 1.0;
            let mut other_unit = vec![0f32; length];
            other_unit[length - 1] = 1.0;
            let ones = vec![1f32; length];
            let minus_ones = vec![-1f32; length];
            let mut with_nan = ones.clone();
            with_nan[length / 2] = f32::NAN;
            for &kernel in &kernels {
                let distance = |a: &[f32], b: &[f32]| kernel.distance(a, b).unwrap();
                assert_eq!(distance(&zeros, &zeros), 0.0, "{kernel:?} {length}");
                assert_eq!(distance(&zeros, &ones), 1.0, "{kernel:?} {length}");
                assert_eq!(distance(&ones, &zeros), 1.0, "{kernel:?} {length}");
                if length > 1 {
                    assert_eq!(distance(&unit, &other_unit), 1.0, "{kernel:?} {length}");
                }
                let same = distance(&ones, &ones);
                assert!((0.0..=1e-6).contains(&same), "{kernel:?} {length}: {same}");
                let opposite = distance(&ones, &minus_ones);
                assert!(
                    (opposite - 2.0).abs() <= 1e-6,
                    "{kernel:?} {length}: {opposite}"
                );
                assert!(distance(&with_nan, &ones).is_nan(), "{kernel:?} {length}");
            }
        }
    }

    #[test]
    fn distance_rejects_slices_of_different_lengths() {
        for kernel in F32Kernel::ALL {
            assert_eq!(kernel.distance(&[1.0, 2.0], &[1.0]), None);
        }
    }

    /// The kernel picked for each CPU is the one NumKong's dispatch picked with the patched
    /// NumKong: SVE before NEON.
    #[cfg(target_arch = "aarch64")]
    #[test]
    fn dispatch_follows_numkong() {
        let neon = Features {
            neon: true,
            ..Features::default()
        };
        let graviton3 = Features {
            sve: true,
            fp16: true,
            fhm: true,
            ..neon
        };
        let graviton4 = Features {
            sve2: true,
            ..graviton3
        };
        let cases = [
            (Features::default(), None),
            (neon, Some(F32Kernel::Neon)),
            (graviton3, Some(F32Kernel::Sve)),
            (graviton4, Some(F32Kernel::Sve)),
        ];
        for (features, expected) in cases {
            assert_eq!(F32Kernel::best_with(features), expected, "{features:?}");
        }
    }

    /// The kernel picked for each CPU is the one NumKong's dispatch picked with the patched
    /// NumKong: AVX-512 before AVX2.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn dispatch_follows_numkong() {
        let haswell = Features {
            haswell: true,
            ..Features::default()
        };
        let sapphire_rapids = Features {
            skylake: true,
            ..haswell
        };
        let cases = [
            (Features::default(), None),
            (haswell, Some(F32Kernel::Haswell)),
            (sapphire_rapids, Some(F32Kernel::Skylake)),
        ];
        for (features, expected) in cases {
            assert_eq!(F32Kernel::best_with(features), expected, "{features:?}");
        }
    }
}
