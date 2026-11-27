/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! The f16 cosine kernels, and which one this CPU runs. They exist on aarch64 only: on x86-64 an
//! f16 index keeps NumKong's kernel.

use crate::kernel::Features;
use crate::kernel::ffi;
use crate::kernel::ffi::Metric;
use usearch::f16;

/// A cosine kernel for f16 vectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum F16Kernel {
    /// aarch64 Advanced SIMD with FEAT_FHM.
    NeonFhm,
    /// aarch64 SVE2, of any vector length.
    Sve2,
}

impl F16Kernel {
    /// Every kernel, in the order of preference of NumKong's dispatch (which `best_with` follows
    /// with a rule of its own for NumKong's SVE kernel).
    #[cfg(test)]
    const ALL: [Self; 2] = [Self::Sve2, Self::NeonFhm];

    /// The kernel's name, as logged.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::NeonFhm => "neonfhm",
            Self::Sve2 => "sve2",
        }
    }

    /// The kernel for this CPU, or `None` if NumKong's kernel should stay.
    pub(crate) fn best() -> Option<Self> {
        Self::best_with(Features::detect())
    }

    /// The kernel as a usearch metric over vectors of `dimensions` elements, or `None` if it
    /// cannot run on this CPU. See [`ffi::f16_metric`] for how it may be called.
    pub(crate) fn metric(self, dimensions: usize) -> Option<Metric<f16>> {
        ffi::f16_metric(self, dimensions)
    }

    /// The cosine distance between `a` and `b`, or `None` if the kernel cannot run on this CPU
    /// or the slices differ in length.
    #[cfg(test)]
    pub(crate) fn distance(self, a: &[f16], b: &[f16]) -> Option<f32> {
        ffi::f16_distance(self, a, b)
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
            Self::NeonFhm => features.fp16 && features.fhm,
            Self::Sve2 => features.sve2,
        }
    }

    /// NumKong prefers its own SVE f16 kernel (`svehalf`) to the FEAT_FHM one, so a CPU with
    /// SVE and FP16 but not SVE2, like Graviton3, keeps NumKong's kernel, as it did with the
    /// patched NumKong. That assumes NumKong was compiled with its SVE kernels, as the release
    /// toolchain (clang) and gcc >= 13 do; without them such a CPU gets NumKong's NEON f16 kernel.
    fn best_with(features: Features) -> Option<Self> {
        if Self::Sve2.runs_with(features) {
            return Some(Self::Sve2);
        }
        let numkong_svehalf = features.sve && features.fp16;
        if numkong_svehalf {
            return None;
        }
        Self::NeonFhm.runs_with(features).then_some(Self::NeonFhm)
    }
}

#[cfg(test)]
mod tests {
    use super::F16Kernel;
    use crate::kernel::Features;
    use crate::kernel::tests::MAX_LENGTH;
    use crate::kernel::tests::Rng;
    use crate::kernel::tests::SPECIAL_LENGTHS;
    use crate::kernel::tests::TOLERANCE;
    use crate::kernel::tests::reference;
    use usearch::f16;

    fn supported() -> Vec<F16Kernel> {
        let kernels: Vec<_> = F16Kernel::supported().collect();
        eprintln!("f16 cosine kernels exercised: {kernels:?}");
        kernels
    }

    /// The exact value of the f16 with these bits.
    fn half_to_f32(bits: u16) -> f32 {
        let sign = if bits & 0x8000 != 0 { -1.0 } else { 1.0 };
        let exponent = i32::from((bits >> 10) & 0x1f);
        let mantissa = f32::from(bits & 0x3ff);
        match exponent {
            0 => sign * mantissa * 2f32.powi(-24),
            31 if mantissa == 0.0 => sign * f32::INFINITY,
            31 => f32::NAN,
            _ => sign * (1.0 + mantissa / 1024.0) * 2f32.powi(exponent - 15),
        }
    }

    fn halves(bits: &[u16]) -> Vec<f16> {
        let signed: Vec<i16> = bits.iter().map(|&b| b as i16).collect();
        f16::from_i16s(&signed).to_vec()
    }

    /// Checks one distance against the reference and returns the difference.
    fn check(kernel: F16Kernel, a: &[u16], b: &[u16]) -> f64 {
        let distance = kernel
            .distance(&halves(a), &halves(b))
            .expect("the kernel is supported");
        let a: Vec<f32> = a.iter().map(|&bits| half_to_f32(bits)).collect();
        let b: Vec<f32> = b.iter().map(|&bits| half_to_f32(bits)).collect();
        let expected = reference(&a, &b);
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
                for (signed, exponents) in [(true, 9..20), (false, 9..20), (true, 0..5)] {
                    let a: Vec<u16> = (0..length)
                        .map(|_| rng.half_bits(signed, exponents.clone()))
                        .collect();
                    let b: Vec<u16> = (0..length)
                        .map(|_| rng.half_bits(signed, exponents.clone()))
                        .collect();
                    largest = largest.max(check(kernel, &a, &b));
                }
            }
            eprintln!("{kernel:?}: largest difference from the reference {largest:.2e}");
        }
    }

    /// The special cases of NumKong's angular kernel: two zero vectors (also two empty ones) are
    /// at distance 0, a zero and a non-zero vector at 1, a vector from itself at about 0 (never
    /// below), from its opposite at 2, and a NaN makes the distance NaN.
    #[test]
    fn kernels_keep_the_special_cases_of_numkong() {
        let kernels = supported();
        for &kernel in &kernels {
            assert_eq!(kernel.distance(&[], &[]), Some(0.0), "{kernel:?}");
        }
        let (one, minus_one, nan) = (0x3c00, 0xbc00, 0x7e00);
        for length in SPECIAL_LENGTHS {
            let zeros = vec![0u16; length];
            let ones = vec![one; length];
            let minus_ones = vec![minus_one; length];
            let mut with_nan = ones.clone();
            with_nan[length / 2] = nan;
            for &kernel in &kernels {
                let distance =
                    |a: &[u16], b: &[u16]| kernel.distance(&halves(a), &halves(b)).unwrap();
                assert_eq!(distance(&zeros, &zeros), 0.0, "{kernel:?} {length}");
                assert_eq!(distance(&zeros, &ones), 1.0, "{kernel:?} {length}");
                assert_eq!(distance(&ones, &zeros), 1.0, "{kernel:?} {length}");
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
        let two = halves(&[0x3c00, 0x3c00]);
        for kernel in F16Kernel::ALL {
            assert_eq!(kernel.distance(&two, &two[..1]), None);
        }
    }

    /// The kernel picked for each CPU is the one NumKong's dispatch picked with the patched
    /// NumKong: SVE2, then NumKong's own SVE kernel (so no kernel of ours), then FEAT_FHM.
    #[test]
    fn dispatch_follows_numkong() {
        let neon = Features {
            neon: true,
            ..Features::default()
        };
        let graviton2 = Features { fp16: true, ..neon };
        let neon_fhm = Features {
            fhm: true,
            ..graviton2
        };
        let graviton3 = Features {
            sve: true,
            ..neon_fhm
        };
        let graviton4 = Features {
            sve2: true,
            ..graviton3
        };
        let cases = [
            (Features::default(), None),
            (neon, None),
            (graviton2, None),
            (neon_fhm, Some(F16Kernel::NeonFhm)),
            (graviton3, None),
            (graviton4, Some(F16Kernel::Sve2)),
        ];
        for (features, expected) in cases {
            assert_eq!(F16Kernel::best_with(features), expected, "{features:?}");
        }
    }
}
