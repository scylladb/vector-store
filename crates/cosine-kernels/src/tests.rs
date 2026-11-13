/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! The kernels against an f64 reference, and the dispatch against NumKong's.
//!
//! The kernel tests run every kernel this CPU supports, not only the one `best` picks, and say
//! which on stderr (`cargo test -- --nocapture`), with the largest error they saw. A kernel the
//! CPU lacks is skipped, so set `COSINE_KERNELS_EXPECTED` to the comma-separated names of the
//! kernels a machine must run (e.g. `neon,sve,neonfhm,sve2`): CI does, on x86 and on an arm64
//! runner. Under `qemu-aarch64 -cpu max` every aarch64 kernel runs.

use super::F16Kernel;
use super::F32Kernel;
use super::Features;
use std::collections::BTreeSet;
use usearch::f16;

/// The largest difference allowed between a kernel and the f64 reference. The kernels accumulate
/// in f32 and refine reciprocal square root estimates; the tests print the largest difference
/// they saw, which was 1.1e-7 to 4.1e-7 for each kernel (2026-11-13).
const TOLERANCE: f64 = 2e-6;

/// The longest vectors tested, every length from 1 on: the kernels have separate paths for the
/// main loop, the tails and the reductions.
const MAX_LENGTH: usize = 1024;

/// SplitMix64: deterministic test inputs without a dependency.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [low, high).
    fn uniform(&mut self, low: f32, high: f32) -> f32 {
        let unit = (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32;
        low + (high - low) * unit
    }

    /// A finite f16 with a biased exponent in `exponents`, as its bits: 9..20 gives magnitudes in
    /// [2^-6, 2^5), 0..5 subnormals and the smallest normals.
    fn half_bits(&mut self, signed: bool, exponents: std::ops::Range<u16>) -> u16 {
        let bits = self.next_u64();
        let sign = if signed { (bits & 1) as u16 } else { 0 };
        let span = u64::from(exponents.end - exponents.start);
        let exponent = exponents.start + ((bits >> 1) % span) as u16;
        let mantissa = ((bits >> 8) & 0x3ff) as u16;
        (sign << 15) | (exponent << 10) | mantissa
    }
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

/// NumKong's angular distance in f64: 0 for two zero vectors, 1 for a zero dot product,
/// otherwise 1 - ab / sqrt(a2 * b2) clamped at 0.
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

fn supported_f32() -> Vec<F32Kernel> {
    let kernels: Vec<_> = F32Kernel::ALL
        .into_iter()
        .filter(|kernel| kernel.is_supported())
        .collect();
    eprintln!("f32 cosine kernels exercised: {kernels:?}");
    kernels
}

fn supported_f16() -> Vec<F16Kernel> {
    let kernels: Vec<_> = F16Kernel::ALL
        .into_iter()
        .filter(|kernel| kernel.is_supported())
        .collect();
    eprintln!("f16 cosine kernels exercised: {kernels:?}");
    kernels
}

/// Pairs of f32 vectors of `length` elements: signed, positive (distances near 0, where the
/// subtraction cancels) and near-duplicates.
fn f32_pairs(rng: &mut Rng, length: usize) -> Vec<(Vec<f32>, Vec<f32>)> {
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
fn check_f32(kernel: F32Kernel, a: &[f32], b: &[f32]) -> f64 {
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

/// Checks one distance against the reference and returns the difference.
fn check_f16(kernel: F16Kernel, a: &[u16], b: &[u16]) -> f64 {
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
fn f32_kernels_match_the_reference_at_every_length() {
    for kernel in supported_f32() {
        let mut rng = Rng(1086);
        let mut largest = 0f64;
        for length in 1..=MAX_LENGTH {
            for (a, b) in f32_pairs(&mut rng, length) {
                largest = largest.max(check_f32(kernel, &a, &b));
            }
        }
        eprintln!("{kernel:?}: largest difference from the reference {largest:.2e}");
    }
}

#[test]
fn f16_kernels_match_the_reference_at_every_length() {
    for kernel in supported_f16() {
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
                largest = largest.max(check_f16(kernel, &a, &b));
            }
        }
        eprintln!("{kernel:?}: largest difference from the reference {largest:.2e}");
    }
}

/// The special cases of NumKong's angular kernel: two zero vectors (also two empty ones) are at
/// distance 0, a zero and a non-zero vector (or any zero dot product) at 1, a vector from itself
/// at about 0 (never below), from its opposite at 2, and a NaN makes the distance NaN.
#[test]
fn kernels_keep_the_special_cases_of_numkong() {
    let f32_kernels = supported_f32();
    let f16_kernels = supported_f16();
    for &kernel in &f32_kernels {
        assert_eq!(kernel.distance(&[], &[]), Some(0.0), "{kernel:?}");
    }
    for &kernel in &f16_kernels {
        assert_eq!(kernel.distance(&[], &[]), Some(0.0), "{kernel:?}");
    }
    let one_f16 = 0x3c00;
    let minus_one_f16 = 0xbc00;
    let nan_f16 = 0x7e00;
    for length in [1, 3, 8, 17, 64, 768, 1000] {
        let zeros = vec![0f32; length];
        let mut unit = vec![0f32; length];
        unit[0] = 1.0;
        let mut other_unit = vec![0f32; length];
        other_unit[length - 1] = 1.0;
        let ones = vec![1f32; length];
        let minus_ones = vec![-1f32; length];
        let mut with_nan = ones.clone();
        with_nan[length / 2] = f32::NAN;
        for &kernel in &f32_kernels {
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

        let zeros = vec![0u16; length];
        let ones = vec![one_f16; length];
        let minus_ones = vec![minus_one_f16; length];
        let mut with_nan = ones.clone();
        with_nan[length / 2] = nan_f16;
        for &kernel in &f16_kernels {
            let distance = |a: &[u16], b: &[u16]| kernel.distance(&halves(a), &halves(b)).unwrap();
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
    for kernel in F32Kernel::ALL {
        assert_eq!(kernel.distance(&[1.0, 2.0], &[1.0]), None);
    }
    let two = halves(&[0x3c00, 0x3c00]);
    for kernel in F16Kernel::ALL {
        assert_eq!(kernel.distance(&two, &two[..1]), None);
    }
}

/// Every kernel named in `COSINE_KERNELS_EXPECTED` runs on this CPU, so that the tests cannot pass
/// by skipping it. More kernels may run: an x86 machine with AVX-512 runs the Skylake kernel too.
#[test]
fn this_cpu_supports_the_expected_kernels() {
    let Ok(expected) = std::env::var("COSINE_KERNELS_EXPECTED") else {
        return;
    };
    let expected: BTreeSet<&str> = expected.split(',').map(str::trim).collect();
    let supported: BTreeSet<&str> = F32Kernel::ALL
        .into_iter()
        .filter(|kernel| kernel.is_supported())
        .map(F32Kernel::name)
        .chain(
            F16Kernel::ALL
                .into_iter()
                .filter(|kernel| kernel.is_supported())
                .map(F16Kernel::name),
        )
        .collect();
    let missing: BTreeSet<_> = expected.difference(&supported).collect();
    assert!(
        missing.is_empty(),
        "expected kernels {missing:?} do not run here; these do: {supported:?}"
    );
}

/// The kernel picked for each CPU is the one NumKong's dispatch picked with the patched NumKong:
/// SVE before NEON and AVX-512 before AVX2 for f32; for f16 SVE2, then NumKong's own SVE kernel
/// (so no kernel of ours), then FEAT_FHM.
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
    let haswell = Features {
        haswell: true,
        ..Features::default()
    };
    let sapphire_rapids = Features {
        skylake: true,
        ..haswell
    };
    let cases = [
        (Features::default(), None, None),
        (neon, Some(F32Kernel::Neon), None),
        (graviton2, Some(F32Kernel::Neon), None),
        (neon_fhm, Some(F32Kernel::Neon), Some(F16Kernel::NeonFhm)),
        (graviton3, Some(F32Kernel::Sve), None),
        (graviton4, Some(F32Kernel::Sve), Some(F16Kernel::Sve2)),
        (haswell, Some(F32Kernel::Haswell), None),
        (sapphire_rapids, Some(F32Kernel::Skylake), None),
    ];
    for (features, f32_kernel, f16_kernel) in cases {
        // A kernel of the other architecture is not compiled in, whatever the features say.
        let arch = if features.haswell {
            "x86_64"
        } else {
            "aarch64"
        };
        assert_eq!(
            F32Kernel::best_with(features),
            on_this_target(arch, f32_kernel),
            "{features:?}"
        );
        assert_eq!(
            F16Kernel::best_with(features),
            on_this_target(arch, f16_kernel),
            "{features:?}"
        );
    }
}

fn on_this_target<T>(arch: &str, kernel: Option<T>) -> Option<T> {
    kernel.filter(|_| std::env::consts::ARCH == arch && !cfg!(target_env = "msvc"))
}
