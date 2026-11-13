/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! The cosine kernels as safe Rust: which kernels this CPU can run and the order they are
//! preferred in ([`F32Kernel`], [`F16Kernel`]), and the distance functions usearch calls. The C
//! kernels themselves are reached only through `ffi`, the one module with `unsafe` code, whose
//! functions check this CPU's features before they call one.

#[cfg(target_arch = "aarch64")]
mod f16_kernel;
mod f32_kernel;
mod ffi;

#[cfg(target_arch = "aarch64")]
pub(crate) use f16_kernel::F16Kernel;
pub(crate) use f32_kernel::F32Kernel;

/// The CPU features that decide which aarch64 kernels can run.
#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Features {
    neon: bool,
    sve: bool,
    sve2: bool,
    /// FEAT_FP16: half-precision arithmetic.
    fp16: bool,
    /// FEAT_FHM: the widening f16 multiply-adds FMLAL and FMLAL2.
    fhm: bool,
}

#[cfg(target_arch = "aarch64")]
impl Features {
    fn detect() -> Self {
        use std::arch::is_aarch64_feature_detected;
        Self {
            neon: is_aarch64_feature_detected!("neon"),
            sve: is_aarch64_feature_detected!("sve"),
            sve2: is_aarch64_feature_detected!("sve2"),
            fp16: is_aarch64_feature_detected!("fp16"),
            fhm: is_aarch64_feature_detected!("fhm"),
        }
    }
}

/// The CPU features that decide which x86-64 kernels can run.
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Features {
    /// Everything the Haswell kernel is compiled for: AVX2, F16C, FMA, BMI1 and BMI2.
    haswell: bool,
    /// Everything the Skylake kernel is compiled for: the Haswell set and AVX-512 F, VL, BW, DQ.
    skylake: bool,
}

#[cfg(target_arch = "x86_64")]
impl Features {
    fn detect() -> Self {
        use std::arch::is_x86_feature_detected;
        let haswell = is_x86_feature_detected!("avx2")
            && is_x86_feature_detected!("f16c")
            && is_x86_feature_detected!("fma")
            && is_x86_feature_detected!("bmi1")
            && is_x86_feature_detected!("bmi2");
        let skylake = haswell
            && is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx512vl")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512dq");
        Self { haswell, skylake }
    }
}

/// Test inputs and the reference the kernels' tests share.
///
/// The kernel tests run every kernel this CPU supports, not only the one `best` picks, and say
/// which on stderr (`cargo test -- --nocapture`), with the largest error they saw. A kernel the
/// CPU lacks is skipped, so set `COSINE_KERNELS_EXPECTED` to the comma-separated names of the
/// kernels a machine must run (e.g. `neon,sve,neonfhm,sve2`): CI does, on x86 and on an arm64
/// runner. Under `qemu-aarch64 -cpu max` every aarch64 kernel runs.
#[cfg(test)]
mod tests {
    #[cfg(target_arch = "aarch64")]
    use super::F16Kernel;
    use super::F32Kernel;
    use std::collections::BTreeSet;

    /// The largest difference allowed between a kernel and the f64 reference. The kernels
    /// accumulate in f32 and refine reciprocal square root estimates; the tests print the largest
    /// difference they saw, which was 1.1e-7 to 4.1e-7 for each kernel (2026-11-13).
    pub(super) const TOLERANCE: f64 = 2e-6;

    /// The longest vectors tested, every length from 1 on: the kernels have separate paths for
    /// the main loop, the tails and the reductions.
    pub(super) const MAX_LENGTH: usize = 1024;

    /// The vector lengths of the special-case tests: below, at and across the kernels' strides.
    pub(super) const SPECIAL_LENGTHS: [usize; 7] = [1, 3, 8, 17, 64, 768, 1000];

    /// SplitMix64: deterministic test inputs without a dependency.
    pub(super) struct Rng(pub(super) u64);

    impl Rng {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        /// Uniform in [low, high).
        pub(super) fn uniform(&mut self, low: f32, high: f32) -> f32 {
            let unit = (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32;
            low + (high - low) * unit
        }

        /// A finite f16 with a biased exponent in `exponents`, as its bits: 9..20 gives
        /// magnitudes in [2^-6, 2^5), 0..5 subnormals and the smallest normals.
        #[cfg(target_arch = "aarch64")]
        pub(super) fn half_bits(&mut self, signed: bool, exponents: std::ops::Range<u16>) -> u16 {
            let bits = self.next_u64();
            let sign = if signed { (bits & 1) as u16 } else { 0 };
            let span = u64::from(exponents.end - exponents.start);
            let exponent = exponents.start + ((bits >> 1) % span) as u16;
            let mantissa = ((bits >> 8) & 0x3ff) as u16;
            (sign << 15) | (exponent << 10) | mantissa
        }
    }

    /// NumKong's angular distance in f64: 0 for two zero vectors, 1 for a zero dot product,
    /// otherwise 1 - ab / sqrt(a2 * b2) clamped at 0.
    pub(super) fn reference(a: &[f32], b: &[f32]) -> f64 {
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

    /// Every kernel named in `COSINE_KERNELS_EXPECTED` runs on this CPU, so that the tests cannot
    /// pass by skipping it. More kernels may run: an x86 machine with AVX-512 runs the Skylake
    /// kernel too.
    #[test]
    fn this_cpu_supports_the_expected_kernels() {
        let Ok(expected) = std::env::var("COSINE_KERNELS_EXPECTED") else {
            return;
        };
        let expected: BTreeSet<&str> = expected.split(',').map(str::trim).collect();
        let supported = F32Kernel::supported().map(F32Kernel::name);
        #[cfg(target_arch = "aarch64")]
        let supported = supported.chain(F16Kernel::supported().map(F16Kernel::name));
        let supported: BTreeSet<&str> = supported.collect();
        let missing: BTreeSet<_> = expected.difference(&supported).collect();
        assert!(
            missing.is_empty(),
            "expected kernels {missing:?} do not run here; these do: {supported:?}"
        );
    }
}
