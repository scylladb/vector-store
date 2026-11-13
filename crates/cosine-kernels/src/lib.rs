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
//! [`install`] plugs them into an index through usearch's custom-metric API.
//!
//! The kernels (`csrc/cosine.c`) are the ones Vector Store used from a patched NumKong:
//! f32 accumulation for f32 vectors on NEON, SVE, AVX2 (Haswell) and AVX-512 (Skylake), and
//! widening f16 multiply-adds on FEAT_FHM and SVE2. A kernel is picked at run time from the
//! CPU's features, in the order NumKong's dispatch used for those kernels. An index or CPU
//! with no kernel here keeps NumKong's.

use usearch::Distance;
use usearch::Index;
use usearch::MetricKind;
use usearch::ScalarKind;
use usearch::f16;

#[cfg(test)]
mod tests;

/// The C kernels. Each reads `n` elements from `a` and `b` and returns their cosine distance;
/// it may only run on a CPU with the instruction set extensions its name stands for.
mod ffi {
    #[cfg(all(target_arch = "aarch64", not(target_env = "msvc")))]
    unsafe extern "C" {
        pub(crate) fn vs_cosine_f32_neon(a: *const f32, b: *const f32, n: usize) -> f32;
        pub(crate) fn vs_cosine_f32_sve(a: *const f32, b: *const f32, n: usize) -> f32;
        pub(crate) fn vs_cosine_f16_neonfhm(a: *const u16, b: *const u16, n: usize) -> f32;
        pub(crate) fn vs_cosine_f16_sve2(a: *const u16, b: *const u16, n: usize) -> f32;
    }

    #[cfg(all(target_arch = "x86_64", not(target_env = "msvc")))]
    unsafe extern "C" {
        pub(crate) fn vs_cosine_f32_haswell(a: *const f32, b: *const f32, n: usize) -> f32;
        pub(crate) fn vs_cosine_f32_skylake(a: *const f32, b: *const f32, n: usize) -> f32;
    }
}

/// Whether this build has the C kernels of each architecture: csrc/cosine.c needs GCC's or Clang's target
/// pragmas, so an MSVC build has none (build.rs does not compile it) and keeps NumKong's kernels.
const AARCH64_KERNELS: bool = cfg!(all(target_arch = "aarch64", not(target_env = "msvc")));
const X86_64_KERNELS: bool = cfg!(all(target_arch = "x86_64", not(target_env = "msvc")));

/// A C kernel over two vectors of `n` elements of `T` (f32, or f16 as its bits).
type Kernel<T> = unsafe extern "C" fn(a: *const T, b: *const T, n: usize) -> f32;

/// A distance function in the form usearch's `Index::change_metric` takes.
type Metric<T> = Box<dyn Fn(*const T, *const T) -> Distance + Send + Sync>;

/// The CPU features that decide which kernels can run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Features {
    neon: bool,
    sve: bool,
    sve2: bool,
    /// FEAT_FP16: half-precision arithmetic.
    fp16: bool,
    /// FEAT_FHM: the widening f16 multiply-adds FMLAL and FMLAL2.
    fhm: bool,
    /// Everything the Haswell kernel is compiled for: AVX2, F16C, FMA, BMI1 and BMI2.
    haswell: bool,
    /// Everything the Skylake kernel is compiled for: the Haswell set and AVX-512 F, VL, BW, DQ.
    skylake: bool,
}

impl Features {
    #[cfg(target_arch = "aarch64")]
    fn detect() -> Self {
        use std::arch::is_aarch64_feature_detected;
        Self {
            neon: is_aarch64_feature_detected!("neon"),
            sve: is_aarch64_feature_detected!("sve"),
            sve2: is_aarch64_feature_detected!("sve2"),
            fp16: is_aarch64_feature_detected!("fp16"),
            fhm: is_aarch64_feature_detected!("fhm"),
            ..Self::default()
        }
    }

    #[cfg(target_arch = "x86_64")]
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
        Self {
            haswell,
            skylake,
            ..Self::default()
        }
    }

    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    fn detect() -> Self {
        Self::default()
    }
}

/// A cosine kernel for f32 vectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum F32Kernel {
    /// aarch64 Advanced SIMD.
    Neon,
    /// aarch64 SVE, of any vector length.
    Sve,
    /// x86-64 AVX2 and FMA.
    Haswell,
    /// x86-64 AVX-512.
    Skylake,
}

impl F32Kernel {
    /// Every kernel, in the order of preference of NumKong's dispatch.
    pub const ALL: [Self; 4] = [Self::Sve, Self::Neon, Self::Skylake, Self::Haswell];

    /// The kernel's name, as logged.
    pub fn name(self) -> &'static str {
        match self {
            Self::Neon => "neon",
            Self::Sve => "sve",
            Self::Haswell => "haswell",
            Self::Skylake => "skylake",
        }
    }

    /// Whether this kernel is compiled for this target and can run on this CPU.
    pub fn is_supported(self) -> bool {
        self.runs_with(Features::detect())
    }

    /// The kernel for this CPU, or `None` if NumKong's kernel should stay.
    pub fn best() -> Option<Self> {
        Self::best_with(Features::detect())
    }

    /// The cosine distance between `a` and `b`, or `None` if the kernel cannot run on this CPU
    /// or the slices differ in length.
    pub fn distance(self, a: &[f32], b: &[f32]) -> Option<f32> {
        if a.len() != b.len() {
            return None;
        }
        let kernel = self.function()?;
        // SAFETY: the CPU has the kernel's instruction set extensions (checked by `function`),
        // and both slices hold `a.len()` elements.
        Some(unsafe { kernel(a.as_ptr(), b.as_ptr(), a.len()) })
    }

    /// The C kernel, or `None` if it cannot run on this CPU.
    fn function(self) -> Option<Kernel<f32>> {
        if !self.is_supported() {
            return None;
        }
        match self {
            #[cfg(all(target_arch = "aarch64", not(target_env = "msvc")))]
            Self::Neon => Some(ffi::vs_cosine_f32_neon),
            #[cfg(all(target_arch = "aarch64", not(target_env = "msvc")))]
            Self::Sve => Some(ffi::vs_cosine_f32_sve),
            #[cfg(all(target_arch = "x86_64", not(target_env = "msvc")))]
            Self::Haswell => Some(ffi::vs_cosine_f32_haswell),
            #[cfg(all(target_arch = "x86_64", not(target_env = "msvc")))]
            Self::Skylake => Some(ffi::vs_cosine_f32_skylake),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    fn runs_with(self, features: Features) -> bool {
        match self {
            Self::Neon => AARCH64_KERNELS && features.neon,
            Self::Sve => AARCH64_KERNELS && features.sve,
            Self::Haswell => X86_64_KERNELS && features.haswell,
            Self::Skylake => X86_64_KERNELS && features.skylake,
        }
    }

    fn best_with(features: Features) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kernel| kernel.runs_with(features))
    }

    /// The kernel as a usearch metric over vectors of `dimensions` elements, or `None` if it
    /// cannot run on this CPU.
    ///
    /// The metric dereferences the pointers it is called with: it must only be called with two
    /// vectors of `dimensions` f32 elements, as usearch calls the metric of an f32 index.
    fn metric(self, dimensions: usize) -> Option<Metric<f32>> {
        if !self.is_supported() {
            return None;
        }
        // SAFETY (every arm): the CPU has the kernel's instruction set extensions (checked
        // above), and the caller passes two vectors of `dimensions` elements.
        match self {
            #[cfg(all(target_arch = "aarch64", not(target_env = "msvc")))]
            Self::Neon => Some(Box::new(move |a, b| unsafe {
                ffi::vs_cosine_f32_neon(a, b, dimensions)
            })),
            #[cfg(all(target_arch = "aarch64", not(target_env = "msvc")))]
            Self::Sve => Some(Box::new(move |a, b| unsafe {
                ffi::vs_cosine_f32_sve(a, b, dimensions)
            })),
            #[cfg(all(target_arch = "x86_64", not(target_env = "msvc")))]
            Self::Haswell => Some(Box::new(move |a, b| unsafe {
                ffi::vs_cosine_f32_haswell(a, b, dimensions)
            })),
            #[cfg(all(target_arch = "x86_64", not(target_env = "msvc")))]
            Self::Skylake => Some(Box::new(move |a, b| unsafe {
                ffi::vs_cosine_f32_skylake(a, b, dimensions)
            })),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }
}

/// A cosine kernel for f16 vectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum F16Kernel {
    /// aarch64 Advanced SIMD with FEAT_FHM.
    NeonFhm,
    /// aarch64 SVE2, of any vector length.
    Sve2,
}

impl F16Kernel {
    /// Every kernel, in the order of preference of NumKong's dispatch.
    pub const ALL: [Self; 2] = [Self::Sve2, Self::NeonFhm];

    /// The kernel's name, as logged.
    pub fn name(self) -> &'static str {
        match self {
            Self::NeonFhm => "neonfhm",
            Self::Sve2 => "sve2",
        }
    }

    /// Whether this kernel is compiled for this target and can run on this CPU.
    pub fn is_supported(self) -> bool {
        self.runs_with(Features::detect())
    }

    /// The kernel for this CPU, or `None` if NumKong's kernel should stay.
    pub fn best() -> Option<Self> {
        Self::best_with(Features::detect())
    }

    /// The cosine distance between `a` and `b`, or `None` if the kernel cannot run on this CPU
    /// or the slices differ in length.
    pub fn distance(self, a: &[f16], b: &[f16]) -> Option<f32> {
        if a.len() != b.len() {
            return None;
        }
        let kernel = self.function()?;
        // SAFETY: the CPU has the kernel's instruction set extensions (checked by `function`),
        // both slices hold `a.len()` elements, and `f16` is a transparent wrapper of the 16 bits
        // of an IEEE half.
        Some(unsafe { kernel(a.as_ptr().cast(), b.as_ptr().cast(), a.len()) })
    }

    /// The C kernel, or `None` if it cannot run on this CPU.
    fn function(self) -> Option<Kernel<u16>> {
        if !self.is_supported() {
            return None;
        }
        match self {
            #[cfg(all(target_arch = "aarch64", not(target_env = "msvc")))]
            Self::NeonFhm => Some(ffi::vs_cosine_f16_neonfhm),
            #[cfg(all(target_arch = "aarch64", not(target_env = "msvc")))]
            Self::Sve2 => Some(ffi::vs_cosine_f16_sve2),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    fn runs_with(self, features: Features) -> bool {
        match self {
            Self::NeonFhm => AARCH64_KERNELS && features.fp16 && features.fhm,
            Self::Sve2 => AARCH64_KERNELS && features.sve2,
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
        let numkong_svehalf = cfg!(target_arch = "aarch64") && features.sve && features.fp16;
        if numkong_svehalf {
            return None;
        }
        Self::NeonFhm.runs_with(features).then_some(Self::NeonFhm)
    }

    /// The kernel as a usearch metric over vectors of `dimensions` elements, or `None` if it
    /// cannot run on this CPU.
    ///
    /// The metric dereferences the pointers it is called with: it must only be called with two
    /// vectors of `dimensions` f16 elements, as usearch calls the metric of an f16 index.
    #[cfg_attr(
        not(all(target_arch = "aarch64", not(target_env = "msvc"))),
        allow(unused_variables)
    )]
    fn metric(self, dimensions: usize) -> Option<Metric<f16>> {
        if !self.is_supported() {
            return None;
        }
        // SAFETY (every arm): the CPU has the kernel's instruction set extensions (checked
        // above), the caller passes two vectors of `dimensions` elements, and `f16` is a
        // transparent wrapper of the 16 bits of an IEEE half.
        match self {
            #[cfg(all(target_arch = "aarch64", not(target_env = "msvc")))]
            Self::NeonFhm => Some(Box::new(move |a, b| unsafe {
                ffi::vs_cosine_f16_neonfhm(a.cast(), b.cast(), dimensions)
            })),
            #[cfg(all(target_arch = "aarch64", not(target_env = "msvc")))]
            Self::Sve2 => Some(Box::new(move |a, b| unsafe {
                ffi::vs_cosine_f16_sve2(a.cast(), b.cast(), dimensions)
            })),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }
}

/// Replaces the distance function of `index` with Vector Store's cosine kernel for this CPU,
/// and returns the kernel's name. Only a `MetricKind::Cos` index of `ScalarKind::F32` or
/// `ScalarKind::F16` on a CPU with one of the kernels gets one; any other index keeps its
/// NumKong kernel, and the function returns `None`.
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
        ScalarKind::F16 => {
            let kernel = F16Kernel::best()?;
            index.change_metric::<f16>(kernel.metric(dimensions)?);
            Some(kernel.name())
        }
        _ => None,
    }
}
