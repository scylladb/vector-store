/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! The C kernels of `csrc/cosine.c`, and the safe functions that call them: the crate's only
//! `unsafe` code.
//!
//! A kernel may only run on a CPU with the instruction set extensions its name stands for. Each
//! function here detects this CPU's features itself, never taking them from its caller, and
//! returns `None` when the kernel cannot run. The metric closures call their kernel directly,
//! not through a function pointer.

#[cfg(target_arch = "aarch64")]
use super::F16Kernel;
use super::F32Kernel;
use super::Features;
use usearch::Distance;
#[cfg(target_arch = "aarch64")]
use usearch::f16;

/// A distance function in the form usearch's `Index::change_metric` takes.
pub(crate) type Metric<T> = Box<dyn Fn(*const T, *const T) -> Distance + Send + Sync>;

// Each kernel reads `n` elements from `a` and `b` and returns their cosine distance. An f16
// kernel reads the IEEE half bits of its elements.
#[cfg(target_arch = "aarch64")]
unsafe extern "C" {
    fn vs_cosine_f32_neon(a: *const f32, b: *const f32, n: usize) -> f32;
    fn vs_cosine_f32_sve(a: *const f32, b: *const f32, n: usize) -> f32;
    fn vs_cosine_f16_neonfhm(a: *const u16, b: *const u16, n: usize) -> f32;
    fn vs_cosine_f16_sve2(a: *const u16, b: *const u16, n: usize) -> f32;
}

#[cfg(target_arch = "x86_64")]
unsafe extern "C" {
    fn vs_cosine_f32_haswell(a: *const f32, b: *const f32, n: usize) -> f32;
    fn vs_cosine_f32_skylake(a: *const f32, b: *const f32, n: usize) -> f32;
}

/// `kernel` as a usearch metric over f32 vectors of `dimensions` elements, or `None` if it
/// cannot run on this CPU.
///
/// The metric reads `dimensions` f32 elements behind each pointer it is given: usearch calls the
/// metric of an f32 index of `dimensions` dimensions that way, and `install` gives it to no other
/// index.
pub(super) fn f32_metric(kernel: F32Kernel, dimensions: usize) -> Option<Metric<f32>> {
    if !kernel.runs_with(Features::detect()) {
        return None;
    }
    // SAFETY (every arm): the CPU has the kernel's extensions (checked above), and usearch passes
    // two vectors of `dimensions` elements.
    let metric: Metric<f32> = match kernel {
        #[cfg(target_arch = "aarch64")]
        F32Kernel::Neon => Box::new(move |a, b| unsafe { vs_cosine_f32_neon(a, b, dimensions) }),
        #[cfg(target_arch = "aarch64")]
        F32Kernel::Sve => Box::new(move |a, b| unsafe { vs_cosine_f32_sve(a, b, dimensions) }),
        #[cfg(target_arch = "x86_64")]
        F32Kernel::Haswell => {
            Box::new(move |a, b| unsafe { vs_cosine_f32_haswell(a, b, dimensions) })
        }
        #[cfg(target_arch = "x86_64")]
        F32Kernel::Skylake => {
            Box::new(move |a, b| unsafe { vs_cosine_f32_skylake(a, b, dimensions) })
        }
    };
    Some(metric)
}

/// `kernel` as a usearch metric over f16 vectors of `dimensions` elements, or `None` if it
/// cannot run on this CPU. As [`f32_metric`], for an f16 index.
#[cfg(target_arch = "aarch64")]
pub(super) fn f16_metric(kernel: F16Kernel, dimensions: usize) -> Option<Metric<f16>> {
    if !kernel.runs_with(Features::detect()) {
        return None;
    }
    // SAFETY (every arm): the CPU has the kernel's extensions (checked above), usearch passes two
    // vectors of `dimensions` elements, and `f16` is a transparent wrapper of the 16 bits of an
    // IEEE half.
    let metric: Metric<f16> = match kernel {
        F16Kernel::NeonFhm => {
            Box::new(move |a, b| unsafe { vs_cosine_f16_neonfhm(a.cast(), b.cast(), dimensions) })
        }
        F16Kernel::Sve2 => {
            Box::new(move |a, b| unsafe { vs_cosine_f16_sve2(a.cast(), b.cast(), dimensions) })
        }
    };
    Some(metric)
}

/// The cosine distance between `a` and `b` by `kernel`, or `None` if it cannot run on this CPU
/// or the slices differ in length.
#[cfg(test)]
pub(super) fn f32_distance(kernel: F32Kernel, a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() || !kernel.runs_with(Features::detect()) {
        return None;
    }
    let (n, a, b) = (a.len(), a.as_ptr(), b.as_ptr());
    // SAFETY (every arm): the CPU has the kernel's extensions (checked above), and both slices
    // hold `n` elements.
    let distance = unsafe {
        match kernel {
            #[cfg(target_arch = "aarch64")]
            F32Kernel::Neon => vs_cosine_f32_neon(a, b, n),
            #[cfg(target_arch = "aarch64")]
            F32Kernel::Sve => vs_cosine_f32_sve(a, b, n),
            #[cfg(target_arch = "x86_64")]
            F32Kernel::Haswell => vs_cosine_f32_haswell(a, b, n),
            #[cfg(target_arch = "x86_64")]
            F32Kernel::Skylake => vs_cosine_f32_skylake(a, b, n),
        }
    };
    Some(distance)
}

/// The cosine distance between `a` and `b` by `kernel`, or `None` if it cannot run on this CPU
/// or the slices differ in length.
#[cfg(all(test, target_arch = "aarch64"))]
pub(super) fn f16_distance(kernel: F16Kernel, a: &[f16], b: &[f16]) -> Option<f32> {
    if a.len() != b.len() || !kernel.runs_with(Features::detect()) {
        return None;
    }
    let (n, a, b) = (a.len(), a.as_ptr().cast(), b.as_ptr().cast());
    // SAFETY (every arm): the CPU has the kernel's extensions (checked above), both slices hold
    // `n` elements, and `f16` is a transparent wrapper of the 16 bits of an IEEE half.
    let distance = unsafe {
        match kernel {
            F16Kernel::NeonFhm => vs_cosine_f16_neonfhm(a, b, n),
            F16Kernel::Sve2 => vs_cosine_f16_sve2(a, b, n),
        }
    };
    Some(distance)
}
