/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Compiles the cosine kernels (`csrc/cosine.c`) with the flags NumKong's own build uses for
//! its kernels, so that they compile to the same code as in the scylladb/NumKong fork they come
//! from: `-O3` in every profile, the target's baseline ISA for the translation unit (each kernel
//! enables its extensions with a target pragma), and no auto-vectorization.
//!
//! Vector Store supports x86-64 and aarch64, built with GCC or Clang. The kernels exist for
//! those two only and need GCC's or Clang's target pragmas, so any other target is a build error
//! here rather than a build without them.

fn main() {
    println!("cargo:rerun-if-changed=csrc/cosine.c");
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let target_env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    if target_env == "msvc" {
        panic!(
            "cosine-kernels does not build with MSVC: csrc/cosine.c needs GCC's or Clang's target \
             pragmas, and Vector Store is not supported on MSVC"
        );
    }
    let march = match target_arch.as_str() {
        "x86_64" => "-march=x86-64",
        "aarch64" => "-march=armv8-a",
        other => panic!(
            "cosine-kernels supports x86_64 and aarch64 only, not {other:?}: Vector Store is not \
             supported there"
        ),
    };
    cc::Build::new()
        .file("csrc/cosine.c")
        .std("c11")
        .opt_level(3)
        .warnings(true)
        .extra_warnings(true)
        .flag_if_supported("-fno-tree-vectorize")
        .flag_if_supported("-fno-tree-slp-vectorize")
        .flag_if_supported(march)
        .compile("vs_cosine_kernels");
}
