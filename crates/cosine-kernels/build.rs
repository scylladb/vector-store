/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Compiles the cosine kernels (`csrc/cosine.c`) with the flags NumKong's own build uses for
//! its kernels, so that they compile to the same code as in the scylladb/NumKong fork they come
//! from: `-O3` in every profile, the target's baseline ISA for the translation unit (each kernel
//! enables its extensions with a target pragma), and no auto-vectorization.

fn main() {
    println!("cargo:rerun-if-changed=csrc/cosine.c");
    // The kernels use GCC's and Clang's target pragmas: an MSVC build compiles none, and src/lib.rs
    // keeps NumKong's kernels there.
    if std::env::var("CARGO_CFG_TARGET_ENV").is_ok_and(|env| env == "msvc") {
        return;
    }
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let mut build = cc::Build::new();
    build
        .file("csrc/cosine.c")
        .std("c11")
        .opt_level(3)
        .warnings(true)
        .extra_warnings(true)
        .flag_if_supported("-fno-tree-vectorize")
        .flag_if_supported("-fno-tree-slp-vectorize");
    match target_arch.as_str() {
        "x86_64" => {
            build.flag_if_supported("-march=x86-64");
        }
        "aarch64" => {
            build.flag_if_supported("-march=armv8-a");
        }
        _ => {}
    }
    build.compile("vs_cosine_kernels");
}
