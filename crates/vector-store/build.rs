// Copyright 2026-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

//! `cuvs-sys`'s build script can succeed even when `libcuvs`'s shared
//! library file is actually missing (e.g. a partial/interrupted install,
//! or a build script cached from before the conda env was wiped), so the
//! failure only surfaces much later at the final link step, as a cryptic
//! `unable to find library -lcuvs_c` error.
//!
//! Fail fast here instead, with a clear pointer to `scripts/setup-gpu`.

use std::env;
use std::path::Path;

fn main() {
    if env::var_os("CARGO_FEATURE_GPU").is_none() {
        return;
    }

    println!("cargo:rerun-if-env-changed=CMAKE_PREFIX_PATH");

    let Some(cmake_prefix_path) = env::var_os("CMAKE_PREFIX_PATH") else {
        panic!(
            "\n\n\
             CMAKE_PREFIX_PATH is not set (required by the `gpu` Cargo feature to locate libcuvs).\n\
             \n\
             This should be set automatically by .cargo/config.toml, which is checked into this \
             repo. Running `sudo ./scripts/setup-gpu` does NOT set it directly -- it only edits \
             that file for cargo to pick up on a later invocation. If you're seeing this, check \
             that .cargo/config.toml exists and has a [env] CMAKE_PREFIX_PATH entry, and that \
             cargo is actually reading it (e.g. you're building from within this repo, not via \
             a --manifest-path pointing elsewhere).\n\
             \n",
        );
    };
    let lib_dir = Path::new(&cmake_prefix_path).join("lib");

    println!("cargo:rerun-if-changed={}", lib_dir.display());

    let has_libcuvs_c =
        lib_dir.join("libcuvs_c.so").is_file() || lib_dir.join("libcuvs_c.a").is_file();

    if !has_libcuvs_c {
        panic!(
            "\n\n\
             libcuvs_c was not found in {lib_dir} (required by the `gpu` Cargo feature).\n\
             \n\
             Run `sudo ./scripts/setup-gpu` to install it, then re-run the build.\n\
             \n",
            lib_dir = lib_dir.display(),
        );
    }
}
