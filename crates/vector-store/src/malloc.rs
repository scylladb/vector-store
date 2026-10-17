/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Tuning of glibc malloc, which serves the C and C++ code linked into Vector Store.
//!
//! Rust allocations go to mimalloc (the global allocator in `main.rs`), but usearch (C++)
//! and aws-lc (C) call `malloc`, and that is glibc's. usearch keeps an index's vectors and
//! graph in mappings of its own, but its per-slot lookup tables (the node and vector
//! pointers and the key-to-slot hash) are glibc blocks of 8-16 MiB per million slots of
//! capacity, reallocated each time the index grows.
//!
//! glibc gives a block of `M_MMAP_THRESHOLD` (128 KiB) or more its own mapping and unmaps
//! it on free. But freeing such a block also raises the threshold to the block's size (up
//! to 32 MiB) and the trim threshold to twice that. After the first index is dropped, the
//! next index's tables up to 32 MiB are carved from the per-thread arenas instead, and with
//! a 64 MiB trim threshold an arena (at most 64 MiB) never gives the space back: the RSS
//! grows with every DROP/CREATE INDEX cycle (VECTOR-1076). Setting the threshold
//! explicitly turns that adjustment off.

/// glibc's initial `M_MMAP_THRESHOLD`: blocks this large or larger get their own mapping.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const MMAP_THRESHOLD: libc::c_int = 128 * 1024;

/// Pins glibc's mmap threshold at its initial value, so that freeing a large block no
/// longer raises it, and large blocks always go back to the OS when freed.
/// A no-op where the C library is not glibc.
///
/// Call it at startup, before other threads allocate.
pub fn pin_mmap_threshold() -> anyhow::Result<()> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: mallopt only sets an allocator parameter, under the allocator's lock.
        if unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD) } != 1 {
            anyhow::bail!("mallopt(M_MMAP_THRESHOLD, {MMAP_THRESHOLD}) failed");
        }
    }
    Ok(())
}
