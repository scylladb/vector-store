/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! A test binary of its own: glibc serves a large request from a free chunk its arena
//! already holds before the mmap threshold comes into play, so the check needs a process
//! where no other test leaves such chunks behind.

#![cfg(all(target_os = "linux", target_env = "gnu"))]

/// Whether glibc served `block` with its own mapping: the IS_MMAPPED bit (0x2) of the size
/// field in the chunk header, the word just before the returned pointer. This is glibc's
/// internal layout, but it has not changed since mmapped chunks were added.
fn is_mmapped(block: *mut libc::c_void) -> bool {
    // SAFETY: `block` is a live glibc allocation, so its chunk header precedes it.
    let size_field = unsafe { *(block as *const usize).sub(1) };
    size_field & 0x2 != 0
}

#[test]
fn freed_large_block_does_not_raise_mmap_threshold() {
    const SIZE: usize = 1 << 20;
    vector_store::malloc::pin_mmap_threshold().unwrap();

    // Without a pinned threshold, freeing the first block raises the threshold above
    // 1 MiB, and glibc carves the second one from the arena.
    for round in 1..=2 {
        // SAFETY: plain malloc/free pair; the block is not used otherwise.
        let block = unsafe { libc::malloc(SIZE) };
        assert!(!block.is_null());
        let mmapped = is_mmapped(block);
        // SAFETY: `block` was returned by malloc above and is freed once.
        unsafe { libc::free(block) };
        assert!(mmapped, "1 MiB block of round {round} is not mmapped");
    }
}
