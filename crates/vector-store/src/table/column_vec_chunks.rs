/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::table::Chunk;
use crate::table::Idx;
use crate::table::VecChunks;

/// ColumnVecChunks is a wrapper around VecChunks and generic index type. It is used to safely
/// access columns by specific index types.
#[derive(Debug)]
pub(super) struct ColumnVecChunks<I, T: Chunk> {
    vec: VecChunks<T>,
    _index: std::marker::PhantomData<I>,
}

impl<I: Idx, T: Chunk> ColumnVecChunks<I, T> {
    /// Create a new ColumnVecChunks with the given chunk.
    pub(super) fn new(chunk: T) -> Self {
        Self {
            vec: VecChunks::new(chunk),
            _index: std::marker::PhantomData,
        }
    }

    /// Resize the underlying VecChunks to the given size.
    pub(super) fn resize(&mut self, size: usize) {
        self.vec.resize(size);
    }

    /// Get the shared reference to the chunk at the given index.
    pub(super) fn get(&self, idx: I) -> Option<T::Shared<'_>> {
        self.vec.get(idx.idx())
    }

    /// Get the exclusive reference to the chunk at the given index.
    pub(super) fn get_mut(&mut self, idx: I) -> Option<T::Exclusive<'_>> {
        self.vec.get_mut(idx.idx())
    }
}
