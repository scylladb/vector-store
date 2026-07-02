/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Timestamp;
use crate::table::Chunk;
use crate::table::Epoch;
use crate::timestamp::Timestamped;
use std::mem;
use std::num::NonZeroUsize;

const EPOCH_SIZE: usize = mem::size_of::<u8>() * 2;
const TIMESTAMPED_SIZE: usize = mem::size_of::<u8>() * 8;

const _: () = assert!(Epoch::new().to_ne_bytes().len() == EPOCH_SIZE);
const _: () =
    assert!(Timestamped::new_valid(Timestamp::MIN).into_ne_bytes().len() == TIMESTAMPED_SIZE);

/// A chunk of timestamps, which is a fixed-size array of `Timestamped<()>` values, along with an
/// `Epoch` value.
#[derive(Debug)]
pub(super) struct ChunkTimestamps(usize);

impl ChunkTimestamps {
    /// Create a new `ChunkTimestamps` with the given size.
    pub(super) fn new(size: NonZeroUsize) -> Self {
        Self(size.get())
    }
}

/// A shared view of a `ChunkTimestamps`, which allows reading the `Epoch` and individual
/// `Timestamped<()>` values.
pub(super) struct ChunkTimestampsShared<'a> {
    bytes: &'a [u8],
}

impl ChunkTimestampsShared<'_> {
    /// Get the `Epoch` value from the chunk.
    pub(super) fn epoch(&self) -> Epoch {
        epoch(self.bytes)
    }
}

/// An exclusive view of a `ChunkTimestamps`, which allows reading and writing the `Epoch` and
/// individual `Timestamped<()>` values.
pub(super) struct ChunkTimestampsExclusive<'a> {
    size: usize,
    bytes: &'a mut [u8],
}

impl ChunkTimestampsExclusive<'_> {
    /// Get the `Epoch` value from the chunk.
    pub(super) fn epoch(&self) -> Epoch {
        epoch(self.bytes)
    }

    /// Set the `Epoch` value in the chunk.
    pub(super) fn set_epoch(&mut self, epoch: Epoch) {
        set_epoch(self.bytes, epoch);
    }

    /// Get the `Timestamped<()>` value at the given index in the chunk.
    pub(super) fn timestamp(&self, idx: usize) -> Option<Timestamped<()>> {
        timestamp(self.size, self.bytes, idx)
    }

    /// Set the `Timestamped<()>` value at the given index in the chunk.
    pub(super) fn set_timestamp(&mut self, idx: usize, value: Timestamped<()>) -> Option<()> {
        set_timestamp(self.size, self.bytes, idx, value)
    }
}

impl Chunk for ChunkTimestamps {
    type Shared<'a> = ChunkTimestampsShared<'a>;
    type Exclusive<'a> = ChunkTimestampsExclusive<'a>;

    fn size(&self) -> NonZeroUsize {
        chunk_size(self.0).try_into().unwrap()
    }

    fn fill_default(&self, bytes: &mut [u8]) -> Option<()> {
        if chunk_size(self.0) != bytes.len() {
            return None;
        }
        set_epoch(bytes, Epoch::new());
        for idx in 0..self.0 {
            set_timestamp(
                self.0,
                bytes,
                idx,
                Timestamped::new_tombstone(Timestamp::MIN),
            )
            .expect("chunk size is correct");
        }
        Some(())
    }

    fn get<'a>(&self, bytes: &'a [u8]) -> Option<Self::Shared<'a>> {
        (chunk_size(self.0) == bytes.len()).then_some(ChunkTimestampsShared { bytes })
    }

    fn get_mut<'a>(&self, bytes: &'a mut [u8]) -> Option<Self::Exclusive<'a>> {
        (chunk_size(self.0) == bytes.len()).then_some(ChunkTimestampsExclusive {
            size: self.0,
            bytes,
        })
    }
}

fn chunk_size(elements: usize) -> usize {
    EPOCH_SIZE + elements * TIMESTAMPED_SIZE
}

fn epoch(bytes: &[u8]) -> Epoch {
    let mut dst = [0; EPOCH_SIZE];
    dst.copy_from_slice(&bytes[..EPOCH_SIZE]);
    Epoch::from_ne_bytes(dst)
}

fn set_epoch(bytes: &mut [u8], epoch: Epoch) {
    let src = epoch.to_ne_bytes();
    bytes[..EPOCH_SIZE].copy_from_slice(&src);
}

fn timestamp(size: usize, bytes: &[u8], idx: usize) -> Option<Timestamped<()>> {
    if idx >= size {
        return None;
    }
    let mut dst = [0; TIMESTAMPED_SIZE];
    let offset = EPOCH_SIZE + idx * TIMESTAMPED_SIZE;
    dst.copy_from_slice(&bytes[offset..offset + TIMESTAMPED_SIZE]);
    Some(Timestamped::from_ne_bytes(dst))
}

fn set_timestamp(size: usize, bytes: &mut [u8], idx: usize, value: Timestamped<()>) -> Option<()> {
    if idx >= size {
        return None;
    }
    let src = value.into_ne_bytes();
    let offset = EPOCH_SIZE + idx * TIMESTAMPED_SIZE;
    bytes[offset..offset + TIMESTAMPED_SIZE].copy_from_slice(&src);
    Some(())
}
