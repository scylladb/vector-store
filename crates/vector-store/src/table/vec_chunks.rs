/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use std::num::NonZeroUsize;

/// A trait that defines a chunk of data that can be stored in a VecChunks.
pub(crate) trait Chunk {
    type Shared<'a>;
    type Exclusive<'a>;

    fn size(&self) -> NonZeroUsize;
    fn fill_default(&self, bytes: &mut [u8]) -> Option<()>;
    fn get<'a>(&self, bytes: &'a [u8]) -> Option<Self::Shared<'a>>;
    fn get_mut<'a>(&self, bytes: &'a mut [u8]) -> Option<Self::Exclusive<'a>>;
}

/// VecChunks is a wrapper around Vec<u8> that allows us to store chunks of data in a contiguous
/// memory layout.
#[derive(Debug)]
pub(crate) struct VecChunks<T: Chunk> {
    vec: Vec<u8>,
    chunk_size: usize,
    chunk: T,
}

impl<T: Chunk> VecChunks<T> {
    /// Creates a new VecChunks with the given chunk type.
    pub(crate) fn new(chunk: T) -> Self {
        let chunk_size = chunk.size().get();
        Self {
            vec: Vec::new(),
            chunk_size,
            chunk,
        }
    }

    /// Returns the number of chunks in the VecChunks.
    pub(crate) fn len(&self) -> usize {
        self.vec.len() / self.chunk_size
    }

    /// Resizes the VecChunks to the given size.
    ///
    /// If the new size is larger than the current size, the new chunks will be filled with the
    /// default value for the chunk type.
    pub(crate) fn resize(&mut self, size: usize) {
        let len = self.len();
        if size <= len {
            self.vec.truncate(size * self.chunk_size);
            return;
        }
        let mut chunk_bytes = vec![0u8; self.chunk_size];
        self.chunk
            .fill_default(&mut chunk_bytes)
            .expect("chunk_bytes is sized to chunk_size");
        self.vec.reserve((size - len) * self.chunk_size);
        for _ in len..size {
            self.vec.extend_from_slice(&chunk_bytes);
        }
    }

    /// Returns a shared reference to the chunk at the given index.
    pub(crate) fn get(&self, idx: usize) -> Option<T::Shared<'_>> {
        if idx >= self.len() {
            return None;
        }
        self.chunk
            .get(&self.vec[idx * self.chunk_size..(idx + 1) * self.chunk_size])
    }

    /// Returns an exclusive reference to the chunk at the given index.
    pub(crate) fn get_mut(&mut self, idx: usize) -> Option<T::Exclusive<'_>> {
        if idx >= self.len() {
            return None;
        }
        self.chunk
            .get_mut(&mut self.vec[idx * self.chunk_size..(idx + 1) * self.chunk_size])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    struct TestChunk;
    struct TestChunkShared<'a>(&'a [u8]);
    struct TestChunkExclusive<'a>(&'a mut [u8]);

    impl TestChunkShared<'_> {
        fn get(&self) -> u32 {
            let mut arr = [0u8; 4];
            arr.copy_from_slice(self.0);
            u32::from_le_bytes(arr)
        }
    }

    impl TestChunkExclusive<'_> {
        fn set(&mut self, value: u32) {
            self.0.copy_from_slice(&value.to_le_bytes());
        }
    }

    impl Chunk for TestChunk {
        type Shared<'a> = TestChunkShared<'a>;
        type Exclusive<'a> = TestChunkExclusive<'a>;

        fn size(&self) -> NonZeroUsize {
            NonZeroUsize::new(mem::size_of::<u32>()).unwrap()
        }

        fn fill_default(&self, bytes: &mut [u8]) -> Option<()> {
            bytes.copy_from_slice(&42u32.to_le_bytes());
            Some(())
        }

        fn get<'a>(&self, bytes: &'a [u8]) -> Option<TestChunkShared<'a>> {
            (bytes.len() == mem::size_of::<u32>()).then_some(TestChunkShared(bytes))
        }

        fn get_mut<'a>(&self, bytes: &'a mut [u8]) -> Option<TestChunkExclusive<'a>> {
            (bytes.len() == mem::size_of::<u32>()).then_some(TestChunkExclusive(bytes))
        }
    }

    #[test]
    fn simple_vec_chunks() {
        let mut vec_chunks = VecChunks::new(TestChunk);
        assert_eq!(vec_chunks.len(), 0);

        vec_chunks.resize(3);
        assert_eq!(vec_chunks.len(), 3);
        assert_eq!(vec_chunks.get(0).unwrap().get(), 42);
        assert_eq!(vec_chunks.get(1).unwrap().get(), 42);
        assert_eq!(vec_chunks.get(2).unwrap().get(), 42);

        vec_chunks.get_mut(1).unwrap().set(100);
        assert_eq!(vec_chunks.get(0).unwrap().get(), 42);
        assert_eq!(vec_chunks.get(1).unwrap().get(), 100);
        assert_eq!(vec_chunks.get(2).unwrap().get(), 42);

        vec_chunks.resize(2);
        assert_eq!(vec_chunks.len(), 2);
        assert_eq!(vec_chunks.get(0).unwrap().get(), 42);
        assert_eq!(vec_chunks.get(1).unwrap().get(), 100);
        assert!(vec_chunks.get(2).is_none());
    }
}
