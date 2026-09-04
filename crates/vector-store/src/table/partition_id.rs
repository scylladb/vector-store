/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::table::Idx;
use anyhow::bail;
use std::mem;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::From)]
pub struct PartitionId(u64);

const _: () = assert!(
    mem::size_of::<PartitionId>() == mem::size_of::<usize>(),
    "PartitionId should be the same size as usize"
);

impl PartitionId {
    const INDEX_ID_SHIFT: usize = (mem::size_of::<u64>() - mem::size_of::<IndexId>()) * 8;
    const MAX: u64 = !((IndexId::MASK as u64) << Self::INDEX_ID_SHIFT);

    pub(crate) fn try_new(idx: usize, index_id: IndexId) -> anyhow::Result<Self> {
        if idx as u64 > Self::MAX {
            bail!("PartitionId is too large: {idx}");
        }
        Ok(Self(
            (*index_id.as_ref() as u64) << Self::INDEX_ID_SHIFT | idx as u64,
        ))
    }

    pub(crate) fn global(index_id: IndexId) -> Self {
        Self((*index_id.as_ref() as u64) << Self::INDEX_ID_SHIFT)
    }

    pub(crate) fn index_id(&self) -> IndexId {
        IndexId((self.0 >> Self::INDEX_ID_SHIFT) as u16)
    }
}

impl Idx for PartitionId {
    fn idx(&self) -> usize {
        (self.0 & Self::MAX) as usize
    }
}

/// IndexId provides unique IndexIds for indexes in the table. We can have up to
/// IndexId::MAX indexes (0x7fff), which seems reasonable for a single table.
/// IndexId::MAX is used as a sentinel to mark that there are no more IndexIds
/// available. IndexId::GLOBAL_BIT is used to mark that the index is global.
#[derive(
    Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::AsRef, derive_more::From,
)]
pub(crate) struct IndexId(u16);

impl IndexId {
    const MASK: u16 = u16::MAX;
    const GLOBAL_BIT: u16 = 1 << (u16::BITS - 1);
    const MAX: u16 = Self::MASK & !Self::GLOBAL_BIT;

    fn local(id: u16) -> anyhow::Result<Self> {
        if id > Self::MAX {
            bail!("Base value {id} is too large for local IndexId");
        }
        Ok(Self(id))
    }

    fn global(id: u16) -> anyhow::Result<Self> {
        if id > Self::MAX {
            bail!("Base value {id} is too large for global IndexId");
        }
        Ok(Self(id | Self::GLOBAL_BIT))
    }

    pub(crate) fn is_global(&self) -> bool {
        self.0 & IndexId::GLOBAL_BIT != 0
    }
}

#[derive(Debug)]
pub(crate) struct IndexIdGenerator {
    next: u16,
}

impl IndexIdGenerator {
    pub(crate) fn new() -> Self {
        Self { next: 0 }
    }

    pub(crate) fn next(&mut self, global: bool) -> anyhow::Result<IndexId> {
        if self.next == IndexId::MAX {
            bail!("No more IndexIds available");
        }
        let index_id = if global {
            IndexId::global(self.next)?
        } else {
            IndexId::local(self.next)?
        };
        self.next += 1;
        Ok(index_id)
    }
}
