/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::table::Idx;
use anyhow::bail;
use std::mem;
use tracing::info;

/// PrimaryId is a unique identifier for a row in the table. It consists of an index and an
/// epoch. The index is used to access the row in the column vectors, while the epoch is used
/// to determine if the row has been updated.
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Into,
)]
pub struct PrimaryId(u64);

const _: () = assert!(
    mem::size_of::<PrimaryId>() == mem::size_of::<usize>(),
    "PrimaryId should be the same size as usize"
);

impl PrimaryId {
    const EPOCH_SHIFT: usize = (mem::size_of::<u64>() - mem::size_of::<Epoch>()) * 8;
    const MAX: u64 = !((Epoch::MAX as u64) << Self::EPOCH_SHIFT);

    pub(super) fn try_new(idx: usize, epoch: Epoch) -> anyhow::Result<Self> {
        if idx as u64 > Self::MAX {
            bail!("PrimaryId is too large: {idx}");
        }
        Ok(Self(
            (*epoch.as_ref() as u64) << Self::EPOCH_SHIFT | idx as u64,
        ))
    }

    pub(super) fn new_epoch(mut self, epoch: Epoch) -> Self {
        self.0 &= Self::MAX;
        self.0 |= (epoch.0 as u64) << Self::EPOCH_SHIFT;
        self
    }

    pub(super) fn next_epoch(self) -> Self {
        self.new_epoch(self.epoch().next())
    }

    pub(super) fn epoch(&self) -> Epoch {
        Epoch((self.0 >> Self::EPOCH_SHIFT) as u16)
    }
}

impl Idx for PrimaryId {
    fn idx(&self) -> usize {
        (self.0 & Self::MAX) as usize
    }
}

/// Epoch is a counter that is incremented every time a row is updated. New values are
/// cyclically assigned epochs, so we can have at most Epoch::MAX updates (around 65k), so
/// assuming one change per millisecond, we have around 65 seconds for unique epochs.
/// Vectors with old epochs must be updated within that time (it seems reasonable).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, derive_more::AsRef)]
pub(super) struct Epoch(u16);

impl Epoch {
    const MIN: u16 = 0;
    const MAX: u16 = u16::MAX;

    pub(super) const fn new() -> Self {
        Self(Self::MIN)
    }

    pub(super) fn next(self) -> Self {
        if self.0 == Self::MAX {
            info!("Epoch overflow: all epochs are used, starting from the minimum epoch");
            Self(Self::MIN)
        } else {
            Self(self.0 + 1)
        }
    }

    pub(super) const fn to_ne_bytes(self) -> [u8; 2] {
        self.0.to_ne_bytes()
    }

    pub(super) const fn from_ne_bytes(bytes: [u8; 2]) -> Self {
        Self(u16::from_ne_bytes(bytes))
    }
}
