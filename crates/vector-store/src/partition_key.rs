/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::invariant_key::InvariantKey;
use scylla::value::CqlValue;

/// This is a thin newtype around [`InvariantKey`] providing partition-key-specific
/// semantics.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionKey(InvariantKey);

impl PartitionKey {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, idx: usize) -> Option<CqlValue> {
        self.0.get(idx)
    }
}

impl FromIterator<CqlValue> for PartitionKey {
    fn from_iter<I: IntoIterator<Item = CqlValue>>(iter: I) -> Self {
        Self(InvariantKey::from_iter(iter))
    }
}

impl<I: IntoIterator<Item = CqlValue>> From<I> for PartitionKey {
    fn from(iter: I) -> Self {
        Self(InvariantKey::from_iter(iter))
    }
}
