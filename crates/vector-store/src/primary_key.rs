/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

//! CQL primary key type backed by [`InvariantKey`](crate::invariant_key::InvariantKey).

use crate::invariant_key::InvariantKey;
use derive_more::Deref;
use derive_more::From;
use std::fmt;

/// A memory-optimized CQL primary key.
///
/// This is a thin newtype around [`InvariantKey`] providing primary-key-specific
/// semantics. Cloning is O(1) because `InvariantKey` uses `Arc` internally.
#[derive(Clone, Deref, PartialEq, Eq, From, Hash)]
pub struct PrimaryKey(InvariantKey);

impl fmt::Debug for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.debug_fmt(f, "PrimaryKey")
    }
}

// Static assertion: PrimaryKey must be exactly 16 bytes (same as InvariantKey).
const _: () = assert!(std::mem::size_of::<PrimaryKey>() == 16);
