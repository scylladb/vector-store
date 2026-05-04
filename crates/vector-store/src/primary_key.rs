/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::invariant_key::InvariantKey;
use bigdecimal::BigDecimal;
use scylla::value::CqlDecimal;
use scylla::value::CqlValue;

/// This is a thin newtype around [`InvariantKey`] providing primary-key-specific
/// semantics.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrimaryKey(InvariantKey);

impl PrimaryKey {
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

impl FromIterator<CqlValue> for PrimaryKey {
    fn from_iter<I: IntoIterator<Item = CqlValue>>(iter: I) -> Self {
        Self(InvariantKey::from_iter(iter))
    }
}

impl<I: IntoIterator<Item = CqlValue>> From<I> for PrimaryKey {
    fn from(iter: I) -> Self {
        Self(InvariantKey::from_iter(iter))
    }
}

/// Normalize a [`CqlValue`] so that semantically equal values produce identical bytes.
///
/// Currently only affects `Decimal`: strips trailing zeros via `BigDecimal::normalized()`
/// so that e.g. `3.14` and `3.140` map to the same [`PrimaryKey`] entry.
/// This is needed because Scylla clustering keys use value-based comparison,
/// but the raw bytes differ for different decimal representations.
///
/// Used exclusively for BTreeMap lookup keys in [`Table`]. The original
/// (unnormalized) representation is stored separately and returned in ANN responses.
pub(crate) fn normalize(value: CqlValue) -> CqlValue {
    match value {
        CqlValue::Decimal(d) => {
            let normalized = BigDecimal::from(d).normalized();
            // TryFrom can only fail if scale overflows i32, which shouldn't
            // happen after normalization (it only strips trailing zeros).
            CqlValue::Decimal(
                CqlDecimal::try_from(normalized).expect("normalized decimal scale fits i32"),
            )
        }
        other => other,
    }
}
