/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use scylla::cluster::metadata::ColumnType;
use scylla::serialize::SerializationError;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;
use scylla::serialize::writers::WrittenCellProof;

#[derive(
    Copy,
    Clone,
    Debug,
    derive_more::AsRef,
    derive_more::Deref,
    serde::Serialize,
    serde::Deserialize,
    utoipa::ToSchema,
)]
/// Distance between vectors measured using the distance function defined while creating the index.
pub struct Distance(f32);

impl SerializeValue for Distance {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        <f32 as SerializeValue>::serialize(&self.0, typ, writer)
    }
}

impl TryFrom<f32> for Distance {
    type Error = anyhow::Error;

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        anyhow::ensure!(value.is_finite(), "distance must be finite");
        anyhow::ensure!(
            value >= 0.0,
            "distance must be >= 0.0. If using Dot Product similarity make sure to L2 normalize vectors before indexing/searching"
        );
        Ok(Self(value))
    }
}

impl From<Distance> for f32 {
    fn from(distance: Distance) -> Self {
        distance.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_validation() {
        assert!(Distance::try_from(0.0).is_ok());
        assert!(Distance::try_from(1.0).is_ok());
        assert!(Distance::try_from(100.5).is_ok());
        assert!(Distance::try_from(f32::MAX).is_ok());

        assert!(Distance::try_from(-0.1).is_err());
        assert!(Distance::try_from(-1.0).is_err());

        assert!(Distance::try_from(f32::INFINITY).is_err());
        assert!(Distance::try_from(f32::NEG_INFINITY).is_err());
        assert!(Distance::try_from(f32::NAN).is_err());
    }
}
