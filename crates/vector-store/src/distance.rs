/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Dimensions;
use crate::SpaceType;

#[derive(
    Copy,
    Debug,
    Clone,
    PartialEq,
    serde::Deserialize,
    derive_more::Deref,
    derive_more::AsRef,
    serde::Serialize,
    PartialOrd,
)]
pub struct DistanceValue(f32);

impl From<DistanceValue> for f32 {
    fn from(v: DistanceValue) -> Self {
        v.0
    }
}

impl From<Distance> for DistanceValue {
    fn from(distance: Distance) -> Self {
        match distance {
            Distance::Euclidean(v) => v,
            Distance::Cosine(v) => v,
            Distance::DotProduct(v) => v,
            Distance::Hamming((v, _)) => v,
        }
    }
}

#[derive(Copy, Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize, PartialOrd)]
pub enum Distance {
    Euclidean(DistanceValue),
    Cosine(DistanceValue),
    DotProduct(DistanceValue),
    Hamming((DistanceValue, Dimensions)),
}

impl Distance {
    pub fn new_euclidean(value: f32) -> Result<Self, anyhow::Error> {
        Self::try_from((value, SpaceType::Euclidean, None))
    }

    pub fn new_cosine(value: f32) -> Result<Self, anyhow::Error> {
        Self::try_from((value, SpaceType::Cosine, None))
    }

    pub fn new_dot_product(value: f32) -> Result<Self, anyhow::Error> {
        Self::try_from((value, SpaceType::DotProduct, None))
    }

    pub fn new_hamming(value: f32, dimensions: Dimensions) -> Result<Self, anyhow::Error> {
        Self::try_from((value, SpaceType::Hamming, Some(dimensions)))
    }
}

impl TryFrom<(f32, SpaceType, Option<Dimensions>)> for Distance {
    type Error = anyhow::Error;

    fn try_from(
        (value, space_type, dimensions): (f32, SpaceType, Option<Dimensions>),
    ) -> Result<Self, Self::Error> {
        match space_type {
            SpaceType::Cosine => {
                anyhow::ensure!(
                    (0.0..=2.0).contains(&value),
                    "Cosine distance must be in range [0.0, 2.0]"
                );
                Ok(Self::Cosine(DistanceValue(value)))
            }
            SpaceType::Euclidean => {
                anyhow::ensure!(value >= 0.0, "Euclidean distance must be >= 0.0");
                Ok(Self::Euclidean(DistanceValue(value)))
            }
            SpaceType::DotProduct => {
                // Dot Product distance can be unbounded, but when indexing L2-normalized vectors it will be in range [0.0, 2.0]
                anyhow::ensure!(
                    !value.is_nan(),
                    "Dot Product distance must be a valid number, got NaN"
                );
                Ok(Self::DotProduct(DistanceValue(value)))
            }
            SpaceType::Hamming => {
                anyhow::ensure!(value >= 0.0, "Hamming distance must be >= 0.0");
                anyhow::ensure!(
                    value.is_finite(),
                    "Hamming distance must be a finite number"
                );
                anyhow::ensure!(
                    value.fract() == 0.0,
                    "Hamming distance must be an integer value"
                );
                let dimensions = dimensions.ok_or(anyhow::anyhow!(
                    "Dimensions must be provided for Hamming distance"
                ))?;
                anyhow::ensure!(
                    value <= dimensions.0.get() as f32,
                    "Hamming distance cannot be greater than the number of dimensions"
                );
                Ok(Self::Hamming((DistanceValue(value), dimensions)))
            }
        }
    }
}

impl From<Distance> for f32 {
    fn from(distance: Distance) -> Self {
        match distance {
            Distance::Euclidean(d)
            | Distance::Cosine(d)
            | Distance::DotProduct(d)
            | Distance::Hamming((d, _)) => d.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NonZeroUsize;

    #[test]
    fn test_euclidean_distance_validation() {
        assert!(Distance::new_euclidean(0.0).is_ok());
        assert!(Distance::new_euclidean(0.123).is_ok());
        assert!(Distance::new_euclidean(1.0).is_ok());
        assert!(Distance::new_euclidean(2.0).is_ok());
        assert!(Distance::new_euclidean(5.0).is_ok());
        assert!(Distance::new_euclidean(100.5).is_ok());
        assert!(Distance::new_euclidean(f32::MAX).is_ok());

        assert!(Distance::new_euclidean(-0.1).is_err());
        assert!(Distance::new_euclidean(-1.0).is_err());

        assert!(Distance::new_euclidean(f32::INFINITY).is_ok());
        assert!(Distance::new_euclidean(f32::NEG_INFINITY).is_err());
        assert!(Distance::new_euclidean(f32::NAN).is_err());
    }

    #[test]
    fn test_cosine_distance_validation() {
        assert!(Distance::new_cosine(0.0).is_ok());
        assert!(Distance::new_cosine(0.123).is_ok());
        assert!(Distance::new_cosine(1.0).is_ok());
        assert!(Distance::new_cosine(2.0).is_ok());
        assert!(Distance::new_cosine(5.0).is_err());
        assert!(Distance::new_cosine(100.5).is_err());
        assert!(Distance::new_cosine(f32::MAX).is_err());

        assert!(Distance::new_cosine(-0.1).is_err());
        assert!(Distance::new_cosine(-1.0).is_err());

        assert!(Distance::new_cosine(f32::INFINITY).is_err());
        assert!(Distance::new_cosine(f32::NEG_INFINITY).is_err());
        assert!(Distance::new_cosine(f32::NAN).is_err());
    }

    #[test]
    fn test_dot_product_distance_validation() {
        assert!(Distance::new_dot_product(0.0).is_ok());
        assert!(Distance::new_dot_product(0.123).is_ok());
        assert!(Distance::new_dot_product(1.0).is_ok());
        assert!(Distance::new_dot_product(2.0).is_ok());
        assert!(Distance::new_dot_product(5.0).is_ok());
        assert!(Distance::new_dot_product(100.5).is_ok());
        assert!(Distance::new_dot_product(f32::MAX).is_ok());

        assert!(Distance::new_dot_product(-0.1).is_ok());
        assert!(Distance::new_dot_product(-1.0).is_ok());

        assert!(Distance::new_dot_product(f32::INFINITY).is_ok());
        assert!(Distance::new_dot_product(f32::NEG_INFINITY).is_ok());
        assert!(Distance::new_dot_product(f32::NAN).is_err());
    }

    #[test]
    fn test_hamming_distance_validation() {
        let dimensions = Dimensions(NonZeroUsize::new(3).unwrap());

        assert!(Distance::new_hamming(0.0, dimensions).is_ok());
        assert!(Distance::new_hamming(0.123, dimensions).is_err());
        assert!(Distance::new_hamming(1.0, dimensions).is_ok());
        assert!(Distance::new_hamming(2.0, dimensions).is_ok());
        assert!(Distance::new_hamming(5.0, dimensions).is_err()); // 5 > dimensions
        assert!(Distance::new_hamming(100.5, dimensions).is_err());
        assert!(Distance::new_hamming(f32::MAX, dimensions).is_err()); // f32::MAX > dimensions

        assert!(Distance::new_hamming(-0.1, dimensions).is_err());
        assert!(Distance::new_hamming(-1.0, dimensions).is_err());

        assert!(Distance::new_hamming(f32::INFINITY, dimensions).is_err());
        assert!(Distance::new_hamming(f32::NEG_INFINITY, dimensions).is_err());
        assert!(Distance::new_hamming(f32::NAN, dimensions).is_err());
    }
}
