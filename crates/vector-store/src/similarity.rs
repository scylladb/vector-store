/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Distance;
use crate::SpaceType;

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, derive_more::From, utoipa::ToSchema,
)]
/// Similarity score between vectors derived from the distance. Higher score means more similar.
pub struct SimilarityScore(f32);

impl From<(Distance, SpaceType)> for SimilarityScore {
    /// Similarity score is in the range \[0.0, 1.0\]. \
    /// - Euclidean distance is mapped from \[0.0, inf) to (0.0, 1.0\] where 0 -> 1 and inf -> 0. \
    ///
    /// - USearch uses Cosine distance in the range \[0.0, 2.0\] which is mapped to similarity as follows: \
    ///   0.0 -> 1.0 (vectors pointing in same direction) \
    ///   1.0 -> 0.5 (vectors orthogonal to each other) \
    ///   2.0 -> 0.0 (vectors pointing in opposite directions)
    ///
    /// - For DotProduct the distance is unbounded, but when indexing L2-normalized vectors it will be same as Cosine.
    fn from((distance, space_type): (Distance, SpaceType)) -> Self {
        match space_type {
            SpaceType::Cosine | SpaceType::DotProduct => SimilarityScore((2.0 - *distance) / 2.0),
            SpaceType::Euclidean => SimilarityScore(1.0 / (1.0 + *distance)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_euclidean_distance() {
        let distance = Distance::try_from(0.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::Euclidean));
        assert_eq!(score.0, 1.0);

        let distance = Distance::try_from(1.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::Euclidean));
        assert_eq!(score.0, 0.5);

        let distance = Distance::try_from(99.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::Euclidean));
        assert_eq!(score.0, 0.01);

        let distance = Distance::try_from(1000.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::Euclidean));
        assert!(score.0 < 0.001);
    }

    #[test]
    fn test_cosine_distance_same_direction() {
        let distance = Distance::try_from(0.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::Cosine));
        assert_eq!(score.0, 1.0);

        let distance = Distance::try_from(1.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::Cosine));
        assert_eq!(score.0, 0.5);

        let distance = Distance::try_from(2.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::Cosine));
        assert_eq!(score.0, 0.0);
    }

    #[test]
    fn test_dotproduct_distance() {
        let distance = Distance::try_from(0.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::DotProduct));
        assert_eq!(score.0, 1.0);

        let distance = Distance::try_from(1.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::DotProduct));
        assert_eq!(score.0, 0.5);

        let distance = Distance::try_from(2.0).unwrap();
        let score = SimilarityScore::from((distance, SpaceType::DotProduct));
        assert_eq!(score.0, 0.0);
    }
}
