/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Distance;

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, derive_more::From, utoipa::ToSchema,
)]
/// Similarity score between vectors derived from the distance. Higher score means more similar.
pub struct SimilarityScore(f32);

impl From<Distance> for SimilarityScore {
    /// Similarity score is in the range \[0.0, 1.0\]. \
    /// - Euclidean distance is mapped from \[0.0, inf) to (0.0, 1.0\] where 0 -> 1 and inf -> 0. \
    ///
    /// - USearch uses Cosine distance in the range \[0.0, 2.0\] which is mapped to similarity as follows: \
    ///   0.0 -> 1.0 (vectors pointing in same direction) \
    ///   1.0 -> 0.5 (vectors orthogonal to each other) \
    ///   2.0 -> 0.0 (vectors pointing in opposite directions)
    ///
    /// - DotProduct distance is unbounded, but when indexing L2-normalized vectors it will be same as Cosine.
    ///   If not normalized, the similarity score can be outside of the \[0.0, 1.0\] range.
    ///
    /// - Hamming distance is mapped from \[0.0, dimensions\] (number of mismatched bits) to \[0.0, 1.0\]
    ///   where 0 -> 1 and dimensions (all bits different) -> 0.
    fn from(distance: Distance) -> Self {
        let d: f32 = distance.into();
        let similarity = match distance {
            Distance::Cosine(_) | Distance::DotProduct(_) => (2.0 - d) / 2.0,
            Distance::Euclidean(_) => 1.0 / (1.0 + d),
            Distance::Hamming((_, dim)) => {
                let n = dim.0.get();
                1.0 - d / n as f32
            }
        };
        SimilarityScore(similarity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Dimensions;
    use crate::NonZeroUsize;

    #[test]
    fn test_euclidean_similarity() {
        let distance = Distance::new_euclidean(0.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 1.0);

        let distance = Distance::new_euclidean(1.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.5);

        let distance = Distance::new_euclidean(99.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.01);

        let distance = Distance::new_euclidean(1000.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert!(score.0 < 0.001);
    }

    #[test]
    fn test_cosine_similarity() {
        let distance = Distance::new_cosine(0.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 1.0);

        let distance = Distance::new_cosine(1.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.5);

        let distance = Distance::new_cosine(2.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.0);
    }

    #[test]
    fn test_dotproduct_similarity() {
        let distance = Distance::new_dot_product(0.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 1.0);

        let distance = Distance::new_dot_product(1.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.5);

        let distance = Distance::new_dot_product(2.0).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.0);

        let distance = Distance::new_dot_product(6.7).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, -2.35);

        let distance = Distance::new_dot_product(-1.8).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 1.9);
    }

    #[test]
    fn test_hamming_similarity() {
        let dimensions = Dimensions(NonZeroUsize::new(128).unwrap());

        let distance = Distance::new_hamming(0.0, dimensions).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 1.0);

        let distance = Distance::new_hamming(64.0, dimensions).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.5);

        let distance = Distance::new_hamming(128.0, dimensions).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.0);

        let dimensions = Dimensions(NonZeroUsize::new(50).unwrap());

        let distance = Distance::new_hamming(0.0, dimensions).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 1.0);

        let distance = Distance::new_hamming(35.0, dimensions).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.3);

        let distance = Distance::new_hamming(50.0, dimensions).unwrap();
        let score = SimilarityScore::from(distance);
        assert_eq!(score.0, 0.0);
    }
}
