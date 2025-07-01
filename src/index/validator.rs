use crate::Dimensions;
use crate::Embedding;
use crate::index::actor::AnnError;

pub fn validate_embedding_dimensions(
    embedding: &Embedding,
    dimensions: Dimensions,
) -> Result<(), AnnError> {
    let Some(embedding_len) = std::num::NonZeroUsize::new(embedding.0.len()) else {
        return Err(AnnError::WrongEmbeddingDimension {
            expected: dimensions.0.get(),
            actual: 0,
        });
    };
    if embedding_len != dimensions.0 {
        return Err(AnnError::WrongEmbeddingDimension {
            expected: dimensions.0.get(),
            actual: embedding_len.get(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    fn dims(n: usize) -> Dimensions {
        Dimensions(NonZeroUsize::new(n).unwrap())
    }

    #[test]
    fn validate_embedding_empty() {
        let embedding = Embedding(vec![]);
        let dimensions = dims(3);

        let result = validate_embedding_dimensions(&embedding, dimensions);

        assert!(matches!(
            result,
            Err(AnnError::WrongEmbeddingDimension {
                expected: 3,
                actual: 0
            })
        ));
    }

    #[test]
    fn validate_embedding_too_short() {
        let embedding = Embedding(vec![0.1, 0.2]);
        let dimensions = dims(3);

        let result = validate_embedding_dimensions(&embedding, dimensions);

        assert!(matches!(
            result,
            Err(AnnError::WrongEmbeddingDimension {
                expected: 3,
                actual: 2
            })
        ));
    }

    #[test]
    fn validate_embedding_too_long() {
        let embedding = Embedding(vec![0.1, 0.2, 0.3, 0.4]);
        let dimensions = dims(3);
        let result = validate_embedding_dimensions(&embedding, dimensions);
        assert!(matches!(
            result,
            Err(AnnError::WrongEmbeddingDimension {
                expected: 3,
                actual: 4
            })
        ));
    }

    #[test]
    fn validate_embedding_ok() {
        let embedding = Embedding(vec![0.1, 0.2, 0.3]);
        let dimensions = dims(3);

        let result = validate_embedding_dimensions(&embedding, dimensions);

        assert!(matches!(result, Ok(())));
    }
}
