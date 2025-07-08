use crate::Dimensions;
use crate::Embedding;
use anyhow::bail;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Wrong embedding dimension: expected {expected}, got {actual}")]
    WrongEmbeddingDimension { expected: usize, actual: usize },
}

pub fn embedding_dimensions(embedding: &Embedding, dimensions: Dimensions) -> anyhow::Result<()> {
    let Some(embedding_len) = std::num::NonZeroUsize::new(embedding.0.len()) else {
        bail!(Error::WrongEmbeddingDimension {
            expected: dimensions.0.get(),
            actual: 0,
        });
    };
    if embedding_len != dimensions.0 {
        bail!(Error::WrongEmbeddingDimension {
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

        let result = embedding_dimensions(&embedding, dimensions);

        assert!(matches!(
            result.unwrap_err().downcast_ref::<Error>(),
            Some(Error::WrongEmbeddingDimension {
                expected: 3,
                actual: 0
            })
        ));
    }

    #[test]
    fn validate_embedding_too_short() {
        let embedding = Embedding(vec![0.1, 0.2]);
        let dimensions = dims(3);

        let result = embedding_dimensions(&embedding, dimensions);

        assert!(matches!(
            result.unwrap_err().downcast_ref::<Error>(),
            Some(Error::WrongEmbeddingDimension {
                expected: 3,
                actual: 2
            })
        ));
    }

    #[test]
    fn validate_embedding_too_long() {
        let embedding = Embedding(vec![0.1, 0.2, 0.3, 0.4]);
        let dimensions = dims(3);
        let result = embedding_dimensions(&embedding, dimensions);
        assert!(matches!(
            result.unwrap_err().downcast_ref::<Error>(),
            Some(Error::WrongEmbeddingDimension {
                expected: 3,
                actual: 4
            })
        ));
    }

    #[test]
    fn validate_embedding_ok() {
        let embedding = Embedding(vec![0.1, 0.2, 0.3]);
        let dimensions = dims(3);

        let result = embedding_dimensions(&embedding, dimensions);

        assert!(matches!(result, Ok(())));
    }
}
