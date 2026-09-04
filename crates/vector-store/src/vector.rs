/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Dimensions;
use crate::alternator;
use anyhow::anyhow;
use anyhow::bail;
use scylla::value::CqlValue;
use std::num::NonZeroUsize;

#[derive(Clone, Debug, PartialEq, derive_more::AsRef, derive_more::From)]
/// The vector to use for the Approximate Nearest Neighbor search. The format of data must match the data_type of the index.
pub struct Vector(Vec<f32>);

impl Vector {
    pub fn as_slice(&self) -> &[f32] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn dim(&self) -> Option<Dimensions> {
        NonZeroUsize::new(self.len()).map(Dimensions)
    }
}

/// Converts a [`CqlValue`] into a [`Vector`].
///
/// Supports two representations:
/// - `CqlValue::Vector` — native CQL `VECTOR<float, N>` type (used by CQL-native tables).
/// - `CqlValue::Blob` — DynamoDB JSON serialized as bytes (used by Alternator).
impl TryFrom<CqlValue> for Vector {
    type Error = anyhow::Error;

    fn try_from(value: CqlValue) -> anyhow::Result<Self> {
        let floats = match value {
            CqlValue::Vector(values) => values
                .into_iter()
                .map(|v| {
                    let CqlValue::Float(f) = v else {
                        bail!("bad type of embedding element: expected float, got {v:?}");
                    };
                    Ok(f)
                })
                .collect(),
            CqlValue::Blob(bytes) => alternator::parse_alternator_vector(&bytes),
            other => Err(anyhow!(
                "unsupported CQL type for embedding column: {other:?}"
            )),
        }?;
        Ok(Self(floats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alternator::ALTERNATOR_TYPE_FLOAT32VECTOR;
    use alternator::ALTERNATOR_TYPE_JSON;

    /// Prepend the [`ALTERNATOR_TYPE_JSON`] tag to a DynamoDB JSON string,
    /// mirroring how Alternator serialises List values.
    fn alternator_list_blob(json: &str) -> Vec<u8> {
        let mut v = vec![ALTERNATOR_TYPE_JSON];
        v.extend_from_slice(json.as_bytes());
        v
    }

    /// Prepend the [`ALTERNATOR_TYPE_FLOAT32VECTOR`] tag to a sequence of big-endian floats,
    /// mirroring how Alternator serialises the `FLOAT32VECTOR` type.
    fn alternator_vector_blob(floats: &[f32]) -> Vec<u8> {
        let mut v = vec![ALTERNATOR_TYPE_FLOAT32VECTOR];
        for &f in floats {
            v.extend_from_slice(&f.to_be_bytes());
        }
        v
    }

    #[test]
    fn extract_from_cql_vector() {
        let value = CqlValue::Vector(vec![
            CqlValue::Float(1.0),
            CqlValue::Float(2.5),
            CqlValue::Float(3.0),
        ]);
        let result = Vector::try_from(value).unwrap();
        assert_eq!(result, Vector::from(vec![1.0, 2.5, 3.0]));
    }

    #[test]
    fn extract_from_dynamodb_json_blob() {
        let json = r#"{"L": [{"N": "123.4"}, {"N": "234.5"}, {"N": "345.6"}]}"#;
        let value = CqlValue::Blob(alternator_list_blob(json));
        let result = Vector::try_from(value).unwrap();
        assert_eq!(result, Vector::from(vec![123.4, 234.5, 345.6]));
    }

    #[test]
    fn extract_from_dynamodb_json_empty_list() {
        let json = r#"{"L": []}"#;
        let value = CqlValue::Blob(alternator_list_blob(json));
        let result = Vector::try_from(value).unwrap();
        assert_eq!(result, Vector::from(vec![]));
    }

    #[test]
    fn extract_from_dynamodb_json_invalid_number() {
        let json = r#"{"L": [{"N": "not_a_number"}]}"#;
        let value = CqlValue::Blob(alternator_list_blob(json));
        assert!(Vector::try_from(value).is_err());
    }

    #[test]
    fn extract_from_blob_unknown_tag() {
        let value = CqlValue::Blob(vec![0x99, 0x00, 0x01]);
        assert!(Vector::try_from(value).is_err());
    }

    #[test]
    fn extract_from_blob_empty() {
        let value = CqlValue::Blob(vec![]);
        assert!(Vector::try_from(value).is_err());
    }

    #[test]
    fn extract_from_unsupported_type() {
        let value = CqlValue::Int(42);
        assert!(Vector::try_from(value).is_err());
    }

    #[test]
    fn extract_from_cql_vector_wrong_element_type() {
        let value = CqlValue::Vector(vec![CqlValue::Int(1)]);
        assert!(Vector::try_from(value).is_err());
    }

    #[test]
    fn extract_from_alternator_vector_blob() {
        let value = CqlValue::Blob(alternator_vector_blob(&[1.0, 2.5, 3.0]));
        let result = Vector::try_from(value).unwrap();
        assert_eq!(result, Vector::from(vec![1.0, 2.5, 3.0]));
    }

    #[test]
    fn extract_from_alternator_vector_empty() {
        let value = CqlValue::Blob(alternator_vector_blob(&[]));
        let result = Vector::try_from(value).unwrap();
        assert_eq!(result, Vector::from(vec![]));
    }

    #[test]
    fn extract_from_alternator_vector_invalid_length() {
        // 5 bytes after the tag — not a multiple of 4
        let mut bytes = vec![ALTERNATOR_TYPE_FLOAT32VECTOR];
        bytes.extend_from_slice(&[0x00, 0x01, 0x02, 0x03, 0x04]);
        let value = CqlValue::Blob(bytes);
        assert!(Vector::try_from(value).is_err());
    }
}
