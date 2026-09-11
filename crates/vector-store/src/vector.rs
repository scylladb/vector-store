/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Dimensions;
use crate::alternator;
use anyhow::anyhow;
use anyhow::bail;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::deserialize::DeserializationError;
use scylla::deserialize::FrameSlice;
use scylla::deserialize::TypeCheckError;
use scylla::deserialize::value::DeserializeValue;
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

/// A failure of [`Vector`] decoding, reported through the driver's error
/// types, which require [`std::error::Error`].
#[derive(Debug, thiserror::Error)]
enum EmbeddingColumnError {
    #[error("unsupported CQL type for an embedding column: {0:?}")]
    UnsupportedType(ColumnType<'static>),
    #[error("invalid Alternator embedding: {0:#}")]
    Alternator(anyhow::Error),
}

/// Deserializes a [`Vector`] straight out of the frame, accepting both
/// representations an embedding column can have: a native
/// `vector<float, N>`, deserialized by the driver's [`Vec<f32>`]
/// deserializer, and a blob, which is an Alternator embedding.
impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Vector {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::Vector { .. } => <Vec<f32>>::type_check(typ),
            ColumnType::Native(NativeType::Blob) => Ok(()),
            other => Err(TypeCheckError::new(EmbeddingColumnError::UnsupportedType(
                other.clone().into_owned(),
            ))),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        match typ {
            ColumnType::Vector { .. } => <Vec<f32>>::deserialize(typ, v).map(Self::from),
            // Type-checked above
            _ => {
                let bytes = <&'frame [u8]>::deserialize(typ, v)?;
                alternator::parse_alternator_vector(bytes)
                    .map(Self::from)
                    .map_err(|err| DeserializationError::new(EmbeddingColumnError::Alternator(err)))
            }
        }
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

    fn deserialize_cql_vector(floats: &[f32]) -> anyhow::Result<Vector> {
        let typ = ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Float)),
            dimensions: floats.len() as u16,
        };
        let bytes: Vec<u8> = floats.iter().flat_map(|f| f.to_be_bytes()).collect();
        deserialize_column(&typ, &bytes)
    }

    fn deserialize_blob(bytes: &[u8]) -> anyhow::Result<Vector> {
        deserialize_column(&ColumnType::Native(NativeType::Blob), bytes)
    }

    fn deserialize_column(typ: &ColumnType<'static>, bytes: &[u8]) -> anyhow::Result<Vector> {
        <Vector>::type_check(typ).map_err(|err| anyhow!("{err}"))?;
        <Vector>::deserialize(typ, Some(FrameSlice::new_borrowed(bytes)))
            .map_err(|err| anyhow!("{err}"))
    }

    #[test]
    fn deserialize_from_cql_vector() {
        let result = deserialize_cql_vector(&[1.0, 2.5, 3.0]).unwrap();
        assert_eq!(result, Vector::from(vec![1.0, 2.5, 3.0]));
    }

    #[test]
    fn deserialize_from_dynamodb_json_blob() {
        let json = r#"{"L": [{"N": "123.4"}, {"N": "234.5"}, {"N": "345.6"}]}"#;
        let result = deserialize_blob(&alternator_list_blob(json)).unwrap();
        assert_eq!(result, Vector::from(vec![123.4, 234.5, 345.6]));
    }

    #[test]
    fn deserialize_from_alternator_vector_blob() {
        let result = deserialize_blob(&alternator_vector_blob(&[1.0, 2.5, 3.0])).unwrap();
        assert_eq!(result, Vector::from(vec![1.0, 2.5, 3.0]));
    }

    #[test]
    fn deserialize_rejects_a_wrong_vector_element_type() {
        let typ = ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Int)),
            dimensions: 1,
        };
        assert!(<Vector>::type_check(&typ).is_err());
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
