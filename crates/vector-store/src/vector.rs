/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Dimensions;
use crate::alternator;
use crate::alternator::parse_alternator_vector;
use anyhow::anyhow;
use anyhow::bail;
use scylla::deserialize::DeserializationError;
use scylla::deserialize::FrameSlice;
use scylla::deserialize::TypeCheckError;
use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::ColumnType;
use scylla::frame::response::result::NativeType;
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

/// The embedding column, decoded straight from the response frame.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VectorColumn(Vector);

impl VectorColumn {
    pub(crate) fn into_vector(self) -> Vector {
        self.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("embedding column: {0}")]
struct VectorColumnError(String);

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for VectorColumn {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            // CQL-native: the driver checks the element type is `float`. Only
            // the check is delegated — `Vec<f32>`'s own `deserialize` collects
            // an iterator of `Result`, whose size hint bottoms out at 0, so it
            // grows the buffer geometrically instead of allocating once.
            ColumnType::Vector { .. } => <Vec<f32>>::type_check(typ),
            // Alternator: a value out of the `:attrs` map column.
            ColumnType::Native(NativeType::Blob) => Ok(()),
            other => Err(TypeCheckError::new(VectorColumnError(format!(
                "expected VECTOR<float, N> or blob, got {other:?}"
            )))),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        // A NULL never reaches here: the read path asks for
        // `Option<VectorColumn>`, and the driver's blanket impl turns NULL
        // into `Ok(None)` without calling here.
        let bytes = v
            .ok_or_else(|| {
                DeserializationError::new(VectorColumnError("unexpected NULL".to_owned()))
            })?
            .as_slice();

        match typ {
            ColumnType::Vector { .. } => parse_cql_vector(bytes),
            _ => parse_alternator_vector(bytes),
        }
        .map(|floats| Self(Vector(floats)))
        .map_err(|err| DeserializationError::new(VectorColumnError(format!("{err:#}"))))
    }
}

/// Decodes a CQL `vector<float, N>` value: N big-endian `f32`s.
fn parse_cql_vector(bytes: &[u8]) -> anyhow::Result<Vec<f32>> {
    let chunks = bytes.chunks_exact(size_of::<f32>());

    if !chunks.remainder().is_empty() {
        bail!(
            "invalid CQL vector encoding: byte length {} is not a multiple of 4",
            bytes.len(),
        );
    }

    Ok(chunks
        .map(|chunk| {
            let arr: [u8; 4] = chunk.try_into().expect("chunks_exact guarantees 4 bytes");
            f32::from_be_bytes(arr)
        })
        .collect())
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

    fn cql_vector_type(dimensions: u16) -> ColumnType<'static> {
        ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Float)),
            dimensions,
        }
    }

    fn cql_vector_bytes(floats: &[f32]) -> Vec<u8> {
        floats.iter().flat_map(|f| f.to_be_bytes()).collect()
    }

    fn deserialize_column(typ: &ColumnType<'static>, bytes: Option<&[u8]>) -> Option<VectorColumn> {
        VectorColumn::type_check(typ).unwrap();
        VectorColumn::deserialize(typ, bytes.map(FrameSlice::new_borrowed)).ok()
    }

    #[test]
    fn vector_column_reads_a_cql_vector() {
        let typ = cql_vector_type(3);
        let bytes = cql_vector_bytes(&[1.0, 2.5, 3.0]);
        assert_eq!(
            deserialize_column(&typ, Some(&bytes)),
            Some(VectorColumn(Vector::from(vec![1.0, 2.5, 3.0])))
        );
    }

    #[test]
    fn vector_column_reads_an_alternator_blob() {
        let typ = ColumnType::Native(NativeType::Blob);
        let bytes = alternator_vector_blob(&[1.0, 2.5, 3.0]);
        assert_eq!(
            deserialize_column(&typ, Some(&bytes)),
            Some(VectorColumn(Vector::from(vec![1.0, 2.5, 3.0])))
        );

        let json = r#"{"L": [{"N": "123.4"}, {"N": "234.5"}]}"#;
        let bytes = alternator_list_blob(json);
        assert_eq!(
            deserialize_column(&typ, Some(&bytes)),
            Some(VectorColumn(Vector::from(vec![123.4, 234.5])))
        );
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
