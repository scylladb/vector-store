/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Dimensions;
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
            CqlValue::Blob(bytes) => parse_alternator_vector(&bytes),
            other => Err(anyhow!(
                "unsupported CQL type for embedding column: {other:?}"
            )),
        }?;
        Ok(Self(floats))
    }
}

/// Alternator type tag for unoptimized JSON encoding.
/// Type `0x04` (`NOT_SUPPORTED_YET`) is used for any type that does not have an optimized encoding.
/// The payload is an unoptimized JSON value.
const ALTERNATOR_TYPE_JSON: u8 = 4;

/// Alternator type tag for the optimized `FLOAT32VECTOR` type.
/// The value is serialized as this 1-byte tag followed by sequential 32-bit big-endian floats,
/// matching the CQL `VECTOR<float, N>` on-wire encoding.
const ALTERNATOR_TYPE_FLOAT32VECTOR: u8 = 5;

/// Parses an Alternator-encoded vector stored as raw bytes.
///
/// Alternator prefixes each attribute value in the `:attrs` map column with a 1-byte type discriminator.
/// Handles two representations based on the discriminator:
/// - [`ALTERNATOR_TYPE_FLOAT32VECTOR`]: optimized sequential 32-bit big-endian floats.
/// - [`ALTERNATOR_TYPE_JSON`]: unoptimized JSON representing List values.
fn parse_alternator_vector(bytes: &[u8]) -> anyhow::Result<Vec<f32>> {
    match bytes.first() {
        Some(&ALTERNATOR_TYPE_FLOAT32VECTOR) => parse_alternator_vector_binary(&bytes[1..]),
        Some(&ALTERNATOR_TYPE_JSON) => parse_alternator_list_json(&bytes[1..]),
        Some(tag) => bail!("unsupported Alternator type tag: {tag:#04x}"),
        None => bail!("empty blob for Alternator attribute value"),
    }
}

/// Parses the optimized Alternator vector encoding: sequential 32-bit big-endian floats.
fn parse_alternator_vector_binary(bytes: &[u8]) -> anyhow::Result<Vec<f32>> {
    let chunks = bytes.chunks_exact(4);

    if !chunks.remainder().is_empty() {
        bail!(
            "invalid Alternator vector encoding: byte length {} is not a multiple of 4",
            bytes.len()
        );
    }

    Ok(chunks
        .map(|chunk| {
            let arr: [u8; 4] = chunk.try_into().expect("chunks_exact guarantees 4 bytes");
            f32::from_be_bytes(arr)
        })
        .collect())
}

/// Parses an Alternator JSON list of numbers: `{"L": [{"N": "..."}, ...]}`.
fn parse_alternator_list_json(bytes: &[u8]) -> anyhow::Result<Vec<f32>> {
    #[derive(serde::Deserialize)]
    struct DynamoDbList {
        #[serde(rename = "L")]
        l: Vec<DynamoDbNumber>,
    }

    #[derive(serde::Deserialize)]
    struct DynamoDbNumber {
        #[serde(rename = "N")]
        n: String,
    }

    let list: DynamoDbList = serde_json::from_slice(bytes)?;
    list.l
        .into_iter()
        .map(|item| {
            item.n
                .parse::<f32>()
                .map_err(|e| anyhow!("invalid value in Alternator list element: {e}"))
        })
        .collect()
}

pub(crate) struct AlternatorAttrs<'a> {
    pub attrs: CqlValue,
    pub target_column: &'a str,
}

/// Extracts a vector from the Alternator `:attrs` map column.
///
/// In Alternator, non-key attributes are stored in a `map<bytes, bytes>` column named `:attrs`.
/// Each entry's key is the attribute name and the value is a serialised attribute prefixed with a 1-byte type tag.
impl TryFrom<AlternatorAttrs<'_>> for Option<Vector> {
    type Error = anyhow::Error;

    fn try_from(input: AlternatorAttrs<'_>) -> anyhow::Result<Self> {
        let AlternatorAttrs {
            attrs,
            target_column,
        } = input;
        let CqlValue::Map(entries) = attrs else {
            bail!("expected Map for :attrs column, got {attrs:?}");
        };

        let target = target_column.as_bytes();

        entries
            .into_iter()
            .find_map(|(key, value)| {
                let matches = match &key {
                    CqlValue::Blob(b) => b.as_slice() == target,
                    CqlValue::Text(s) => s.as_bytes() == target,
                    _ => false,
                };
                matches.then_some(value)
            })
            .map(Vector::try_from)
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn extract_from_attrs_map_with_blob_keys() {
        let json = r#"{"L": [{"N": "1.0"}, {"N": "2.0"}]}"#;
        let attrs = CqlValue::Map(vec![
            (
                CqlValue::Blob(b"other".to_vec()),
                CqlValue::Blob(alternator_list_blob(r#"{"S": "ignored"}"#)),
            ),
            (
                CqlValue::Blob(b"v".to_vec()),
                CqlValue::Blob(alternator_list_blob(json)),
            ),
        ]);
        let result = Option::<Vector>::try_from(AlternatorAttrs {
            attrs,
            target_column: "v",
        })
        .unwrap();
        assert_eq!(result, Some(Vector::from(vec![1.0, 2.0])));
    }

    #[test]
    fn extract_from_attrs_map_with_text_keys() {
        let json = r#"{"L": [{"N": "3.0"}]}"#;
        let attrs = CqlValue::Map(vec![(
            CqlValue::Text("v".to_string()),
            CqlValue::Blob(alternator_list_blob(json)),
        )]);
        let result = Option::<Vector>::try_from(AlternatorAttrs {
            attrs,
            target_column: "v",
        })
        .unwrap();
        assert_eq!(result, Some(Vector::from(vec![3.0])));
    }

    #[test]
    fn extract_from_attrs_map_missing_target() {
        let attrs = CqlValue::Map(vec![(
            CqlValue::Blob(b"other".to_vec()),
            CqlValue::Blob(b"data".to_vec()),
        )]);
        let result = Option::<Vector>::try_from(AlternatorAttrs {
            attrs,
            target_column: "v",
        })
        .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn extract_from_attrs_non_map() {
        let attrs = CqlValue::Int(42);
        assert!(
            Option::<Vector>::try_from(AlternatorAttrs {
                attrs,
                target_column: "v"
            })
            .is_err()
        );
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

    #[test]
    fn extract_from_attrs_map_alternator_vector() {
        let attrs = CqlValue::Map(vec![
            (
                CqlValue::Blob(b"other".to_vec()),
                CqlValue::Blob(alternator_list_blob(r#"{"S": "ignored"}"#)),
            ),
            (
                CqlValue::Blob(b"v".to_vec()),
                CqlValue::Blob(alternator_vector_blob(&[1.0, 2.0, 3.0])),
            ),
        ]);
        let result = Option::<Vector>::try_from(AlternatorAttrs {
            attrs,
            target_column: "v",
        })
        .unwrap();
        assert_eq!(result, Some(Vector::from(vec![1.0, 2.0, 3.0])));
    }
}
