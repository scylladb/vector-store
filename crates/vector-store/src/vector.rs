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
            CqlValue::Blob(bytes) => parse_dynamodb_vector_json(&bytes),
            other => Err(anyhow!(
                "unsupported CQL type for embedding column: {other:?}"
            )),
        }?;
        Ok(Self(floats))
    }
}

/// Alternator type tag for the DynamoDB List type (`L`), which is how vector embeddings are serialised.
/// Alternator prefixes each attribute value in the `:attrs` map column with a 1-byte type discriminator.
/// The List type uses the tag value `0x04` (named `NOT_SUPPORTED_YET`)
const ALTERNATOR_TYPE_NOT_SUPPORTED_YET: u8 = 4;

/// Parses a DynamoDB-style JSON vector stored as raw bytes.
///
/// Handles two representations:
/// - Plain JSON: `{"L": [{"N": "123.4"}, {"N": "234.5"}, ...]}`
/// - Alternator-prefixed: a 1-byte type tag (`0x04`) followed by the JSON above.
///   This prefix is used by Alternator for attribute values in the `:attrs` map.
fn parse_dynamodb_vector_json(bytes: &[u8]) -> anyhow::Result<Vec<f32>> {
    let bytes = match bytes.first() {
        Some(&ALTERNATOR_TYPE_NOT_SUPPORTED_YET) => &bytes[1..],
        _ => bytes,
    };

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
                .map_err(|e| anyhow!("invalid value in DynamoDB vector element: {e}"))
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
        let value = CqlValue::Blob(json.as_bytes().to_vec());
        let result = Vector::try_from(value).unwrap();
        assert_eq!(result, Vector::from(vec![123.4, 234.5, 345.6]));
    }

    #[test]
    fn extract_from_dynamodb_json_empty_list() {
        let json = r#"{"L": []}"#;
        let value = CqlValue::Blob(json.as_bytes().to_vec());
        let result = Vector::try_from(value).unwrap();
        assert_eq!(result, Vector::from(vec![]));
    }

    #[test]
    fn extract_from_dynamodb_json_invalid_number() {
        let json = r#"{"L": [{"N": "not_a_number"}]}"#;
        let value = CqlValue::Blob(json.as_bytes().to_vec());
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

    /// Helper: prepend the Alternator `NOT_SUPPORTED_YET` tag (0x04) to a
    /// DynamoDB JSON string, mirroring how Alternator serialises List values.
    fn alternator_blob(json: &str) -> Vec<u8> {
        let mut v = vec![ALTERNATOR_TYPE_NOT_SUPPORTED_YET];
        v.extend_from_slice(json.as_bytes());
        v
    }

    #[test]
    fn extract_from_attrs_map_with_blob_keys() {
        let json = r#"{"L": [{"N": "1.0"}, {"N": "2.0"}]}"#;
        let attrs = CqlValue::Map(vec![
            (
                CqlValue::Blob(b"other".to_vec()),
                CqlValue::Blob(alternator_blob(r#"{"S": "ignored"}"#)),
            ),
            (
                CqlValue::Blob(b"v".to_vec()),
                CqlValue::Blob(alternator_blob(json)),
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
            CqlValue::Blob(alternator_blob(json)),
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
}
