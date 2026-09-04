/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Decoding for Alternator's `:attrs` map column values.
//!
//! Alternator prefixes each attribute value in the `:attrs` map column with
//! a 1-byte type discriminator, then the value's payload in whatever
//! encoding that type uses. This module decodes both representations this
//! crate needs: embedding vectors (see [`crate::vector::Vector`]) and
//! scalar filtering/partition-key attribute values (see
//! [`crate::IndexMetadata::alternator_attribute_types`]).

use anyhow::anyhow;
use anyhow::bail;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::deserialize::FrameSlice;
use scylla::deserialize::value::DeserializeValue;
use scylla::value::CqlValue;

// Alternator type tags. These match `enum class alternator_type` in
// alternator/serialization.hh - do not reorder, these values are written to
// disk as part of the item encoding.
/// String (S) values: tag followed by the raw UTF-8 bytes.
pub(crate) const ALTERNATOR_TYPE_S: u8 = 0;
/// Bytes (B) values: tag followed by the raw bytes, as-is.
pub(crate) const ALTERNATOR_TYPE_B: u8 = 1;
/// Number (N) values: tag followed by the same wire encoding CQL's native
/// `decimal` type uses (a 4-byte big-endian scale followed by a
/// variable-length signed big-endian magnitude).
pub(crate) const ALTERNATOR_TYPE_N: u8 = 3;
/// Alternator type tag for unoptimized JSON encoding.
/// Type `0x04` (`NOT_SUPPORTED_YET`) is used for any type that does not have an optimized encoding.
/// The payload is an unoptimized JSON value.
pub(crate) const ALTERNATOR_TYPE_JSON: u8 = 4;

/// Alternator type tag for the optimized `FLOAT32VECTOR` type.
/// The value is serialized as this 1-byte tag followed by sequential 32-bit big-endian floats,
/// matching the CQL `VECTOR<float, N>` on-wire encoding.
pub(crate) const ALTERNATOR_TYPE_FLOAT32VECTOR: u8 = 5;

/// Decodes an Alternator-encoded scalar attribute value (a filtering or
/// partition-key column with no real CQL column) into the `CqlValue`
/// matching `native_type`. Like the vector column below, the value is
/// tagged; this checks the tag matches `native_type` and re-deserializes
/// the rest via the CQL driver, rather than reimplementing e.g. decimal
/// parsing here.
pub(crate) fn parse_alternator_scalar(
    bytes: &[u8],
    native_type: &NativeType,
) -> anyhow::Result<CqlValue> {
    let expected_tag = match native_type {
        NativeType::Text | NativeType::Ascii => ALTERNATOR_TYPE_S,
        NativeType::Blob => ALTERNATOR_TYPE_B,
        NativeType::Decimal => ALTERNATOR_TYPE_N,
        other => bail!(
            "unsupported native type {other:?} for an Alternator filtering/partition-key attribute"
        ),
    };
    let Some((&tag, payload)) = bytes.split_first() else {
        bail!("empty blob for Alternator attribute value");
    };
    if tag != expected_tag {
        bail!(
            "Alternator attribute value has type tag {tag:#04x}, but column is declared as \
            {native_type:?} (expected tag {expected_tag:#04x})"
        );
    }
    let column_type = ColumnType::Native(native_type.clone());
    CqlValue::deserialize(&column_type, Some(FrameSlice::new_borrowed(payload))).map_err(|err| {
        anyhow!("failed to decode Alternator attribute value as {native_type:?}: {err}")
    })
}

/// Parses an Alternator-encoded vector stored as raw bytes.
///
/// Alternator prefixes each attribute value in the `:attrs` map column with a 1-byte type discriminator.
/// Handles two representations based on the discriminator:
/// - [`ALTERNATOR_TYPE_FLOAT32VECTOR`]: optimized sequential 32-bit big-endian floats.
/// - [`ALTERNATOR_TYPE_JSON`]: unoptimized JSON representing List values.
pub(crate) fn parse_alternator_vector(bytes: &[u8]) -> anyhow::Result<Vec<f32>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::value::CqlDecimal;

    #[test]
    fn parse_alternator_scalar_valid_text() {
        let mut bytes = vec![ALTERNATOR_TYPE_S];
        bytes.extend_from_slice(b"hello");
        let result = parse_alternator_scalar(&bytes, &NativeType::Text).unwrap();
        assert_eq!(result, CqlValue::Text("hello".to_string()));
    }

    #[test]
    fn parse_alternator_scalar_valid_blob() {
        let mut bytes = vec![ALTERNATOR_TYPE_B];
        bytes.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let result = parse_alternator_scalar(&bytes, &NativeType::Blob).unwrap();
        assert_eq!(result, CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    #[test]
    fn parse_alternator_scalar_valid_number() {
        // 123 encoded as unscaled magnitude 0x7B with scale 0.
        let decimal = CqlDecimal::from_signed_be_bytes_and_exponent(vec![0x7B], 0);
        let (varint_bytes, scale) = decimal.as_signed_be_bytes_slice_and_exponent();
        let mut bytes = vec![ALTERNATOR_TYPE_N];
        bytes.extend_from_slice(&scale.to_be_bytes());
        bytes.extend_from_slice(varint_bytes);
        let result = parse_alternator_scalar(&bytes, &NativeType::Decimal).unwrap();
        assert_eq!(result, CqlValue::Decimal(decimal));
    }

    #[test]
    fn parse_alternator_scalar_empty() {
        assert!(parse_alternator_scalar(&[], &NativeType::Text).is_err());
    }

    #[test]
    fn parse_alternator_scalar_mismatched_tag() {
        // Tagged as a string (S), but the column is declared as Blob (B).
        let mut bytes = vec![ALTERNATOR_TYPE_S];
        bytes.extend_from_slice(b"hello");
        assert!(parse_alternator_scalar(&bytes, &NativeType::Blob).is_err());
    }

    #[test]
    fn parse_alternator_scalar_malformed_number() {
        // Tag matches, but only 2 bytes follow - not enough for the 4-byte scale.
        let bytes = vec![ALTERNATOR_TYPE_N, 0x00, 0x00];
        assert!(parse_alternator_scalar(&bytes, &NativeType::Decimal).is_err());
    }

    #[test]
    fn parse_alternator_scalar_malformed_text() {
        // Tag matches, but the payload is not valid UTF-8.
        let bytes = vec![ALTERNATOR_TYPE_S, 0xFF, 0xFE];
        assert!(parse_alternator_scalar(&bytes, &NativeType::Text).is_err());
    }

    #[test]
    fn parse_alternator_scalar_unsupported_native_type() {
        let bytes = vec![ALTERNATOR_TYPE_S, b'x'];
        assert!(parse_alternator_scalar(&bytes, &NativeType::Int).is_err());
    }
}
