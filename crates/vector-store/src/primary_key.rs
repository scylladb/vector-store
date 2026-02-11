/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

//! A memory-optimized representation of CQL primary keys.
//!
//! The scylla driver's [`CqlValue`] enum is 72 bytes per element because its largest variant
//! (`UserDefinedType`) stores three heap-allocated fields inline. Most primary key values are
//! small scalars like `Int(i32)` (4 bytes of useful data), wasting ~68 bytes per value.
//!
//! [`PrimaryKey`] eliminates this overhead by serializing values into a compact byte buffer.
//! Each value is stored as a 1-byte type tag followed by its minimal binary representation:
//!
//! | Type       | Encoded size | vs CqlValue (72 bytes) |
//! |------------|-------------|------------------------|
//! | `Int`      | 5 bytes     | 14× smaller            |
//! | `BigInt`   | 9 bytes     | 8× smaller             |
//! | `Uuid`     | 17 bytes    | 4× smaller             |
//! | `Text("abc")` | 8 bytes | 9× smaller             |
//!
//! This matters because [`PrimaryKey`] is stored in a [`BiMap`](bimap::BiMap) for **every
//! indexed row** — potentially millions of entries. For a single `Int` primary key column,
//! memory per row drops from ~96 bytes to ~24 bytes (4× improvement).

use scylla::value::Counter;
use scylla::value::CqlDate;
use scylla::value::CqlTime;
use scylla::value::CqlTimestamp;
use scylla::value::CqlTimeuuid;
use scylla::value::CqlValue;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::Arc;
use uuid::Uuid;

// Type tag constants for the compact encoding.
// Fixed-size types have a known data length after the tag.
// Variable-length types (Text, Ascii, Blob) use a 4-byte LE length prefix.
const TAG_EMPTY: u8 = 0;
const TAG_BOOLEAN: u8 = 1;
const TAG_TINY_INT: u8 = 2;
const TAG_SMALL_INT: u8 = 3;
const TAG_INT: u8 = 4;
const TAG_BIG_INT: u8 = 5;
const TAG_FLOAT: u8 = 6;
const TAG_DOUBLE: u8 = 7;
const TAG_TEXT: u8 = 8;
const TAG_ASCII: u8 = 9;
const TAG_UUID: u8 = 10;
const TAG_TIMEUUID: u8 = 11;
const TAG_DATE: u8 = 12;
const TAG_TIME: u8 = 13;
const TAG_TIMESTAMP: u8 = 14;
const TAG_INET_V4: u8 = 15;
const TAG_INET_V6: u8 = 16;
const TAG_COUNTER: u8 = 17;
const TAG_BLOB: u8 = 18;

/// A memory-optimized CQL primary key.
///
/// Values are serialized into a contiguous byte buffer as:
/// `[count: u8][value₀][value₁]…[valueₙ₋₁]`
///
/// Each value is `[tag: u8][data…]` with minimal encoding per type.
/// Values are decoded on demand via [`get()`](Self::get) or [`iter()`](Self::iter).
///
/// Equality and hashing operate directly on the raw bytes, which is both faster
/// and more correct than the previous `format!("{:?}")` hashing approach.
#[derive(Clone)]
pub struct PrimaryKey {
    data: Arc<[u8]>,
}

impl PrimaryKey {
    /// Encode a vector of CQL values into a compact primary key.
    ///
    /// # Panics
    ///
    /// Panics if `values.len() > 255` or if a value has an unsupported CQL type
    /// for primary key columns (e.g., collections, UDTs).
    pub fn new(values: Vec<CqlValue>) -> Self {
        assert!(
            values.len() <= u8::MAX as usize,
            "PrimaryKey supports at most 255 columns, got {}",
            values.len()
        );

        let total: usize = 1 + values.iter().map(encoded_size).sum::<usize>();
        let mut buf = Vec::with_capacity(total);
        buf.push(values.len() as u8);
        for value in &values {
            encode_value(&mut buf, value);
        }
        debug_assert_eq!(buf.len(), total);

        PrimaryKey {
            data: Arc::from(buf),
        }
    }

    /// Returns the number of columns in this primary key.
    pub fn len(&self) -> usize {
        self.data[0] as usize
    }

    /// Returns `true` if this primary key has no columns.
    pub fn is_empty(&self) -> bool {
        self.data[0] == 0
    }

    /// Decode the value at `index`, returning it as a [`CqlValue`].
    ///
    /// For a primary key with N columns, this scans through the first `index`
    /// values to find the offset (O(index)), then decodes the value. Since
    /// primary keys typically have 1–3 columns, this is effectively O(1).
    pub fn get(&self, index: usize) -> Option<CqlValue> {
        let count = self.data[0] as usize;
        if index >= count {
            return None;
        }

        let mut offset = 1; // skip the count byte
        for _ in 0..index {
            offset += skip_value(&self.data[offset..]);
        }
        Some(decode_value(&self.data[offset..]).0)
    }

    /// Iterate over all decoded values in this primary key.
    pub fn iter(&self) -> PrimaryKeyIter<'_> {
        PrimaryKeyIter {
            data: &self.data,
            offset: 1,
            remaining: self.data[0] as usize,
        }
    }
}

/// Iterator over the decoded [`CqlValue`]s in a [`PrimaryKey`].
pub struct PrimaryKeyIter<'a> {
    data: &'a [u8],
    offset: usize,
    remaining: usize,
}

impl Iterator for PrimaryKeyIter<'_> {
    type Item = CqlValue;

    fn next(&mut self) -> Option<CqlValue> {
        if self.remaining == 0 {
            return None;
        }
        let (value, consumed) = decode_value(&self.data[self.offset..]);
        self.offset += consumed;
        self.remaining -= 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for PrimaryKeyIter<'_> {}

impl PartialEq for PrimaryKey {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for PrimaryKey {}

impl Hash for PrimaryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl fmt::Debug for PrimaryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values: Vec<CqlValue> = self.iter().collect();
        f.debug_tuple("PrimaryKey").field(&values).finish()
    }
}

impl From<Vec<CqlValue>> for PrimaryKey {
    fn from(values: Vec<CqlValue>) -> Self {
        Self::new(values)
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn encoded_size(value: &CqlValue) -> usize {
    match value {
        CqlValue::Empty => 1,
        CqlValue::Boolean(_) | CqlValue::TinyInt(_) => 2,
        CqlValue::SmallInt(_) => 3,
        CqlValue::Int(_) | CqlValue::Float(_) | CqlValue::Date(_) => 5,
        CqlValue::BigInt(_)
        | CqlValue::Double(_)
        | CqlValue::Time(_)
        | CqlValue::Timestamp(_)
        | CqlValue::Counter(_) => 9,
        CqlValue::Uuid(_) | CqlValue::Timeuuid(_) => 17,
        CqlValue::Inet(IpAddr::V4(_)) => 5,  // tag + 4 octets
        CqlValue::Inet(IpAddr::V6(_)) => 17, // tag + 16 octets
        CqlValue::Text(s) => 1 + 4 + s.len(),
        CqlValue::Ascii(s) => 1 + 4 + s.len(),
        CqlValue::Blob(b) => 1 + 4 + b.len(),
        _ => unsupported(value),
    }
}

fn encode_value(buf: &mut Vec<u8>, value: &CqlValue) {
    match value {
        CqlValue::Empty => buf.push(TAG_EMPTY),

        CqlValue::Boolean(v) => {
            buf.push(TAG_BOOLEAN);
            buf.push(u8::from(*v));
        }
        CqlValue::TinyInt(v) => {
            buf.push(TAG_TINY_INT);
            buf.push(*v as u8);
        }
        CqlValue::SmallInt(v) => {
            buf.push(TAG_SMALL_INT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        CqlValue::Int(v) => {
            buf.push(TAG_INT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        CqlValue::BigInt(v) => {
            buf.push(TAG_BIG_INT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        CqlValue::Float(v) => {
            buf.push(TAG_FLOAT);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        CqlValue::Double(v) => {
            buf.push(TAG_DOUBLE);
            buf.extend_from_slice(&v.to_le_bytes());
        }

        CqlValue::Text(s) => {
            buf.push(TAG_TEXT);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        CqlValue::Ascii(s) => {
            buf.push(TAG_ASCII);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        CqlValue::Blob(b) => {
            buf.push(TAG_BLOB);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }

        CqlValue::Uuid(v) => {
            buf.push(TAG_UUID);
            buf.extend_from_slice(v.as_bytes());
        }
        CqlValue::Timeuuid(v) => {
            buf.push(TAG_TIMEUUID);
            buf.extend_from_slice(v.as_bytes());
        }

        CqlValue::Date(v) => {
            buf.push(TAG_DATE);
            buf.extend_from_slice(&v.0.to_le_bytes());
        }
        CqlValue::Time(v) => {
            buf.push(TAG_TIME);
            buf.extend_from_slice(&v.0.to_le_bytes());
        }
        CqlValue::Timestamp(v) => {
            buf.push(TAG_TIMESTAMP);
            buf.extend_from_slice(&v.0.to_le_bytes());
        }

        CqlValue::Inet(IpAddr::V4(addr)) => {
            buf.push(TAG_INET_V4);
            buf.extend_from_slice(&addr.octets());
        }
        CqlValue::Inet(IpAddr::V6(addr)) => {
            buf.push(TAG_INET_V6);
            buf.extend_from_slice(&addr.octets());
        }

        CqlValue::Counter(v) => {
            buf.push(TAG_COUNTER);
            buf.extend_from_slice(&v.0.to_le_bytes());
        }

        _ => unsupported(value),
    }
}

#[cold]
fn unsupported(value: &CqlValue) -> ! {
    panic!(
        "CqlValue variant not supported for PrimaryKey encoding: {:?}. \
         Only scalar CQL types valid for primary key columns are supported.",
        value
    );
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Skip over one encoded value, returning the number of bytes consumed.
fn skip_value(data: &[u8]) -> usize {
    match data[0] {
        TAG_EMPTY => 1,
        TAG_BOOLEAN | TAG_TINY_INT => 2,
        TAG_SMALL_INT => 3,
        TAG_INT | TAG_FLOAT | TAG_DATE | TAG_INET_V4 => 5,
        TAG_BIG_INT | TAG_DOUBLE | TAG_TIME | TAG_TIMESTAMP | TAG_COUNTER => 9,
        TAG_UUID | TAG_TIMEUUID | TAG_INET_V6 => 17,
        TAG_TEXT | TAG_ASCII | TAG_BLOB => {
            let len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
            5 + len
        }
        other => panic!("Unknown tag in PrimaryKey data: {other}"),
    }
}

/// Decode one value from the buffer, returning `(value, bytes_consumed)`.
fn decode_value(data: &[u8]) -> (CqlValue, usize) {
    match data[0] {
        TAG_EMPTY => (CqlValue::Empty, 1),

        TAG_BOOLEAN => (CqlValue::Boolean(data[1] != 0), 2),

        TAG_TINY_INT => (CqlValue::TinyInt(data[1] as i8), 2),

        TAG_SMALL_INT => {
            let v = i16::from_le_bytes(data[1..3].try_into().unwrap());
            (CqlValue::SmallInt(v), 3)
        }
        TAG_INT => {
            let v = i32::from_le_bytes(data[1..5].try_into().unwrap());
            (CqlValue::Int(v), 5)
        }
        TAG_BIG_INT => {
            let v = i64::from_le_bytes(data[1..9].try_into().unwrap());
            (CqlValue::BigInt(v), 9)
        }
        TAG_FLOAT => {
            let v = f32::from_le_bytes(data[1..5].try_into().unwrap());
            (CqlValue::Float(v), 5)
        }
        TAG_DOUBLE => {
            let v = f64::from_le_bytes(data[1..9].try_into().unwrap());
            (CqlValue::Double(v), 9)
        }

        TAG_TEXT => {
            let len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
            let s = String::from_utf8(data[5..5 + len].to_vec())
                .expect("invalid UTF-8 in PrimaryKey Text value");
            (CqlValue::Text(s), 5 + len)
        }
        TAG_ASCII => {
            let len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
            // ASCII is valid UTF-8
            let s = String::from_utf8(data[5..5 + len].to_vec())
                .expect("invalid UTF-8 in PrimaryKey Ascii value");
            (CqlValue::Ascii(s), 5 + len)
        }
        TAG_BLOB => {
            let len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
            (CqlValue::Blob(data[5..5 + len].to_vec()), 5 + len)
        }

        TAG_UUID => {
            let bytes: [u8; 16] = data[1..17].try_into().unwrap();
            (CqlValue::Uuid(Uuid::from_bytes(bytes)), 17)
        }
        TAG_TIMEUUID => {
            let bytes: [u8; 16] = data[1..17].try_into().unwrap();
            (CqlValue::Timeuuid(CqlTimeuuid::from_bytes(bytes)), 17)
        }

        TAG_DATE => {
            let v = u32::from_le_bytes(data[1..5].try_into().unwrap());
            (CqlValue::Date(CqlDate(v)), 5)
        }
        TAG_TIME => {
            let v = i64::from_le_bytes(data[1..9].try_into().unwrap());
            (CqlValue::Time(CqlTime(v)), 9)
        }
        TAG_TIMESTAMP => {
            let v = i64::from_le_bytes(data[1..9].try_into().unwrap());
            (CqlValue::Timestamp(CqlTimestamp(v)), 9)
        }

        TAG_INET_V4 => {
            let octets: [u8; 4] = data[1..5].try_into().unwrap();
            (CqlValue::Inet(IpAddr::V4(Ipv4Addr::from(octets))), 5)
        }
        TAG_INET_V6 => {
            let octets: [u8; 16] = data[1..17].try_into().unwrap();
            (CqlValue::Inet(IpAddr::V6(Ipv6Addr::from(octets))), 17)
        }

        TAG_COUNTER => {
            let v = i64::from_le_bytes(data[1..9].try_into().unwrap());
            (CqlValue::Counter(Counter(v)), 9)
        }

        other => panic!("Unknown tag in PrimaryKey data: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_key_struct_is_16_bytes() {
        assert_eq!(
            std::mem::size_of::<PrimaryKey>(),
            16,
            "PrimaryKey should be 16 bytes (Arc<[u8]> = ptr + len)"
        );
    }

    #[test]
    fn single_int_overhead() {
        let pk = PrimaryKey::new(vec![CqlValue::Int(42)]);
        // 1 byte count + 1 byte tag + 4 bytes i32 = 6 bytes on heap
        assert_eq!(pk.data.len(), 6);
    }

    #[test]
    fn roundtrip_int() {
        let pk: PrimaryKey = vec![CqlValue::Int(42)].into();
        assert_eq!(pk.len(), 1);
        assert_eq!(pk.get(0), Some(CqlValue::Int(42)));
        assert_eq!(pk.get(1), None);
    }

    #[test]
    fn roundtrip_multiple_columns() {
        let pk: PrimaryKey = vec![CqlValue::Int(1), CqlValue::Text("hello".to_string())].into();
        assert_eq!(pk.len(), 2);
        assert_eq!(pk.get(0), Some(CqlValue::Int(1)));
        assert_eq!(pk.get(1), Some(CqlValue::Text("hello".to_string())));
    }

    #[test]
    fn roundtrip_all_scalar_types() {
        let uuid = Uuid::new_v4();
        let values = vec![
            CqlValue::Empty,
            CqlValue::Boolean(true),
            CqlValue::TinyInt(7),
            CqlValue::SmallInt(256),
            CqlValue::Int(100_000),
            CqlValue::BigInt(123_456_789_000),
            CqlValue::Float(std::f32::consts::PI),
            CqlValue::Double(std::f64::consts::E),
            CqlValue::Text("hello world".to_string()),
            CqlValue::Ascii("ascii".to_string()),
            CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            CqlValue::Uuid(uuid),
            CqlValue::Timeuuid(CqlTimeuuid::from_bytes(*uuid.as_bytes())),
            CqlValue::Date(CqlDate(19000)),
            CqlValue::Time(CqlTime(43_200_000_000_000)),
            CqlValue::Timestamp(CqlTimestamp(1_700_000_000_000)),
            CqlValue::Inet(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            CqlValue::Inet(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            CqlValue::Counter(Counter(42)),
        ];
        let pk = PrimaryKey::new(values.clone());
        assert_eq!(pk.len(), values.len());
        for (i, expected) in values.into_iter().enumerate() {
            assert_eq!(pk.get(i), Some(expected), "mismatch at index {i}");
        }
    }

    #[test]
    fn equality_and_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;

        let pk1: PrimaryKey = vec![CqlValue::Int(42), CqlValue::Text("foo".to_string())].into();
        let pk2: PrimaryKey = vec![CqlValue::Int(42), CqlValue::Text("foo".to_string())].into();
        let pk3: PrimaryKey = vec![CqlValue::Int(99)].into();

        assert_eq!(pk1, pk2);
        assert_ne!(pk1, pk3);

        let hash = |pk: &PrimaryKey| {
            let mut h = DefaultHasher::new();
            pk.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&pk1), hash(&pk2));
        // Hash collision is theoretically possible but practically won't happen here
        assert_ne!(hash(&pk1), hash(&pk3));
    }

    #[test]
    fn iter_yields_all_values() {
        let pk: PrimaryKey = vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)].into();
        let collected: Vec<_> = pk.iter().collect();
        assert_eq!(
            collected,
            vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)]
        );
    }

    #[test]
    fn debug_format() {
        let pk: PrimaryKey = vec![CqlValue::Int(42)].into();
        let dbg = format!("{pk:?}");
        assert!(
            dbg.contains("PrimaryKey"),
            "Debug should contain 'PrimaryKey': {dbg}"
        );
        assert!(
            dbg.contains("Int(42)"),
            "Debug should contain 'Int(42)': {dbg}"
        );
    }

    #[test]
    fn memory_savings_vs_cqlvalue() {
        // CqlValue is 72 bytes. For a single Int PK:
        // Old: Vec<CqlValue> = 24 + 72 = 96 bytes
        // New: Box<[u8]> = 16 bytes on stack + 6 bytes heap = 22 bytes
        let cqlvalue_size = std::mem::size_of::<CqlValue>();
        let pk_size = std::mem::size_of::<PrimaryKey>();

        assert!(
            cqlvalue_size >= 48,
            "CqlValue should be at least 48 bytes, got {cqlvalue_size}"
        );
        assert_eq!(pk_size, 16, "PrimaryKey should be 16 bytes");
    }
}
