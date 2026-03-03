/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

//! A memory-optimized storage for vectors of CQL values.
//!
//! The scylla driver's [`CqlValue`] enum is 72 bytes per element because its largest variant
//! (`UserDefinedType`) stores three heap-allocated fields inline. Most primary key values are
//! small scalars like `Int(i32)` (4 bytes of useful data), wasting ~68 bytes per value.
//!
//! [`InvariantKey`] eliminates this overhead by serializing values into a compact byte buffer.
//! Each value is stored as a 1-byte type tag followed by its minimal binary representation:
//!
//! | Type           | Encoded size | vs CqlValue (72 bytes) |
//! |----------------|--------------|------------------------|
//! | `Int`          | 5 bytes      | 14× smaller            |
//! | `BigInt`       | 9 bytes      | 8× smaller             |
//! | `Uuid`         | 17 bytes     | 4× smaller             |
//! | `Text("abc")`  | 8 bytes      | 9× smaller             |
//!
//! This matters because [`InvariantKey`] is stored in a [`BiMap`](bimap::BiMap) for **every
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
#[cfg(test)]
use std::hash::Hasher;
use std::iter::FusedIterator;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use uuid::Uuid;

// Type tag constants for the compact encoding.
// Fixed-size types have a known data length after the tag.
// Variable-length types (Text, Ascii, Blob) use a length prefix after the tag.
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

/// Size of the leading count byte that stores the number of values.
const COUNT_SIZE: usize = std::mem::size_of::<u8>();

/// Size of a type tag in bytes.
const TAG_SIZE: usize = 1;

/// Size of the length prefix for variable-length types (u32 LE).
const VAR_LEN_SIZE: usize = std::mem::size_of::<u32>();

/// Byte offset where encoded data begins (immediately after the tag byte).
const DATA_OFFSET: usize = TAG_SIZE;

/// Byte offset where variable-length payload begins (after tag + length prefix).
const VAR_DATA_OFFSET: usize = TAG_SIZE + VAR_LEN_SIZE;

/// Size of a single-byte data value (Boolean, TinyInt).
const BYTE_SIZE: usize = std::mem::size_of::<u8>();

/// Size of IPv4 address in bytes.
const IPV4_SIZE: usize = 4;

/// Size of IPv6 address / UUID in bytes.
const UUID_SIZE: usize = 16;

/// A memory-optimized storage for a vector of CQL values.
///
/// Values are serialized into a contiguous byte buffer as:
/// `[count: u8][value₀][value₁]…[valueₙ₋₁]`
///
/// Each value is `[tag: u8][data…]` with minimal encoding per type.
/// Values are decoded on demand via [`get()`](Self::get) or [`iter()`](Self::iter).
///
/// Equality and hashing operate directly on the raw bytes, which is both faster
/// and more correct than the previous `format!("{:?}")` hashing approach.
///
/// The inner buffer is reference-counted via [`Arc`], so cloning is O(1).
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct InvariantKey {
    data: Box<[u8]>,
}

impl InvariantKey {
    /// The maximum number of columns an `InvariantKey` can hold.
    ///
    /// The column count is stored as a single `u8`, so the hard limit is 255.
    pub(crate) const MAX_COLUMNS: usize = u8::MAX as usize;

    /// Encode a vector of CQL values into a compact representation.
    ///
    /// # Panics
    ///
    /// Panics if `values.len() > 255` or if a value has an unsupported CQL type
    /// for primary key columns (e.g., collections, UDTs).
    pub(crate) fn new(values: Vec<CqlValue>) -> Self {
        assert!(
            values.len() <= Self::MAX_COLUMNS,
            "InvariantKey supports at most {} columns, got {}",
            Self::MAX_COLUMNS,
            values.len()
        );

        // SAFETY: length already validated above.
        Self::encode(values)
    }

    /// Fallible version of [`new`](Self::new).
    ///
    /// Returns an error instead of panicking when `values.len() > 255`.
    /// Use this when the input comes from an external source (e.g. ScyllaDB)
    /// where the column count is not under our control.
    #[cfg(test)]
    pub(crate) fn try_new(values: Vec<CqlValue>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            values.len() <= Self::MAX_COLUMNS,
            "InvariantKey supports at most {} columns, got {}",
            Self::MAX_COLUMNS,
            values.len()
        );

        Ok(Self::encode(values))
    }

    /// Shared encoding logic used by both `new` and `try_new`.
    ///
    /// # Precondition
    ///
    /// `values.len() <= 255` — callers must validate before calling.
    pub(crate) fn encode(values: Vec<CqlValue>) -> Self {
        debug_assert!(values.len() <= Self::MAX_COLUMNS);

        let total: usize = COUNT_SIZE + values.iter().map(encoded_size).sum::<usize>();
        let mut buf = Vec::with_capacity(total);
        buf.push(values.len() as u8);
        for value in &values {
            encode_value(&mut buf, value);
        }
        debug_assert_eq!(buf.len(), total);

        InvariantKey {
            data: buf.into_boxed_slice(),
        }
    }

    /// Returns the number of values in this key.
    pub(crate) fn len(&self) -> usize {
        self.data[0] as usize
    }

    /// Returns `true` if this key has no values.
    pub(crate) fn is_empty(&self) -> bool {
        self.data[0] == 0
    }

    /// Decode the value at `index`, returning it as a [`CqlValue`].
    ///
    /// For N values, this scans through the first `index` values to find the
    /// offset (O(index)), then decodes the value. Since primary keys typically
    /// have 1–3 columns, this is effectively O(1).
    pub(crate) fn get(&self, index: usize) -> Option<CqlValue> {
        let count = self.data[0] as usize;
        if index >= count {
            return None;
        }

        let mut offset = COUNT_SIZE; // skip the count byte
        for _ in 0..index {
            offset += skip_value(&self.data[offset..]);
        }
        Some(decode_value(&self.data[offset..]).0)
    }

    /// Compute a hash of the first `n` columns.
    ///
    /// This is useful for partition key hashing, where the partition key is a
    /// prefix of the full primary key.
    ///
    /// **Note:** This produces a different hash than [`Hash::hash`] even when
    /// `n == self.len()`, because it uses `n` as a discriminant instead of the
    /// full buffer (which includes the count byte). The two are intentionally
    /// in separate hash domains.
    ///
    /// # Panics
    ///
    /// Panics if `n > self.len()`.
    #[cfg(test)]
    pub(crate) fn hash_prefix<H: Hasher>(&self, state: &mut H, n: usize) {
        let count = self.data[0] as usize;
        assert!(
            n <= count,
            "hash_prefix({n}) called on InvariantKey with {count} columns"
        );

        // Find the byte range covering exactly the first `n` encoded values.
        let mut offset = COUNT_SIZE; // skip the count byte
        for _ in 0..n {
            offset += skip_value(&self.data[offset..]);
        }
        // Hash the count and the raw bytes of the first n values.
        (n as u8).hash(state);
        self.data[COUNT_SIZE..offset].hash(state);
    }

    /// Iterate over all decoded values.
    pub(crate) fn iter(&self) -> InvariantKeyIter<'_> {
        InvariantKeyIter {
            data: &self.data,
            offset: COUNT_SIZE,
            remaining: self.data[0] as usize,
        }
    }

    /// Format this key as a debug tuple with the given `name`.
    ///
    /// Shared by [`InvariantKey`] and newtype wrappers (e.g. `PrimaryKey`).
    pub(crate) fn debug_fmt(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut t = f.debug_tuple(name);
        for value in self.iter() {
            t.field(&value);
        }
        t.finish()
    }
}

impl FromIterator<CqlValue> for InvariantKey {
    fn from_iter<I: IntoIterator<Item = CqlValue>>(iter: I) -> Self {
        let mut buf = vec![0];
        iter.into_iter().take(Self::MAX_COLUMNS).for_each(|value| {
            encode_value(&mut buf, &value);
            buf[0] += 1;
        });
        Self {
            data: buf.into_boxed_slice(),
        }
    }
}

/// Iterator over the decoded [`CqlValue`]s in an [`InvariantKey`].
pub(crate) struct InvariantKeyIter<'a> {
    data: &'a [u8],
    offset: usize,
    remaining: usize,
}

impl Iterator for InvariantKeyIter<'_> {
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

impl ExactSizeIterator for InvariantKeyIter<'_> {}
impl FusedIterator for InvariantKeyIter<'_> {}

impl fmt::Debug for InvariantKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.debug_fmt(f, "InvariantKey")
    }
}

impl From<Vec<CqlValue>> for InvariantKey {
    fn from(values: Vec<CqlValue>) -> Self {
        Self::new(values)
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn encoded_size(value: &CqlValue) -> usize {
    match value {
        CqlValue::Empty => TAG_SIZE,
        CqlValue::Boolean(_) | CqlValue::TinyInt(_) => TAG_SIZE + BYTE_SIZE,
        CqlValue::SmallInt(_) => TAG_SIZE + std::mem::size_of::<i16>(),
        CqlValue::Int(_) | CqlValue::Float(_) | CqlValue::Date(_) => {
            TAG_SIZE + std::mem::size_of::<i32>()
        }
        CqlValue::BigInt(_)
        | CqlValue::Double(_)
        | CqlValue::Time(_)
        | CqlValue::Timestamp(_)
        | CqlValue::Counter(_) => TAG_SIZE + std::mem::size_of::<i64>(),
        CqlValue::Uuid(_) | CqlValue::Timeuuid(_) => TAG_SIZE + UUID_SIZE,
        CqlValue::Inet(IpAddr::V4(_)) => TAG_SIZE + IPV4_SIZE,
        CqlValue::Inet(IpAddr::V6(_)) => TAG_SIZE + UUID_SIZE,
        CqlValue::Text(s) => TAG_SIZE + VAR_LEN_SIZE + s.len(),
        CqlValue::Ascii(s) => TAG_SIZE + VAR_LEN_SIZE + s.len(),
        CqlValue::Blob(b) => TAG_SIZE + VAR_LEN_SIZE + b.len(),
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
            buf.extend_from_slice(&v.to_le_bytes());
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
            let len: u32 = s
                .len()
                .try_into()
                .expect("Text value too large for InvariantKey encoding");
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        CqlValue::Ascii(s) => {
            buf.push(TAG_ASCII);
            let len: u32 = s
                .len()
                .try_into()
                .expect("Ascii value too large for InvariantKey encoding");
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        CqlValue::Blob(b) => {
            buf.push(TAG_BLOB);
            let len: u32 = b
                .len()
                .try_into()
                .expect("Blob value too large for InvariantKey encoding");
            buf.extend_from_slice(&len.to_le_bytes());
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
        "CqlValue variant not supported for InvariantKey encoding: {:?}. \
         Only scalar CQL types valid for primary key columns are supported.",
        value
    );
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Read a fixed-size byte array starting at [`DATA_OFFSET`].
fn read_fixed<const N: usize>(data: &[u8]) -> [u8; N] {
    data[DATA_OFFSET..DATA_OFFSET + N].try_into().unwrap()
}

/// Read the variable-length payload length from [`DATA_OFFSET`].
fn read_var_len(data: &[u8]) -> usize {
    u32::from_le_bytes(data[DATA_OFFSET..VAR_DATA_OFFSET].try_into().unwrap()) as usize
}

/// Skip over one encoded value, returning the number of bytes consumed.
fn skip_value(data: &[u8]) -> usize {
    match data[0] {
        TAG_EMPTY => TAG_SIZE,
        TAG_BOOLEAN | TAG_TINY_INT => TAG_SIZE + BYTE_SIZE,
        TAG_SMALL_INT => TAG_SIZE + std::mem::size_of::<i16>(),
        TAG_INT | TAG_FLOAT | TAG_DATE => TAG_SIZE + std::mem::size_of::<i32>(),
        TAG_INET_V4 => TAG_SIZE + IPV4_SIZE,
        TAG_BIG_INT | TAG_DOUBLE | TAG_TIME | TAG_TIMESTAMP | TAG_COUNTER => {
            TAG_SIZE + std::mem::size_of::<i64>()
        }
        TAG_UUID | TAG_TIMEUUID | TAG_INET_V6 => TAG_SIZE + UUID_SIZE,
        TAG_TEXT | TAG_ASCII | TAG_BLOB => VAR_DATA_OFFSET + read_var_len(data),
        other => panic!("Unknown tag in InvariantKey data: {other}"),
    }
}

/// Decode one value from the buffer, returning `(value, bytes_consumed)`.
fn decode_value(data: &[u8]) -> (CqlValue, usize) {
    match data[0] {
        TAG_EMPTY => (CqlValue::Empty, TAG_SIZE),

        TAG_BOOLEAN => (
            CqlValue::Boolean(data[DATA_OFFSET] != 0),
            TAG_SIZE + BYTE_SIZE,
        ),

        TAG_TINY_INT => {
            let v = i8::from_le_bytes(read_fixed::<1>(data));
            (CqlValue::TinyInt(v), TAG_SIZE + BYTE_SIZE)
        }

        TAG_SMALL_INT => {
            let v = i16::from_le_bytes(read_fixed::<2>(data));
            (CqlValue::SmallInt(v), TAG_SIZE + std::mem::size_of::<i16>())
        }
        TAG_INT => {
            let v = i32::from_le_bytes(read_fixed::<4>(data));
            (CqlValue::Int(v), TAG_SIZE + std::mem::size_of::<i32>())
        }
        TAG_BIG_INT => {
            let v = i64::from_le_bytes(read_fixed::<8>(data));
            (CqlValue::BigInt(v), TAG_SIZE + std::mem::size_of::<i64>())
        }
        TAG_FLOAT => {
            let v = f32::from_le_bytes(read_fixed::<4>(data));
            (CqlValue::Float(v), TAG_SIZE + std::mem::size_of::<f32>())
        }
        TAG_DOUBLE => {
            let v = f64::from_le_bytes(read_fixed::<8>(data));
            (CqlValue::Double(v), TAG_SIZE + std::mem::size_of::<f64>())
        }

        TAG_TEXT => {
            let len = read_var_len(data);
            let s = String::from_utf8(data[VAR_DATA_OFFSET..VAR_DATA_OFFSET + len].to_vec())
                .expect("invalid UTF-8 in InvariantKey Text value");
            (CqlValue::Text(s), VAR_DATA_OFFSET + len)
        }
        TAG_ASCII => {
            let len = read_var_len(data);
            // ASCII is valid UTF-8
            let s = String::from_utf8(data[VAR_DATA_OFFSET..VAR_DATA_OFFSET + len].to_vec())
                .expect("invalid UTF-8 in InvariantKey Ascii value");
            (CqlValue::Ascii(s), VAR_DATA_OFFSET + len)
        }
        TAG_BLOB => {
            let len = read_var_len(data);
            (
                CqlValue::Blob(data[VAR_DATA_OFFSET..VAR_DATA_OFFSET + len].to_vec()),
                VAR_DATA_OFFSET + len,
            )
        }

        TAG_UUID => {
            let bytes: [u8; 16] = read_fixed::<16>(data);
            (
                CqlValue::Uuid(Uuid::from_bytes(bytes)),
                TAG_SIZE + UUID_SIZE,
            )
        }
        TAG_TIMEUUID => {
            let bytes: [u8; 16] = read_fixed::<16>(data);
            (
                CqlValue::Timeuuid(CqlTimeuuid::from_bytes(bytes)),
                TAG_SIZE + UUID_SIZE,
            )
        }

        TAG_DATE => {
            let v = u32::from_le_bytes(read_fixed::<4>(data));
            (
                CqlValue::Date(CqlDate(v)),
                TAG_SIZE + std::mem::size_of::<u32>(),
            )
        }
        TAG_TIME => {
            let v = i64::from_le_bytes(read_fixed::<8>(data));
            (
                CqlValue::Time(CqlTime(v)),
                TAG_SIZE + std::mem::size_of::<i64>(),
            )
        }
        TAG_TIMESTAMP => {
            let v = i64::from_le_bytes(read_fixed::<8>(data));
            (
                CqlValue::Timestamp(CqlTimestamp(v)),
                TAG_SIZE + std::mem::size_of::<i64>(),
            )
        }

        TAG_INET_V4 => {
            let octets: [u8; 4] = read_fixed::<4>(data);
            (
                CqlValue::Inet(IpAddr::V4(Ipv4Addr::from(octets))),
                TAG_SIZE + IPV4_SIZE,
            )
        }
        TAG_INET_V6 => {
            let octets: [u8; 16] = read_fixed::<16>(data);
            (
                CqlValue::Inet(IpAddr::V6(Ipv6Addr::from(octets))),
                TAG_SIZE + UUID_SIZE,
            )
        }

        TAG_COUNTER => {
            let v = i64::from_le_bytes(read_fixed::<8>(data));
            (
                CqlValue::Counter(Counter(v)),
                TAG_SIZE + std::mem::size_of::<i64>(),
            )
        }

        other => panic!("Unknown tag in InvariantKey data: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Static assertion: InvariantKey must be exactly 16 bytes (Box<[u8]> = ptr + len).
    const _: () = assert!(std::mem::size_of::<InvariantKey>() == 16);

    #[test]
    fn single_int_overhead() {
        let ik = InvariantKey::new(vec![CqlValue::Int(42)]);
        // 1 byte count + 1 byte tag + 4 bytes i32 = 6 bytes on heap
        assert_eq!(ik.data.len(), 6);
    }

    #[test]
    fn roundtrip_int() {
        let ik: InvariantKey = vec![CqlValue::Int(42)].into();
        assert_eq!(ik.len(), 1);
        assert_eq!(ik.get(0), Some(CqlValue::Int(42)));
        assert_eq!(ik.get(1), None);
    }

    #[test]
    fn roundtrip_multiple_columns() {
        let ik: InvariantKey = vec![CqlValue::Int(1), CqlValue::Text("hello".to_string())].into();
        assert_eq!(ik.len(), 2);
        assert_eq!(ik.get(0), Some(CqlValue::Int(1)));
        assert_eq!(ik.get(1), Some(CqlValue::Text("hello".to_string())));
    }

    #[test]
    fn roundtrip_all_scalar_types() {
        let uuid = Uuid::new_v4();
        let values = vec![
            CqlValue::Empty,
            CqlValue::Boolean(true),
            CqlValue::TinyInt(7),
            CqlValue::TinyInt(-128),
            CqlValue::TinyInt(127),
            CqlValue::SmallInt(256),
            CqlValue::SmallInt(-256),
            CqlValue::Int(100_000),
            CqlValue::Int(-100_000),
            CqlValue::BigInt(123_456_789_000),
            CqlValue::BigInt(-123_456_789_000),
            CqlValue::Float(std::f32::consts::PI),
            CqlValue::Float(-std::f32::consts::PI),
            CqlValue::Double(std::f64::consts::E),
            CqlValue::Double(-std::f64::consts::E),
            CqlValue::Text("hello world".to_string()),
            CqlValue::Ascii("ascii".to_string()),
            CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            CqlValue::Uuid(uuid),
            CqlValue::Timeuuid(CqlTimeuuid::from_bytes(*uuid.as_bytes())),
            CqlValue::Date(CqlDate(19000)),
            CqlValue::Time(CqlTime(43_200_000_000_000)),
            CqlValue::Timestamp(CqlTimestamp(1_700_000_000_000)),
            CqlValue::Timestamp(CqlTimestamp(-1_700_000_000_000)),
            CqlValue::Inet(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            CqlValue::Inet(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            CqlValue::Counter(Counter(42)),
        ];
        let ik = InvariantKey::new(values.clone());
        assert_eq!(ik.len(), values.len());
        for (i, expected) in values.into_iter().enumerate() {
            assert_eq!(ik.get(i), Some(expected), "mismatch at index {i}");
        }
    }

    #[test]
    fn equality_and_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;

        let ik1: InvariantKey = vec![CqlValue::Int(42), CqlValue::Text("foo".to_string())].into();
        let ik2: InvariantKey = vec![CqlValue::Int(42), CqlValue::Text("foo".to_string())].into();
        let ik3: InvariantKey = vec![CqlValue::Int(99)].into();

        assert_eq!(ik1, ik2);
        assert_ne!(ik1, ik3);

        let hash = |ik: &InvariantKey| {
            let mut h = DefaultHasher::new();
            ik.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&ik1), hash(&ik2));
        // Hash collision is theoretically possible but practically won't happen here
        assert_ne!(hash(&ik1), hash(&ik3));
    }

    #[test]
    fn hash_prefix_consistency() {
        use std::collections::hash_map::DefaultHasher;

        let ik1: InvariantKey = vec![CqlValue::Int(42), CqlValue::Text("foo".to_string())].into();
        let ik2: InvariantKey = vec![CqlValue::Int(42), CqlValue::Text("bar".to_string())].into();

        // Hash of first column should be the same (both have Int(42))
        let hash_prefix = |ik: &InvariantKey, n: usize| {
            let mut h = DefaultHasher::new();
            ik.hash_prefix(&mut h, n);
            h.finish()
        };
        assert_eq!(hash_prefix(&ik1, 1), hash_prefix(&ik2, 1));

        // Hash of both columns should differ
        assert_ne!(hash_prefix(&ik1, 2), hash_prefix(&ik2, 2));
    }

    #[test]
    fn iter_yields_all_values() {
        let ik: InvariantKey = vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)].into();
        let collected: Vec<_> = ik.iter().collect();
        assert_eq!(
            collected,
            vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)]
        );
    }

    #[test]
    fn debug_format() {
        let ik: InvariantKey = vec![CqlValue::Int(42)].into();
        let dbg = format!("{ik:?}");
        assert!(
            dbg.contains("InvariantKey"),
            "Debug should contain 'InvariantKey': {dbg}"
        );
        assert!(
            dbg.contains("Int(42)"),
            "Debug should contain 'Int(42)': {dbg}"
        );
    }

    #[test]
    fn max_255_columns_is_accepted() {
        let values: Vec<CqlValue> = (0..255).map(CqlValue::Int).collect();
        let ik = InvariantKey::new(values);
        assert_eq!(ik.len(), 255);
    }

    #[test]
    fn try_new_max_255_columns_is_accepted() {
        let values: Vec<CqlValue> = (0..255).map(CqlValue::Int).collect();
        let ik = InvariantKey::try_new(values).unwrap();
        assert_eq!(ik.len(), 255);
    }

    #[test]
    fn try_new_more_than_255_columns_returns_error() {
        let values: Vec<CqlValue> = (0..256).map(CqlValue::Int).collect();
        let err = InvariantKey::try_new(values).unwrap_err();
        assert!(
            err.to_string().contains("at most 255 columns"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    #[should_panic(expected = "InvariantKey supports at most 255 columns")]
    fn more_than_255_columns_panics() {
        let values: Vec<CqlValue> = (0..256).map(CqlValue::Int).collect();
        let _ik = InvariantKey::new(values);
    }
}
