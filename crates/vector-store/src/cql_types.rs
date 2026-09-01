/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use anyhow::anyhow;
use anyhow::bail;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use regex::Regex;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlDecimal;
use scylla::value::CqlDecimalBorrowed;
use scylla::value::CqlTimeuuid;
use scylla::value::CqlValue;
use scylla::value::CqlVarint;
use scylla::value::CqlVarintBorrowed;
use serde_json::Number;
use serde_json::Value;
use std::cmp::Ordering;
use std::net::IpAddr;
use std::num::NonZero;
use std::sync::LazyLock;
use time::Date;
use time::OffsetDateTime;
use time::Time;
use time::format_description::well_known::Iso8601;
use time::format_description::well_known::iso8601::Config;
use time::format_description::well_known::iso8601::TimePrecision;
use uuid::Uuid;

pub(crate) const SUPPORTED: &[NativeType] = &[
    NativeType::Ascii,
    NativeType::BigInt,
    NativeType::Blob,
    NativeType::Boolean,
    NativeType::Date,
    NativeType::Decimal,
    NativeType::Double,
    NativeType::Float,
    NativeType::Inet,
    NativeType::Int,
    NativeType::SmallInt,
    NativeType::Text,
    NativeType::Time,
    NativeType::Timestamp,
    NativeType::Timeuuid,
    NativeType::TinyInt,
    NativeType::Uuid,
    NativeType::Varint,
];

pub(crate) fn is_supported(column_type: &ColumnType) -> bool {
    matches!(column_type, ColumnType::Native(typ) if SUPPORTED.contains(typ))
}

pub(crate) fn to_json(value: CqlValue) -> anyhow::Result<Value> {
    match value {
        CqlValue::Ascii(value) => Ok(Value::String(value)),
        CqlValue::Text(value) => Ok(Value::String(value)),

        CqlValue::Boolean(value) => Ok(Value::Bool(value)),

        CqlValue::Double(value) => {
            Ok(Value::Number(Number::from_f64(value).ok_or_else(|| {
                anyhow!("CqlValue::Double should be finite")
            })?))
        }
        CqlValue::Float(value) => Ok(Value::Number(
            Number::from_f64(value.into())
                .ok_or_else(|| anyhow!("CqlValue::Float should be finite"))?,
        )),

        CqlValue::Int(value) => Ok(Value::Number(value.into())),
        CqlValue::BigInt(value) => Ok(Value::Number(value.into())),
        CqlValue::SmallInt(value) => Ok(Value::Number(value.into())),
        CqlValue::TinyInt(value) => Ok(Value::Number(value.into())),

        CqlValue::Uuid(value) => Ok(Value::String(value.into())),
        CqlValue::Timeuuid(value) => Ok(Value::String((*value.as_ref()).into())),

        CqlValue::Date(value) => Ok(Value::String(
            TryInto::<Date>::try_into(value)?.format(&Iso8601::DATE)?,
        )),
        CqlValue::Time(value) => Ok(Value::String(
            TryInto::<Time>::try_into(value)?
                .format(&Iso8601::TIME)?
                .strip_prefix("T")
                .ok_or_else(|| anyhow!("CqlValue::Time: wrong formatting detected"))?
                .to_string(), // remove 'T' prefix added by time crate
        )),
        CqlValue::Timestamp(value) => Ok(Value::String(
            TryInto::<OffsetDateTime>::try_into(value)?.format({
                const CONFIG: u128 = Config::DEFAULT
                    .set_time_precision(TimePrecision::Second {
                        decimal_digits: NonZero::new(3),
                    })
                    .encode();
                &Iso8601::<CONFIG>
            })?,
        )),

        CqlValue::Blob(value) => Ok(Value::String(const_hex::encode_prefixed(&value))),

        CqlValue::Varint(value) => Ok(Value::String(BigInt::from(value).to_string())),

        CqlValue::Decimal(value) => Ok(Value::String(BigDecimal::from(value).to_string())),

        CqlValue::Inet(value) => Ok(Value::String(value.to_string())),

        CqlValue::Empty => {
            bail!("a primary key column holds an empty value, which has no JSON representation")
        }

        _ => bail!("unsupported CQL type for a primary key column"),
    }
}

pub(crate) fn from_json(value: Value, cql_type: &NativeType) -> anyhow::Result<CqlValue> {
    match value {
        Value::String(value) => match cql_type {
            NativeType::Ascii => Ok(CqlValue::Ascii(value)),
            NativeType::Text => Ok(CqlValue::Text(value)),
            NativeType::Uuid => {
                let uuid = value
                    .parse()
                    .map_err(|err| anyhow!("Failed to parse UUID from string '{value}': {err}"))?;
                Ok(CqlValue::Uuid(uuid))
            }
            NativeType::Timeuuid => {
                let timeuuid: CqlTimeuuid = value.parse().map_err(|err| {
                    anyhow!("Failed to parse TimeUUID from string '{value}': {err}")
                })?;
                Ok(CqlValue::Timeuuid(timeuuid))
            }
            NativeType::Date => {
                let date = Date::parse(&value, &Iso8601::DATE)
                    .map_err(|err| anyhow!("Failed to parse Date from string '{value}': {err}"))?;
                Ok(CqlValue::Date(date.into()))
            }
            NativeType::Time => {
                let time = Time::parse(value.strip_prefix("T").unwrap_or(&value), &Iso8601::TIME)
                    .map_err(|err| {
                    anyhow!("Failed to parse Time from string '{value}': {err}")
                })?;
                Ok(CqlValue::Time(time.into()))
            }
            NativeType::Timestamp => {
                // CQL timestamps may use a space as the date-time separator
                // (e.g. '2024-01-01 00:00:00.000Z'), but ISO 8601 requires 'T'.
                // Only normalize when the space occurs at the expected date-time
                // boundary after a YYYY-MM-DD prefix; otherwise, leave the input
                // unchanged so that error reporting reflects the original value.
                static CQL_TIMESTAMP_RE: LazyLock<Regex> =
                    LazyLock::new(|| Regex::new(r"^(\d{4}-\d{2}-\d{2}) ").expect("valid regex"));
                let normalized = CQL_TIMESTAMP_RE.replace(&value, "${1}T");
                let datetime = OffsetDateTime::parse(&normalized, {
                    const CONFIG: u128 = Config::DEFAULT
                        .set_time_precision(TimePrecision::Second {
                            decimal_digits: NonZero::new(3),
                        })
                        .encode();
                    &Iso8601::<CONFIG>
                })
                .map_err(|err| anyhow!("Failed to parse Timestamp from string '{value}': {err}"))?;
                Ok(CqlValue::Timestamp(datetime.into()))
            }
            NativeType::Blob => {
                if !value.starts_with("0x") {
                    bail!("Blob value must be a '0x'-prefixed hex string");
                }
                let bytes = const_hex::decode(&value)
                    .map_err(|err| anyhow!("Invalid hex in blob value: {err}"))?;
                Ok(CqlValue::Blob(bytes))
            }
            NativeType::Varint => {
                let bi: BigInt = value.parse().map_err(|err| {
                    anyhow!("Failed to parse Varint from string '{value}': {err}")
                })?;
                Ok(CqlValue::Varint(CqlVarint::from(bi)))
            }
            NativeType::Decimal => {
                let bd: BigDecimal = value.parse().map_err(|err| {
                    anyhow!("Failed to parse Decimal from string '{value}': {err}")
                })?;
                Ok(CqlValue::Decimal(CqlDecimal::try_from(bd).map_err(
                    |err| anyhow!("Decimal value out of range: {err}"),
                )?))
            }
            NativeType::Inet => {
                let addr: IpAddr = value
                    .parse()
                    .map_err(|err| anyhow!("Failed to parse Inet from string '{value}': {err}"))?;
                Ok(CqlValue::Inet(addr))
            }
            _ => bail!("Cannot convert string to CqlValue::{cql_type:?}, unsupported type"),
        },

        Value::Bool(value) => match cql_type {
            NativeType::Boolean => Ok(CqlValue::Boolean(value)),
            _ => bail!("Cannot convert bool to CqlValue::{cql_type:?}, unsupported type"),
        },
        Value::Number(value) => match cql_type {
            NativeType::Double => {
                Ok(CqlValue::Double(value.as_f64().ok_or_else(|| {
                    anyhow!("Expected f64 for CqlValue::Double")
                })?))
            }
            NativeType::Float => {
                Ok(CqlValue::Float({
                    // there is no TryFrom<f64> for f32, so we use explicit conversion
                    let value = value
                        .as_f64()
                        .ok_or_else(|| anyhow!("Expected f32 (type f64) for CqlValue::Float"))?;
                    if !value.is_finite() || value < f32::MIN as f64 || value > f32::MAX as f64 {
                        bail!("Expected f32 for CqlValue::Float: value out of range");
                    }
                    let value = value as f32;
                    if !value.is_finite() {
                        bail!("Expected finite f32 for CqlValue::Float");
                    }
                    value
                }))
            }
            NativeType::Int => Ok(CqlValue::Int(
                value
                    .as_i64()
                    .ok_or_else(|| anyhow!("Expected i32 (type i64) for CqlValue::Int"))?
                    .try_into()
                    .map_err(|err| anyhow!("Expected i32 for CqlValue::Int: {err}"))?,
            )),
            NativeType::BigInt => {
                Ok(CqlValue::BigInt(value.as_i64().ok_or_else(|| {
                    anyhow!("Expected i64 for CqlValue::BigInt")
                })?))
            }
            NativeType::SmallInt => Ok(CqlValue::SmallInt(
                value
                    .as_i64()
                    .ok_or_else(|| anyhow!("Expected i16 (type i64) for CqlValue::SmallInt"))?
                    .try_into()
                    .map_err(|err| anyhow!("Expected i16 for CqlValue::SmallInt: {err}"))?,
            )),
            NativeType::TinyInt => Ok(CqlValue::TinyInt(
                value
                    .as_i64()
                    .ok_or_else(|| anyhow!("Expected i8 (type i64) for CqlValue::TinyInt"))?
                    .try_into()
                    .map_err(|err| anyhow!("Expected i8 for CqlValue::TinyInt: {err}"))?,
            )),
            NativeType::Varint => {
                // Varint is always an integer; reject fractional JSON numbers.
                let s = value.to_string();
                let bi: BigInt = s
                    .parse()
                    .map_err(|err| anyhow!("Failed to parse Varint from number '{s}': {err}"))?;
                Ok(CqlValue::Varint(CqlVarint::from(bi)))
            }
            NativeType::Decimal => {
                let s = value.to_string();
                let bd: BigDecimal = s
                    .parse()
                    .map_err(|err| anyhow!("Failed to parse Decimal from number '{s}': {err}"))?;
                Ok(CqlValue::Decimal(CqlDecimal::try_from(bd).map_err(
                    |err| anyhow!("Decimal value out of range: {err}"),
                )?))
            }
            _ => bail!("Cannot convert number to CqlValue::{cql_type:?}, unsupported type"),
        },

        _ => {
            bail!("Cannot convert JSON value '{value}' to CqlValue::{cql_type:?}, unsupported type")
        }
    }
}

/// Orders two regular (non-time) UUIDs the way ScyllaDB's `uuid_type_impl`
/// comparator does: by version nibble first; version-1 (time-based) UUIDs
/// then compare by reassembled timestamp and, on a timestamp tie, by
/// bytes 8..16 as plain unsigned bytes (`utils::uuid_tri_compare_timeuuid`
/// in UUID.hh); everything else compares by unsigned bytes.
///
/// The timestamp tie-break is deliberately NOT `CqlTimeuuid: Ord` /
/// `a.cmp(b)` on the raw bytes: those match `utils::timeuuid_tri_compare`,
/// which is the *TIMEUUID-column* comparator - it XORs bytes 8..16 with
/// 0x80 (a signed-byte compare, kept for legacy Cassandra sstable
/// ordering) before comparing. That disagrees with the plain-UUID-column
/// comparator above whenever two version-1 UUIDs share a timestamp and
/// their tie-break bytes straddle the 0x7f/0x80 boundary - confirmed
/// against a live scylladb/scylla:2026.4.0 (see the pinned unit test
/// below and the boundary case in ann_filter_by_uuid_column_ordering_matches_scylla).
///
/// Plain `uuid::Uuid: Ord` is a raw 128-bit byte compare and would
/// disagree with ScyllaDB too.
fn uuid_cmp(a: &Uuid, b: &Uuid) -> Ordering {
    let a = a.as_bytes();
    let b = b.as_bytes();
    let va = (a[6] >> 4) & 0x0f;
    let vb = (b[6] >> 4) & 0x0f;
    if va != vb {
        return va.cmp(&vb);
    }
    if va == 1 {
        // && vb == 1
        return uuid_timeuuid_msb(a)
            .cmp(&uuid_timeuuid_msb(b))
            .then_with(|| a[8..].cmp(&b[8..]));
    }
    a.cmp(b)
}

/// Reassembles the 60-bit timeuuid timestamp (time_hi | time_mid |
/// time_low) into a directly comparable `u64`, matching ScyllaDB's
/// `timeuuid_read_msb` (UUID.hh) exactly.
fn uuid_timeuuid_msb(bytes: &[u8; 16]) -> u64 {
    u64::from(bytes[6] & 0x0f) << 56
        | u64::from(bytes[7]) << 48
        | u64::from(bytes[4]) << 40
        | u64::from(bytes[5]) << 32
        | u64::from(bytes[0]) << 24
        | u64::from(bytes[1]) << 16
        | u64::from(bytes[2]) << 8
        | u64::from(bytes[3])
}

/// Compare two CqlValues, returning an Ordering if they are comparable. `None` means "does
/// not match" to callers, not an error, so a missing arm silently matches no rows. Supports
/// Numeric, Text, Date, Time, Timestamp, Inet, Blob, Boolean, Uuid, and Timeuuid types.
pub(crate) fn cmp(lhs: &CqlValue, rhs: &CqlValue) -> Option<Ordering> {
    match (lhs, rhs) {
        // Numeric types
        (CqlValue::TinyInt(a), CqlValue::TinyInt(b)) => Some(a.cmp(b)),
        (CqlValue::SmallInt(a), CqlValue::SmallInt(b)) => Some(a.cmp(b)),
        (CqlValue::Int(a), CqlValue::Int(b)) => Some(a.cmp(b)),
        (CqlValue::BigInt(a), CqlValue::BigInt(b)) => Some(a.cmp(b)),
        (CqlValue::Float(a), CqlValue::Float(b)) => a.partial_cmp(b),
        (CqlValue::Double(a), CqlValue::Double(b)) => a.partial_cmp(b),
        (CqlValue::Counter(a), CqlValue::Counter(b)) => Some(a.0.cmp(&b.0)),
        // Varint: semantic comparison via num-bigint
        (CqlValue::Varint(a), CqlValue::Varint(b)) => {
            let a_bi = BigInt::from(CqlVarintBorrowed::from_signed_bytes_be_slice(
                a.as_signed_bytes_be_slice(),
            ));
            let b_bi = BigInt::from(CqlVarintBorrowed::from_signed_bytes_be_slice(
                b.as_signed_bytes_be_slice(),
            ));
            Some(a_bi.cmp(&b_bi))
        }
        (CqlValue::Decimal(a), CqlValue::Decimal(b)) => {
            let (a_bytes, a_scale) = a.as_signed_be_bytes_slice_and_exponent();
            let (b_bytes, b_scale) = b.as_signed_be_bytes_slice_and_exponent();
            let a_bd = BigDecimal::from(
                CqlDecimalBorrowed::from_signed_be_bytes_slice_and_exponent(a_bytes, a_scale),
            );
            let b_bd = BigDecimal::from(
                CqlDecimalBorrowed::from_signed_be_bytes_slice_and_exponent(b_bytes, b_scale),
            );
            Some(a_bd.cmp(&b_bd))
        }
        // Text types
        (CqlValue::Text(a), CqlValue::Text(b)) => Some(a.cmp(b)),
        (CqlValue::Ascii(a), CqlValue::Ascii(b)) => Some(a.cmp(b)),
        // Date and Time types (access inner values directly)
        (CqlValue::Date(a), CqlValue::Date(b)) => Some(a.0.cmp(&b.0)),
        (CqlValue::Time(a), CqlValue::Time(b)) => Some(a.0.cmp(&b.0)),
        (CqlValue::Timestamp(a), CqlValue::Timestamp(b)) => Some(a.0.cmp(&b.0)),
        // Inet type
        (CqlValue::Inet(a), CqlValue::Inet(b)) => Some(inet_cmp(a, b)),
        // Blob: byte-wise unsigned comparison, matching Cassandra's BytesType.
        (CqlValue::Blob(a), CqlValue::Blob(b)) => Some(a.cmp(b)),
        // Boolean: false < true, matching a single 0/1 byte comparison.
        (CqlValue::Boolean(a), CqlValue::Boolean(b)) => Some(a.cmp(b)),
        // Timeuuid: the driver's CqlTimeuuid: Ord already implements
        // ScyllaDB's timeuuid ordering exactly (see uuid_cmp's doc comment).
        (CqlValue::Timeuuid(a), CqlValue::Timeuuid(b)) => Some(a.cmp(b)),
        // Uuid: version-aware comparison, see uuid_cmp().
        (CqlValue::Uuid(a), CqlValue::Uuid(b)) => Some(uuid_cmp(a, b)),
        // Unsupported or mismatched types
        _ => None,
    }
}

fn inet_cmp(lhs: &IpAddr, rhs: &IpAddr) -> Ordering {
    match (lhs, rhs) {
        (IpAddr::V4(lhs), IpAddr::V4(rhs)) => lhs.octets().cmp(&rhs.octets()),
        (IpAddr::V6(lhs), IpAddr::V6(rhs)) => lhs.octets().cmp(&rhs.octets()),
        (IpAddr::V4(lhs), IpAddr::V6(rhs)) => lhs.octets().as_slice().cmp(rhs.octets().as_slice()),
        (IpAddr::V6(lhs), IpAddr::V4(rhs)) => lhs.octets().as_slice().cmp(rhs.octets().as_slice()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::value::Counter;
    use scylla::value::CqlDecimal;
    use scylla::value::CqlDuration;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use uuid::Uuid;

    pub(crate) fn sample_value(typ: &NativeType) -> CqlValue {
        match typ {
            NativeType::Ascii => CqlValue::Ascii("ascii".to_string()),
            NativeType::BigInt => CqlValue::BigInt(20),
            NativeType::Blob => CqlValue::Blob(vec![0xde, 0xad, 0xbe, 0xef]),
            NativeType::Boolean => CqlValue::Boolean(true),
            NativeType::Date => CqlValue::Date(
                Date::from_calendar_date(2025, time::Month::September, 1)
                    .expect("valid date")
                    .into(),
            ),
            NativeType::Decimal => CqlValue::Decimal(
                CqlDecimal::try_from("-1.25".parse::<BigDecimal>().expect("valid decimal"))
                    .expect("in range"),
            ),
            NativeType::Double => CqlValue::Double(101.5),
            NativeType::Float => CqlValue::Float(201.5),
            NativeType::Inet => CqlValue::Inet(IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1))),
            NativeType::Int => CqlValue::Int(10),
            NativeType::SmallInt => CqlValue::SmallInt(30),
            NativeType::Text => CqlValue::Text("text".to_string()),
            NativeType::Time => {
                CqlValue::Time(Time::from_hms(12, 10, 10).expect("valid time").into())
            }
            NativeType::Timestamp => CqlValue::Timestamp(
                OffsetDateTime::from_unix_timestamp(1_700_000_000)
                    .expect("valid timestamp")
                    .into(),
            ),
            NativeType::Timeuuid => CqlValue::Timeuuid(CqlTimeuuid::from_bytes([
                0x84, 0x16, 0x85, 0xb2, 0x88, 0x03, 0x11, 0xf0, 0x8d, 0xe9, 0x02, 0x42, 0xac, 0x12,
                0x00, 0x02,
            ])),
            NativeType::TinyInt => CqlValue::TinyInt(40),
            NativeType::Uuid => {
                CqlValue::Uuid(Uuid::from_u128(0x1234_5678_9abc_4def_8000_0000_0000_0001))
            }
            NativeType::Varint => CqlValue::Varint(CqlVarint::from(
                "-98765432109876543210"
                    .parse::<BigInt>()
                    .expect("valid varint"),
            )),
            other => panic!("sample_value: {other:?} is not in SUPPORTED"),
        }
    }

    pub(crate) const CMP_UNSUPPORTED: &[NativeType] = &[];

    #[test]
    fn every_supported_type_is_handled() {
        for typ in SUPPORTED {
            let value = sample_value(typ);
            assert!(
                is_supported(&ColumnType::Native(typ.clone())),
                "{typ:?}: is_supported"
            );
            let json =
                to_json(value.clone()).unwrap_or_else(|err| panic!("{typ:?}: to_json: {err}"));
            let back = from_json(json.clone(), typ)
                .unwrap_or_else(|err| panic!("{typ:?}: from_json({json}): {err}"));
            assert_eq!(back, value, "{typ:?}: JSON round trip");
            if CMP_UNSUPPORTED.contains(typ) {
                assert_eq!(
                    cmp(&value, &value),
                    None,
                    "{typ:?}: comparable - drop it from CMP_UNSUPPORTED"
                );
            } else {
                assert_eq!(
                    cmp(&value, &value),
                    Some(Ordering::Equal),
                    "{typ:?}: a value must compare equal to itself"
                );
            }
        }
    }

    #[test]
    fn unsupported_types_are_rejected() {
        assert!(!is_supported(&ColumnType::Native(NativeType::Counter)));
        assert!(!is_supported(&ColumnType::Native(NativeType::Duration)));
        assert!(!is_supported(&ColumnType::Tuple(vec![ColumnType::Native(
            NativeType::Int
        )])));
        assert!(!is_supported(&ColumnType::Collection {
            frozen: true,
            typ: scylla::cluster::metadata::CollectionType::List(Box::new(ColumnType::Native(
                NativeType::Int
            ))),
        }));
    }

    #[test]
    fn to_json_conversion() {
        assert_eq!(
            to_json(CqlValue::Ascii("ascii".to_string())).unwrap(),
            Value::String("ascii".to_string())
        );
        assert_eq!(
            to_json(CqlValue::Text("text".to_string())).unwrap(),
            Value::String("text".to_string())
        );

        assert_eq!(to_json(CqlValue::Boolean(true)).unwrap(), Value::Bool(true));

        assert_eq!(
            to_json(CqlValue::Double(101.)).unwrap(),
            Value::Number(Number::from_f64(101.).unwrap())
        );
        assert_eq!(
            to_json(CqlValue::Float(201.)).unwrap(),
            Value::Number(Number::from_f64(201.).unwrap())
        );

        assert_eq!(
            to_json(CqlValue::Int(10)).unwrap(),
            Value::Number(10.into())
        );
        assert_eq!(
            to_json(CqlValue::BigInt(20)).unwrap(),
            Value::Number(20.into())
        );
        assert_eq!(
            to_json(CqlValue::SmallInt(30)).unwrap(),
            Value::Number(30.into())
        );
        assert_eq!(
            to_json(CqlValue::TinyInt(40)).unwrap(),
            Value::Number(40.into())
        );

        let uuid = Uuid::new_v4();
        assert_eq!(
            to_json(CqlValue::Uuid(uuid)).unwrap(),
            Value::String(uuid.into())
        );
        let uuid = Uuid::new_v4();
        assert_eq!(
            to_json(CqlValue::Timeuuid(uuid.into())).unwrap(),
            Value::String(uuid.into())
        );

        assert_eq!(
            to_json(CqlValue::Date(
                Date::from_calendar_date(2025, time::Month::September, 1)
                    .unwrap()
                    .into()
            ))
            .unwrap(),
            Value::String("2025-09-01".to_string())
        );
        assert_eq!(
            to_json(CqlValue::Time(Time::from_hms(12, 10, 10).unwrap().into())).unwrap(),
            Value::String("12:10:10.000000000".to_string())
        );
        assert_eq!(
            to_json(CqlValue::Timestamp(
                OffsetDateTime::from_unix_timestamp(123456789)
                    .unwrap()
                    .into()
            ))
            .unwrap(),
            Value::String(
                // truncate microseconds
                OffsetDateTime::from_unix_timestamp(123456789)
                    .unwrap()
                    .format({
                        const CONFIG: u128 = Config::DEFAULT
                            .set_time_precision(TimePrecision::Second {
                                decimal_digits: NonZero::new(3),
                            })
                            .encode();
                        &Iso8601::<CONFIG>
                    })
                    .unwrap()
            )
        );
        assert!(to_json(CqlValue::Float(f32::NAN)).is_err());
        assert!(to_json(CqlValue::Double(f64::NAN)).is_err());

        assert_eq!(
            to_json(CqlValue::Blob(vec![0xde, 0xad, 0xbe, 0xef])).unwrap(),
            Value::String("0xdeadbeef".to_string())
        );
        assert_eq!(
            to_json(CqlValue::Blob(vec![])).unwrap(),
            Value::String("0x".to_string())
        );
        assert_eq!(
            to_json(CqlValue::Blob(vec![0x00])).unwrap(),
            Value::String("0x00".to_string())
        );

        assert_eq!(
            to_json(CqlValue::Varint(CqlVarint::from(
                "-98765432109876543210987654321098765432109876543210"
                    .parse::<BigInt>()
                    .unwrap()
            )))
            .unwrap(),
            Value::String("-98765432109876543210987654321098765432109876543210".to_string())
        );

        assert_eq!(
            to_json(CqlValue::Decimal(
                CqlDecimal::try_from(
                    "-98765432109876543210.123456789"
                        .parse::<BigDecimal>()
                        .unwrap()
                )
                .unwrap()
            ))
            .unwrap(),
            Value::String("-98765432109876543210.123456789".to_string())
        );

        assert_eq!(
            to_json(CqlValue::Inet(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)))).unwrap(),
            Value::String("10.0.0.1".to_string())
        );
        assert_eq!(
            to_json(CqlValue::Inet(IpAddr::V6(Ipv6Addr::LOCALHOST))).unwrap(),
            Value::String("::1".to_string())
        );
        assert_eq!(
            to_json(CqlValue::Inet(IpAddr::V6(Ipv6Addr::new(
                0x2001, 0xdb8, 0, 0, 0, 0, 0, 1
            ))))
            .unwrap(),
            Value::String("2001:db8::1".to_string())
        );

        assert!(to_json(CqlValue::Counter(Counter(1))).is_err());
        assert!(to_json(CqlValue::Empty).is_err());
        assert!(
            to_json(CqlValue::Duration(CqlDuration {
                months: 1,
                days: 2,
                nanoseconds: 3
            }))
            .is_err()
        );
        assert!(to_json(CqlValue::List(vec![CqlValue::Int(1)])).is_err());
        assert!(to_json(CqlValue::Tuple(vec![Some(CqlValue::Int(1))])).is_err());
        assert!(
            to_json(CqlValue::UserDefinedType {
                keyspace: "ks".to_string(),
                name: "udt".to_string(),
                fields: vec![],
            })
            .is_err()
        );
    }

    #[test]
    fn from_json_conversion() {
        assert_eq!(
            from_json(Value::String("ascii".to_string()), &NativeType::Ascii).unwrap(),
            CqlValue::Ascii("ascii".to_string())
        );
        assert_eq!(
            from_json(Value::String("text".to_string()), &NativeType::Text).unwrap(),
            CqlValue::Text("text".to_string())
        );

        assert_eq!(
            from_json(Value::Bool(true), &NativeType::Boolean).unwrap(),
            CqlValue::Boolean(true)
        );

        assert_eq!(
            from_json(
                Value::Number(Number::from_f64(101.).unwrap()),
                &NativeType::Double
            )
            .unwrap(),
            CqlValue::Double(101.)
        );
        assert_eq!(
            from_json(
                Value::Number(Number::from_f64(201.).unwrap()),
                &NativeType::Float
            )
            .unwrap(),
            CqlValue::Float(201.)
        );
        assert!(
            from_json(
                Value::Number(Number::from_f64((f32::MAX as f64) * 10.).unwrap()),
                &NativeType::Float
            )
            .is_err()
        );
        assert!(
            from_json(
                Value::Number(Number::from_f64((f32::MIN as f64) * 10.).unwrap()),
                &NativeType::Float
            )
            .is_err()
        );

        assert_eq!(
            from_json(Value::Number(10.into()), &NativeType::Int).unwrap(),
            CqlValue::Int(10)
        );
        assert!(
            from_json(
                Value::Number((i32::MAX as i64 + 1).into()),
                &NativeType::Int
            )
            .is_err()
        );
        assert_eq!(
            from_json(Value::Number(20.into()), &NativeType::BigInt).unwrap(),
            CqlValue::BigInt(20)
        );
        assert_eq!(
            from_json(Value::Number(30.into()), &NativeType::SmallInt).unwrap(),
            CqlValue::SmallInt(30)
        );
        assert!(
            from_json(
                Value::Number((i16::MAX as i64 + 1).into()),
                &NativeType::SmallInt
            )
            .is_err()
        );
        assert_eq!(
            from_json(Value::Number(40.into()), &NativeType::TinyInt).unwrap(),
            CqlValue::TinyInt(40)
        );
        assert!(
            from_json(
                Value::Number((i8::MAX as i64 + 1).into()),
                &NativeType::TinyInt
            )
            .is_err()
        );

        let uuid = Uuid::new_v4();
        assert_eq!(
            from_json(Value::String(uuid.into()), &NativeType::Uuid).unwrap(),
            CqlValue::Uuid(uuid)
        );
        let uuid = Uuid::new_v4();
        assert_eq!(
            from_json(Value::String(uuid.into()), &NativeType::Timeuuid).unwrap(),
            CqlValue::Timeuuid(uuid.into())
        );

        assert_eq!(
            from_json(Value::String("2025-09-01".to_string()), &NativeType::Date).unwrap(),
            CqlValue::Date(
                Date::from_calendar_date(2025, time::Month::September, 1)
                    .unwrap()
                    .into()
            )
        );
        assert_eq!(
            from_json(
                Value::String("12:10:10.000000000".to_string()),
                &NativeType::Time
            )
            .unwrap(),
            CqlValue::Time(Time::from_hms(12, 10, 10).unwrap().into())
        );
        assert_eq!(
            from_json(
                Value::String(
                    // truncate microseconds
                    OffsetDateTime::from_unix_timestamp(123456789)
                        .unwrap()
                        .format({
                            const CONFIG: u128 = Config::DEFAULT
                                .set_time_precision(TimePrecision::Second {
                                    decimal_digits: NonZero::new(3),
                                })
                                .encode();
                            &Iso8601::<CONFIG>
                        })
                        .unwrap()
                ),
                &NativeType::Timestamp
            )
            .unwrap(),
            CqlValue::Timestamp(
                OffsetDateTime::from_unix_timestamp(123456789)
                    .unwrap()
                    .into()
            )
        );

        // CQL-style timestamp with space separator and Z offset
        assert_eq!(
            from_json(
                Value::String("2024-01-01 00:00:00.000Z".to_string()),
                &NativeType::Timestamp
            )
            .unwrap(),
            CqlValue::Timestamp(
                OffsetDateTime::from_unix_timestamp(1704067200)
                    .unwrap()
                    .into()
            )
        );

        // CQL-style timestamp with space separator, Z offset, and non-zero time
        assert_eq!(
            from_json(
                Value::String("1970-01-01 00:01:04.000Z".to_string()),
                &NativeType::Timestamp
            )
            .unwrap(),
            CqlValue::Timestamp(OffsetDateTime::from_unix_timestamp(64).unwrap().into())
        );

        assert_eq!(
            from_json(Value::String("0xdeadbeef".to_string()), &NativeType::Blob).unwrap(),
            CqlValue::Blob(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(
            from_json(Value::String("0x".to_string()), &NativeType::Blob).unwrap(),
            CqlValue::Blob(vec![])
        );
        assert_eq!(
            from_json(Value::String("0x00".to_string()), &NativeType::Blob).unwrap(),
            CqlValue::Blob(vec![0x00])
        );

        // missing 0x prefix
        assert!(from_json(Value::String("deadbeef".to_string()), &NativeType::Blob).is_err());
        // invalid hex characters
        assert!(from_json(Value::String("0xgg".to_string()), &NativeType::Blob).is_err());
        // odd-length hex digits (after stripping prefix)
        assert!(from_json(Value::String("0xabc".to_string()), &NativeType::Blob).is_err());

        // Varint from string
        assert_eq!(
            from_json(
                Value::String("-98765432109876543210987654321098765432109876543210".to_string()),
                &NativeType::Varint
            )
            .unwrap(),
            CqlValue::Varint(CqlVarint::from(
                "-98765432109876543210987654321098765432109876543210"
                    .parse::<BigInt>()
                    .unwrap()
            ))
        );
        assert!(
            from_json(
                Value::String("not_a_number".to_string()),
                &NativeType::Varint
            )
            .is_err()
        );
        // Varint from JSON number
        assert_eq!(
            from_json(Value::Number((-9876543210i64).into()), &NativeType::Varint).unwrap(),
            CqlValue::Varint(CqlVarint::from(BigInt::from(-9876543210i64)))
        );

        // Decimal from string
        assert_eq!(
            from_json(
                Value::String("-98765432109876543210.123456789".to_string()),
                &NativeType::Decimal
            )
            .unwrap(),
            CqlValue::Decimal(
                CqlDecimal::try_from(
                    "-98765432109876543210.123456789"
                        .parse::<BigDecimal>()
                        .unwrap()
                )
                .unwrap()
            )
        );
        assert!(
            from_json(
                Value::String("not_a_decimal".to_string()),
                &NativeType::Decimal
            )
            .is_err()
        );
        // Decimal from JSON number
        assert_eq!(
            from_json(
                Value::Number(Number::from_f64(-1.25).unwrap()),
                &NativeType::Decimal
            )
            .unwrap(),
            CqlValue::Decimal(
                CqlDecimal::try_from("-1.25".parse::<BigDecimal>().unwrap()).unwrap()
            )
        );

        assert_eq!(
            from_json(Value::String("10.0.0.1".to_string()), &NativeType::Inet).unwrap(),
            CqlValue::Inet(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)))
        );
        assert_eq!(
            from_json(Value::String("::1".to_string()), &NativeType::Inet).unwrap(),
            CqlValue::Inet(IpAddr::V6(Ipv6Addr::LOCALHOST))
        );
        assert_eq!(
            from_json(Value::String("2001:db8::1".to_string()), &NativeType::Inet).unwrap(),
            CqlValue::Inet(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)))
        );
        assert!(from_json(Value::String("not-an-ip".to_string()), &NativeType::Inet).is_err());
        assert!(from_json(Value::String("10.0.0.256".to_string()), &NativeType::Inet).is_err());
        assert!(from_json(Value::Number(10.into()), &NativeType::Inet).is_err());
    }

    #[test]
    fn cmp_integers() {
        assert_eq!(
            cmp(&CqlValue::Int(1), &CqlValue::Int(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&CqlValue::Int(2), &CqlValue::Int(2)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            cmp(&CqlValue::Int(3), &CqlValue::Int(2)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn cmp_bigints() {
        assert_eq!(
            cmp(&CqlValue::BigInt(100), &CqlValue::BigInt(200)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&CqlValue::BigInt(-50), &CqlValue::BigInt(-50)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn cmp_floats() {
        assert_eq!(
            cmp(&CqlValue::Float(1.0), &CqlValue::Float(2.0)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&CqlValue::Float(2.5), &CqlValue::Float(2.5)),
            Some(Ordering::Equal)
        );
        // NaN comparison returns None
        assert_eq!(cmp(&CqlValue::Float(f32::NAN), &CqlValue::Float(1.0)), None);
    }

    #[test]
    fn cmp_doubles() {
        assert_eq!(
            cmp(&CqlValue::Double(1.0), &CqlValue::Double(2.0)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&CqlValue::Double(f64::NAN), &CqlValue::Double(1.0)),
            None
        );
    }

    #[test]
    fn cmp_text() {
        assert_eq!(
            cmp(
                &CqlValue::Text("apple".to_string()),
                &CqlValue::Text("banana".to_string())
            ),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(
                &CqlValue::Text("same".to_string()),
                &CqlValue::Text("same".to_string())
            ),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn cmp_ascii() {
        assert_eq!(
            cmp(
                &CqlValue::Ascii("aaa".to_string()),
                &CqlValue::Ascii("bbb".to_string())
            ),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn cmp_smallint_and_tinyint() {
        assert_eq!(
            cmp(&CqlValue::SmallInt(10), &CqlValue::SmallInt(20)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&CqlValue::TinyInt(5), &CqlValue::TinyInt(3)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn cmp_mismatched_types_return_none() {
        assert_eq!(cmp(&CqlValue::Int(1), &CqlValue::BigInt(1)), None);
        assert_eq!(
            cmp(&CqlValue::Int(1), &CqlValue::Text("1".to_string())),
            None
        );
        assert_eq!(cmp(&CqlValue::Float(1.0), &CqlValue::Double(1.0)), None);
    }

    #[test]
    fn cmp_varint() {
        use num_bigint::BigInt;
        use scylla::value::CqlVarint;
        let make = |s: &str| CqlValue::Varint(CqlVarint::from(s.parse::<BigInt>().unwrap()));

        assert_eq!(
            cmp(&make("-1000000000000000000000"), &make("0")),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&make("99999999999999999999"), &make("99999999999999999999")),
            Some(Ordering::Equal)
        );
        assert_eq!(
            cmp(
                &make("100000000000000000001"),
                &make("99999999999999999999")
            ),
            Some(Ordering::Greater)
        );
        // negative values
        assert_eq!(
            cmp(
                &make("-98765432109876543210"),
                &make("-12345678901234567890")
            ),
            Some(Ordering::Less)
        );
        assert_eq!(cmp(&make("-1"), &make("1")), Some(Ordering::Less));
        // large positive vs large negative
        assert_eq!(
            cmp(
                &make("98765432109876543210987654321098765432109876543210"),
                &make("-98765432109876543210987654321098765432109876543210")
            ),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn cmp_decimal() {
        let make = |s: &str| {
            CqlValue::Decimal(CqlDecimal::try_from(s.parse::<BigDecimal>().unwrap()).unwrap())
        };

        assert_eq!(
            cmp(&make("-98765432109876543210.123456789"), &make("0")),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(
                &make("3.14159265358979323846"),
                &make("3.14159265358979323846")
            ),
            Some(Ordering::Equal)
        );
        assert_eq!(
            cmp(
                &make("1000000000000000000.000000001"),
                &make("999999999999999999.999999999")
            ),
            Some(Ordering::Greater)
        );
        // different scales for semantically equal value: 1.50 == 1.5
        assert_eq!(cmp(&make("1.50"), &make("1.5")), Some(Ordering::Equal));
        // negative comparisons
        assert_eq!(
            cmp(&make("-0.000000001"), &make("0.000000001")),
            Some(Ordering::Less)
        );
        assert_eq!(cmp(&make("-1.25"), &make("-1.125")), Some(Ordering::Less));
    }

    #[test]
    fn cmp_unsupported_types_return_none() {
        // Duration and other complex/collection types still have no cmp arm.
        use scylla::value::CqlDuration;
        let duration = CqlDuration {
            months: 0,
            days: 0,
            nanoseconds: 0,
        };
        assert_eq!(
            cmp(&CqlValue::Duration(duration), &CqlValue::Duration(duration)),
            None
        );
    }

    // Regression tests for VECTOR-889: cmp() had no arm for Blob, Boolean,
    // Uuid or Timeuuid, so an ANN filter restriction on a column of any of
    // these types silently matched zero rows - even a value compared against
    // itself.
    #[test]
    fn cmp_blob() {
        assert_eq!(
            cmp(
                &CqlValue::Blob(vec![1, 2, 3]),
                &CqlValue::Blob(vec![1, 2, 3])
            ),
            Some(Ordering::Equal)
        );
        assert_eq!(
            cmp(&CqlValue::Blob(vec![1, 2]), &CqlValue::Blob(vec![1, 2, 3])),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&CqlValue::Blob(vec![1, 3]), &CqlValue::Blob(vec![1, 2, 3])),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn cmp_boolean() {
        assert_eq!(
            cmp(&CqlValue::Boolean(true), &CqlValue::Boolean(true)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            cmp(&CqlValue::Boolean(false), &CqlValue::Boolean(true)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&CqlValue::Boolean(true), &CqlValue::Boolean(false)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn cmp_timeuuid() {
        let make = |bytes: [u8; 16]| CqlValue::Timeuuid(CqlTimeuuid::from_bytes(bytes));
        let t = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x11, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
            0xff, 0x00,
        ];
        assert_eq!(cmp(&make(t), &make(t)), Some(Ordering::Equal));
        // Earlier timestamp (lower first byte of the time-low field) orders first.
        let mut earlier = t;
        earlier[0] = 0x01;
        assert_eq!(cmp(&make(earlier), &make(t)), Some(Ordering::Less));
        assert_eq!(cmp(&make(t), &make(earlier)), Some(Ordering::Greater));
    }

    #[test]
    fn cmp_uuid_equality() {
        let u = Uuid::parse_str("841685b2-8803-11f0-8de9-0242ac120002").unwrap();
        assert_eq!(
            cmp(&CqlValue::Uuid(u), &CqlValue::Uuid(u)),
            Some(Ordering::Equal)
        );
    }

    // Pins the authoritative clustering order for a mixed-version set of
    // UUIDs, verified against a live scylladb/scylla:2026.2.2 (see
    // VECTOR-889): version orders first, and version-1 (time-based) UUIDs
    // order by timestamp rather than by raw bytes.
    #[test]
    fn cmp_uuid_mixed_versions_matches_scylladb_order() {
        let ordered_uuids = [
            "00000000-0000-1000-8000-000000000000", // v1
            "841685b2-8803-11f0-8de9-0242ac120002", // v1
            "ffffffff-ffff-1fff-bfff-ffffffffffff", // v1
            "00000000-0000-3000-8000-000000000000", // v3
            "00000000-0000-4000-8000-000000000000", // v4
            "7fffffff-ffff-4fff-7fff-ffffffffffff", // v4
            "ffffffff-ffff-4fff-bfff-ffffffffffff", // v4
            "00000000-0000-5000-8000-000000000000", // v5
        ]
        .map(|s| Uuid::parse_str(s).unwrap());

        for pair in ordered_uuids.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            assert_eq!(
                cmp(&CqlValue::Uuid(a), &CqlValue::Uuid(b)),
                Some(Ordering::Less),
                "expected {a} < {b}"
            );
        }
    }

    /// Regression test: two version-1 UUIDs with the *same* reassembled
    /// timestamp, but tie-break bytes (8..16) straddling the 0x7f/0x80
    /// boundary. `uuid_type_impl`'s `UUID`-column comparator
    /// (`uuid_tri_compare_timeuuid`) breaks the tie by plain unsigned
    /// bytes, so `0x7f... < 0x80...` - confirmed against a live
    /// scylladb/scylla 2026.4.0 with a UUID-clustering-key table. This is
    /// the opposite of what `CqlTimeuuid: Ord` (the *TIMEUUID*-column
    /// comparator, `utils::timeuuid_tri_compare`, which XORs those same
    /// bytes with 0x80 - a signed compare) would give, confirmed against
    /// the same live cluster with a TIMEUUID-clustering-key table instead.
    /// `cmp_uuid_mixed_versions_matches_scylladb_order` above never
    /// exercises this because none of its UUIDs share a timestamp.
    #[test]
    fn cmp_uuid_same_timestamp_lsb_tie_break_is_unsigned() {
        let lo = Uuid::parse_str("00000000-0000-1000-7f00-000000000000").unwrap();
        let hi = Uuid::parse_str("00000000-0000-1000-8000-000000000000").unwrap();
        assert_eq!(
            cmp(&CqlValue::Uuid(lo), &CqlValue::Uuid(hi)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn cmp_inet() {
        let v4 = |a, b, c, d| CqlValue::Inet(IpAddr::V4(Ipv4Addr::new(a, b, c, d)));
        let v6 = |s: &str| CqlValue::Inet(IpAddr::V6(s.parse::<Ipv6Addr>().expect("valid addr")));

        assert_eq!(
            cmp(&v4(10, 0, 0, 1), &v4(10, 0, 0, 1)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            cmp(&v4(10, 0, 0, 1), &v4(10, 0, 0, 2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp(&v4(10, 0, 1, 0), &v4(10, 0, 0, 255)),
            Some(Ordering::Greater)
        );
        assert_eq!(cmp(&v6("::1"), &v6("::2")), Some(Ordering::Less));

        // Serialized byte order.
        assert_eq!(
            cmp(&v4(255, 255, 255, 255), &v6("::")),
            Some(Ordering::Greater)
        );
        assert_eq!(
            cmp(&v6("2001:db8::1"), &v4(255, 255, 255, 255)),
            Some(Ordering::Less)
        );
        // On an equal prefix the shorter IPv4 form sorts first.
        assert_eq!(cmp(&v4(0, 0, 0, 0), &v6("::")), Some(Ordering::Less));
    }
}
