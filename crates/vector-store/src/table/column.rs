/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::PrimaryKey;
use crate::Timestamp;
use crate::table::ColumnVec;
use crate::table::PrimaryId;
use crate::timestamp::Timestamped;
use anyhow::bail;
use scylla::cluster::metadata::NativeType;
use scylla::value::CqlDate;
use scylla::value::CqlDecimal;
use scylla::value::CqlTime;
use scylla::value::CqlTimestamp;
use scylla::value::CqlTimeuuid;
use scylla::value::CqlValue;
use scylla::value::CqlVarint;
use std::net::IpAddr;
use uuid::Uuid;

/// A newtype for defining the offset of the key column.
#[derive(Clone, Copy, Debug, derive_more::From, derive_more::Into)]
pub(super) struct KeyOffset(usize);

/// An enum that represents a column in the table. It can be a column with values of a specific
/// type or a primary key column (as an offset in the primary key columns).
#[derive(Debug)]
pub(super) enum Column {
    Ascii(ColumnVec<PrimaryId, Timestamped<String>>),
    BigInt(ColumnVec<PrimaryId, Timestamped<i64>>),
    Blob(ColumnVec<PrimaryId, Timestamped<Vec<u8>>>),
    Boolean(ColumnVec<PrimaryId, Timestamped<bool>>),
    Date(ColumnVec<PrimaryId, Timestamped<CqlDate>>),
    Decimal(ColumnVec<PrimaryId, Timestamped<CqlDecimal>>),
    Double(ColumnVec<PrimaryId, Timestamped<f64>>),
    Float(ColumnVec<PrimaryId, Timestamped<f32>>),
    Inet(ColumnVec<PrimaryId, Timestamped<IpAddr>>),
    Int(ColumnVec<PrimaryId, Timestamped<i32>>),
    SmallInt(ColumnVec<PrimaryId, Timestamped<i16>>),
    Text(ColumnVec<PrimaryId, Timestamped<String>>),
    Time(ColumnVec<PrimaryId, Timestamped<CqlTime>>),
    Timestamp(ColumnVec<PrimaryId, Timestamped<CqlTimestamp>>),
    Timeuuid(ColumnVec<PrimaryId, Timestamped<CqlTimeuuid>>),
    TinyInt(ColumnVec<PrimaryId, Timestamped<i8>>),
    Uuid(ColumnVec<PrimaryId, Timestamped<Uuid>>),
    Varint(ColumnVec<PrimaryId, Timestamped<CqlVarint>>),
    PrimaryKey(KeyOffset),
}

impl Column {
    pub(super) fn new(native_type: &NativeType) -> anyhow::Result<Self> {
        Ok(match native_type {
            NativeType::Ascii => Self::Ascii(ColumnVec::new()),
            NativeType::BigInt => Self::BigInt(ColumnVec::new()),
            NativeType::Blob => Self::Blob(ColumnVec::new()),
            NativeType::Boolean => Self::Boolean(ColumnVec::new()),
            NativeType::Date => Self::Date(ColumnVec::new()),
            NativeType::Decimal => Self::Decimal(ColumnVec::new()),
            NativeType::Double => Self::Double(ColumnVec::new()),
            NativeType::Float => Self::Float(ColumnVec::new()),
            NativeType::Inet => Self::Inet(ColumnVec::new()),
            NativeType::Int => Self::Int(ColumnVec::new()),
            NativeType::SmallInt => Self::SmallInt(ColumnVec::new()),
            NativeType::Text => Self::Text(ColumnVec::new()),
            NativeType::Time => Self::Time(ColumnVec::new()),
            NativeType::Timestamp => Self::Timestamp(ColumnVec::new()),
            NativeType::Timeuuid => Self::Timeuuid(ColumnVec::new()),
            NativeType::TinyInt => Self::TinyInt(ColumnVec::new()),
            NativeType::Uuid => Self::Uuid(ColumnVec::new()),
            NativeType::Varint => Self::Varint(ColumnVec::new()),
            _ => bail!("Unsupported native type: {native_type:?}"),
        })
    }

    pub(super) fn resize_with(&mut self, size: usize) {
        let timestamp = Timestamp::MIN;
        match self {
            Self::Ascii(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::BigInt(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Blob(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Boolean(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Date(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Decimal(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Double(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Float(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Inet(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Int(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::SmallInt(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Text(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Time(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Timestamp(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Timeuuid(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::TinyInt(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Uuid(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::Varint(vec) => vec.resize_with(size, || Timestamped::new(timestamp, None)),
            Self::PrimaryKey(_) => {}
        }
    }

    pub(super) fn insert(
        &mut self,
        primary_id: PrimaryId,
        timestamp: Timestamp,
        value: CqlValue,
    ) -> anyhow::Result<()> {
        match self {
            Self::Ascii(vec) => {
                let CqlValue::Ascii(value) = value else {
                    bail!("Failed to convert value to Ascii");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::BigInt(vec) => {
                let CqlValue::BigInt(value) = value else {
                    bail!("Failed to convert value to BigInt");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Blob(vec) => {
                let CqlValue::Blob(value) = value else {
                    bail!("Failed to convert value to Blob");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Boolean(vec) => {
                let CqlValue::Boolean(value) = value else {
                    bail!("Failed to convert value to Boolean");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Date(vec) => {
                let CqlValue::Date(value) = value else {
                    bail!("Failed to convert value to Date");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Decimal(vec) => {
                let CqlValue::Decimal(value) = value else {
                    bail!("Failed to convert value to Decimal");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Double(vec) => {
                let CqlValue::Double(value) = value else {
                    bail!("Failed to convert value to Double");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Float(vec) => {
                let CqlValue::Float(value) = value else {
                    bail!("Failed to convert value to Float");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Inet(vec) => {
                let CqlValue::Inet(value) = value else {
                    bail!("Failed to convert value to Inet");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Int(vec) => {
                let CqlValue::Int(value) = value else {
                    bail!("Failed to convert value to Int");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::SmallInt(vec) => {
                let CqlValue::SmallInt(value) = value else {
                    bail!("Failed to convert value to SmallInt");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Text(vec) => {
                let CqlValue::Text(value) = value else {
                    bail!("Failed to convert value to Text");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Time(vec) => {
                let CqlValue::Time(value) = value else {
                    bail!("Failed to convert value to Time");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Timestamp(vec) => {
                let CqlValue::Timestamp(value) = value else {
                    bail!("Failed to convert value to Timestamp");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Timeuuid(vec) => {
                let CqlValue::Timeuuid(value) = value else {
                    bail!("Failed to convert value to Timeuuid");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::TinyInt(vec) => {
                let CqlValue::TinyInt(value) = value else {
                    bail!("Failed to convert value to TinyInt");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Uuid(vec) => {
                let CqlValue::Uuid(value) = value else {
                    bail!("Failed to convert value to Uuid");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::Varint(vec) => {
                let CqlValue::Varint(value) = value else {
                    bail!("Failed to convert value to Varint");
                };
                vec.update(primary_id, Timestamped::new(timestamp, Some(value)))
            }
            Self::PrimaryKey(_) => bail!("Cannot insert value into PrimaryKey column"),
        }
    }

    pub(super) fn remove(
        &mut self,
        primary_id: PrimaryId,
        timestamp: Timestamp,
    ) -> anyhow::Result<()> {
        match self {
            Self::Ascii(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::BigInt(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Blob(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Boolean(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Date(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Decimal(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Double(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Float(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Inet(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Int(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::SmallInt(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Text(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Time(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Timestamp(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Timeuuid(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::TinyInt(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Uuid(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::Varint(vec) => vec.update(primary_id, Timestamped::new(timestamp, None)),
            Self::PrimaryKey(_) => bail!("Cannot remove value from PrimaryKey column"),
        }
    }

    pub(super) fn get(
        &self,
        primary_id: PrimaryId,
        primary_keys: &ColumnVec<PrimaryId, Option<PrimaryKey>>,
    ) -> Option<CqlValue> {
        match self {
            Self::Ascii(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Ascii),
            Self::BigInt(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::BigInt),
            Self::Blob(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Blob),
            Self::Boolean(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Boolean),
            Self::Date(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Date),
            Self::Decimal(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Decimal),
            Self::Double(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Double),
            Self::Float(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Float),
            Self::Inet(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Inet),
            Self::Int(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Int),
            Self::SmallInt(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::SmallInt),
            Self::Text(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Text),
            Self::Time(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Time),
            Self::Timestamp(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Timestamp),
            Self::Timeuuid(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Timeuuid),
            Self::TinyInt(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::TinyInt),
            Self::Uuid(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Uuid),
            Self::Varint(vec) => vec
                .get(primary_id)
                .and_then(|timestamped| timestamped.value())
                .cloned()
                .map(CqlValue::Varint),
            Self::PrimaryKey(key_offset) => primary_keys
                .get(primary_id)
                .and_then(|opt_key| opt_key.as_ref())
                .and_then(|key| key.get((*key_offset).into())),
        }
    }
}
