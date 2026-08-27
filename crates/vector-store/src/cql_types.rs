/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use anyhow::anyhow;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use scylla::value::CqlValue;
use serde_json::Number;
use serde_json::Value;
use std::num::NonZero;
use time::Date;
use time::OffsetDateTime;
use time::Time;
use time::format_description::well_known::Iso8601;
use time::format_description::well_known::iso8601::Config;
use time::format_description::well_known::iso8601::TimePrecision;

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

        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::value::CqlDecimal;
    use scylla::value::CqlVarint;
    use uuid::Uuid;

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
    }
}
