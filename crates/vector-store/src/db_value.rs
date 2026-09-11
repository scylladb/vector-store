/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Row and value types for reads of an indexed base table.
//!
//! The rows read by a full scan ([`crate::db_index`]) and by a CDC upsert's
//! point read ([`crate::db_cdc`]) have a column layout that is only known at
//! runtime, which is why they used to be read as [`CqlValue`]s. That is
//! needlessly expensive for the embedding column: a `vector<float, N>`
//! becomes a `Vec<CqlValue>`, one 72-byte enum per 4-byte element, which is
//! then walked a second time to unwrap the floats.
//!
//! [`DbRow`] keeps the dynamic layout, but dispatches per value on the
//! column's CQL type, so the embedding is deserialized straight into a
//! [`Vector`] while every other value keeps its [`CqlValue`]
//! representation.

use crate::Vector;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::deserialize::DeserializationError;
use scylla::deserialize::FrameSlice;
use scylla::deserialize::TypeCheckError;
use scylla::deserialize::row::ColumnIterator;
use scylla::deserialize::row::DeserializeRow;
use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::ColumnSpec;
use scylla::value::CqlValue;

/// A single value of a [`DbRow`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DbValue {
    /// A value of a native CQL `vector<float, N>` column.
    Vector(Vector),
    /// Any other value, in the dynamic representation its runtime-only CQL
    /// type requires. For an Alternator table this includes the embedding
    /// column, which arrives as a tagged `CqlValue::Blob`.
    Value(CqlValue),
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for DbValue {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            // Only a vector<float, N> is an embedding. A vector of any other
            // element type is an ordinary column and keeps its CqlValue
            // representation, instead of failing the type check of the whole
            // row.
            ColumnType::Vector { typ: element, .. }
                if matches!(**element, ColumnType::Native(NativeType::Float)) =>
            {
                <Vector>::type_check(typ)
            }
            _ => <CqlValue>::type_check(typ),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        match typ {
            ColumnType::Vector { typ: element, .. }
                if matches!(**element, ColumnType::Native(NativeType::Float)) =>
            {
                <Vector>::deserialize(typ, v).map(Self::Vector)
            }
            _ => <CqlValue>::deserialize(typ, v).map(Self::Value),
        }
    }
}

/// A row of an indexed base table: all selected columns, in the order the
/// query selected them, with a `None` for a null column.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DbRow {
    pub(crate) columns: Vec<Option<DbValue>>,
}

/// Deserializes the row eagerly, column by column.
impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for DbRow {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        specs
            .iter()
            .try_for_each(|spec| <Option<DbValue>>::type_check(spec.typ()))
    }

    fn deserialize(row: ColumnIterator<'frame, 'metadata>) -> Result<Self, DeserializationError> {
        let mut columns = Vec::with_capacity(row.columns_remaining());
        for column in row {
            let column = column?;
            columns.push(<Option<DbValue>>::deserialize(
                column.spec.typ(),
                column.slice,
            )?);
        }
        Ok(Self { columns })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use scylla::frame::response::result::TableSpec;

    fn float_vector_type(dimensions: u16) -> ColumnType<'static> {
        ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Float)),
            dimensions,
        }
    }

    fn spec(name: &str, typ: ColumnType<'static>) -> ColumnSpec<'static> {
        ColumnSpec::owned(
            name.to_string(),
            typ,
            TableSpec::owned("ks".into(), "tbl".into()),
        )
    }

    fn deserialize_row(columns: &[(ColumnSpec<'static>, Option<Vec<u8>>)]) -> DbRow {
        let specs = columns
            .iter()
            .map(|(spec, _)| spec.clone())
            .collect::<Vec<_>>();
        <DbRow>::type_check(&specs).unwrap();

        // The frame of a row is the concatenation of its [len][value] cells,
        // with a length of -1 for a null column.
        let mut frame = Vec::new();
        for (_, value) in columns {
            match value {
                Some(value) => {
                    frame.extend_from_slice(&(value.len() as i32).to_be_bytes());
                    frame.extend_from_slice(value);
                }
                None => frame.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        let frame = FrameSlice::new_borrowed(&frame);
        <DbRow>::deserialize(ColumnIterator::new(&specs, frame)).unwrap()
    }

    fn float_cells(floats: &[f32]) -> Vec<u8> {
        floats.iter().flat_map(|f| f.to_be_bytes()).collect()
    }

    #[test]
    fn vector_column_deserializes_into_a_vector() {
        let row = deserialize_row(&[(
            spec("embedding", float_vector_type(3)),
            Some(float_cells(&[1.0, 2.5, 3.0])),
        )]);
        assert_eq!(
            row.columns,
            vec![Some(DbValue::Vector(Vector::from(vec![1.0, 2.5, 3.0])))]
        );
    }

    #[test]
    fn other_columns_stay_cql_values() {
        let row = deserialize_row(&[
            (
                spec("color", ColumnType::Native(NativeType::Text)),
                Some(b"red".to_vec()),
            ),
            (
                spec("writetime(color)", ColumnType::Native(NativeType::BigInt)),
                Some(1234567890i64.to_be_bytes().to_vec()),
            ),
        ]);
        assert_eq!(
            row.columns,
            vec![
                Some(DbValue::Value(CqlValue::Text("red".to_string()))),
                Some(DbValue::Value(CqlValue::BigInt(1234567890))),
            ]
        );
    }

    #[test]
    fn alternator_blob_column_stays_a_cql_value() {
        let row = deserialize_row(&[(
            spec(":attrs['embedding']", ColumnType::Native(NativeType::Blob)),
            Some(vec![0x05, 0x3f, 0x80, 0x00, 0x00]),
        )]);
        assert_eq!(
            row.columns,
            vec![Some(DbValue::Value(CqlValue::Blob(vec![
                0x05, 0x3f, 0x80, 0x00, 0x00
            ])))]
        );
    }

    #[test]
    fn null_columns_deserialize_into_none() {
        let row = deserialize_row(&[
            (spec("embedding", float_vector_type(3)), None),
            (spec("color", ColumnType::Native(NativeType::Text)), None),
        ]);
        assert_eq!(row.columns, vec![None, None]);
    }

    #[test]
    fn a_mixed_row_keeps_the_column_order() {
        let row = deserialize_row(&[
            (
                spec("pk", ColumnType::Native(NativeType::BigInt)),
                Some(7i64.to_be_bytes().to_vec()),
            ),
            (
                spec("embedding", float_vector_type(2)),
                Some(float_cells(&[1.0, 2.0])),
            ),
            (
                spec("color", ColumnType::Native(NativeType::Text)),
                Some(b"red".to_vec()),
            ),
        ]);
        assert_eq!(
            row.columns,
            vec![
                Some(DbValue::Value(CqlValue::BigInt(7))),
                Some(DbValue::Vector(Vector::from(vec![1.0, 2.0]))),
                Some(DbValue::Value(CqlValue::Text("red".to_string()))),
            ]
        );
    }

    #[test]
    fn a_non_float_vector_column_stays_a_cql_value() {
        let typ = ColumnType::Vector {
            typ: Box::new(ColumnType::Native(NativeType::Int)),
            dimensions: 2,
        };
        let cells = [1i32, 2].iter().flat_map(|i| i.to_be_bytes()).collect();
        let row = deserialize_row(&[(spec("ints", typ), Some(cells))]);
        assert_eq!(
            row.columns,
            vec![Some(DbValue::Value(CqlValue::Vector(vec![
                CqlValue::Int(1),
                CqlValue::Int(2),
            ])))]
        );
    }
}
