/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::ColumnName;
use crate::CqlLiteral;
use crate::Dimensions;
use crate::IndexMetadata;
use crate::IndexName;
use crate::KeyspaceIdentifier;
use crate::KeyspaceName;
use crate::TableIdentifier;
use crate::TableName;
use crate::Vector;
use crate::vector;
use anyhow::bail;
use futures::TryStreamExt;
use itertools::Itertools;
use regex::Regex;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla_cdc::CqlIdentifier;
use scylla_cdc::consumer::CDCRow;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

pub(crate) struct IndexLocation {
    pub keyspace: KeyspaceName,
    pub table: TableName,
    pub index: IndexName,
}

pub(crate) enum DbIndexBackend {
    Cql { target_columns: Vec<ColumnName> },
    Alternator { target_columns: Vec<ColumnName> },
}

impl From<&IndexMetadata> for DbIndexBackend {
    fn from(metadata: &IndexMetadata) -> Self {
        let target_columns = metadata.target_columns.clone();
        if metadata.keyspace_name.is_alternator() {
            Self::Alternator { target_columns }
        } else {
            Self::Cql { target_columns }
        }
    }
}

impl DbIndexBackend {
    /// Extracts embeddings from a CDC row for all target columns.
    ///
    /// Returns a vector where each element corresponds to a target column:
    /// - `None` — column was not modified in this CDC event
    /// - `Some(None)` — column was set to null (deleted)
    /// - `Some(Some(vector))` — column was updated to this vector
    pub fn extract_cdc_embeddings(
        &self,
        row: &mut CDCRow<'_>,
    ) -> anyhow::Result<Vec<Option<Option<Vector>>>> {
        match self {
            Self::Cql { target_columns } => target_columns
                .iter()
                .map(|column| {
                    let col_name = column.as_ref();
                    if !row.column_deletable(col_name) {
                        return Ok(None);
                    }
                    Ok(Some(
                        row.take_value(col_name).map(Vector::try_from).transpose()?,
                    ))
                })
                .collect(),
            Self::Alternator { target_columns } => {
                let col_name = ":attrs";
                if !row.column_deletable(col_name) {
                    bail!("CDC error: column {col_name} should be deletable");
                }
                let Some(attrs) = row.take_value(col_name) else {
                    return Ok(vec![None; target_columns.len()]);
                };
                target_columns
                    .iter()
                    .map(|col| {
                        vector::AlternatorAttrs {
                            attrs: attrs.clone(),
                            target_column: col.as_ref(),
                        }
                        .try_into()
                        .map(Some)
                    })
                    .collect()
            }
        }
    }
}

/// Builds the CQL range scan query appropriate for the given keyspace.
///
/// For CQL-native tables, selects each vector column and its writetime directly.
/// For Alternator tables, selects from the `:attrs` map column for each target.
pub(crate) fn range_scan_query(
    keyspace: &KeyspaceIdentifier,
    table: &TableIdentifier,
    target_columns: &[ColumnName],
    primary_key_list: &str,
    partition_key_list: &str,
) -> String {
    let vector_columns = if keyspace.is_alternator() {
        let attributes = CqlIdentifier::new(":attrs");
        target_columns
            .iter()
            .map(|col| {
                let vector = CqlLiteral::new(col.as_ref());
                format!("{attributes}[{vector}], writetime({attributes}[{vector}])")
            })
            .join(", ")
    } else {
        target_columns
            .iter()
            .map(|col| {
                let vector = CqlIdentifier::new(col.as_ref());
                format!("{vector}, writetime({vector})")
            })
            .join(", ")
    };

    format!(
        "
            SELECT {primary_key_list}, {vector_columns}
            FROM {keyspace}.{table}
            WHERE
                token({partition_key_list}) >= ?
                AND token({partition_key_list}) <= ?
            BYPASS CACHE
            "
    )
}

/// Retrieves the vector dimensions for the given index, dispatching to the
/// appropriate strategy based on whether the keyspace is Alternator- or CQL-backed.
pub(crate) async fn get_dimensions(
    target_column: &ColumnName,
    session: &Session,
    st_get_index_target_type: &PreparedStatement,
    re_get_index_target_type: &Regex,
    st_get_index_options: &PreparedStatement,
    location: IndexLocation,
) -> anyhow::Result<Option<Dimensions>> {
    if location.keyspace.is_alternator() {
        get_dimensions_from_index_options(session, st_get_index_options, location).await
    } else {
        get_dimensions_from_column_type(
            target_column,
            session,
            st_get_index_target_type,
            re_get_index_target_type,
            location,
        )
        .await
    }
}

/// Retrieves the vector dimensions for a CQL-native table by parsing the column type.
async fn get_dimensions_from_column_type(
    target_column: &ColumnName,
    session: &Session,
    st_get_index_target_type: &PreparedStatement,
    re_get_index_target_type: &Regex,
    location: IndexLocation,
) -> anyhow::Result<Option<Dimensions>> {
    let column_type = session
        .execute_iter(
            st_get_index_target_type.clone(),
            (location.keyspace, location.table, target_column.clone()),
        )
        .await?
        .rows_stream::<(String,)>()?
        .try_next()
        .await?;
    let dimensions = column_type
        .and_then(|(typ,)| {
            re_get_index_target_type
                .captures(&typ)
                .and_then(|captures| captures["dimensions"].parse::<usize>().ok())
        })
        .and_then(|dimensions| NonZeroUsize::new(dimensions).map(|dimensions| dimensions.into()));
    Ok(dimensions)
}

/// Retrieves the vector dimensions for an Alternator table from the index options.
///
/// In Alternator, the schema has no native `VECTOR` type, so the dimension
/// is stored in the index option `"dimensions"`.
async fn get_dimensions_from_index_options(
    session: &Session,
    st_get_index_options: &PreparedStatement,
    location: IndexLocation,
) -> anyhow::Result<Option<Dimensions>> {
    let index_options = session
        .execute_iter(
            st_get_index_options.clone(),
            (location.keyspace, location.table, location.index),
        )
        .await?
        .rows_stream::<(BTreeMap<String, String>,)>()?
        .try_next()
        .await?;
    let dimensions = index_options
        .and_then(|(mut options,)| {
            options
                .remove("dimensions")
                .and_then(|s| s.parse::<usize>().ok())
        })
        .and_then(|dimensions| NonZeroUsize::new(dimensions).map(|dimensions| dimensions.into()));
    Ok(dimensions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use scylla::frame::response::result::ColumnSpec;
    use scylla::frame::response::result::ColumnType;
    use scylla::frame::response::result::NativeType;
    use scylla::frame::response::result::TableSpec;
    use scylla::response::query_result::ColumnSpecs;
    use scylla::value::CqlTimeuuid;
    use scylla::value::CqlValue;
    use scylla::value::Row;
    use scylla_cdc::consumer::CDCRow;
    use scylla_cdc::consumer::CDCRowSchema;

    const ALTERNATOR_TYPE_FLOAT32VECTOR: u8 = 5;

    fn alternator_vector_blob(floats: &[f32]) -> Vec<u8> {
        let mut v = vec![ALTERNATOR_TYPE_FLOAT32VECTOR];
        for f in floats {
            v.extend_from_slice(&f.to_be_bytes());
        }
        v
    }

    fn dummy_table_spec() -> TableSpec<'static> {
        TableSpec::owned("ks".to_string(), "tbl".to_string())
    }

    fn cdc_column_spec(name: &'static str) -> ColumnSpec<'static> {
        ColumnSpec::borrowed(
            name,
            ColumnType::Native(NativeType::Blob),
            dummy_table_spec(),
        )
    }

    fn build_alternator_schema() -> CDCRowSchema {
        let specs: Vec<ColumnSpec<'static>> = vec![
            ColumnSpec::borrowed(
                "cdc$stream_id",
                ColumnType::Native(NativeType::Blob),
                dummy_table_spec(),
            ),
            ColumnSpec::borrowed(
                "cdc$time",
                ColumnType::Native(NativeType::Timeuuid),
                dummy_table_spec(),
            ),
            ColumnSpec::borrowed(
                "cdc$batch_seq_no",
                ColumnType::Native(NativeType::Int),
                dummy_table_spec(),
            ),
            ColumnSpec::borrowed(
                "cdc$end_of_batch",
                ColumnType::Native(NativeType::Boolean),
                dummy_table_spec(),
            ),
            ColumnSpec::borrowed(
                "cdc$operation",
                ColumnType::Native(NativeType::TinyInt),
                dummy_table_spec(),
            ),
            ColumnSpec::borrowed(
                "cdc$ttl",
                ColumnType::Native(NativeType::BigInt),
                dummy_table_spec(),
            ),
            cdc_column_spec(":attrs"),
            cdc_column_spec("cdc$deleted_:attrs"),
        ];
        CDCRowSchema::new(ColumnSpecs::new(&specs))
    }

    fn build_cdc_row(schema: &CDCRowSchema, attrs_value: Option<CqlValue>) -> CDCRow<'_> {
        let columns = vec![
            Some(CqlValue::Blob(vec![0u8; 16])),
            Some(CqlValue::Timeuuid(CqlTimeuuid::nil())),
            Some(CqlValue::Int(0)),
            Some(CqlValue::Boolean(false)),
            Some(CqlValue::TinyInt(1)),
            None,
            attrs_value,
            Some(CqlValue::Boolean(true)),
        ];
        CDCRow::from_row(Row { columns }, schema)
    }

    #[test]
    fn range_scan_query_quotes_lowercase_identifiers() {
        let query = range_scan_query(
            &KeyspaceIdentifier::from("ks"),
            &TableIdentifier::from("tbl"),
            &[ColumnName::from("embedding")],
            &CqlIdentifier::new("id").to_string(),
            &CqlIdentifier::new("id").to_string(),
        );
        assert!(query.contains(r#""embedding""#));
        assert!(query.contains(r#"FROM "ks"."tbl""#));
        assert!(query.contains(r#"token("id")"#));
    }

    #[test]
    fn range_scan_query_quotes_mixed_case_identifiers() {
        let pk_list = [
            CqlIdentifier::new("UserId"),
            CqlIdentifier::new("CreatedAt"),
        ]
        .iter()
        .join(", ");
        let query = range_scan_query(
            &KeyspaceIdentifier::from("MyKeyspace"),
            &TableIdentifier::from("MyTable"),
            &[ColumnName::from("EmbeddingCol")],
            &pk_list,
            &CqlIdentifier::new("UserId").to_string(),
        );
        assert!(
            query.contains(r#""EmbeddingCol""#),
            "mixed-case embedding column must be quoted"
        );
        assert!(
            query.contains(r#"FROM "MyKeyspace"."MyTable""#),
            "mixed-case keyspace/table must be quoted"
        );
        assert!(
            query.contains(r#""UserId", "CreatedAt""#),
            "mixed-case primary key columns must be quoted"
        );
    }

    #[test]
    fn range_scan_query_quotes_uppercase_identifiers() {
        let query = range_scan_query(
            &KeyspaceIdentifier::from("UPPER_KS"),
            &TableIdentifier::from("UPPER_TBL"),
            &[ColumnName::from("VEC")],
            &CqlIdentifier::new("ID").to_string(),
            &CqlIdentifier::new("ID").to_string(),
        );
        assert!(
            query.contains(r#""VEC""#),
            "uppercase embedding column must be quoted"
        );
        assert!(
            query.contains(r#"FROM "UPPER_KS"."UPPER_TBL""#),
            "uppercase keyspace/table must be quoted"
        );
    }

    #[test]
    fn range_scan_query_quotes_special_character_identifiers() {
        let pk_list = [CqlIdentifier::new(":pk"), CqlIdentifier::new(":sk")]
            .iter()
            .join(", ");
        let query = range_scan_query(
            &KeyspaceIdentifier::from("my-app"),
            &TableIdentifier::from("my-table:v1"),
            &[ColumnName::from("my-vector")],
            &pk_list,
            &CqlIdentifier::new(":pk").to_string(),
        );
        assert!(
            query.contains(r#""my-vector""#),
            "hyphenated embedding column must be quoted"
        );
        assert!(
            query.contains(r#"FROM "my-app"."my-table:v1""#),
            "special-character keyspace/table must be quoted"
        );
        assert!(
            query.contains(r#"token(":pk")"#),
            "special-character partition key must be quoted"
        );
    }

    #[test]
    fn alternator_range_scan_query_basic() {
        let pk_list = [CqlIdentifier::new(":pk"), CqlIdentifier::new(":sk")]
            .iter()
            .join(", ");
        let query = range_scan_query(
            &KeyspaceIdentifier::from("alternator_my-app"),
            &TableIdentifier::from("my-table"),
            &[ColumnName::from("v")],
            &pk_list,
            &CqlIdentifier::new(":pk").to_string(),
        );
        assert!(
            query.contains(r#"":attrs"['v']"#),
            "attribute name must be single-quoted inside :attrs map access: {query}"
        );
        assert!(
            query.contains(r#"writetime(":attrs"['v'])"#),
            "writetime must wrap the same :attrs map access: {query}"
        );
        assert!(
            query.contains(r#"FROM "alternator_my-app"."my-table""#),
            "keyspace and table must be double-quoted: {query}"
        );
        assert!(
            query.contains(r#"token(":pk")"#),
            "partition key must be double-quoted: {query}"
        );
    }

    #[test]
    fn alternator_range_scan_query_special_attribute_name() {
        let pk_list = CqlIdentifier::new(":pk").to_string();
        let query = range_scan_query(
            &KeyspaceIdentifier::from("alternator_ks"),
            &TableIdentifier::from("tbl"),
            &[ColumnName::from("my-vector:v1")],
            &pk_list,
            &pk_list,
        );
        assert!(
            query.contains(r#"":attrs"['my-vector:v1']"#),
            "special characters in attribute name must appear verbatim inside single quotes: {query}"
        );
        assert!(
            query.contains(r#"writetime(":attrs"['my-vector:v1'])"#),
            "writetime must use the same single-quoted attribute access: {query}"
        );
    }

    #[test]
    fn alternator_range_scan_query_mixed_case_attribute() {
        let pk_list = CqlIdentifier::new("pk").to_string();
        let query = range_scan_query(
            &KeyspaceIdentifier::from("alternator_Ks"),
            &TableIdentifier::from("Tbl"),
            &[ColumnName::from("EmbeddingCol")],
            &pk_list,
            &pk_list,
        );
        assert!(
            query.contains(r#"":attrs"['EmbeddingCol']"#),
            "mixed-case attribute name must be preserved as-is inside single quotes: {query}"
        );
        assert!(
            query.contains(r#"FROM "alternator_Ks"."Tbl""#),
            "mixed-case keyspace/table must be double-quoted: {query}"
        );
    }

    #[test]
    fn alternator_range_scan_query_attribute_with_quotes() {
        let pk_list = CqlIdentifier::new(":pk").to_string();
        let query = range_scan_query(
            &KeyspaceIdentifier::from("alternator_ks"),
            &TableIdentifier::from("tbl"),
            &[ColumnName::from("it's a \"test\"")],
            &pk_list,
            &pk_list,
        );
        assert!(
            query.contains(r#"":attrs"['it''s a "test"']"#),
            "single quotes in attribute name must be escaped by doubling: {query}"
        );
        assert!(
            query.contains(r#"writetime(":attrs"['it''s a "test"'])"#),
            "writetime must use the same escaped attribute access: {query}"
        );
    }

    #[test]
    fn range_scan_query_multi_column_cql() {
        let query = range_scan_query(
            &KeyspaceIdentifier::from("ks"),
            &TableIdentifier::from("tbl"),
            &[
                ColumnName::from("vec_a"),
                ColumnName::from("vec_b"),
                ColumnName::from("vec_c"),
            ],
            &CqlIdentifier::new("id").to_string(),
            &CqlIdentifier::new("id").to_string(),
        );
        assert!(
            query.contains(r#""vec_a", writetime("vec_a"), "vec_b", writetime("vec_b"), "vec_c", writetime("vec_c")"#),
            "multi-column CQL must select all columns with writetimes: {query}"
        );
    }

    #[test]
    fn range_scan_query_multi_column_alternator() {
        let pk_list = CqlIdentifier::new(":pk").to_string();
        let query = range_scan_query(
            &KeyspaceIdentifier::from("alternator_ks"),
            &TableIdentifier::from("tbl"),
            &[ColumnName::from("v1"), ColumnName::from("v2")],
            &pk_list,
            &pk_list,
        );
        assert!(
            query.contains(r#"":attrs"['v1'], writetime(":attrs"['v1']), ":attrs"['v2'], writetime(":attrs"['v2'])"#),
            "multi-column Alternator must select all attrs entries with writetimes: {query}"
        );
    }

    #[test]
    fn alternator_extract_cdc_vector_updated() {
        let attrs = CqlValue::Map(vec![(
            CqlValue::Blob(b"v".to_vec()),
            CqlValue::Blob(alternator_vector_blob(&[1.0, 2.0, 3.0])),
        )]);
        let schema = build_alternator_schema();
        let mut row = build_cdc_row(&schema, Some(attrs));
        let backend = DbIndexBackend::Alternator {
            target_columns: vec![ColumnName::from("v")],
        };
        let result = backend.extract_cdc_embeddings(&mut row).unwrap();
        assert_eq!(result, vec![Some(Some(Vector::from(vec![1.0, 2.0, 3.0])))]);
    }

    #[test]
    fn alternator_extract_cdc_vector_not_touched() {
        let schema = build_alternator_schema();
        let mut row = build_cdc_row(&schema, None);
        let backend = DbIndexBackend::Alternator {
            target_columns: vec![ColumnName::from("v")],
        };
        let result = backend.extract_cdc_embeddings(&mut row).unwrap();
        assert_eq!(result, vec![None]);
    }

    #[test]
    fn alternator_extract_cdc_vector_deleted() {
        let attrs = CqlValue::Map(vec![(
            CqlValue::Blob(b"other".to_vec()),
            CqlValue::Blob(alternator_vector_blob(&[9.0])),
        )]);
        let schema = build_alternator_schema();
        let mut row = build_cdc_row(&schema, Some(attrs));
        let backend = DbIndexBackend::Alternator {
            target_columns: vec![ColumnName::from("v")],
        };
        let result = backend.extract_cdc_embeddings(&mut row).unwrap();
        assert_eq!(result, vec![Some(None)]);
    }
}
