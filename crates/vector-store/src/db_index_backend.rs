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
use crate::NonemptyArc;
use crate::TableIdentifier;
use crate::TableName;
use crate::Vector;
use crate::vector;
use futures::TryStreamExt;
use regex::Regex;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;
use scylla_cdc::CqlIdentifier;
use scylla_cdc::consumer::CDCRow;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::OnceLock;

/// Alternator tables store all user attributes in a single map column `:attrs`.
/// Because Alternator (DynamoDB-compatible) is schemaless, different items can
/// have different types for the same attribute name, so a map is used instead
/// of dedicated typed columns.
const ALTERNATOR_ATTRS_COLUMN: &str = ":attrs";

pub(crate) enum CdcValueStatus {
    NewValue(CqlValue),
    Deleted,
    Skip,
}

pub(crate) struct IndexLocation {
    pub keyspace: KeyspaceName,
    pub table: TableName,
    pub index: IndexName,
}

pub(crate) enum DbIndexBackend {
    Cql {
        target_columns: NonemptyArc<ColumnName>,
    },
    Alternator {
        target_columns: NonemptyArc<ColumnName>,
    },
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
    pub fn target_column_names(&self) -> &NonemptyArc<ColumnName> {
        match self {
            Self::Cql { target_columns } => target_columns,
            Self::Alternator { .. } => {
                static ALTERNATOR_ATTRS_COLUMN_NAMES: OnceLock<NonemptyArc<ColumnName>> =
                    OnceLock::new();
                ALTERNATOR_ATTRS_COLUMN_NAMES.get_or_init(|| {
                    NonemptyArc::new([ColumnName::from(ALTERNATOR_ATTRS_COLUMN)]).unwrap()
                })
            }
        }
    }

    pub fn extract_vector(&self, value: CqlValue) -> anyhow::Result<Option<Vector>> {
        match self {
            Self::Cql { .. } => Vector::try_from(value).map(Some),
            Self::Alternator { target_columns } => vector::AlternatorAttrs {
                attrs: value,
                target_column: target_columns.first().as_ref(),
            }
            .try_into(),
        }
    }

    pub fn extract_document(&self, value: CqlValue) -> anyhow::Result<Option<String>> {
        match self {
            Self::Cql { .. } => match value {
                CqlValue::Text(s) => Ok(Some(s)),
                CqlValue::Ascii(s) => Ok(Some(s)),
                other => anyhow::bail!("expected text column, got {:?}", other),
            },
            Self::Alternator { .. } => {
                anyhow::bail!("FTS not supported for Alternator")
            }
        }
    }

    pub fn take_cdc_value(&self, row: &mut CDCRow<'_>) -> CdcValueStatus {
        match self {
            Self::Cql { target_columns } => {
                let column = target_columns.first().as_ref();
                if row.is_value_deleted(column) {
                    return CdcValueStatus::Deleted;
                }
                match row.take_value(column) {
                    Some(value) => CdcValueStatus::NewValue(value),
                    None => CdcValueStatus::Skip,
                }
            }
            Self::Alternator { target_columns } => {
                // Check take_value before is_value_deleted.
                // PutItem in Alternator deletes and re-adds :attrs in a single CDC event,
                // so is_value_deleted can be true even when a new value is present.
                // We must check for a value first to avoid misreporting it as deleted.
                let column = ALTERNATOR_ATTRS_COLUMN;
                match row.take_value(column) {
                    Some(value) => CdcValueStatus::NewValue(value),
                    None if row.is_value_deleted(column) => CdcValueStatus::Deleted,
                    None => {
                        let target_deleted = row.take_deleted_elements(column).iter().any(|el| {
                            el.as_text().map(String::as_str)
                                == Some(target_columns.first().as_ref())
                        });
                        if target_deleted {
                            CdcValueStatus::Deleted
                        } else {
                            CdcValueStatus::Skip
                        }
                    }
                }
            }
        }
    }
}

/// Builds the CQL range scan query appropriate for the given keyspace.
///
/// For CQL-native tables, selects the vector column directly.
/// For Alternator tables, selects from the `:attrs` map column.
pub(crate) fn range_scan_query<'a>(
    keyspace: &KeyspaceIdentifier,
    table: &TableIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
    primary_key_list: &str,
    partition_key_list: &str,
) -> String {
    let columns = if keyspace.is_alternator() {
        let attributes = CqlIdentifier::new(ALTERNATOR_ATTRS_COLUMN);
        itertools::join(
            columns.into_iter().map(CqlLiteral::new).flat_map(|column| {
                [
                    format!("{attributes}[{column}]"),
                    format!("writetime({attributes}[{column}])"),
                ]
            }),
            ", ",
        )
    } else {
        itertools::join(
            columns
                .into_iter()
                .map(AsRef::as_ref)
                .map(CqlIdentifier::new)
                .flat_map(|column| [format!("{column}"), format!("writetime({column})")]),
            ", ",
        )
    };
    format!(
        "
        SELECT {primary_key_list}, {columns}
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
}
