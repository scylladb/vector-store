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
use regex::Regex;
use scylla::client::session::Session;
use scylla::cluster::metadata::ColumnType;
use scylla::cluster::metadata::NativeType;
use scylla::cluster::metadata::Table;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;
use scylla_cdc::CqlIdentifier;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub(crate) struct IndexLocation {
    pub keyspace: KeyspaceName,
    pub table: TableName,
    pub index: IndexName,
}

#[derive(Clone)]
pub(crate) enum DbIndexBackend {
    Cql {
        target_column: ColumnName,
        filtering_columns: Arc<Vec<ColumnName>>,
    },
    Alternator {
        target_column: ColumnName,
        /// Non-primary-key filtering columns, extracted from `:attrs` during scan/CDC.
        filtering_columns: Arc<Vec<ColumnName>>,
    },
}

impl From<&IndexMetadata> for DbIndexBackend {
    fn from(metadata: &IndexMetadata) -> Self {
        let target_column = metadata.target_column.clone();
        let filtering_columns = Arc::clone(&metadata.filtering_columns);
        if metadata.keyspace_name.is_alternator() {
            Self::Alternator {
                target_column,
                filtering_columns,
            }
        } else {
            Self::Cql {
                target_column,
                filtering_columns,
            }
        }
    }
}

impl DbIndexBackend {
    pub fn vector_column_name(&self) -> &str {
        match self {
            Self::Cql { target_column, .. } => target_column.as_ref(),
            Self::Alternator { .. } => ":attrs",
        }
    }

    pub fn extract_vector(&self, value: CqlValue) -> anyhow::Result<Option<Vector>> {
        match self {
            Self::Cql { .. } => Vector::try_from(value).map(Some),
            Self::Alternator { target_column, .. } => vector::AlternatorAttrs {
                attrs: value,
                target_column: target_column.as_ref(),
            }
            .try_into(),
        }
    }

    /// Converts a raw scan value for a filtering column into its stored form.
    ///
    /// For Alternator tables the scan returns a raw blob from `:attrs['col']`;
    /// pass it through `extract_alternator_scalar` so only scalar S/N attributes
    /// are kept (as `CqlValue::Blob` with the type-tag byte preserved) and
    /// unsupported types are dropped.
    /// For CQL tables the value is already typed; return it directly.
    pub(crate) fn extract_scan_column_value(&self, raw: CqlValue) -> Option<CqlValue> {
        match self {
            Self::Alternator { .. } => {
                if let CqlValue::Blob(blob) = &raw {
                    vector::extract_alternator_scalar(blob)
                } else {
                    Some(raw)
                }
            }
            Self::Cql { .. } => Some(raw),
        }
    }
    /// Injects Alternator filtering columns (absent from the CQL schema) into the
    /// table-columns map as `NativeType::Blob` so that downstream code can create
    /// storage for them.  For CQL backends the map is returned unchanged.
    pub(crate) fn enrich_table_columns(
        &self,
        table_columns: Arc<HashMap<ColumnName, NativeType>>,
        filtering_columns: &[ColumnName],
    ) -> Arc<HashMap<ColumnName, NativeType>> {
        match self {
            Self::Alternator { .. } => {
                let mut cols = (*table_columns).clone();
                for fc in filtering_columns {
                    if !cols.contains_key(fc) {
                        cols.insert(fc.clone(), NativeType::Blob);
                    }
                }
                Arc::new(cols)
            }
            Self::Cql { .. } => table_columns,
        }
    }
}

/// Validates that the target column of an index is a CQL vector column.
///
/// For Alternator keyspaces the vector attribute is stored inside the `:attrs`
/// map and is not a real CQL column, so the check is skipped for those.
pub(crate) fn validate_target_type(
    table: &Table,
    keyspace_name: &KeyspaceName,
    target_name: &str,
) -> anyhow::Result<()> {
    if keyspace_name.is_alternator() {
        return Ok(());
    }
    let column = table.columns.get(target_name).ok_or_else(|| {
        anyhow::anyhow!("invalid target option: column {target_name} does not exist in a table")
    })?;
    if !matches!(column.typ, ColumnType::Vector { .. }) {
        bail!("invalid target option: column {target_name} is not a vector column in a table");
    }
    Ok(())
}

/// Builds the CQL range scan query appropriate for the given keyspace.
///
/// For CQL-native tables, selects the vector column and any filtering columns directly.
/// For Alternator tables, selects from the `:attrs` map column for the vector,
/// and from individual `:attrs['col']` subscripts for each non-primary-key filtering column.
pub(crate) fn range_scan_query(
    keyspace: &KeyspaceIdentifier,
    table: &TableIdentifier,
    target_column: &ColumnName,
    primary_key_columns: &[ColumnName],
    primary_key_list: &str,
    partition_key_list: &str,
    filtering_columns: &[ColumnName],
) -> String {
    if keyspace.is_alternator() {
        let attributes = CqlIdentifier::new(":attrs");
        let vector = CqlLiteral::new(target_column.as_ref());
        // Collect non-primary-key filtering columns to fetch from :attrs.
        let extra_cols: Vec<_> = filtering_columns
            .iter()
            .filter(|c| !primary_key_columns.contains(c))
            .collect();
        let extra_select: String = extra_cols
            .iter()
            .map(|c| {
                let lit = CqlLiteral::new(c.as_ref());
                format!(", {attributes}[{lit}], writetime({attributes}[{lit}])")
            })
            .collect();
        format!(
            "
            SELECT {primary_key_list}, {attributes}[{vector}], writetime({attributes}[{vector}]){extra_select}
            FROM {keyspace}.{table}
            WHERE
                token({partition_key_list}) >= ?
                AND token({partition_key_list}) <= ?
            BYPASS CACHE
            "
        )
    } else {
        let vector = CqlIdentifier::new(target_column.as_ref());
        // Collect non-primary-key filtering columns to fetch directly.
        let extra_cols: Vec<_> = filtering_columns
            .iter()
            .filter(|c| !primary_key_columns.contains(c))
            .collect();
        let extra_select: String = extra_cols
            .iter()
            .map(|c| {
                let ident = CqlIdentifier::new(c.as_ref());
                format!(", {ident}, writetime({ident})")
            })
            .collect();
        format!(
            "
            SELECT {primary_key_list}, {vector}, writetime({vector}){extra_select}
            FROM {keyspace}.{table}
            WHERE
                token({partition_key_list}) >= ?
                AND token({partition_key_list}) <= ?
            BYPASS CACHE
            "
        )
    }
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
            &ColumnName::from("embedding"),
            &[],
            &CqlIdentifier::new("id").to_string(),
            &CqlIdentifier::new("id").to_string(),
            &[],
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
            &ColumnName::from("EmbeddingCol"),
            &[],
            &pk_list,
            &CqlIdentifier::new("UserId").to_string(),
            &[],
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
            &ColumnName::from("VEC"),
            &[],
            &CqlIdentifier::new("ID").to_string(),
            &CqlIdentifier::new("ID").to_string(),
            &[],
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
            &ColumnName::from("my-vector"),
            &[],
            &pk_list,
            &CqlIdentifier::new(":pk").to_string(),
            &[],
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
            &ColumnName::from("v"),
            &[],
            &pk_list,
            &CqlIdentifier::new(":pk").to_string(),
            &[],
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
            &ColumnName::from("my-vector:v1"),
            &[],
            &pk_list,
            &pk_list,
            &[],
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
            &ColumnName::from("EmbeddingCol"),
            &[],
            &pk_list,
            &pk_list,
            &[],
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
            &ColumnName::from("it's a \"test\""),
            &[],
            &pk_list,
            &pk_list,
            &[],
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
