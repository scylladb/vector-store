/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::ColumnName;
use crate::CqlLiteral;
use crate::KeyspaceIdentifier;
use crate::TableIdentifier;
use scylla_cdc::CqlIdentifier;

/// Alternator tables store all user attributes in a single map column `:attrs`.
/// Because Alternator (DynamoDB-compatible) is schemaless, different items can
/// have different types for the same attribute name, so a map is used instead
/// of dedicated typed columns.
///
/// Current Alternator schema stores this as map<utf8, bytes>: attribute names
/// are text keys and attribute values are serialized blobs.
const ALTERNATOR_ATTRS_COLUMN: &str = ":attrs";

fn column_accessors<'a>(
    keyspace: &KeyspaceIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
) -> impl Iterator<Item = String> {
    let attributes = keyspace
        .is_alternator()
        .then(|| CqlIdentifier::new(ALTERNATOR_ATTRS_COLUMN));
    columns.into_iter().map(move |column| match &attributes {
        Some(attributes) => format!("{attributes}[{}]", CqlLiteral::new(column)),
        None => CqlIdentifier::new(column.as_ref()).to_string(),
    })
}

fn build_columns_list<'a>(
    keyspace: &KeyspaceIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
) -> String {
    itertools::join(
        column_accessors(keyspace, columns).flat_map(|column| {
            let writetime = format!("writetime({column})");
            [column, writetime]
        }),
        ", ",
    )
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
    let columns = build_columns_list(keyspace, columns);
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

/// Builds the CQL request query appropriate for the given keyspace.
///
/// For CQL-native tables, selects the vector column directly.
/// For Alternator tables, selects from the `:attrs` map column.
pub(crate) fn request_query<'a, 'b>(
    keyspace: &KeyspaceIdentifier,
    table: &TableIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
    primary_key_columns: impl IntoIterator<Item = &'b ColumnName>,
) -> String {
    let columns = build_columns_list(keyspace, columns);
    let restrictions = itertools::join(
        primary_key_columns
            .into_iter()
            .map(AsRef::as_ref)
            .map(CqlIdentifier::new)
            .map(|column| format!("{column} = ?")),
        " AND ",
    );
    format!(
        "
            SELECT {columns}
            FROM {keyspace}.{table}
            WHERE {restrictions}
            "
    )
}

/// Builds the CQL query fetching the values of the given columns for a single
/// row, appropriate for the given keyspace.
pub(crate) fn fetch_vector_query<'a, 'b>(
    keyspace: &KeyspaceIdentifier,
    table: &TableIdentifier,
    columns: impl IntoIterator<Item = &'a ColumnName>,
    primary_key_columns: impl IntoIterator<Item = &'b ColumnName>,
) -> String {
    let columns = itertools::join(column_accessors(keyspace, columns), ", ");
    let restrictions = itertools::join(
        primary_key_columns
            .into_iter()
            .map(AsRef::as_ref)
            .map(CqlIdentifier::new)
            .map(|column| format!("{column} = ?")),
        " AND ",
    );
    format!(
        "
            SELECT {columns}
            FROM {keyspace}.{table}
            WHERE {restrictions}
            "
    )
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
