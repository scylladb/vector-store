/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::table::PrimaryId;
use std::sync::Mutex;
use tantivy::IndexReader;
use tantivy::IndexWriter;
use tantivy::ReloadPolicy;
use tantivy::schema::Field;
use tantivy::schema::NumericOptions;
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;

/// Full-text search index backed by Tantivy with an in-memory directory.
///
/// Each `FtsIndex` instance manages a single Tantivy index with a fixed schema:
/// - `doc_id`: u64 field (stored + indexed) mapping to [`PrimaryId`]
/// - `text_content`: TEXT field with positions enabled for phrase queries
pub(crate) struct FtsIndex {
    doc_id_field: Field,
    text_content_field: Field,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
}

impl FtsIndex {
    /// Creates a new in-memory FTS index.
    pub(crate) fn new() -> anyhow::Result<Self> {
        let (schema, doc_id_field, text_content_field) = build_schema();
        let index = tantivy::Index::create_in_ram(schema);
        let writer = index.writer(50_000_000)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self {
            doc_id_field,
            text_content_field,
            writer: Mutex::new(writer),
            reader,
        })
    }

    pub(crate) fn doc_id_field(&self) -> Field {
        self.doc_id_field
    }

    pub(crate) fn text_content_field(&self) -> Field {
        self.text_content_field
    }

    pub(crate) fn reader(&self) -> &IndexReader {
        &self.reader
    }

    pub(crate) fn writer(&self) -> &Mutex<IndexWriter> {
        &self.writer
    }
}

fn build_schema() -> (Schema, Field, Field) {
    let mut builder = Schema::builder();

    let doc_id_field = builder.add_u64_field(
        "doc_id",
        NumericOptions::default().set_stored().set_indexed(),
    );

    let text_content_field = builder.add_text_field(
        "text_content",
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
        ),
    );

    (builder.build(), doc_id_field, text_content_field)
}
