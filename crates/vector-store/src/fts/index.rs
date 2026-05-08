/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Limit;
use crate::table::PrimaryId;
use anyhow::anyhow;
use std::sync::Mutex;
use tantivy::IndexReader;
use tantivy::IndexWriter;
use tantivy::ReloadPolicy;
use tantivy::TantivyDocument;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Field;
use tantivy::schema::NumericOptions;
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::schema::Value;
use tantivy::Term;

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

    /// Adds a document to the index and commits immediately.
    pub(crate) fn add_document(
        &self,
        primary_id: PrimaryId,
        text_content: &str,
    ) -> anyhow::Result<()> {
        let mut writer = self.writer.lock().map_err(|e| anyhow!("{e}"))?;
        let mut doc = TantivyDocument::new();
        doc.add_u64(self.doc_id_field, u64::from(primary_id));
        doc.add_text(self.text_content_field, text_content);
        writer.add_document(doc)?;
        writer.commit()?;
        Ok(())
    }

    /// Removes all documents matching the given `primary_id` and commits.
    pub(crate) fn remove_document(&self, primary_id: PrimaryId) -> anyhow::Result<()> {
        let mut writer = self.writer.lock().map_err(|e| anyhow!("{e}"))?;
        let term = Term::from_field_u64(self.doc_id_field, u64::from(primary_id));
        writer.delete_term(term);
        writer.commit()?;
        Ok(())
    }

    /// Searches the index and returns matching `(PrimaryId, score)` pairs.
    pub(crate) fn search(
        &self,
        query_str: &str,
        limit: Limit,
    ) -> anyhow::Result<Vec<(PrimaryId, f32)>> {
        self.reader.reload()?;
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(
            searcher.index(),
            vec![self.text_content_field],
        );
        let query = query_parser.parse_query(query_str)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit.0.get()))?;

        top_docs
            .into_iter()
            .map(|(score, doc_address)| {
                let doc: TantivyDocument = searcher.doc(doc_address)?;
                let doc_id = extract_doc_id(&doc, self.doc_id_field)?;
                Ok((PrimaryId::from(doc_id), score))
            })
            .collect()
    }

    /// Returns the total number of documents in the index.
    pub(crate) fn count(&self) -> anyhow::Result<usize> {
        self.reader.reload()?;
        let searcher = self.reader.searcher();
        Ok(searcher
            .segment_readers()
            .iter()
            .map(|r| r.num_docs() as usize)
            .sum())
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

fn extract_doc_id(doc: &TantivyDocument, doc_id_field: Field) -> anyhow::Result<u64> {
    doc.get_first(doc_id_field)
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("document missing doc_id field"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    fn limit(n: usize) -> Limit {
        Limit::from(NonZeroUsize::new(n).unwrap())
    }

    fn pid(id: u64) -> PrimaryId {
        PrimaryId::from(id)
    }

    #[test]
    fn create_fts_index() {
        let index = FtsIndex::new();
        assert!(index.is_ok());
    }

    #[test]
    fn add_and_search_document() {
        let index = FtsIndex::new().unwrap();
        index
            .add_document(pid(1), "the quick brown fox jumps over the lazy dog")
            .unwrap();

        let results = index.search("fox", limit(10)).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, pid(1));
        assert!(results[0].1 > 0.0);
    }

    #[test]
    fn search_no_match() {
        let index = FtsIndex::new().unwrap();
        index
            .add_document(pid(1), "the quick brown fox")
            .unwrap();

        let results = index.search("elephant", limit(10)).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn remove_document() {
        let index = FtsIndex::new().unwrap();
        index.add_document(pid(1), "hello world").unwrap();
        index.remove_document(pid(1)).unwrap();

        let results = index.search("hello", limit(10)).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn count_documents() {
        let index = FtsIndex::new().unwrap();
        assert_eq!(index.count().unwrap(), 0);

        index.add_document(pid(1), "first document").unwrap();
        index.add_document(pid(2), "second document").unwrap();
        index.add_document(pid(3), "third document").unwrap();

        assert_eq!(index.count().unwrap(), 3);
    }

    #[test]
    fn search_relevance_ordering() {
        let index = FtsIndex::new().unwrap();
        index
            .add_document(pid(1), "the cat sat on the mat")
            .unwrap();
        index
            .add_document(pid(2), "rust rust rust is great for rust developers who love rust")
            .unwrap();
        index
            .add_document(pid(3), "rust is a programming language")
            .unwrap();

        let results = index.search("rust", limit(10)).unwrap();
        assert_eq!(results.len(), 2);
        assert!(
            results[0].1 >= results[1].1,
            "results should be ordered by descending score"
        );
    }

    #[test]
    fn search_respects_limit() {
        let index = FtsIndex::new().unwrap();
        index.add_document(pid(1), "apple banana cherry").unwrap();
        index.add_document(pid(2), "apple date elderberry").unwrap();
        index.add_document(pid(3), "apple fig grape").unwrap();

        let results = index.search("apple", limit(2)).unwrap();
        assert_eq!(results.len(), 2);
    }
}
