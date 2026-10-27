/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! A LIKE pattern with the rules of ScyllaDB's LIKE operator: the pattern matches the whole
//! value, `%` matches any sequence of characters, `_` matches one character, `\` makes the
//! character after it literal, and a `\` ending the pattern matches `\`.

use super::gram;
use super::gram::Token;
use anyhow::anyhow;
use itertools::Either;
use regex::Regex;
use std::collections::BTreeSet;
use tantivy::DocId;
use tantivy::DocSet;
use tantivy::SegmentReader;
use tantivy::TERMINATED;
use tantivy::TantivyDocument;
use tantivy::Term;
use tantivy::query::AllQuery;
use tantivy::query::BooleanQuery;
use tantivy::query::EnableScoring;
use tantivy::query::Query;
use tantivy::query::TermQuery;
use tantivy::query::Weight;
use tantivy::schema::Field;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::Schema;
use tantivy::schema::Value;
use tantivy::store::StoreReader;

/// The query for the grams every stored value matching a LIKE pattern holds, and the matcher a
/// stored value must still pass when the grams do not prove the match.
pub(super) struct LikeQuery {
    field: Field,
    weight: Box<dyn Weight>,
    regex_matcher: Option<Regex>,
}

impl LikeQuery {
    pub(super) fn new(schema: &Schema, field: Field, pattern: &str) -> anyhow::Result<Self> {
        let framed = gram::framed_pattern(&parse(pattern));
        let regex_matcher = matcher_unless_grams_prove_match(&framed)?;
        // Scoring is off, as LIKE results have no order, so the weight needs only the schema.
        let weight = all_grams_query(field, gram::grams(&framed))
            .weight(EnableScoring::disabled_from_schema(schema))
            .map_err(|e| anyhow!("pattern: failed to build the query: {e}"))?;
        Ok(Self {
            field,
            weight,
            regex_matcher,
        })
    }

    /// The doc ids of the live documents of `segment` whose value matches the pattern, in order.
    pub(super) fn matching_docs(
        &self,
        segment: &SegmentReader,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<DocId>>> {
        // Candidates are read in doc id order, so one cached block of the store is enough.
        const STORE_CACHE_BLOCKS: usize = 1;

        let live_candidates = self
            .candidates(segment)?
            .filter(|&doc_id| !segment.is_deleted(doc_id));
        // No regex means the grams already prove the match, so no value is read.
        let Some(regex) = &self.regex_matcher else {
            return Ok(Either::Left(live_candidates.map(Ok)));
        };
        let store = segment
            .get_store_reader(STORE_CACHE_BLOCKS)
            .map_err(|e| anyhow!("pattern: failed to open the document store: {e}"))?;
        Ok(Either::Right(live_candidates.filter_map(move |doc_id| {
            self.value_matches(&store, regex, doc_id)
                .map(|matches| matches.then_some(doc_id))
                .transpose()
        })))
    }

    /// The doc ids of the documents of `segment` that hold every gram of the pattern, in order.
    fn candidates(&self, segment: &SegmentReader) -> anyhow::Result<impl Iterator<Item = DocId>> {
        let mut scorer = self
            .weight
            .scorer(segment, 1.0)
            .map_err(|e| anyhow!("pattern: failed to run the query: {e}"))?;
        let first = scorer.doc();
        Ok(
            std::iter::successors(Some(first), move |_| Some(scorer.advance()))
                .take_while(|&doc_id| doc_id != TERMINATED),
        )
    }

    fn value_matches(
        &self,
        store: &StoreReader,
        regex: &Regex,
        doc_id: DocId,
    ) -> anyhow::Result<bool> {
        let doc: TantivyDocument = store
            .get(doc_id)
            .map_err(|e| anyhow!("pattern: failed to read the document: {e}"))?;
        Ok(doc
            .get_first(self.field)
            .and_then(|value| value.as_str())
            .and_then(gram::unframed)
            .is_some_and(|value| regex.is_match(value)))
    }
}

/// Consecutive `%` are collapsed into one [`Token::AnySeq`], as they match the same values.
fn parse(pattern: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = pattern.chars();
    while let Some(pattern_char) = chars.next() {
        tokens.push(match pattern_char {
            '\\' => Token::Literal(chars.next().unwrap_or('\\')),
            '%' => Token::AnySeq,
            '_' => Token::AnyChar,
            literal => Token::Literal(literal),
        });
    }
    tokens.dedup_by(|token, previous| *token == Token::AnySeq && *previous == Token::AnySeq);
    tokens
}

/// Documents that hold every gram, or all documents when there is no gram.
fn all_grams_query(field: Field, grams: BTreeSet<String>) -> Box<dyn Query> {
    if grams.is_empty() {
        return Box::new(AllQuery);
    }
    Box::new(BooleanQuery::intersection(
        grams
            .into_iter()
            .map(|gram| {
                Box::new(TermQuery::new(
                    Term::from_field_text(field, &gram),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>
            })
            .collect(),
    ))
}

/// The regex that checks the stored value, or `None` when holding every gram already proves a
/// match, so no value has to be read.
fn matcher_unless_grams_prove_match(framed: &[Token]) -> anyhow::Result<Option<Regex>> {
    if gram::grams_prove_match(framed) {
        return Ok(None);
    }
    matcher(framed).map(Some)
}

/// A regex matching a value that matches the pattern.
fn matcher(framed: &[Token]) -> anyhow::Result<Regex> {
    let body: String = framed
        .iter()
        .map(|token| match token {
            Token::AnyChar => ".".to_string(),
            Token::AnySeq => ".*".to_string(),
            Token::Literal(literal) => regex::escape(&literal.to_string()),
            Token::ValueStart => "^".to_string(),
            Token::ValueEnd => "$".to_string(),
        })
        .collect();
    Regex::new(&format!("(?s){body}"))
        .map_err(|e| anyhow!("pattern: failed to build the matcher: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn like(pattern: &str, value: &str) -> bool {
        matcher(&gram::framed_pattern(&parse(pattern)))
            .unwrap()
            .is_match(value)
    }

    /// The rules of ScyllaDB's LIKE.
    #[rstest]
    #[case::literal("abc", "abc", true)]
    #[case::literal("abc", "abcd", false)]
    #[case::case_sensitive("abc", "ABC", false)]
    #[case::empty("", "", true)]
    #[case::empty("", "a", false)]
    #[case::underscore_start("_a", "Шa", true)]
    #[case::underscore_start("_a", "a", false)]
    #[case::underscore_middle("a_c", "abc", true)]
    #[case::underscore_middle("a_c", "ac", false)]
    #[case::underscore_end("a_", "ab", true)]
    #[case::underscore_end("a_", "abc", false)]
    #[case::underscore_consecutive("a__", "abc", true)]
    #[case::underscore_consecutive("a__", "ab", false)]
    #[case::percent_start("%a", "ba", true)]
    #[case::percent_start("%a", "ab", false)]
    #[case::percent_middle("a%z", "az", true)]
    #[case::percent_middle("a%z", "abz", true)]
    #[case::percent_middle("a%z", "za", false)]
    #[case::percent_end("a%", "a", true)]
    #[case::percent_end("a%", "ba", false)]
    #[case::percent_multiple("%a%z%", "xaxzx", true)]
    #[case::percent_multiple("%a%z%", "xzxax", false)]
    #[case::escape_percent(r"a\%", "a%", true)]
    #[case::escape_percent(r"a\%", "ab", false)]
    #[case::escape_underscore(r"a\_", "a_", true)]
    #[case::escape_underscore(r"a\_", "ab", false)]
    #[case::escape_any_char(r"a\b", "ab", true)]
    #[case::single_backslash_at_end(r"a\", r"a\", true)]
    #[case::single_backslash_at_end(r"a\", "a", false)]
    #[case::double_backslash_at_end(r"a\\", r"a\", true)]
    #[case::regex_special_characters(".*+?^$()[]{}|", ".*+?^$()[]{}|", true)]
    #[case::regex_special_characters("a.c", "abc", false)]
    #[case::any_seq_spans_newlines("a%z", "a\nb\nz", true)]
    #[case::any_char_matches_a_newline("a_z", "a\nz", true)]
    #[case::any_char_is_a_code_point_not_a_byte("a_c", "a€c", true)]
    #[case::cjk_any_char_is_one_code_point("__将军", "宇将军", false)]
    fn regex_matcher_matches_like_scylladb(
        #[case] pattern: &str,
        #[case] value: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(like(pattern, value), expected);
    }
}
