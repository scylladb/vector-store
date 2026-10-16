/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Turning a LIKE pattern into a query over the n-gram index.
//!
//! The pattern is applied to the stored form of a value, which is framed by [`VALUE_START`] and
//! [`VALUE_END`]: a pattern that does not start with `%` must match at the start of the frame,
//! and one that does not end with `%` at its end. The literal runs between the wildcards are
//! then looked up as grams, and the documents holding all of them are the candidates.

use super::like_pattern::LikePattern;
use super::like_pattern::LikeToken;
use super::tantivy::MAX_GRAM;
use super::tantivy::MIN_GRAM;
use super::tantivy::VALUE_END;
use super::tantivy::VALUE_START;
use crate::QueryError;
use std::collections::BTreeSet;
use tantivy::Term;
use tantivy::query::BooleanQuery;
use tantivy::query::Query;
use tantivy::query::TermQuery;
use tantivy::schema::Field;
use tantivy::schema::IndexRecordOption;

/// How a LIKE pattern is answered: the grams every matching value holds, and the pattern the
/// candidates must still be verified against, if the grams alone do not prove a match.
#[derive(Debug)]
pub(super) struct LikePlan {
    grams: BTreeSet<String>,
    verify_with: Option<LikePattern>,
}

impl LikePlan {
    pub(super) fn new(pattern: LikePattern) -> anyhow::Result<Self> {
        ensure_has_literal(&pattern)?;
        let grams = grams_of(&framed_tokens(pattern.tokens()));
        let verify_with = needs_verification(pattern.tokens()).then_some(pattern);
        Ok(Self { grams, verify_with })
    }

    pub(super) fn query(&self, text_field: Field) -> Box<dyn Query> {
        let clauses = self
            .grams
            .iter()
            .map(|gram| term_query(text_field, gram))
            .collect();
        Box::new(BooleanQuery::intersection(clauses))
    }

    pub(super) fn verify_with(&self) -> Option<&LikePattern> {
        self.verify_with.as_ref()
    }
}

/// A pattern of wildcards only has no gram to look up, so answering it would mean reading every
/// value of the index. The empty pattern is fine: it is looked up as the empty frame.
fn ensure_has_literal(pattern: &LikePattern) -> anyhow::Result<()> {
    let tokens = pattern.tokens();
    if tokens.is_empty() || tokens.iter().any(|token| literal_char(token).is_some()) {
        return Ok(());
    }
    Err(QueryError(
        "pattern: a non-empty pattern must contain at least one literal character".to_string(),
    )
    .into())
}

fn framed_tokens(tokens: &[LikeToken]) -> Vec<LikeToken> {
    let start = (tokens.first() != Some(&LikeToken::AnyRun)).then_some(VALUE_START);
    let end = (tokens.last() != Some(&LikeToken::AnyRun)).then_some(VALUE_END);
    start
        .map(LikeToken::Literal)
        .into_iter()
        .chain(tokens.iter().copied())
        .chain(end.map(LikeToken::Literal))
        .collect()
}

fn grams_of(tokens: &[LikeToken]) -> BTreeSet<String> {
    literal_runs(tokens)
        .filter(|run| run.len() >= MIN_GRAM)
        .flat_map(|run| run_grams(&run))
        .collect()
}

fn literal_runs(tokens: &[LikeToken]) -> impl Iterator<Item = Vec<char>> + '_ {
    tokens
        .split(|token| literal_char(token).is_none())
        .map(|run| run.iter().filter_map(literal_char).collect())
}

fn literal_char(token: &LikeToken) -> Option<char> {
    match token {
        LikeToken::Literal(literal) => Some(*literal),
        LikeToken::AnyOne | LikeToken::AnyRun => None,
    }
}

/// A run of at most `MAX_GRAM` characters is a gram itself. A longer one is looked up through
/// its `MAX_GRAM`-long grams only: every shorter piece of it lies inside one of them, so it would
/// add a posting list to intersect without narrowing the result.
fn run_grams(run: &[char]) -> Vec<String> {
    if run.len() <= MAX_GRAM {
        return vec![run.iter().collect()];
    }
    run.windows(MAX_GRAM)
        .map(|gram| gram.iter().collect())
        .collect()
}

/// Only `%literal%`, with a literal short enough to be a single gram and holding no frame mark,
/// is proven by the index: a value holds that gram exactly when it contains the literal.
fn needs_verification(tokens: &[LikeToken]) -> bool {
    let [LikeToken::AnyRun, literal @ .., LikeToken::AnyRun] = tokens else {
        return true;
    };
    literal.len() > MAX_GRAM || !literal.iter().all(is_value_literal)
}

fn is_value_literal(token: &LikeToken) -> bool {
    literal_char(token).is_some_and(|literal| literal != VALUE_START && literal != VALUE_END)
}

fn term_query(text_field: Field, term: &str) -> Box<dyn Query> {
    Box::new(TermQuery::new(
        Term::from_field_text(text_field, term),
        IndexRecordOption::Basic,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryError;
    use rstest::rstest;
    use tantivy::schema::Schema;
    use tantivy::schema::TEXT;

    fn plan(pattern: &str) -> LikePlan {
        LikePlan::new(LikePattern::parse(pattern)).unwrap()
    }

    fn text_field() -> Field {
        let mut schema = Schema::builder();
        schema.add_text_field("text", TEXT)
    }

    #[rstest]
    #[case::short_substring("%ab%", &["ab"])]
    #[case::max_gram_substring("%abc%", &["abc"])]
    #[case::long_substring("%abcd%", &["abc", "bcd"])]
    #[case::repeated_grams_are_deduplicated("%aaaaa%", &["aaa"])]
    #[case::prefix("AB-%", &["\u{2}AB", "AB-"])]
    #[case::short_prefix("A%", &["\u{2}A"])]
    #[case::suffix("%-XL", &["-XL", "XL\u{3}"])]
    #[case::exact("abc", &["\u{2}ab", "abc", "bc\u{3}"])]
    #[case::empty("", &["\u{2}\u{3}"])]
    #[case::runs_between_wildcards("%a%bc_d%", &["a", "bc", "d"])]
    #[case::any_one_at_the_end("%-_", &["\u{3}", "-"])]
    #[case::any_one_at_the_start("_a", &["\u{2}", "a\u{3}"])]
    #[case::escaped_wildcards(r"%\%\_%", &["%_"])]
    #[case::cjk("%将军来了%", &["军来了", "将军来"])]
    fn plan_looks_up_the_grams_of_the_framed_literal_runs(
        #[case] pattern: &str,
        #[case] expected: &[&str],
    ) {
        assert_eq!(
            plan(pattern).grams.into_iter().collect::<Vec<_>>(),
            expected
        );
    }

    #[rstest]
    #[case::short_substring("%a%", false)]
    #[case::max_gram_substring("%abc%", false)]
    #[case::cjk_substring("%将军%", false)]
    #[case::long_substring("%abcd%", true)]
    #[case::prefix("ab%", true)]
    #[case::suffix("%ab", true)]
    #[case::exact("ab", true)]
    #[case::empty("", true)]
    #[case::any_one_inside("%a_b%", true)]
    #[case::two_runs("%a%b%", true)]
    #[case::substring_holding_the_start_mark("%\u{2}%", true)]
    #[case::substring_holding_the_end_mark("%a\u{3}%", true)]
    fn plan_verifies_unless_the_gram_proves_the_match(
        #[case] pattern: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(plan(pattern).verify_with().is_some(), expected);
    }

    #[test]
    fn plan_verifies_against_the_unframed_pattern() {
        assert_eq!(plan("ab%").verify_with(), Some(&LikePattern::parse("ab%")));
    }

    #[rstest]
    #[case::any_run("%")]
    #[case::any_runs("%%")]
    #[case::any_one("_")]
    #[case::any_ones("___")]
    #[case::any_one_between_any_runs("%_%")]
    fn plan_rejects_a_pattern_without_literal_characters(#[case] pattern: &str) {
        let err = LikePlan::new(LikePattern::parse(pattern)).unwrap_err();

        assert!(err.downcast_ref::<QueryError>().is_some(), "got: {err}");
        assert!(err.to_string().contains("at least one literal character"));
    }

    #[test]
    fn query_of_several_terms_is_their_intersection() {
        let query = plan("%abcd%").query(text_field());

        let boolean_query = query.downcast_ref::<BooleanQuery>().unwrap();
        assert_eq!(boolean_query.clauses().len(), 2);
    }
}
