/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use anyhow::anyhow;
use std::collections::BTreeSet;
use tantivy::tokenizer::NgramTokenizer;
use tantivy::tokenizer::TextAnalyzer;

const MIN_GRAM: usize = 1;
/// A gram is a short piece of a value: from 1 to `MAX_GRAM` characters that stand next to each
/// other. The index keeps all grams of every value. For example, with `MAX_GRAM` = 3 the value
/// `abcd` gives `a`, `b`, `c`, `d`, `ab`, `bc`, `cd`, `abc` and `bcd`. The marks around the value
/// (see [`VALUE_START`]) are part of the grams too.
const MAX_GRAM: usize = 3;

/// Every value is indexed and stored between these two marks. The marks are an optimization for
/// patterns tied to the start or the end of a value: the regex would reject wrong values anyway,
/// but with the marks it has fewer candidates to check. For example, `ab%` looks for the gram
/// `\u{2}ab`, so only values that start with `ab` are candidates. Without the marks it would
/// look for `ab`, so values like `xaby` would be candidates too, and only the regex would reject
/// them. The marks also give the empty pattern a gram, `\u{2}\u{3}`, which only empty values
/// hold. Values that hold these characters themselves still get correct LIKE results.
const VALUE_START: char = '\u{2}';
const VALUE_END: char = '\u{3}';

pub(super) fn framed(value: &str) -> String {
    format!("{VALUE_START}{value}{VALUE_END}")
}

pub(super) fn build_ngram_analyzer() -> anyhow::Result<TextAnalyzer> {
    let tokenizer = NgramTokenizer::new(MIN_GRAM, MAX_GRAM, false)
        .map_err(|e| anyhow!("pattern: failed to create the n-gram tokenizer: {e}"))?;
    Ok(TextAnalyzer::builder(tokenizer).build())
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Token {
    /// A character that must be in the value as it is.
    Literal(char),
    /// `_`: exactly one character.
    AnyChar,
    /// `%`: a sequence of zero or more characters.
    AnySeq,
    /// The start mark of a stored value.
    ValueStart,
    /// The end mark of a stored value.
    ValueEnd,
}

impl Token {
    pub(super) fn is_literal(&self) -> bool {
        matches!(self, Token::Literal(_))
    }

    /// The character the token stands for in a stored value, or `None` for a wildcard.
    pub(super) fn as_char(&self) -> Option<char> {
        match self {
            Token::Literal(literal) => Some(*literal),
            Token::ValueStart => Some(VALUE_START),
            Token::ValueEnd => Some(VALUE_END),
            Token::AnyChar | Token::AnySeq => None,
        }
    }
}

/// The tokens of the pattern as it applies to a stored value: the start mark comes first unless
/// the pattern starts with `%`, and the end mark comes last unless it ends with `%`.
pub(super) fn framed_pattern(tokens: &[Token]) -> Vec<Token> {
    let start = (tokens.first() != Some(&Token::AnySeq)).then_some(Token::ValueStart);
    let end = (tokens.last() != Some(&Token::AnySeq)).then_some(Token::ValueEnd);
    start
        .into_iter()
        .chain(tokens.iter().copied())
        .chain(end)
        .collect()
}

/// A literal run of up to `MAX_GRAM` characters is a gram itself; a longer one gives all its
/// `MAX_GRAM`-long grams, as each shorter piece of it lies inside one of them.
pub(super) fn grams(framed: &[Token]) -> BTreeSet<String> {
    framed
        .split(|token| token.as_char().is_none())
        .filter(|run| !run.is_empty())
        .flat_map(|run| {
            let run: Vec<char> = run.iter().filter_map(Token::as_char).collect();
            if run.len() <= MAX_GRAM {
                return vec![run.into_iter().collect()];
            }
            run.windows(MAX_GRAM)
                .map(|gram| gram.iter().collect())
                .collect()
        })
        .collect()
}

/// Only `%literal%` with a literal of up to `MAX_GRAM` characters and no frame mark is proven by
/// its gram: a value holds that gram exactly when it contains the literal.
pub(super) fn grams_prove_match(tokens: &[Token]) -> bool {
    let [Token::AnySeq, run @ .., Token::AnySeq] = tokens else {
        return false;
    };
    run.len() <= MAX_GRAM
        && run.iter().all(|token| {
            matches!(
                token,
                Token::Literal(literal) if *literal != VALUE_START && *literal != VALUE_END
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grams_of(text: &str) -> BTreeSet<String> {
        let mut analyzer = build_ngram_analyzer().unwrap();
        let mut stream = analyzer.token_stream(text);
        let mut grams = BTreeSet::new();
        while stream.advance() {
            grams.insert(stream.token().text.clone());
        }
        grams
    }

    fn set_of(grams: &[&str]) -> BTreeSet<String> {
        grams.iter().map(|gram| gram.to_string()).collect()
    }

    #[test]
    fn ngram_analyzer_emits_a_gram_for_the_empty_value() {
        assert_eq!(
            grams_of(&framed("")),
            set_of(&["\u{2}", "\u{2}\u{3}", "\u{3}"])
        );
    }

    #[test]
    fn ngram_analyzer_emits_grams_with_frame_marks() {
        assert_eq!(
            grams_of(&framed("ab")),
            set_of(&[
                "\u{2}", "\u{2}a", "\u{2}ab", "a", "ab", "ab\u{3}", "b", "b\u{3}", "\u{3}",
            ])
        );
    }

    #[test]
    fn ngram_analyzer_keeps_whitespace_and_letter_case() {
        let grams = grams_of("a B");

        assert!(grams.contains("a "));
        assert!(grams.contains(" B"));
        assert!(!grams.contains("b"));
    }

    #[test]
    fn ngram_analyzer_emits_cjk_grams_of_up_to_max_gram_chars() {
        assert_eq!(
            grams_of(&framed("将军来了")),
            set_of(&[
                "\u{2}",
                "\u{2}将",
                "\u{2}将军",
                "将",
                "将军",
                "将军来",
                "军",
                "军来",
                "军来了",
                "来",
                "来了",
                "来了\u{3}",
                "了",
                "了\u{3}",
                "\u{3}",
            ])
        );
    }
}
