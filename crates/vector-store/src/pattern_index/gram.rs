/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use anyhow::anyhow;
use tantivy::tokenizer::NgramTokenizer;
use tantivy::tokenizer::TextAnalyzer;

const MIN_GRAM: usize = 1;
const MAX_GRAM: usize = 3;

/// Every value is indexed between these two marks; they are C0 control characters, which text
/// values are not expected to carry.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

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
