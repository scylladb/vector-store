/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! The LIKE pattern language of ScyllaDB: parsing a pattern and matching a value against it,
//! with the very same semantics as ScyllaDB's own `like_matcher`.
//!
//! A pattern matches a whole value, character (Unicode code point) by character. An unescaped
//! `%` matches any run of characters, the empty one included, and an unescaped `_` matches
//! exactly one character; both match a newline as well. A `\` makes the character following it
//! literal, whatever it is, and a lone `\` ending the pattern is a literal `\`. Every other
//! character matches only itself, so the empty pattern matches only the empty value.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LikeToken {
    Literal(char),
    AnyOne,
    AnyRun,
}

impl LikeToken {
    fn accepts(self, value_char: char) -> bool {
        match self {
            LikeToken::Literal(literal) => literal == value_char,
            LikeToken::AnyOne => true,
            LikeToken::AnyRun => false,
        }
    }
}

/// A parsed LIKE pattern. Consecutive `%` are collapsed into one [`LikeToken::AnyRun`], as they
/// match exactly the same values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LikePattern(Vec<LikeToken>);

impl LikePattern {
    pub(super) fn parse(pattern: &str) -> Self {
        let mut tokens = Vec::new();
        let mut chars = pattern.chars();
        while let Some(pattern_char) = chars.next() {
            let token = match pattern_char {
                '\\' => LikeToken::Literal(chars.next().unwrap_or('\\')),
                '%' => LikeToken::AnyRun,
                '_' => LikeToken::AnyOne,
                literal => LikeToken::Literal(literal),
            };
            push_collapsing_any_runs(&mut tokens, token);
        }
        Self(tokens)
    }

    pub(super) fn tokens(&self) -> &[LikeToken] {
        &self.0
    }

    pub(super) fn matches(&self, value: &str) -> bool {
        let value: Vec<char> = value.chars().collect();
        matches_chars(&self.0, &value)
    }
}

fn push_collapsing_any_runs(tokens: &mut Vec<LikeToken>, token: LikeToken) {
    if token == LikeToken::AnyRun && tokens.last() == Some(&LikeToken::AnyRun) {
        return;
    }
    tokens.push(token);
}

/// Wildcard matching with backtracking to the most recent `%` only: once a later `%` is
/// reached, the run an earlier `%` matched never needs to change, which bounds the work to
/// O(value length * pattern length).
fn matches_chars(tokens: &[LikeToken], value: &[char]) -> bool {
    let mut cursor = Cursor::default();
    while cursor.value < value.len() {
        if !cursor.step(tokens, value) {
            return false;
        }
    }
    tokens[cursor.token..]
        .iter()
        .all(|token| *token == LikeToken::AnyRun)
}

#[derive(Default)]
struct Cursor {
    token: usize,
    value: usize,
    last_any_run: Option<Restart>,
}

/// Where matching resumes when the tokens after the last `%` fail: the `%` takes one more
/// character of the value.
struct Restart {
    token: usize,
    value: usize,
}

impl Cursor {
    fn step(&mut self, tokens: &[LikeToken], value: &[char]) -> bool {
        match tokens.get(self.token) {
            Some(LikeToken::AnyRun) => {
                self.enter_any_run();
                true
            }
            Some(token) if token.accepts(value[self.value]) => {
                self.token += 1;
                self.value += 1;
                true
            }
            _ => self.extend_last_any_run(),
        }
    }

    fn enter_any_run(&mut self) {
        self.token += 1;
        self.last_any_run = Some(Restart {
            token: self.token,
            value: self.value,
        });
    }

    fn extend_last_any_run(&mut self) -> bool {
        let Some(restart) = &mut self.last_any_run else {
            return false;
        };
        restart.value += 1;
        self.token = restart.token;
        self.value = restart.value;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn like(pattern: &str, value: &str) -> bool {
        LikePattern::parse(pattern).matches(value)
    }

    #[rstest]
    #[case::literals("abc", &[LikeToken::Literal('a'), LikeToken::Literal('b'), LikeToken::Literal('c')])]
    #[case::empty("", &[])]
    #[case::wildcards("%_", &[LikeToken::AnyRun, LikeToken::AnyOne])]
    #[case::consecutive_any_runs_collapse("a%%%b", &[LikeToken::Literal('a'), LikeToken::AnyRun, LikeToken::Literal('b')])]
    #[case::any_runs_around_any_one_stay("%_%", &[LikeToken::AnyRun, LikeToken::AnyOne, LikeToken::AnyRun])]
    #[case::escaped_percent(r"\%", &[LikeToken::Literal('%')])]
    #[case::escaped_underscore(r"\_", &[LikeToken::Literal('_')])]
    #[case::escaped_backslash(r"\\", &[LikeToken::Literal('\\')])]
    #[case::escaped_letter(r"\b", &[LikeToken::Literal('b')])]
    #[case::escaped_non_ascii(r"\Ш", &[LikeToken::Literal('Ш')])]
    #[case::trailing_lone_backslash(r"a\", &[LikeToken::Literal('a'), LikeToken::Literal('\\')])]
    #[case::escaped_percent_does_not_collapse(r"%\%%", &[LikeToken::AnyRun, LikeToken::Literal('%'), LikeToken::AnyRun])]
    #[case::cjk("将_", &[LikeToken::Literal('将'), LikeToken::AnyOne])]
    fn parse_turns_a_pattern_into_tokens(#[case] pattern: &str, #[case] expected: &[LikeToken]) {
        assert_eq!(LikePattern::parse(pattern).tokens(), expected);
    }

    /// The cases of ScyllaDB's `test/boost/like_matcher_test.cc`, named after its test cases.
    #[rstest]
    #[case::literal("abc", "abc", true)]
    #[case::literal("abc", "", false)]
    #[case::literal("abc", "a", false)]
    #[case::literal("abc", "b", false)]
    #[case::literal("abc", "ab", false)]
    #[case::literal("abc", "abcd", false)]
    #[case::literal("abc", " abc", false)]
    #[case::empty("", "", true)]
    #[case::empty("", " ", false)]
    #[case::empty("", "abcd", false)]
    #[case::underscore_start("_a", "aa", true)]
    #[case::underscore_start("_a", "Шa", true)]
    #[case::underscore_start("_a", "", false)]
    #[case::underscore_start("_a", "a", false)]
    #[case::underscore_start("_a", ".aa", false)]
    #[case::underscore_end("a_", "aa", true)]
    #[case::underscore_end("a_", "aШ", true)]
    #[case::underscore_end("a_", "", false)]
    #[case::underscore_end("a_", "a", false)]
    #[case::underscore_end("a_", "aa.", false)]
    #[case::underscore_middle("a_c", "abc", true)]
    #[case::underscore_middle("a_c", "aШc", true)]
    #[case::underscore_middle("a_c", "", false)]
    #[case::underscore_middle("a_c", "ac", false)]
    #[case::underscore_middle("a_c", "abcd", false)]
    #[case::underscore_middle("a_c", "abb", false)]
    #[case::underscore_consecutive("a__d", "abcd", true)]
    #[case::underscore_consecutive("a__d", "a__d", true)]
    #[case::underscore_consecutive("a__d", "aШШd", true)]
    #[case::underscore_consecutive("a__d", "", false)]
    #[case::underscore_consecutive("a__d", "abcde", false)]
    #[case::underscore_consecutive("a__d", "a__e", false)]
    #[case::underscore_consecutive("a__d", "e__d", false)]
    #[case::underscore_multiple("a_c_", "abcd", true)]
    #[case::underscore_multiple("a_c_", "arc.", true)]
    #[case::underscore_multiple("a_c_", "aШcШ", true)]
    #[case::underscore_multiple("a_c_", "", false)]
    #[case::underscore_multiple("a_c_", "abcde", false)]
    #[case::underscore_multiple("a_c_", "abdc", false)]
    #[case::underscore_multiple("a_c_", "4bcd", false)]
    #[case::underscore_multiple("_cyll_D_", "ScyllaDB", true)]
    #[case::underscore_multiple("_cyll_D_", "ШcyllaD2", true)]
    #[case::underscore_multiple("_cyll_D_", "", false)]
    #[case::underscore_multiple("_cyll_D_", "ScyllaDB2", false)]
    #[case::percent_start("%bcd", "bcd", true)]
    #[case::percent_start("%bcd", "abcd", true)]
    #[case::percent_start("%bcd", "ШШabcd", true)]
    #[case::percent_start("%bcd", "", false)]
    #[case::percent_start("%bcd", "bcde", false)]
    #[case::percent_start("%bcd", "abcde", false)]
    #[case::percent_start("%bcd", "aaaaaaaaaaaaabce", false)]
    #[case::percent_end("abc%", "abc", true)]
    #[case::percent_end("abc%", "abcd", true)]
    #[case::percent_end("abc%", "abccccccccccccccccccccc", true)]
    #[case::percent_end("abc%", "abcdШШ", true)]
    #[case::percent_end("abc%", "", false)]
    #[case::percent_end("abc%", "a", false)]
    #[case::percent_end("abc%", "ab", false)]
    #[case::percent_end("abc%", "abd", false)]
    #[case::percent_middle("a%z", "az", true)]
    #[case::percent_middle("a%z", "aaz", true)]
    #[case::percent_middle("a%z", "aШШz", true)]
    #[case::percent_middle("a%z", "a...................................z", true)]
    #[case::percent_middle("a%z", "", false)]
    #[case::percent_middle("a%z", "a", false)]
    #[case::percent_middle("a%z", "ab", false)]
    #[case::percent_middle("a%z", "aza", false)]
    #[case::percent_middle("a%z", "aШШШШШШШШШШza", false)]
    #[case::percent_middle("a%_", "a_", true)]
    #[case::percent_middle("a%_", "ab", true)]
    #[case::percent_middle("a%_", "aШШШШШШШШШШ", true)]
    #[case::percent_middle("a%_", "", false)]
    #[case::percent_middle("a%_", "a", false)]
    #[case::percent_middle("a%_", "b_", false)]
    #[case::percent_multiple("a%%z", "az", true)]
    #[case::percent_multiple("a%%z", "aaz", true)]
    #[case::percent_multiple("a%%z", "aШШz", true)]
    #[case::percent_multiple("a%%z", "a...................................z", true)]
    #[case::percent_multiple("a%%z", "", false)]
    #[case::percent_multiple("a%%z", "a", false)]
    #[case::percent_multiple("a%%z", "ab", false)]
    #[case::percent_multiple("a%%z", "aza", false)]
    #[case::percent_multiple("a%%z", "aШШШШШШШШШШza", false)]
    #[case::percent_multiple("|%|%|", "|||", true)]
    #[case::percent_multiple("|%|%|", "|a||", true)]
    #[case::percent_multiple("|%|%|", "||b|", true)]
    #[case::percent_multiple("|%|%|", "|a|b|", true)]
    #[case::percent_multiple("|%|%|", "|||||||", true)]
    #[case::percent_multiple("|%|%|", "|ШШШШШШШШШШza||", true)]
    #[case::percent_multiple("|%|%|", "||ШШШШШШШШШШza|", true)]
    #[case::percent_multiple("|%|%|", "|ШШШШШШШШШШza|....................|", true)]
    #[case::percent_multiple("|%|%|", "", false)]
    #[case::percent_multiple("|%|%|", "|", false)]
    #[case::percent_multiple("|%|%|", "|+", false)]
    #[case::percent_multiple("|%|%|", "|+++++", false)]
    #[case::percent_multiple("|%|%|", "||", false)]
    #[case::percent_multiple("|%|%|", "|.......................|", false)]
    #[case::percent_multiple("|%|%|", "|.......................|++++++++++", false)]
    #[case::percent_multiple("%ac%", "ac", true)]
    #[case::percent_multiple("%ac%", "ack", true)]
    #[case::percent_multiple("%ac%", "lac", true)]
    #[case::percent_multiple("%ac%", "sack", true)]
    #[case::percent_multiple("%ac%", "stack", true)]
    #[case::percent_multiple("%ac%", "backend", true)]
    #[case::percent_multiple("%ac%", "", false)]
    #[case::percent_multiple("%ac%", "a", false)]
    #[case::percent_multiple("%ac%", "c", false)]
    #[case::percent_multiple("%ac%", "abc", false)]
    #[case::percent_multiple("%ac%", "stuck", false)]
    #[case::percent_multiple("%ac%", "dark", false)]
    #[case::escape_underscore(r"a\_", "a_", true)]
    #[case::escape_underscore(r"a\_", "ab", false)]
    #[case::escape_underscore(r"a\__", "a_Ш", true)]
    #[case::escape_underscore(r"a\__", "abc", false)]
    #[case::escape_underscore(r"\__", "_Ш", true)]
    #[case::escape_underscore(r"\__", "a_", false)]
    #[case::escape_percent(r"a\%", "a%", true)]
    #[case::escape_percent(r"a\%", "ab", false)]
    #[case::escape_percent(r"a\%", "abc", false)]
    #[case::escape_percent(r"a%\%", "a%", true)]
    #[case::escape_percent(r"a%\%", "ab%", true)]
    #[case::escape_percent(r"a%\%", "aШШШШШШШШШШ%", true)]
    #[case::escape_percent(r"a%\%", "a", false)]
    #[case::escape_percent(r"a%\%", "abcd", false)]
    #[case::escape_percent(r"a\%z", "a%z", true)]
    #[case::escape_percent(r"a\%z", "az", false)]
    #[case::escape_percent(r"a\%z", "a.z", false)]
    #[case::escape_percent(r"a\%z", "a%.z", false)]
    #[case::escape_percent(r"a%\%z", "a%z", true)]
    #[case::escape_percent(r"a%\%z", "aa%z", true)]
    #[case::escape_percent(r"a%\%z", "aШШШШШШШШШШza%z", true)]
    #[case::escape_percent(r"a%\%z", "az", false)]
    #[case::escape_percent(r"a%\%z", "a.z", false)]
    #[case::escape_percent(r"a%\%z", "a%.z", false)]
    #[case::escape_percent(r"%\%\%.", "%%.", true)]
    #[case::escape_percent(r"%\%\%.", ".%%.", true)]
    #[case::escape_percent(r"%\%\%.", "abcdefgh%%.", true)]
    #[case::escape_percent(r"%\%\%.", "%%", false)]
    #[case::escape_percent(r"%\%\%.", "%.", false)]
    #[case::escape_percent(r"%\%\%.", ".%%.extra", false)]
    #[case::escape_percent(r"\%%", "%", true)]
    #[case::escape_percent(r"\%%", "%.", true)]
    #[case::escape_percent(r"\%%", "%abcdefgh", true)]
    #[case::escape_percent(r"\%%", "%ШШШШШШШШШШ%", true)]
    #[case::escape_percent(r"\%%", "", false)]
    #[case::escape_percent(r"\%%", "a%", false)]
    #[case::escape_percent(r"\%%", "abcde", false)]
    #[case::escape_percent(r"\%a%z", "%az", true)]
    #[case::escape_percent(r"\%a%z", "%azzzzzzz", true)]
    #[case::escape_percent(r"\%a%z", "%a.z", true)]
    #[case::escape_percent(r"\%a%z", "", false)]
    #[case::escape_percent(r"\%a%z", "%", false)]
    #[case::escape_percent(r"\%a%z", "%a", false)]
    #[case::escape_percent(r"a\%\%z", "a%%z", true)]
    #[case::escape_percent(r"a\%\%z", "a%+%z", false)]
    #[case::escape_any_char(r"a\.", "a.", true)]
    #[case::escape_any_char(r"a\.", "az", false)]
    #[case::escape_any_char(r"\bc", "bc", true)]
    #[case::escape_any_char(r"\bc", r"\bc", false)]
    #[case::escape_any_char(r"\Ш", "Ш", true)]
    #[case::escape_any_char(r"\Ш", "ШШ", false)]
    #[case::escape_any_char(r"\Ш", r"\Ш", false)]
    #[case::escape_any_char(r"a\\c", r"a\c", true)]
    #[case::escape_any_char(r"a\\c", r"a\\c", false)]
    #[case::escape_any_char(r"a\\", r"a\", true)]
    #[case::escape_any_char(r"a\\", r"a\\", false)]
    #[case::single_backslash_at_end(r"a%\", r"a\", true)]
    #[case::single_backslash_at_end(r"a%\", r"az\", true)]
    #[case::single_backslash_at_end(r"a%\", r"aaaaaaaaaaaaaaaaaaaaaa\", true)]
    #[case::single_backslash_at_end(r"a%\", "a", false)]
    #[case::single_backslash_at_end(r"a%\", "az", false)]
    #[case::single_backslash_at_end(r"a%\", r"a\\a", false)]
    #[case::double_backslash_at_end(r"a%\\", r"a\", true)]
    #[case::double_backslash_at_end(r"a%\\", r"az\", true)]
    #[case::double_backslash_at_end(r"a%\\", r"a\\", true)]
    #[case::double_backslash_at_end(r"a%\\", r"aaaaaaaaaaaaaaaaaaaaaa\", true)]
    #[case::double_backslash_at_end(r"a%\\", "a", false)]
    #[case::double_backslash_at_end(r"a%\\", "az", false)]
    #[case::double_backslash_at_end(r"a%\\", r"a\\a", false)]
    #[case::brackets("[ab", "[ab", true)]
    #[case::brackets("[ab", "[ba", false)]
    #[case::brackets("[ab]", "[ab]", true)]
    #[case::brackets("[ab]", "a", false)]
    #[case::brackets(r"\[ab\]", "[ab]", true)]
    #[case::brackets(r"\[ab\]", "a", false)]
    #[case::brackets("[^a]", "[^a]", true)]
    #[case::brackets("[^a]", "b", false)]
    #[case::brackets("[[]", "[[]", true)]
    #[case::asterisk("*", "*", true)]
    #[case::asterisk("*", "", false)]
    #[case::asterisk("a*", "a*", true)]
    #[case::asterisk("a*", "", false)]
    #[case::asterisk("a*", "a", false)]
    #[case::asterisk("a*", "aa", false)]
    #[case::asterisk(r"a\*", "a*", true)]
    #[case::asterisk(r"a\*", "", false)]
    #[case::asterisk(r"a\*", "a", false)]
    #[case::asterisk(r"a\*", "aa", false)]
    #[case::period(".", ".", true)]
    #[case::period(".", "a", false)]
    #[case::period("a.c", "a.c", true)]
    #[case::period("a.c", "abc", false)]
    #[case::period(r"a\.c", "a.c", true)]
    #[case::period(r"a\.c", "abc", false)]
    #[case::parentheses("(ab)", "(ab)", true)]
    #[case::parentheses("(ab)", "ab", false)]
    #[case::parentheses(r"\(ab\)", "(ab)", true)]
    #[case::parentheses(r"\(ab\)", "ab", false)]
    #[case::plus("a+", "a+", true)]
    #[case::plus("a+", "aa", false)]
    #[case::question_mark("a?", "a?", true)]
    #[case::question_mark("a?", "", false)]
    #[case::question_mark(r"a\?", "a?", true)]
    #[case::question_mark(r"a\?", "", false)]
    #[case::escaped_digit(r"\3", "3", true)]
    #[case::escaped_digit(r"\3", r"\3", false)]
    #[case::escaped_braces(r"a\{3\}", "a{3}", true)]
    #[case::escaped_braces(r"a\{3\}", "aaa", false)]
    #[case::circumflex("^", "^", true)]
    #[case::circumflex("^", "", false)]
    #[case::circumflex("^abc", "^abc", true)]
    #[case::circumflex("^abc", "abc", false)]
    #[case::circumflex(r"\^abc", "^abc", true)]
    #[case::circumflex(r"\^abc", "abc", false)]
    #[case::circumflex("abc^", "abc^", true)]
    #[case::circumflex(r"abc\^", "abc^", true)]
    #[case::circumflex("a^bc", "a^bc", true)]
    #[case::circumflex(r"a\^bc", "a^bc", true)]
    #[case::dollar("$", "$", true)]
    #[case::dollar("$", "", false)]
    #[case::dollar("abc$", "abc$", true)]
    #[case::dollar("abc$", "abc", false)]
    #[case::dollar(r"abc\$", "abc$", true)]
    #[case::dollar(r"abc\$", "abc", false)]
    #[case::dollar("$abc", "$abc", true)]
    #[case::dollar(r"\$abc", "$abc", true)]
    #[case::dollar("a$bc", "a$bc", true)]
    #[case::dollar(r"a\$bc", "a$bc", true)]
    fn matches_like_scylladb_like_matcher(
        #[case] pattern: &str,
        #[case] value: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(like(pattern, value), expected, "{pattern:?} LIKE {value:?}");
    }

    #[rstest]
    #[case::any_run_spans_newlines("a%z", "a\nb\nz", true)]
    #[case::any_run_matches_a_lone_newline("%", "\n", true)]
    #[case::any_one_matches_a_newline("a_z", "a\nz", true)]
    #[case::any_one_matches_one_newline_only("a_z", "a\n\nz", false)]
    #[case::literal_newline("a\nz", "a\nz", true)]
    #[case::literal_newline_needs_a_newline("a\nz", "a z", false)]
    #[case::substring_across_lines("%b\nc%", "a b\nc d", true)]
    fn matches_newlines_like_any_other_character(
        #[case] pattern: &str,
        #[case] value: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(like(pattern, value), expected, "{pattern:?} LIKE {value:?}");
    }

    #[rstest]
    #[case::cjk_substring("%将军%", "宇将军", true)]
    #[case::cjk_any_one("_将军", "宇将军", true)]
    #[case::cjk_any_one_is_one_code_point("__将军", "宇将军", false)]
    #[case::any_one_is_a_code_point_not_a_byte("a_c", "a€c", true)]
    #[case::backtracks_past_a_partial_match("%aab", "aaab", true)]
    #[case::backtracks_only_the_last_any_run("%a%b%c", "xaxbxbxc", true)]
    #[case::long_value_without_match("%ab%ab%ac", "abababababababababababab", false)]
    fn matches_characters_and_backtracks(
        #[case] pattern: &str,
        #[case] value: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(like(pattern, value), expected, "{pattern:?} LIKE {value:?}");
    }
}
