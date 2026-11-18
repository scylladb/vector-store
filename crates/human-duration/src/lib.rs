/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Parsing of human readable durations such as `100ms`, `1h 30m` or `2days`.
//!
//! The format is jiff's "friendly" duration format: `<number><unit>` parts
//! with units in descending order, each at most once (`1h 30m`, not
//! `30m 1h`). ISO 8601 durations (`PT5S`) are not accepted. Days and weeks are
//! taken as 24 hours and 7 days. Nonzero months and years are rejected,
//! because their length is not fixed, and so are negative durations.

use jiff::fmt::friendly::SpanParser;
use std::time::Duration;

static PARSER: SpanParser = SpanParser::new();

mod sealed {
    pub trait Sealed {}

    impl<T: AsRef<str> + ?Sized> Sealed for T {}
}

/// The error returned when a value is not a valid human readable duration.
#[derive(Debug, thiserror::Error)]
#[error("invalid duration {value:?}: {source}")]
pub struct ParseDurationError {
    value: String,
    #[source]
    source: jiff::Error,
}

/// Parses a human readable duration out of a string.
pub trait ParseDuration: sealed::Sealed {
    /// Parses the value, ignoring surrounding whitespace, into a [`Duration`].
    fn parse_duration(&self) -> Result<Duration, ParseDurationError>;
}

impl<T: AsRef<str> + ?Sized> ParseDuration for T {
    fn parse_duration(&self) -> Result<Duration, ParseDurationError> {
        let value = self.as_ref();
        parse(value.trim()).map_err(|source| ParseDurationError {
            value: value.to_string(),
            source,
        })
    }
}

fn parse(value: &str) -> Result<Duration, jiff::Error> {
    let span = PARSER.parse_span(value)?;
    let duration = span.to_duration(jiff::SpanRelativeTo::days_are_24_hours())?;
    Duration::try_from(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_human_readable_values() {
        for (value, expected) in [
            ("100ms", Duration::from_millis(100)),
            ("1s", Duration::from_secs(1)),
            ("1.5s", Duration::from_millis(1500)),
            ("3 secs", Duration::from_secs(3)),
            ("2min", Duration::from_secs(120)),
            ("5m 30s", Duration::from_secs(330)),
            ("1h30m", Duration::from_secs(5400)),
            ("1hour", Duration::from_secs(3600)),
            ("2days", Duration::from_secs(2 * 24 * 3600)),
            ("1w", Duration::from_secs(7 * 24 * 3600)),
            ("15days 2min 2s", Duration::from_secs(1_296_122)),
            ("10us", Duration::from_micros(10)),
            ("10ns", Duration::from_nanos(10)),
            ("0s", Duration::ZERO),
            (" 1s ", Duration::from_secs(1)),
        ] {
            assert_eq!(value.parse_duration().unwrap(), expected, "{value:?}");
        }
    }

    #[test]
    fn works_for_owned_strings() {
        assert_eq!(
            String::from("1s").parse_duration().unwrap(),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn rejects_invalid_values() {
        for value in [
            "", " ", "0", "10", "abc", "-1s", "1s ago", "1y", "1 month", "30s 1m", "1s 1s", "PT5S",
            "PT0S",
        ] {
            assert!(value.parse_duration().is_err(), "{value:?}");
        }
    }

    #[test]
    fn error_names_the_value() {
        let err = "abc".parse_duration().unwrap_err();
        assert!(
            err.to_string().starts_with("invalid duration \"abc\": "),
            "{err}"
        );
    }
}
