/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Where and how Vector Store writes its log lines.
//!
//! journald stores every line read from a service's stdout at one priority (info), so ERROR and
//! WARN lines cannot be told apart with `journalctl -p`. A line that starts with a syslog priority
//! in angle brackets (`<3>`) is stored at that priority instead, with the prefix removed
//! (sd-daemon(3), `SyslogLevelPrefix=` in systemd.exec(5)), so the stored message does not change.
//!
//! Which lines are written is up to [`log_filter`]: `RUST_LOG`, with the routine INFO lines of
//! some dependencies left out.

use std::fmt;
use std::io::IsTerminal;
use tracing::Event;
use tracing::Level;
use tracing::Subscriber;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::FormatEvent;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::format::DefaultFields;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::registry::LookupSpan;

/// The kind of stdout Vector Store writes its log lines to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TracingOutput {
    /// Plain lines, e.g. to a file or a pipe.
    StdOut,
    /// Lines with ANSI colors, to a terminal.
    StdOutWithAnsi,
    /// Plain lines to the journal, each prefixed with the syslog priority of its level.
    Journald,
}

/// A stdout layer that writes events with `format`, the way `output` needs them.
pub fn tracing_layer<S, F>(output: TracingOutput, format: F) -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    F: FormatEvent<S, DefaultFields> + 'static,
{
    tracing_layer_with_writer(output, format, std::io::stdout)
}

fn tracing_layer_with_writer<S, F, W>(output: TracingOutput, format: F, writer: W) -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    F: FormatEvent<S, DefaultFields> + 'static,
    W: for<'w> MakeWriter<'w> + 'static,
{
    tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_ansi(output == TracingOutput::StdOutWithAnsi)
        .event_format(OutputFormat {
            format,
            level_prefix: output == TracingOutput::Journald,
        })
}

/// The targets whose INFO lines are routine bookkeeping, so they log only from WARN up unless the
/// filter has a directive of its own for them. tantivy logs every segment merge and each file it
/// deletes after one, six lines per merged segment, every few seconds under writes (VECTOR-1106).
/// A directive's target matches by prefix, so `tantivy` covers tantivy's own crates
/// (`tantivy_fst`, `tantivy_columnar`, ...) too.
const QUIET_TARGETS: [&str; 1] = ["tantivy"];

/// The log filter of `rust_log`, the value of `RUST_LOG` (`info` when it is unset or invalid),
/// with a `<target>=warn` directive for each of [`QUIET_TARGETS`] that `rust_log` has no directive
/// for. A more specific directive still wins over it, so `RUST_LOG=info,tantivy=info` keeps all of
/// tantivy's INFO lines and `RUST_LOG=info,tantivy::indexer=info` those of its indexer.
pub fn log_filter(rust_log: Option<&str>) -> EnvFilter {
    let (spec, filter) = rust_log
        .and_then(|spec| EnvFilter::try_new(spec).ok().map(|filter| (spec, filter)))
        .unwrap_or_else(|| ("info", EnvFilter::new("info")));
    QUIET_TARGETS
        .into_iter()
        .filter(|target| !has_directive_for(spec, target))
        .filter_map(|target| format!("{target}=warn").parse().ok())
        .fold(filter, EnvFilter::add_directive)
}

/// Whether one of the comma-separated directives of `spec` (`target[span{field}]=level`) is for
/// exactly `target`, which a directive added for the same target would be ambiguous with.
fn has_directive_for(spec: &str, target: &str) -> bool {
    spec.split(',')
        .filter_map(|directive| directive.trim().split(['[', '=']).next())
        .any(|name| name == target)
}

/// Whether stdout is a terminal.
pub fn stdout_is_terminal() -> bool {
    std::io::stdout().is_terminal()
}

/// Whether stdout is connected to the journal.
///
/// systemd sets `JOURNAL_STREAM` to `<device>:<inode>` of the stream it connects to stdout or
/// stderr; comparing it with stdout tells whether stdout is that stream (systemd.exec(5)).
pub fn stdout_is_journal() -> bool {
    let Ok(journal_stream) = std::env::var("JOURNAL_STREAM") else {
        return false;
    };
    stdout_dev_ino().is_some_and(|(dev, ino)| is_journal_stream(&journal_stream, dev, ino))
}

/// Formats an event with the inner format, prefixed for [`TracingOutput::Journald`] with the
/// syslog priority of its level. journald reads the prefix per line, so each line of a multi-line
/// event gets it.
struct OutputFormat<F> {
    format: F,
    level_prefix: bool,
}

impl<S, N, F> FormatEvent<S, N> for OutputFormat<F>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        if !self.level_prefix {
            return self.format.format_event(ctx, writer, event);
        }
        let mut lines = LinePrefixWriter {
            writer: &mut writer,
            priority: priority(*event.metadata().level()),
            line_start: true,
        };
        // Without ANSI escapes, as the layer for the journal has them disabled
        self.format
            .format_event(ctx, Writer::new(&mut lines), event)
    }
}

/// Writes through to `writer`, starting each line with the priority prefix, without buffering the
/// event.
struct LinePrefixWriter<'a, 'w> {
    writer: &'a mut Writer<'w>,
    priority: u8,
    line_start: bool,
}

impl fmt::Write for LinePrefixWriter<'_, '_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for line in s.split_inclusive('\n') {
            if self.line_start {
                write!(self.writer, "<{}>", self.priority)?;
            }
            fmt::Write::write_str(self.writer, line)?;
            self.line_start = line.ends_with('\n');
        }
        Ok(())
    }
}

/// The syslog priority of a level. INFO stays at 6, the priority journald gives stdout lines, so
/// that only ERROR and WARN change for `journalctl -p` and log shippers.
fn priority(level: Level) -> u8 {
    match level {
        Level::ERROR => 3,
        Level::WARN => 4,
        Level::INFO => 6,
        Level::DEBUG | Level::TRACE => 7,
    }
}

fn is_journal_stream(journal_stream: &str, dev: u64, ino: u64) -> bool {
    let Some((stream_dev, stream_ino)) = journal_stream.split_once(':') else {
        return false;
    };
    stream_dev.parse() == Ok(dev) && stream_ino.parse() == Ok(ino)
}

#[cfg(unix)]
fn stdout_dev_ino() -> Option<(u64, u64)> {
    use std::os::fd::AsFd;
    use std::os::unix::fs::MetadataExt;

    let stdout = std::io::stdout().as_fd().try_clone_to_owned().ok()?;
    let metadata = std::fs::File::from(stdout).metadata().ok()?;
    Some((metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn stdout_dev_ino() -> Option<(u64, u64)> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tracing_subscriber::prelude::*;

    #[test]
    fn journal_stream_matches_same_device_and_inode() {
        assert!(is_journal_stream("8:12345", 8, 12345));
    }

    #[test]
    fn journal_stream_does_not_match_other_device_or_inode() {
        assert!(!is_journal_stream("8:12345", 9, 12345));
        assert!(!is_journal_stream("8:12345", 8, 12346));
    }

    #[test]
    fn malformed_journal_stream_does_not_match() {
        for value in ["", "8", "8:", ":12345", "8:12345:1", "a:b", " 8:12345"] {
            assert!(!is_journal_stream(value, 8, 12345), "{value:?}");
        }
    }

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Buffer {
        type Writer = Buffer;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// The lines `output` gets for one event per level.
    fn lines(output: TracingOutput) -> Vec<String> {
        let buffer = Buffer::default();
        let layer = tracing_layer_with_writer(
            output,
            tracing_subscriber::fmt::format().with_target(false),
            buffer.clone(),
        );

        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::error!(status = 503, "an error");
            tracing::warn!("a warning");
            tracing::info!("an info");
            tracing::debug!("a debug");
            tracing::trace!("a trace");
        });

        let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        output.lines().map(String::from).collect()
    }

    const EVENTS: [&str; 5] = [
        "ERROR an error status=503",
        " WARN a warning",
        " INFO an info",
        "DEBUG a debug",
        "TRACE a trace",
    ];

    /// Asserts that `line` is the usual line, the RFC 3339 timestamp and then `event`.
    fn assert_usual_line(line: &str, event: &str) {
        assert!(line.split(' ').next().unwrap().ends_with('Z'), "{line}");
        assert!(line.ends_with(event), "{line}");
    }

    #[test]
    fn journald_lines_start_with_the_priority_of_their_level_before_the_usual_line() {
        let lines = lines(TracingOutput::Journald);
        assert_eq!(lines.len(), EVENTS.len(), "{lines:?}");
        for ((line, event), prefix) in lines
            .iter()
            .zip(EVENTS)
            .zip(["<3>", "<4>", "<6>", "<7>", "<7>"])
        {
            assert_usual_line(line.strip_prefix(prefix).expect(line), event);
        }
    }

    #[test]
    fn each_journald_line_of_a_multi_line_event_starts_with_its_priority() {
        let buffer = Buffer::default();
        let layer = tracing_layer_with_writer(
            TracingOutput::Journald,
            tracing_subscriber::fmt::format().with_target(false),
            buffer.clone(),
        );

        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::warn!("changes:\n  first\n  second");
        });

        let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<_> = output.lines().collect();
        assert_eq!(lines.len(), 3, "{output}");
        assert_usual_line(
            lines[0].strip_prefix("<4>").expect(lines[0]),
            " WARN changes:",
        );
        assert_eq!(lines[1..], ["<4>  first", "<4>  second"]);
    }

    #[test]
    fn line_prefix_writer_prefixes_each_line_across_writes() {
        use std::fmt::Write;

        let mut output = String::new();
        let mut writer = Writer::new(&mut output);
        let mut lines = LinePrefixWriter {
            writer: &mut writer,
            priority: 4,
            line_start: true,
        };
        for chunk in ["a", "b\nc", "\n", "", "d\n"] {
            lines.write_str(chunk).unwrap();
        }
        assert_eq!(output, "<4>ab\n<4>c\n<4>d\n");
    }

    /// The messages that pass `log_filter(rust_log)` of INFO and WARN events from Vector Store,
    /// from two tantivy modules and from one of tantivy's own crates.
    fn filtered(rust_log: Option<&str>) -> Vec<&'static str> {
        let buffer = Buffer::default();
        let layer = tracing_layer_with_writer(
            TracingOutput::StdOut,
            tracing_subscriber::fmt::format().with_target(false),
            buffer.clone(),
        );
        let subscriber = tracing_subscriber::registry()
            .with(log_filter(rust_log))
            .with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "vector_store::db", "vs info");
            tracing::warn!(target: "vector_store::db", "vs warn");
            tracing::info!(target: "tantivy::indexer::segment_updater", "tantivy merge info");
            tracing::warn!(target: "tantivy::directory", "tantivy warn");
            tracing::info!(target: "tantivy_fst", "tantivy_fst info");
        });

        let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        output
            .lines()
            .map(|line| *ALL.iter().find(|m| line.ends_with(*m)).expect(line))
            .collect()
    }

    const ALL: [&str; 5] = [
        "vs info",
        "vs warn",
        "tantivy merge info",
        "tantivy warn",
        "tantivy_fst info",
    ];

    const QUIETED: [&str; 3] = ["vs info", "vs warn", "tantivy warn"];

    #[test]
    fn log_filter_leaves_out_tantivy_info_lines_by_default() {
        for rust_log in [
            None,
            Some("info"),
            Some("debug"),
            Some(" info , vector_store=debug"),
        ] {
            assert_eq!(filtered(rust_log), QUIETED, "{rust_log:?}");
        }
        assert_eq!(filtered(Some("warn")), ["vs warn", "tantivy warn"]);
    }

    #[test]
    fn log_filter_keeps_tantivy_info_lines_when_rust_log_asks_for_them() {
        for rust_log in [
            "info,tantivy=info",
            "tantivy,info",
            "info,tantivy[merge]=info",
        ] {
            assert_eq!(filtered(Some(rust_log)), ALL, "{rust_log}");
        }
        // A directive for one module is more specific than the added `tantivy=warn`, so it wins.
        assert_eq!(
            filtered(Some("info,tantivy::indexer=info")),
            ["vs info", "vs warn", "tantivy merge info", "tantivy warn"]
        );
    }

    #[test]
    fn log_filter_falls_back_to_info_for_an_invalid_rust_log() {
        assert_eq!(filtered(Some("vector_store=loud")), QUIETED);
    }

    #[test]
    fn a_directive_for_a_target_names_exactly_that_target() {
        assert!(has_directive_for("info,tantivy=debug", "tantivy"));
        assert!(has_directive_for("tantivy[merge]=info", "tantivy"));
        assert!(has_directive_for(" tantivy ,info", "tantivy"));
        assert!(!has_directive_for("tantivy::indexer=info", "tantivy"));
        assert!(!has_directive_for("info,tantivy_fst=debug", "tantivy"));
        assert!(!has_directive_for("info", "tantivy"));
    }

    #[test]
    fn stdout_lines_are_the_usual_lines() {
        let lines = lines(TracingOutput::StdOut);
        assert_eq!(lines.len(), EVENTS.len(), "{lines:?}");
        for (line, event) in lines.iter().zip(EVENTS) {
            assert_usual_line(line, event);
        }
    }

    #[test]
    fn only_stdout_with_ansi_has_colors() {
        let escape = '\u{1b}';
        assert!(
            lines(TracingOutput::StdOutWithAnsi)
                .iter()
                .all(|line| line.contains(escape))
        );
        assert!(
            !lines(TracingOutput::StdOut)
                .iter()
                .any(|line| line.contains(escape))
        );
        assert!(
            !lines(TracingOutput::Journald)
                .iter()
                .any(|line| line.contains(escape))
        );
    }
}
