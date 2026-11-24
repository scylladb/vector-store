/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Reads `/highlight` request bodies as a stream, one document at a time, so that the
//! FTS index highlights a document while the next one arrives.

use crate::fts_index::HighlightDocument;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use std::cell::Cell;
use std::io;
use std::io::Read;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use struson::reader::JsonReader;
use struson::reader::JsonStreamReader;
use struson::reader::ReaderError;
use struson::reader::ReaderSettings;
use struson::reader::ValueType;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug;

const MIB: usize = 1024 * 1024;
/// Limits of a `/highlight` request.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HighlightLimits {
    pub(crate) max_body_bytes: u64,
    /// Counted after JSON unescaping.
    pub(crate) max_text_bytes: usize,
    /// tantivy copies an invalid query whole into its error, outside the memory budget.
    pub(crate) max_query_bytes: usize,
    pub(crate) max_documents: usize,
    /// struson reads a field name whole.
    pub(crate) max_name_bytes: usize,
    pub(crate) drain_timeout: Duration,
}

impl Default for HighlightLimits {
    fn default() -> Self {
        Self {
            // Bounds the work for one request: 100 MiB of text, which ScyllaDB reads at most in
            // one query, escaped up to 6 times longer. A table with clustering columns is read
            // row by row, so many large rows can come to more.
            max_body_bytes: 640 * MIB as u64,
            max_text_bytes: 100 * MIB,
            // What `/bm25` accepts within axum's default body limit.
            max_query_bytes: 2 * MIB,
            // The largest LIMIT of a BM25 query.
            max_documents: 1000,
            max_name_bytes: 4096,
            // As long as a whole body may take to arrive.
            drain_timeout: Duration::from_secs(120),
        }
    }
}

#[derive(Debug)]
pub(crate) struct BodyParts {
    pub(crate) query: oneshot::Sender<String>,
    pub(crate) documents: mpsc::Sender<HighlightDocument>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BodyError {
    #[error("Failed to parse the request body as JSON: {0}")]
    Syntax(String),
    #[error("Failed to deserialize the JSON body into the target type: {0}")]
    Data(String),
    #[error("a highlight document or query may be at most {max} bytes long")]
    TextTooLong { max: usize },
    #[error("a highlight request may carry at most {max} documents")]
    TooManyDocuments { max: usize },
    #[error("a field name may be at most {max} bytes long")]
    NameTooLong { max: usize },
    #[error("the highlight request body is too large")]
    BodyTooLarge,
    #[error("failed to receive the request body")]
    Receive,
    #[error("the request was answered before its body was read")]
    Cancelled,
}

#[derive(Clone, Copy, Debug)]
enum ReceiveFailure {
    TooLarge,
    Failed,
}

impl From<ReceiveFailure> for BodyError {
    fn from(failure: ReceiveFailure) -> Self {
        match failure {
            ReceiveFailure::TooLarge => BodyError::BodyTooLarge,
            ReceiveFailure::Failed => BodyError::Receive,
        }
    }
}

impl From<ReceiveFailure> for io::Error {
    fn from(failure: ReceiveFailure) -> Self {
        io::Error::other(BodyError::from(failure))
    }
}

/// struson wraps the errors of the body stream in its own, so the stream records them here.
#[derive(Debug, Default)]
pub(crate) struct ReceiveState(OnceLock<ReceiveFailure>);

impl ReceiveState {
    /// The first failure is kept.
    fn record(&self, failure: ReceiveFailure) {
        _ = self.0.set(failure);
    }

    fn failure(&self) -> Option<ReceiveFailure> {
        self.0.get().copied()
    }

    fn error(&self) -> Option<BodyError> {
        self.failure().map(BodyError::from)
    }
}

#[derive(Debug)]
pub(crate) struct LimitedBody<S> {
    inner: S,
    received: u64,
    limit: u64,
    state: Arc<ReceiveState>,
}

impl<S> LimitedBody<S> {
    pub(crate) fn new(inner: S, limits: &HighlightLimits) -> Self {
        Self {
            inner,
            received: 0,
            limit: limits.max_body_bytes,
            state: Arc::default(),
        }
    }

    pub(crate) fn state(&self) -> Arc<ReceiveState> {
        Arc::clone(&self.state)
    }

    /// The body without its limits, for discarding the rest of it.
    pub(crate) fn into_inner(self) -> S {
        self.inner
    }
}

impl<S, E> Stream for LimitedBody<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(failure) = self.state.failure() {
            return Poll::Ready(Some(Err(failure.into())));
        }
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                self.received += chunk.len() as u64;
                if self.received > self.limit {
                    self.state.record(ReceiveFailure::TooLarge);
                    return Poll::Ready(Some(Err(ReceiveFailure::TooLarge.into())));
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(err))) => {
                self.state.record(ReceiveFailure::Failed);
                Poll::Ready(Some(Err(io::Error::other(err))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Discards the rest of a body after an early answer, so that a client sending the whole
/// body before reading the answer gets it instead of a reset (RFC 9112 section 9.6).
/// Whatever limit stopped the read, the client may still be sending, and discarding holds no
/// memory, so only time end it.
pub(crate) async fn drain<S, E>(mut body: S, limits: HighlightLimits)
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut discarded: u64 = 0;
    let drained = tokio::time::timeout(limits.drain_timeout, async {
        while let Some(chunk) = body.next().await {
            let Ok(chunk) = chunk else {
                return false;
            };
            discarded += chunk.len() as u64;
        }
        true
    })
    .await;
    if drained != Ok(true) {
        debug!("highlight: stopped discarding the request body after {discarded} bytes");
    }
}

/// Returns the number of documents.
pub(crate) fn read_body(
    body: impl Read,
    limits: &HighlightLimits,
    received: &ReceiveState,
    parts: BodyParts,
) -> Result<usize, BodyError> {
    let guard = Rc::new(ReadGuard::default());
    BodyReader {
        json: JsonStreamReader::new_custom(
            GuardedRead {
                inner: body,
                guard: Rc::clone(&guard),
                documents: parts.documents.clone(),
            },
            // Otherwise struson reads the names inside skipped fields whole. The default nesting
            // limit of 128 levels stays: skipping a field takes memory for each level, and a
            // body may be 640 MiB.
            ReaderSettings {
                track_path: false,
                ..ReaderSettings::default()
            },
        ),
        guard,
        limits,
        received,
        query: Some(parts.query),
        documents: parts.documents,
        chunk: vec![0; 64 * 1024],
    }
    .read()
}

/// Why [`GuardedRead`] stopped reading, which struson's errors don't tell.
#[derive(Default)]
struct ReadGuard {
    /// Set while a field name is read.
    name_remaining: Cell<Option<usize>>,
    name_too_long: Cell<bool>,
    cancelled: Cell<bool>,
}

impl ReadGuard {
    fn error(&self, limits: &HighlightLimits) -> Option<BodyError> {
        if self.cancelled.get() {
            return Some(BodyError::Cancelled);
        }
        self.name_too_long.get().then_some(BodyError::NameTooLong {
            max: limits.max_name_bytes,
        })
    }
}

/// Stops reading once nobody takes the documents any more, and limits field names.
struct GuardedRead<R> {
    inner: R,
    guard: Rc<ReadGuard>,
    documents: mpsc::Sender<HighlightDocument>,
}

impl<R: Read> Read for GuardedRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.documents.is_closed() {
            self.guard.cancelled.set(true);
            return Err(io::Error::other("nobody takes the documents"));
        }
        let Some(remaining) = self.guard.name_remaining.get() else {
            return self.inner.read(buf);
        };
        if remaining == 0 {
            self.guard.name_too_long.set(true);
            return Err(io::Error::other("field name too long"));
        }
        let len = buf.len().min(remaining);
        let read = self.inner.read(&mut buf[..len])?;
        self.guard.name_remaining.set(Some(remaining - read));
        Ok(read)
    }
}

struct BodyReader<'a, R: Read> {
    json: JsonStreamReader<GuardedRead<R>>,
    guard: Rc<ReadGuard>,
    limits: &'a HighlightLimits,
    received: &'a ReceiveState,
    query: Option<oneshot::Sender<String>>,
    documents: mpsc::Sender<HighlightDocument>,
    chunk: Vec<u8>,
}

impl<R: Read> BodyReader<'_, R> {
    fn read(mut self) -> Result<usize, BodyError> {
        let mut documents = None;

        self.json.begin_object().map_err(|err| self.error(err))?;
        while self.json.has_next().map_err(|err| self.error(err))? {
            let name = self.next_name()?;
            match name.as_str() {
                "query" => {
                    let Some(query_tx) = self.query.take() else {
                        return Err(BodyError::Data("duplicate field `query`".to_string()));
                    };
                    let query = self.read_text(self.limits.max_query_bytes)?;
                    query_tx.send(query).map_err(|_| BodyError::Cancelled)?;
                }
                "documents" => {
                    if documents.is_some() {
                        return Err(BodyError::Data("duplicate field `documents`".to_string()));
                    }
                    if self.query.is_some() {
                        return Err(BodyError::Data(
                            "field `query` must come before field `documents`".to_string(),
                        ));
                    }
                    documents = Some(self.read_documents()?);
                }
                // Skipped, as before, so that a newer ScyllaDB can add fields.
                _ => self.json.skip_value().map_err(|err| self.error(err))?,
            }
        }
        self.json.end_object().map_err(|err| self.error(err))?;
        let received = self.received;
        let limits = self.limits;
        let guard = &self.guard;
        self.json.consume_trailing_whitespace().map_err(|err| {
            guard
                .error(limits)
                .unwrap_or_else(|| reader_error(err, received))
        })?;

        if self.query.is_some() {
            return Err(BodyError::Data("missing field `query`".to_string()));
        }
        documents.ok_or_else(|| BodyError::Data("missing field `documents`".to_string()))
    }

    fn read_documents(&mut self) -> Result<usize, BodyError> {
        self.json.begin_array().map_err(|err| self.error(err))?;
        let mut count = 0;
        while self.json.has_next().map_err(|err| self.error(err))? {
            count += 1;
            if count > self.limits.max_documents {
                return Err(BodyError::TooManyDocuments {
                    max: self.limits.max_documents,
                });
            }
            let document = self.read_text(self.limits.max_text_bytes)?;
            self.documents
                .blocking_send(Box::new(document))
                .map_err(|_| BodyError::Cancelled)?;
        }
        self.json.end_array().map_err(|err| self.error(err))?;
        Ok(count)
    }

    fn read_text(&mut self, max_text_bytes: usize) -> Result<String, BodyError> {
        let value_type = self.json.peek().map_err(|err| self.error(err))?;
        if value_type != ValueType::String {
            return Err(BodyError::Data(format!(
                "invalid type: {}, expected a string",
                value_type_name(value_type)
            )));
        }

        let received = self.received;
        let limits = self.limits;
        let guard = &self.guard;
        let chunk = &mut self.chunk;
        let mut text = Vec::new();
        let mut string = self.json.next_string_reader().map_err(|err| {
            guard
                .error(limits)
                .unwrap_or_else(|| reader_error(err, received))
        })?;
        loop {
            let len = string.read(chunk).map_err(|err| {
                guard
                    .error(limits)
                    .unwrap_or_else(|| io_error(&err, received))
            })?;
            if len == 0 {
                break;
            }
            if text.len() + len > max_text_bytes {
                return Err(BodyError::TextTooLong {
                    max: max_text_bytes,
                });
            }
            text.extend_from_slice(&chunk[..len]);
        }
        drop(string);

        // The string reader returns whole UTF-8 characters only, so this never fails.
        String::from_utf8(text)
            .map_err(|err| BodyError::Syntax(format!("invalid UTF-8 in a string: {err}")))
    }

    fn next_name(&mut self) -> Result<String, BodyError> {
        // struson's buffer of 1 KiB isn't counted, so a name may be a little longer.
        self.guard
            .name_remaining
            .set(Some(self.limits.max_name_bytes));
        let name = self.json.next_name_owned();
        self.guard.name_remaining.set(None);
        name.map_err(|err| self.error(err))
    }

    fn error(&self, err: ReaderError) -> BodyError {
        self.guard
            .error(self.limits)
            .unwrap_or_else(|| reader_error(err, self.received))
    }
}

fn reader_error(err: ReaderError, received: &ReceiveState) -> BodyError {
    if let Some(err) = received.error() {
        return err;
    }
    match err {
        // Not from receiving the body, so invalid UTF-8.
        ReaderError::IoError { .. } => BodyError::Syntax(err.to_string()),
        ReaderError::UnexpectedValueType { .. } | ReaderError::UnexpectedStructure { .. } => {
            BodyError::Data(err.to_string())
        }
        _ => BodyError::Syntax(err.to_string()),
    }
}

fn io_error(err: &io::Error, received: &ReceiveState) -> BodyError {
    received
        .error()
        .unwrap_or_else(|| BodyError::Syntax(err.to_string()))
}

fn value_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Array => "sequence",
        ValueType::Object => "map",
        ValueType::String => "string",
        ValueType::Number => "number",
        ValueType::Boolean => "boolean",
        ValueType::Null => "null",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> HighlightLimits {
        HighlightLimits {
            max_body_bytes: 1024 * 1024,
            max_text_bytes: 1000,
            max_query_bytes: 1000,
            max_documents: 3,
            ..HighlightLimits::default()
        }
    }

    fn read(body: &[u8], limits: &HighlightLimits) -> (Vec<String>, Result<usize, BodyError>) {
        let (parts, mut query, mut documents) = body_parts(100);
        let result = read_body(body, limits, &ReceiveState::default(), parts);
        let mut items = Vec::new();
        if let Ok(query) = query.try_recv() {
            items.push(format!("query: {query}"));
        }
        while let Ok(document) = documents.try_recv() {
            items.push(format!("document: {}", (*document).as_ref()));
        }
        (items, result)
    }

    fn body_parts(
        documents: usize,
    ) -> (
        BodyParts,
        oneshot::Receiver<String>,
        mpsc::Receiver<HighlightDocument>,
    ) {
        let (query_tx, query_rx) = oneshot::channel();
        let (documents_tx, documents_rx) = mpsc::channel(documents);
        let parts = BodyParts {
            query: query_tx,
            documents: documents_tx,
        };
        (parts, query_rx, documents_rx)
    }

    #[test]
    fn query_comes_first_then_documents_in_order() {
        let (items, result) = read(
            br#"{"query":"q","documents":["a","b\n\u00e9\ud83d\ude00"]}"#,
            &limits(),
        );

        assert_eq!(result.unwrap(), 2);
        assert_eq!(items, ["query: q", "document: a", "document: b\né😀"]);
    }

    #[test]
    fn documents_before_the_query_are_rejected() {
        let (items, result) = read(br#"{"documents":["a","b"],"query":"q"}"#, &limits());

        assert!(matches!(result, Err(BodyError::Data(_))), "{result:?}");
        assert!(items.is_empty(), "{items:?}");
    }

    #[test]
    fn unknown_fields_are_skipped() {
        let (items, result) = read(
            br#"{"x":[1,{"y":null}],"query":"q","documents":[],"z":"w"}"#,
            &limits(),
        );

        assert_eq!(result.unwrap(), 0);
        assert_eq!(items, ["query: q"]);
    }

    #[test]
    fn too_many_documents_are_too_large() {
        let (items, result) = read(br#"{"query":"q","documents":["a","b","c","d"]}"#, &limits());

        assert!(
            matches!(result, Err(BodyError::TooManyDocuments { max: 3 })),
            "{result:?}"
        );
        assert_eq!(
            items.len(),
            4,
            "the documents before the limit were sent: {items:?}"
        );
    }

    #[test]
    fn a_document_over_the_limit_is_too_large() {
        let body = format!(
            r#"{{"query":"q","documents":["a","{}"]}}"#,
            "x".repeat(1001)
        );

        let (items, result) = read(body.as_bytes(), &limits());

        assert!(
            matches!(result, Err(BodyError::TextTooLong { max: 1000 })),
            "{result:?}"
        );
        assert_eq!(items, ["query: q", "document: a"]);
    }

    #[test]
    fn a_query_has_a_lower_limit_than_a_document() {
        let limits = HighlightLimits {
            max_query_bytes: 10,
            ..limits()
        };
        let body = format!(r#"{{"query":"{}","documents":[]}}"#, "x".repeat(11));

        let (_, result) = read(body.as_bytes(), &limits);

        assert!(
            matches!(result, Err(BodyError::TextTooLong { max: 10 })),
            "{result:?}"
        );
    }

    #[test]
    fn a_query_over_the_limit_is_too_large() {
        let body = format!(r#"{{"query":"{}","documents":[]}}"#, "x".repeat(1001));

        let (_, result) = read(body.as_bytes(), &limits());

        assert!(
            matches!(result, Err(BodyError::TextTooLong { .. })),
            "{result:?}"
        );
    }

    #[test]
    fn a_document_at_the_limit_is_read() {
        let body = format!(r#"{{"query":"q","documents":["{}"]}}"#, "x".repeat(1000));

        let (items, result) = read(body.as_bytes(), &limits());

        assert_eq!(result.unwrap(), 1);
        assert_eq!(items[1].len(), "document: ".len() + 1000);
    }

    #[test]
    fn a_field_name_over_the_limit_is_too_large() {
        let limits = HighlightLimits {
            max_name_bytes: 100,
            ..limits()
        };
        // Past the limit and struson's buffer of 1 KiB, which the limit doesn't count.
        let body = format!(
            r#"{{"{}":0,"query":"q","documents":[]}}"#,
            "x".repeat(10_000)
        );

        let (_, result) = read(body.as_bytes(), &limits);

        assert!(
            matches!(result, Err(BodyError::NameTooLong { max: 100 })),
            "{result:?}"
        );
    }

    #[test]
    fn long_names_inside_a_skipped_field_are_skipped() {
        let limits = HighlightLimits {
            max_name_bytes: 100,
            ..limits()
        };
        let body = format!(
            r#"{{"x":{{"{}":0}},"query":"q","documents":["a"]}}"#,
            "x".repeat(10_000)
        );

        let (items, result) = read(body.as_bytes(), &limits);

        assert_eq!(result.unwrap(), 1);
        assert_eq!(items, ["query: q", "document: a"]);
    }

    #[test]
    fn field_names_within_the_limit_are_read() {
        let limits = HighlightLimits {
            max_name_bytes: 100,
            ..limits()
        };
        let body = format!(
            r#"{{"{}":0,"query":"q","documents":["a"]}}"#,
            "x".repeat(90)
        );

        let (items, result) = read(body.as_bytes(), &limits);

        assert_eq!(result.unwrap(), 1);
        assert_eq!(items, ["query: q", "document: a"]);
    }

    #[test]
    fn skipping_a_field_stops_once_nobody_takes_the_documents() {
        struct Counted<'a>(&'a [u8], usize);
        impl Read for Counted<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                let read = self.0.read(buf)?;
                self.1 += read;
                Ok(read)
            }
        }
        let (parts, _query, documents) = body_parts(1);
        drop(documents);
        let body = format!(
            r#"{{"query":"q","x":"{}","documents":["a"]}}"#,
            "x".repeat(100_000)
        );
        let mut counted = Counted(body.as_bytes(), 0);

        let result = read_body(&mut counted, &limits(), &ReceiveState::default(), parts);

        assert!(matches!(result, Err(BodyError::Cancelled)), "{result:?}");
        assert!(counted.1 < 10_000, "read {} bytes", counted.1);
    }

    #[test]
    fn stopping_at_the_end_of_the_body_is_cancelled() {
        /// Serves the body in two reads, and stops taking documents after the first.
        struct StopsAfterTheObject {
            parts: Vec<&'static [u8]>,
            documents: Option<mpsc::Receiver<HighlightDocument>>,
        }
        impl Read for StopsAfterTheObject {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                let Some(part) = self.parts.pop() else {
                    return Ok(0);
                };
                buf[..part.len()].copy_from_slice(part);
                if self.parts.len() == 1 {
                    self.documents = None;
                }
                Ok(part.len())
            }
        }
        let (parts, _query, documents) = body_parts(1);
        let body = StopsAfterTheObject {
            parts: vec![b" \n", br#"{"query":"q","documents":[]}"#],
            documents: Some(documents),
        };

        let result = read_body(body, &limits(), &ReceiveState::default(), parts);

        assert!(matches!(result, Err(BodyError::Cancelled)), "{result:?}");
    }

    #[test]
    fn reading_stops_once_nobody_takes_the_documents() {
        let (parts, _query, documents) = body_parts(1);
        drop(documents);
        let body = br#"{"query":"q","documents":["a"]}"#;

        let result = read_body(&body[..], &limits(), &ReceiveState::default(), parts);

        assert!(matches!(result, Err(BodyError::Cancelled)), "{result:?}");
    }

    /// The answers of the `Json` extractor before: the bodies it accepted, and 400 or 422 for
    /// the others.
    #[test]
    fn answers_bodies_as_the_json_extractor_did() {
        use serde_json::error::Category;

        let deep = format!(
            r#"{{"query":"q","x":{}{},"documents":["a"]}}"#,
            "[".repeat(100),
            "]".repeat(100)
        );
        let cases: &[(&str, &[u8])] = &[
            ("valid", br#"{"query":"q","documents":["a","b"]}"#),
            ("empty documents", br#"{"query":"q","documents":[]}"#),
            (
                "whitespace around",
                b" \n{ \"query\" : \"q\" , \"documents\" : [ \"a\" ] } \n",
            ),
            (
                "escapes",
                br#"{"query":"\"q\\","documents":["\t\/\b\f\r\u0000"]}"#,
            ),
            (
                "duplicate query",
                br#"{"query":"q","query":"r","documents":["a"]}"#,
            ),
            (
                "duplicate documents",
                br#"{"query":"q","documents":[],"documents":["a"]}"#,
            ),
            (
                "unknown field",
                br#"{"query":"q","x":[1,{"y":2}],"documents":["a"]}"#,
            ),
            ("nested unknown field", deep.as_bytes()),
            ("trailing data", br#"{"query":"q","documents":["a"]} x"#),
            ("second object", br#"{"query":"q","documents":["a"]}{}"#),
            (
                "trailing comma in array",
                br#"{"query":"q","documents":["a",]}"#,
            ),
            (
                "trailing comma in object",
                br#"{"query":"q","documents":["a"],}"#,
            ),
            ("comment", br#"{"query":"q",/*c*/"documents":["a"]}"#),
            ("bad escape", br#"{"query":"q","documents":["\x"]}"#),
            ("lone surrogate", br#"{"query":"q","documents":["\ud800"]}"#),
            (
                "raw control char",
                b"{\"query\":\"q\",\"documents\":[\"a\x01b\"]}",
            ),
            (
                "invalid utf8",
                b"{\"query\":\"q\",\"documents\":[\"\xff\"]}",
            ),
            ("number document", br#"{"query":"q","documents":[1]}"#),
            ("null document", br#"{"query":"q","documents":[null]}"#),
            ("null documents", br#"{"query":"q","documents":null}"#),
            ("number query", br#"{"query":1,"documents":[]}"#),
            ("missing query", br#"{"documents":["a"]}"#),
            ("missing documents", br#"{"query":"q"}"#),
            ("empty body", b""),
            ("truncated", br#"{"query":"q","documents":["a""#),
            ("truncated string", br#"{"query":"q","documents":["a"#),
        ];
        let failures: Vec<_> = cases
            .iter()
            .filter_map(|(name, body)| {
                let serde = serde_json::from_slice::<httpapi::PostIndexHighlightRequest>(body)
                    .map(|_| ())
                    .map_err(|err| err.classify());
                let (_, ours) = read(body, &HighlightLimits::default());
                let same = matches!(
                    (&serde, &ours),
                    (Ok(()), Ok(_))
                        | (Err(Category::Data), Err(BodyError::Data(_)))
                        | (
                            Err(Category::Syntax | Category::Eof),
                            Err(BodyError::Syntax(_))
                        )
                );
                (!same).then(|| format!("{name}: serde_json {serde:?}, streaming {ours:?}"))
            })
            .collect();

        assert!(failures.is_empty(), "{failures:#?}");
    }

    /// serde's derived `Deserialize` takes this; no client sends it.
    #[test]
    fn rejects_the_request_written_as_an_array() {
        let body = br#"["q",["a"]]"#;
        assert!(serde_json::from_slice::<httpapi::PostIndexHighlightRequest>(body).is_ok());

        let (_, result) = read(body, &HighlightLimits::default());

        assert!(matches!(result, Err(BodyError::Data(_))), "{result:?}");
    }

    /// The `Json` extractor accepted any depth here; see `read_body`.
    #[test]
    fn rejects_an_unknown_field_nested_over_128_levels() {
        let nested = |depth: usize| {
            format!(
                r#"{{"query":"q","x":{}{},"documents":["a"]}}"#,
                "[".repeat(depth),
                "]".repeat(depth)
            )
        };
        let deepest = nested(127);
        let too_deep = nested(128);
        assert!(
            serde_json::from_slice::<httpapi::PostIndexHighlightRequest>(too_deep.as_bytes())
                .is_ok()
        );

        let (_, deepest) = read(deepest.as_bytes(), &HighlightLimits::default());
        let (_, too_deep) = read(too_deep.as_bytes(), &HighlightLimits::default());

        assert!(deepest.is_ok(), "{deepest:?}");
        assert!(
            matches!(too_deep, Err(BodyError::Syntax(_))),
            "{too_deep:?}"
        );
    }

    #[tokio::test]
    async fn the_body_limit_fails_the_read() {
        let chunks = futures::stream::iter(
            [
                Bytes::from_static(br#"{"query":"q","documents":["#),
                Bytes::from(vec![b' '; 100]),
            ]
            .map(Ok::<_, io::Error>),
        );
        let body = LimitedBody::new(
            chunks,
            &HighlightLimits {
                max_body_bytes: 50,
                ..limits()
            },
        );
        let received = body.state();
        let reader = tokio_util::io::SyncIoBridge::new(tokio_util::io::StreamReader::new(body));
        let (parts, _query, _documents) = body_parts(100);

        let result =
            tokio::task::spawn_blocking(move || read_body(reader, &limits(), &received, parts))
                .await
                .unwrap();

        assert!(matches!(result, Err(BodyError::BodyTooLarge)), "{result:?}");
    }
}
