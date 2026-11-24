/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Reads `/highlight` request bodies as a stream, one document at a time, so that the
//! FTS index highlights a document while the next one arrives.

use crate::Config;
use crate::fts_index::HighlightDocument;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::io::Read;
use std::num::NonZeroUsize;
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
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug;

const MIB: usize = 1024 * 1024;
/// Discarding a body holds no memory, only a task.
const DRAINS_PER_REQUEST: usize = 16;
/// The query, one document highlighted, one waiting and one being read, and the old buffer
/// of the one being read while it grows.
const TEXTS_HELD_AT_ONCE: usize = 5;

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
    /// Shared by all requests: bounds the text they hold, counted by the capacity of its
    /// buffers. The rest of their memory, such as the excerpts, is small next to it.
    pub(crate) memory_budget_bytes: usize,
    /// Each request holds a blocking thread while it reads its body.
    pub(crate) max_requests: usize,
    /// The longest wait for memory or a place, before answering 503.
    pub(crate) busy_wait: Duration,
    pub(crate) receive_timeout: Duration,
    pub(crate) receive_idle_timeout: Duration,
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
            memory_budget_bytes: 512 * MIB,
            max_requests: 64,
            busy_wait: Duration::from_secs(5),
            receive_timeout: Duration::from_secs(120),
            receive_idle_timeout: Duration::from_secs(30),
            // As long as a whole body may take to arrive.
            drain_timeout: Duration::from_secs(120),
        }
    }
}

impl HighlightLimits {
    pub(crate) fn from_config(config: &Config) -> Self {
        let default = Self::default();
        let memory_budget_bytes = config
            .highlight_memory_budget
            .map_or(default.memory_budget_bytes, NonZeroUsize::get);
        // A text that could never fit in the budget gets 413, not 503.
        let max_text_bytes = default
            .max_text_bytes
            .min(memory_budget_bytes / TEXTS_HELD_AT_ONCE);
        Self {
            max_text_bytes,
            max_query_bytes: default.max_query_bytes.min(max_text_bytes),
            memory_budget_bytes,
            max_requests: config
                .highlight_max_requests
                .map_or(default.max_requests, NonZeroUsize::get),
            busy_wait: config.highlight_busy_wait.unwrap_or(default.busy_wait),
            receive_timeout: config
                .highlight_receive_timeout
                .unwrap_or(default.receive_timeout),
            drain_timeout: config
                .highlight_receive_timeout
                .unwrap_or(default.drain_timeout),
            receive_idle_timeout: config
                .highlight_receive_idle_timeout
                .unwrap_or(default.receive_idle_timeout),
            ..default
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HighlightShared {
    pub(crate) limits: HighlightLimits,
    pub(crate) budget: Arc<Semaphore>,
    pub(crate) requests: Arc<Semaphore>,
    drains: Arc<Semaphore>,
}

impl HighlightShared {
    pub(crate) fn new(limits: HighlightLimits) -> Self {
        Self {
            limits,
            budget: Arc::new(Semaphore::new(limits.memory_budget_bytes)),
            requests: Arc::new(Semaphore::new(limits.max_requests)),
            drains: Arc::new(Semaphore::new(DRAINS_PER_REQUEST * limits.max_requests)),
        }
    }

    /// Past `DRAINS_PER_REQUEST * max_requests` drains at once, the body is dropped, closing
    /// the connection.
    pub(crate) fn drain<S, E>(&self, body: S)
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin + Send + 'static,
    {
        let Ok(permit) = Arc::clone(&self.drains).try_acquire_owned() else {
            debug!("highlight: too many request bodies being discarded, dropping this one");
            return;
        };
        let limits = self.limits;
        tokio::spawn(async move {
            drain(body, limits).await;
            drop(permit);
        });
    }
}

/// Text read from the body, with its share of the memory budget.
#[derive(Debug)]
pub(crate) struct Text {
    text: String,
    _memory: Option<OwnedSemaphorePermit>,
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        &self.text
    }
}

#[derive(Debug)]
pub(crate) struct BodyParts {
    pub(crate) query: oneshot::Sender<Text>,
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
    #[error("the memory for highlight documents is used up")]
    Busy,
    #[error("failed to receive the request body")]
    Receive,
    #[error("the highlight request body did not arrive in time")]
    Timeout,
    #[error("the request was answered before its body was read")]
    Cancelled,
}

#[derive(Clone, Copy, Debug)]
enum ReceiveFailure {
    TooLarge,
    TimedOut,
    Failed,
}

impl From<ReceiveFailure> for BodyError {
    fn from(failure: ReceiveFailure) -> Self {
        match failure {
            ReceiveFailure::TooLarge => BodyError::BodyTooLarge,
            ReceiveFailure::TimedOut => BodyError::Timeout,
            ReceiveFailure::Failed => BodyError::Receive,
        }
    }
}

impl From<ReceiveFailure> for io::Error {
    fn from(failure: ReceiveFailure) -> Self {
        let kind = match failure {
            ReceiveFailure::TimedOut => io::ErrorKind::TimedOut,
            ReceiveFailure::TooLarge | ReceiveFailure::Failed => io::ErrorKind::Other,
        };
        io::Error::new(kind, BodyError::from(failure))
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
    idle_timeout: Duration,
    idle: Pin<Box<tokio::time::Sleep>>,
    time_left: Duration,
    /// Set while the client is waited for.
    waiting_since: Option<tokio::time::Instant>,
}

impl<S> LimitedBody<S> {
    pub(crate) fn new(inner: S, limits: &HighlightLimits) -> Self {
        Self {
            inner,
            received: 0,
            limit: limits.max_body_bytes,
            state: Arc::default(),
            idle_timeout: limits.receive_idle_timeout,
            idle: Box::pin(tokio::time::sleep(limits.receive_idle_timeout)),
            time_left: limits.receive_timeout,
            waiting_since: None,
        }
    }

    pub(crate) fn state(&self) -> Arc<ReceiveState> {
        Arc::clone(&self.state)
    }

    /// The body without its limits, for discarding the rest of it.
    pub(crate) fn into_inner(self) -> S {
        self.inner
    }

    fn time_out(&self) -> io::Error {
        self.state.record(ReceiveFailure::TimedOut);
        ReceiveFailure::TimedOut.into()
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
        // Only time waiting for the client counts, not pauses of the server.
        if self.time_left.is_zero() {
            return Poll::Ready(Some(Err(self.time_out())));
        }
        let waiting_since = match self.waiting_since {
            Some(since) => since,
            None => {
                let now = tokio::time::Instant::now();
                let idle_end = now + self.idle_timeout.min(self.time_left);
                self.idle.as_mut().reset(idle_end);
                self.waiting_since = Some(now);
                now
            }
        };
        let chunk = self.inner.poll_next_unpin(cx);
        if chunk.is_ready() {
            self.waiting_since = None;
            self.time_left = self.time_left.saturating_sub(waiting_since.elapsed());
        }
        match chunk {
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
            Poll::Pending => match self.idle.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Some(Err(self.time_out()))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// Discards the rest of a body after an early answer, so that a client sending the whole
/// body before reading the answer gets it instead of a reset (RFC 9112 section 9.6).
/// Whatever limit stopped the read, the client may still be sending, and discarding holds no
/// memory, so only time, and a pause as long as the idle timeout end it.
pub(crate) async fn drain<S, E>(mut body: S, limits: HighlightLimits)
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut discarded: u64 = 0;
    let drained = tokio::time::timeout(limits.drain_timeout, async {
        loop {
            match tokio::time::timeout(limits.receive_idle_timeout, body.next()).await {
                Ok(Some(Ok(chunk))) => discarded += chunk.len() as u64,
                Ok(None) => return true,
                Ok(Some(Err(_))) | Err(_) => return false,
            }
        }
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
    budget: &Arc<Semaphore>,
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
        budget,
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
    budget: &'a Arc<Semaphore>,
    query: Option<oneshot::Sender<Text>>,
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

    fn read_text(&mut self, max_text_bytes: usize) -> Result<Text, BodyError> {
        let value_type = self.json.peek().map_err(|err| self.error(err))?;
        if value_type != ValueType::String {
            return Err(BodyError::Data(format!(
                "invalid type: {}, expected a string",
                value_type_name(value_type)
            )));
        }

        let budget_wait = self.limits.busy_wait;
        let received = self.received;
        let budget = self.budget;
        let limits = self.limits;
        let guard = &self.guard;
        let chunk = &mut self.chunk;
        let mut text = Vec::new();
        let mut memory: Option<OwnedSemaphorePermit> = None;
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
            let needed = text.len() + len;
            if needed > text.capacity() {
                // Grown here rather than by `Vec`, so that the budget covers the capacity,
                // and the old buffer too while the text is copied to the new one.
                let capacity = needed.max(2 * text.capacity()).min(max_text_bytes);
                reserve(&mut memory, text.capacity() + capacity, budget, budget_wait)?;
                text.reserve_exact(capacity - text.len());
                if let Some(memory) = memory.as_mut() {
                    drop(memory.split(memory.num_permits() - capacity));
                }
            }
            text.extend_from_slice(&chunk[..len]);
        }
        drop(string);

        // The string reader returns whole UTF-8 characters only, so this never fails.
        let text = String::from_utf8(text)
            .map_err(|err| BodyError::Syntax(format!("invalid UTF-8 in a string: {err}")))?;
        Ok(Text {
            text,
            _memory: memory,
        })
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

/// The wait ends a deadlock of requests each holding part of the budget and waiting for more.
/// Runs on a blocking thread of the runtime, so it may block on it.
fn reserve(
    memory: &mut Option<OwnedSemaphorePermit>,
    bytes: usize,
    budget: &Arc<Semaphore>,
    wait: Duration,
) -> Result<(), BodyError> {
    let held = memory.as_ref().map_or(0, OwnedSemaphorePermit::num_permits);
    if bytes <= held {
        return Ok(());
    }
    let more = u32::try_from(bytes - held).map_err(|_| BodyError::Busy)?;
    let more = match Arc::clone(budget).try_acquire_many_owned(more) {
        Ok(more) => more,
        Err(_) => tokio::runtime::Handle::current()
            .block_on(tokio::time::timeout(
                wait,
                Arc::clone(budget).acquire_many_owned(more),
            ))
            .ok()
            .and_then(Result::ok)
            .ok_or(BodyError::Busy)?,
    };
    match memory {
        Some(memory) => memory.merge(more),
        None => *memory = Some(more),
    }
    Ok(())
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
            memory_budget_bytes: 4096,
            ..HighlightLimits::default()
        }
    }

    fn read(body: &[u8], limits: &HighlightLimits) -> (Vec<String>, Result<usize, BodyError>) {
        read_with_budget(
            body,
            limits,
            &Arc::new(Semaphore::new(limits.memory_budget_bytes)),
        )
    }

    fn read_with_budget(
        body: &[u8],
        limits: &HighlightLimits,
        budget: &Arc<Semaphore>,
    ) -> (Vec<String>, Result<usize, BodyError>) {
        let (parts, mut query, mut documents) = body_parts(100);
        let result = read_body(body, limits, &ReceiveState::default(), budget, parts);
        let mut items = Vec::new();
        if let Ok(query) = query.try_recv() {
            items.push(format!("query: {}", query.as_ref()));
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
        oneshot::Receiver<Text>,
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
    fn a_growing_text_takes_its_capacity_from_the_budget() {
        let limits = HighlightLimits {
            max_text_bytes: 200_000,
            ..limits()
        };
        let budget = Arc::new(Semaphore::new(1_000_000));
        let (parts, _query, mut documents) = body_parts(100);
        // Read in pieces of 64 KiB: the buffer grows to 64 KiB, then doubles.
        let body = format!(r#"{{"query":"q","documents":["{}"]}}"#, "x".repeat(100_000));

        read_body(
            body.as_bytes(),
            &limits,
            &ReceiveState::default(),
            &budget,
            parts,
        )
        .unwrap();

        let _document = documents.try_recv().unwrap();
        assert_eq!(budget.available_permits(), 1_000_000 - 1 - 2 * 64 * 1024);
    }

    #[tokio::test]
    async fn a_growing_text_counts_its_old_buffer_while_it_is_copied() {
        // Growing from 64 KiB to 128 KiB holds both buffers for a moment.
        let budget = Arc::new(Semaphore::new(1 + 3 * 64 * 1024 - 1));
        let limits = HighlightLimits {
            max_text_bytes: 200_000,
            busy_wait: Duration::from_millis(50),
            ..limits()
        };
        let body = format!(r#"{{"query":"q","documents":["{}"]}}"#, "x".repeat(100_000));

        let result = {
            let budget = Arc::clone(&budget);
            tokio::task::spawn_blocking(move || {
                read_with_budget(body.as_bytes(), &limits, &budget).1
            })
            .await
            .unwrap()
        };

        assert!(matches!(result, Err(BodyError::Busy)), "{result:?}");
    }

    #[test]
    fn documents_in_memory_take_from_the_budget_until_dropped() {
        let budget = Arc::new(Semaphore::new(100));
        let (parts, mut query, mut documents) = body_parts(100);
        let body = format!(r#"{{"query":"q","documents":["{}"]}}"#, "x".repeat(60));

        read_body(
            body.as_bytes(),
            &limits(),
            &ReceiveState::default(),
            &budget,
            parts,
        )
        .unwrap();

        let query = query.try_recv().unwrap();
        let document = documents.try_recv().unwrap();
        assert_eq!(
            budget.available_permits(),
            100 - 1 - 60,
            "counted by text length"
        );
        drop((query, document));
        assert_eq!(budget.available_permits(), 100);
    }

    #[tokio::test]
    async fn a_text_over_the_whole_budget_makes_the_request_busy() {
        let budget = Arc::new(Semaphore::new(100));
        let body = format!(r#"{{"query":"q","documents":["{}"]}}"#, "x".repeat(200));
        let limits = HighlightLimits {
            busy_wait: Duration::from_millis(50),
            ..limits()
        };

        let result = {
            let budget = Arc::clone(&budget);
            tokio::task::spawn_blocking(move || {
                read_with_budget(body.as_bytes(), &limits, &budget).1
            })
            .await
            .unwrap()
        };

        assert!(matches!(result, Err(BodyError::Busy)), "{result:?}");
        assert_eq!(
            budget.available_permits(),
            100,
            "the memory went back to the budget"
        );
    }

    #[test]
    fn the_default_budget_fits_one_request_alone() {
        let limits = HighlightLimits::default();
        assert!(TEXTS_HELD_AT_ONCE * limits.max_text_bytes <= limits.memory_budget_bytes);
    }

    #[tokio::test]
    async fn a_used_up_budget_is_waited_for_a_while() {
        let budget = Arc::new(Semaphore::new(100));
        let held = Arc::clone(&budget).try_acquire_many_owned(100).unwrap();
        let reading = {
            let budget = Arc::clone(&budget);
            tokio::task::spawn_blocking(move || {
                let (parts, _query, _documents) = body_parts(100);
                let body = br#"{"query":"q","documents":["a"]}"#;
                read_body(
                    &body[..],
                    &limits(),
                    &ReceiveState::default(),
                    &budget,
                    parts,
                )
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;

        drop(held);

        assert_eq!(reading.await.unwrap().unwrap(), 1);
    }

    #[tokio::test]
    async fn a_budget_used_up_for_too_long_makes_the_request_busy() {
        let budget = Arc::new(Semaphore::new(100));
        let _held = Arc::clone(&budget).try_acquire_many_owned(100).unwrap();
        let limits = HighlightLimits {
            busy_wait: Duration::from_millis(50),
            ..limits()
        };

        let result = tokio::task::spawn_blocking(move || {
            let (parts, _query, _documents) = body_parts(100);
            let body = br#"{"query":"q","documents":["a"]}"#;
            read_body(&body[..], &limits, &ReceiveState::default(), &budget, parts)
        })
        .await
        .unwrap();

        assert!(matches!(result, Err(BodyError::Busy)), "{result:?}");
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

        let result = read_body(
            &mut counted,
            &limits(),
            &ReceiveState::default(),
            &Arc::new(Semaphore::new(4096)),
            parts,
        );

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

        let result = read_body(
            body,
            &limits(),
            &ReceiveState::default(),
            &Arc::new(Semaphore::new(4096)),
            parts,
        );

        assert!(matches!(result, Err(BodyError::Cancelled)), "{result:?}");
    }

    #[test]
    fn reading_stops_once_nobody_takes_the_documents() {
        let (parts, _query, documents) = body_parts(1);
        drop(documents);
        let body = br#"{"query":"q","documents":["a"]}"#;

        let result = read_body(
            &body[..],
            &limits(),
            &ReceiveState::default(),
            &Arc::new(Semaphore::new(4096)),
            parts,
        );

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
    async fn discarding_stops_once_the_body_pauses_too_long() {
        let body = futures::stream::iter([Ok::<_, io::Error>(Bytes::from_static(b" "))])
            .chain(futures::stream::pending());
        let limits = HighlightLimits {
            receive_idle_timeout: Duration::from_millis(50),
            ..limits()
        };

        let discarding = tokio::time::timeout(Duration::from_secs(5), drain(body, limits)).await;

        assert!(discarding.is_ok());
    }

    #[tokio::test]
    async fn a_body_that_stops_arriving_fails_the_read() {
        let chunks = futures::stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"{"))])
            .chain(futures::stream::pending());
        let body = LimitedBody::new(
            chunks,
            &HighlightLimits {
                receive_idle_timeout: Duration::from_millis(100),
                ..limits()
            },
        );
        let received = body.state();
        let reader = tokio_util::io::SyncIoBridge::new(tokio_util::io::StreamReader::new(body));
        let (parts, _query, _documents) = body_parts(100);

        let result = tokio::task::spawn_blocking(move || {
            read_body(
                reader,
                &limits(),
                &received,
                &Arc::new(Semaphore::new(4096)),
                parts,
            )
        })
        .await
        .unwrap();

        assert!(matches!(result, Err(BodyError::Timeout)), "{result:?}");
    }

    #[tokio::test]
    async fn pauses_of_the_server_do_not_count_as_idle() {
        let (chunks_tx, chunks_rx) = mpsc::channel::<io::Result<Bytes>>(2);
        let chunks = tokio_stream_from(chunks_rx);
        let mut body = LimitedBody::new(
            chunks,
            &HighlightLimits {
                receive_idle_timeout: Duration::from_millis(100),
                ..limits()
            },
        );
        chunks_tx.send(Ok(Bytes::from_static(b"a"))).await.unwrap();
        assert!(body.next().await.unwrap().is_ok());

        tokio::time::sleep(Duration::from_millis(200)).await;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            chunks_tx.send(Ok(Bytes::from_static(b"b"))).await.unwrap();
        });

        assert!(body.next().await.unwrap().is_ok());
    }

    #[test]
    fn limits_from_config_replace_only_what_is_set() {
        let default = HighlightLimits::default();
        let unset = HighlightLimits::from_config(&Config::default());
        assert_eq!(unset.memory_budget_bytes, default.memory_budget_bytes);
        assert_eq!(unset.max_requests, default.max_requests);
        assert_eq!(unset.busy_wait, default.busy_wait);
        assert_eq!(unset.receive_timeout, default.receive_timeout);

        let set = HighlightLimits::from_config(&Config {
            highlight_memory_budget: NonZeroUsize::new(1000),
            highlight_max_requests: NonZeroUsize::new(2),
            highlight_busy_wait: Some(Duration::from_secs(1)),
            highlight_receive_timeout: Some(Duration::from_secs(3)),
            highlight_receive_idle_timeout: Some(Duration::from_secs(2)),
            ..Config::default()
        });
        assert_eq!(set.memory_budget_bytes, 1000);
        assert_eq!(set.max_requests, 2);
        assert_eq!(set.busy_wait, Duration::from_secs(1));
        assert_eq!(set.receive_timeout, Duration::from_secs(3));
        assert_eq!(set.drain_timeout, Duration::from_secs(3));
        assert_eq!(set.receive_idle_timeout, Duration::from_secs(2));
        assert_eq!(set.max_text_bytes, 1000 / TEXTS_HELD_AT_ONCE);
        assert_eq!(set.max_query_bytes, 1000 / TEXTS_HELD_AT_ONCE);

        let large = HighlightLimits::from_config(&Config {
            highlight_memory_budget: NonZeroUsize::new(4096 * MIB),
            ..Config::default()
        });
        assert_eq!(large.max_text_bytes, default.max_text_bytes);
    }

    #[tokio::test]
    async fn pauses_of_the_server_do_not_count_toward_the_receive_timeout() {
        let (chunks_tx, chunks_rx) = mpsc::channel::<io::Result<Bytes>>(1);
        let mut body = LimitedBody::new(
            tokio_stream_from(chunks_rx),
            &HighlightLimits {
                receive_idle_timeout: Duration::from_millis(100),
                receive_timeout: Duration::from_millis(300),
                ..limits()
            },
        );
        tokio::spawn(async move {
            for _ in 0..5 {
                chunks_tx.send(Ok(Bytes::from_static(b" "))).await.unwrap();
            }
        });

        for _ in 0..5 {
            assert!(body.next().await.unwrap().is_ok());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(body.next().await.is_none());
    }

    #[tokio::test]
    async fn a_body_trickling_in_past_the_receive_timeout_fails() {
        let (chunks_tx, chunks_rx) = mpsc::channel::<io::Result<Bytes>>(1);
        let mut body = LimitedBody::new(
            tokio_stream_from(chunks_rx),
            &HighlightLimits {
                receive_idle_timeout: Duration::from_secs(1),
                receive_timeout: Duration::from_millis(300),
                ..limits()
            },
        );
        tokio::spawn(async move {
            while chunks_tx.send(Ok(Bytes::from_static(b" "))).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });

        let started = tokio::time::Instant::now();
        let failure = loop {
            match body.next().await {
                Some(Ok(_)) => continue,
                Some(Err(err)) => break err,
                None => panic!("the body ended"),
            }
        };

        assert_eq!(failure.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(
            body.state()
                .error()
                .is_some_and(|err| matches!(err, BodyError::Timeout))
        );
    }

    fn tokio_stream_from(
        mut rx: mpsc::Receiver<io::Result<Bytes>>,
    ) -> impl Stream<Item = io::Result<Bytes>> + Unpin {
        Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
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

        let result = tokio::task::spawn_blocking(move || {
            read_body(
                reader,
                &limits(),
                &received,
                &Arc::new(Semaphore::new(4096)),
                parts,
            )
        })
        .await
        .unwrap();

        assert!(matches!(result, Err(BodyError::BodyTooLarge)), "{result:?}");
    }

    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    struct EndlessBody(Arc<AtomicBool>);

    impl Stream for EndlessBody {
        type Item = io::Result<Bytes>;

        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl Drop for EndlessBody {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn bodies_past_the_drain_limit_are_dropped_at_once() {
        let shared = HighlightShared::new(HighlightLimits {
            max_requests: 1,
            ..limits()
        });
        let drained: Vec<_> = (0..DRAINS_PER_REQUEST)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        let past_the_limit = Arc::new(AtomicBool::new(false));

        for dropped in &drained {
            shared.drain(EndlessBody(Arc::clone(dropped)));
        }
        shared.drain(EndlessBody(Arc::clone(&past_the_limit)));
        tokio::task::yield_now().await;

        assert!(
            drained
                .iter()
                .all(|dropped| !dropped.load(Ordering::Relaxed))
        );
        assert!(past_the_limit.load(Ordering::Relaxed));
    }
}
