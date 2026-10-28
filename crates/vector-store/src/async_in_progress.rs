/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Timestamp;
use crate::metrics::DirtyFlag;
use prometheus::Histogram;
use tokio::sync::mpsc;

#[derive(Debug)]
struct CdcInProgress {
    histogram: Histogram,
    timestamp: Timestamp,
}

impl Drop for CdcInProgress {
    fn drop(&mut self) {
        self.histogram
            .observe(self.timestamp.elapsed().as_secs_f64());
    }
}

/// What a change in flight holds until the index has applied it, and drops then.
#[derive(Debug)]
enum State {
    None,
    Fullscan(#[allow(dead_code)] mpsc::Sender<()>),
    Cdc(#[allow(dead_code)] Box<CdcInProgress>),
}

/// The guard of a change sent to an index, which the index drops once it has applied the change.
#[derive(Debug)]
pub struct AsyncInProgress {
    // Dropped first: the index is marked dirty before a full scan sees the change applied.
    _dirty: Option<DirtyFlag>,
    _state: State,
}

impl AsyncInProgress {
    pub fn none() -> Self {
        Self {
            _dirty: None,
            _state: State::None,
        }
    }

    pub fn fullscan(tx: mpsc::Sender<()>) -> Self {
        Self {
            _dirty: None,
            _state: State::Fullscan(tx),
        }
    }

    pub(crate) fn cdc(histogram: Histogram, timestamp: Timestamp) -> Self {
        Self {
            _dirty: None,
            _state: State::Cdc(Box::new(CdcInProgress {
                histogram,
                timestamp,
            })),
        }
    }

    /// This guard, which also marks its index `dirty` for /metrics when the index drops it, that
    /// is once the index has applied the change (a tantivy index once a reload has made it
    /// searchable).
    pub(crate) fn marking(self, dirty: DirtyFlag) -> Self {
        Self {
            _dirty: Some(dirty),
            ..self
        }
    }

    /// A guard for another change of the same row, which marks the index dirty when dropped as
    /// this one does, and holds nothing else.
    pub(crate) fn marker(&self) -> Self {
        Self {
            _dirty: self._dirty.clone(),
            _state: State::None,
        }
    }

    /// Takes this guard, leaving one that only marks the index (`marker`), so that the later
    /// changes of a row, which get the guard left behind, mark the index too.
    pub(crate) fn take(&mut self) -> Self {
        let marker = self.marker();
        std::mem::replace(self, marker)
    }

    #[cfg(test)]
    pub(crate) fn is_none(&self) -> bool {
        matches!(self._state, State::None)
    }

    #[cfg(test)]
    pub(crate) fn is_fullscan(&self) -> bool {
        matches!(self._state, State::Fullscan(_))
    }
}
