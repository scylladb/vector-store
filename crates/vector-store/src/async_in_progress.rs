/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::PrimaryId;
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

/// Tells the monitor of an index, when dropped, that the index has applied the add of a row,
/// so that a remove of the row may follow.
#[derive(Debug)]
pub(crate) struct Applied {
    primary_id: PrimaryId,
    tx: mpsc::UnboundedSender<PrimaryId>,
}

impl Applied {
    pub(crate) fn new(primary_id: PrimaryId, tx: mpsc::UnboundedSender<PrimaryId>) -> Self {
        Self { primary_id, tx }
    }
}

impl Drop for Applied {
    fn drop(&mut self) {
        _ = self.tx.send(self.primary_id);
    }
}

/// The guard of a change sent to an index, which the index drops once it has applied the change.
#[derive(Debug)]
pub struct AsyncInProgress {
    // Dropped first: the index is marked dirty before a full scan sees the change applied.
    _dirty: Option<DirtyFlag>,
    _state: State,
    applied: Option<Applied>,
}

impl AsyncInProgress {
    pub fn none() -> Self {
        Self {
            _dirty: None,
            _state: State::None,
            applied: None,
        }
    }

    pub fn fullscan(tx: mpsc::Sender<()>) -> Self {
        Self {
            _dirty: None,
            _state: State::Fullscan(tx),
            applied: None,
        }
    }

    pub(crate) fn cdc(histogram: Histogram, timestamp: Timestamp) -> Self {
        Self {
            _dirty: None,
            _state: State::Cdc(Box::new(CdcInProgress {
                histogram,
                timestamp,
            })),
            applied: None,
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
            applied: None,
        }
    }

    /// Attaches `applied`, which tells the monitor the change is applied when the index calls
    /// `applied` or drops this guard, whichever comes first.
    pub(crate) fn applying(self, applied: Applied) -> Self {
        Self {
            applied: Some(applied),
            ..self
        }
    }

    /// Called by an index that holds the guard after applying the change.
    pub(crate) fn applied(&mut self) {
        self.applied = None;
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
