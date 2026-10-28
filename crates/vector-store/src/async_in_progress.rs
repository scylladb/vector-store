/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Timestamp;
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
    _state: State,
}

impl AsyncInProgress {
    pub fn none() -> Self {
        Self {
            _state: State::None,
        }
    }

    pub fn fullscan(tx: mpsc::Sender<()>) -> Self {
        Self {
            _state: State::Fullscan(tx),
        }
    }

    pub(crate) fn cdc(histogram: Histogram, timestamp: Timestamp) -> Self {
        Self {
            _state: State::Cdc(Box::new(CdcInProgress {
                histogram,
                timestamp,
            })),
        }
    }

    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(self, Self::none())
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
