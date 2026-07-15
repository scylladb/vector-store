/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

use crate::Timestamp;
use prometheus::Histogram;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct CdcInProgress {
    histogram: Histogram,
    timestamp: Timestamp,
}

#[derive(Debug)]
pub enum AsyncInProgress {
    None,
    Fullscan(mpsc::Sender<()>),
    Cdc(Box<CdcInProgress>),
}

impl AsyncInProgress {
    pub(crate) fn cdc(histogram: Histogram, timestamp: Timestamp) -> Self {
        Self::Cdc(Box::new(CdcInProgress {
            histogram,
            timestamp,
        }))
    }

    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(self, AsyncInProgress::None)
    }
}

impl Drop for AsyncInProgress {
    fn drop(&mut self) {
        if let AsyncInProgress::Cdc(cdc) = self {
            cdc.histogram.observe(cdc.timestamp.elapsed().as_secs_f64());
        }
    }
}
