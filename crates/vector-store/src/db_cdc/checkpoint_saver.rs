/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Metrics;
use async_trait::async_trait;
use scylla_cdc::cdc_types::GenerationTimestamp;
use scylla_cdc::cdc_types::StreamID;
use scylla_cdc::checkpoints::CDCCheckpointSaver;
use scylla_cdc::checkpoints::Checkpoint;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

/// Updates the `cdc_last_processed_timestamp_seconds` gauge instead of persisting to storage.
/// Reports the minimum timestamp across all streams.
pub(super) struct MetricsCheckpointSaver {
    metrics: Arc<Metrics>,
    keyspace: String,
    index_name: String,
    reader_name: String,
    state: Mutex<StreamProgress>,
}

/// Tracks progress across all streams to compute the reader-wide minimum.
#[derive(Default)]
struct StreamProgress {
    positions: HashMap<StreamID, Duration>,
    counts: BTreeMap<Duration, usize>,
}

impl StreamProgress {
    /// Records `timestamp` for `stream_id` and returns the new reader-wide minimum.
    ///
    /// `counts` maps each timestamp to how many streams are currently at it.
    /// Because `BTreeMap` iterates in sorted order, `counts.keys().next()` is
    /// the minimum timestamp across all streams.
    fn record(&mut self, stream_id: StreamID, timestamp: Duration) -> Option<Duration> {
        if let Some(previous) = self.positions.insert(stream_id, timestamp)
            && let Some(count) = self.counts.get_mut(&previous)
        {
            *count -= 1;
            if *count == 0 {
                self.counts.remove(&previous);
            }
        }
        *self.counts.entry(timestamp).or_insert(0) += 1;
        self.counts.keys().next().copied()
    }

    fn clear(&mut self) {
        self.positions.clear();
        self.counts.clear();
    }
}

impl MetricsCheckpointSaver {
    pub(super) fn new(
        metrics: Arc<Metrics>,
        keyspace: String,
        index_name: String,
        reader_name: String,
    ) -> Self {
        Self {
            metrics,
            keyspace,
            index_name,
            reader_name,
            state: Mutex::new(StreamProgress::default()),
        }
    }

    fn record_stream_progress(&self, stream_id: StreamID, timestamp: Duration) {
        let min_progress = self.state.lock().unwrap().record(stream_id, timestamp);
        if let Some(progress) = min_progress {
            self.metrics
                .cdc_last_processed_timestamp_seconds
                .with_label_values(&[&self.keyspace, &self.index_name, &self.reader_name])
                .set(progress.as_secs_f64());
        }
    }

    fn reset(&self) {
        self.state.lock().unwrap().clear();
    }
}

#[async_trait]
impl CDCCheckpointSaver for MetricsCheckpointSaver {
    async fn save_checkpoint(&self, checkpoint: &Checkpoint) -> anyhow::Result<()> {
        self.record_stream_progress(checkpoint.stream_id.clone(), checkpoint.timestamp);
        Ok(())
    }

    async fn save_new_generation(&self, _generation: &GenerationTimestamp) -> anyhow::Result<()> {
        self.reset();
        Ok(())
    }

    async fn load_last_generation(&self) -> anyhow::Result<Option<GenerationTimestamp>> {
        Ok(None)
    }

    async fn load_last_checkpoint(
        &self,
        _stream_id: &StreamID,
    ) -> anyhow::Result<Option<chrono::Duration>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_cdc::READER_FINE;
    use crate::db_cdc::READER_WIDE;
    use prometheus::Encoder;
    use prometheus::TextEncoder;

    fn metric_families_text(metrics: &Metrics) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&metrics.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn stream_id(id: u8) -> StreamID {
        StreamID::new(vec![id])
    }

    #[test]
    fn checkpoint_saver_reports_minimum_across_tracked_streams() {
        let metrics = Arc::new(Metrics::new());
        let saver = MetricsCheckpointSaver::new(
            Arc::clone(&metrics),
            "ks".to_string(),
            "idx".to_string(),
            READER_WIDE.to_string(),
        );

        saver.record_stream_progress(stream_id(1), Duration::from_secs(1_700_000_100));
        saver.record_stream_progress(stream_id(2), Duration::from_secs(1_700_000_050));

        // Progress is the minimum across all tracked streams, which is timestamp of stream 2.
        let output = metric_families_text(&metrics);
        assert!(
            output.contains(
                r#"cdc_last_processed_timestamp_seconds{index_name="idx",keyspace="ks",reader="wide"} 1700000050"#
            ),
            "expected the minimum of the two streams' progress, got:\n{output}"
        );

        // Stream 2 catches up. Minimum should now follow stream 1.
        saver.record_stream_progress(stream_id(2), Duration::from_secs(1_700_000_200));

        let output = metric_families_text(&metrics);
        assert!(
            output.contains(
                r#"cdc_last_processed_timestamp_seconds{index_name="idx",keyspace="ks",reader="wide"} 1700000100"#
            ),
            "expected the minimum to follow the now-slowest stream, got:\n{output}"
        );
    }

    #[test]
    fn checkpoint_saver_resets_tracking_on_new_generation() {
        let metrics = Arc::new(Metrics::new());
        let saver = MetricsCheckpointSaver::new(
            Arc::clone(&metrics),
            "ks".to_string(),
            "idx".to_string(),
            READER_FINE.to_string(),
        );

        saver.record_stream_progress(stream_id(1), Duration::from_secs(1_700_000_000));

        saver.reset();

        // After reset, the new generation's later timestamp must not be dragged down by the previous generation.
        saver.record_stream_progress(stream_id(1), Duration::from_secs(1_700_000_500));

        let output = metric_families_text(&metrics);
        assert!(
            output.contains(
                r#"cdc_last_processed_timestamp_seconds{index_name="idx",keyspace="ks",reader="fine"} 1700000500"#
            ),
            "expected stale generation's progress to have been cleared, got:\n{output}"
        );
    }

    #[test]
    fn checkpoint_saver_tracks_minimum_when_streams_share_a_timestamp() {
        let metrics = Arc::new(Metrics::new());
        let saver = MetricsCheckpointSaver::new(
            Arc::clone(&metrics),
            "ks".to_string(),
            "idx".to_string(),
            READER_WIDE.to_string(),
        );

        // Two streams at the same timestamp.
        saver.record_stream_progress(stream_id(1), Duration::from_secs(1_700_000_000));
        saver.record_stream_progress(stream_id(2), Duration::from_secs(1_700_000_000));

        // Stream 1 advances. Stream 2 still holds the shared timestamp, so minimum must not move.
        saver.record_stream_progress(stream_id(1), Duration::from_secs(1_700_000_100));

        let output = metric_families_text(&metrics);
        assert!(
            output.contains(
                r#"cdc_last_processed_timestamp_seconds{index_name="idx",keyspace="ks",reader="wide"} 1700000000"#
            ),
            "expected the minimum to stay at the shared timestamp, got:\n{output}"
        );

        // Stream 2 also advances.
        saver.record_stream_progress(stream_id(2), Duration::from_secs(1_700_000_200));

        let output = metric_families_text(&metrics);
        assert!(
            output.contains(
                r#"cdc_last_processed_timestamp_seconds{index_name="idx",keyspace="ks",reader="wide"} 1700000100"#
            ),
            "expected the minimum to advance once the shared timestamp is vacated, got:\n{output}"
        );
    }

    #[tokio::test]
    async fn checkpoint_saver_never_loads_progress() {
        let saver = MetricsCheckpointSaver::new(
            Arc::new(Metrics::new()),
            "ks".to_string(),
            "idx".to_string(),
            READER_WIDE.to_string(),
        );

        assert!(saver.load_last_generation().await.unwrap().is_none());
        assert!(
            saver
                .load_last_checkpoint(&stream_id(1))
                .await
                .unwrap()
                .is_none()
        );
    }
}
