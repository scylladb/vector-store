/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
use crate::IndexKey;
use crate::IndexName;
use crate::KeyspaceName;
use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::fts_index::FtsIndexExt;
use crate::pattern_index::PatternIndexExt;
use crate::vs_index::VsIndexSearchExt;
use dashmap::DashSet;
use futures::future;
use prometheus::CounterVec;
use prometheus::GaugeVec;
use prometheus::HistogramVec;
use prometheus::Registry;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::WeakSender;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tracing::Instrument;
use tracing::debug_span;

pub(crate) const OP_INSERT: &str = "insert";
pub(crate) const OP_UPDATE: &str = "update";
pub(crate) const OP_REMOVE: &str = "remove";
pub(crate) const OPERATIONS: &[&str] = &[OP_INSERT, OP_UPDATE, OP_REMOVE];
pub(crate) const OP_ANN: &str = "ann";
pub(crate) const OP_BM25: &str = "bm25";
pub(crate) const OP_HIGHLIGHT: &str = "highlight";
pub(crate) const OP_LIKE: &str = "like";
pub(crate) const REQUEST_OPERATIONS: &[&str] = &[OP_ANN, OP_BM25, OP_HIGHLIGHT, OP_LIKE];

/// How often the gauges of the indexes marked dirty are refreshed, and how long a refresh waits
/// for one index. An index that does not answer in time (a long reserve, a stuck actor) stays dirty
/// for the next refresh.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const INDEX_REFRESH_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct Metrics {
    pub registry: Registry,
    pub latency: HistogramVec,
    pub size: GaugeVec,
    pub modified: CounterVec,
    pub indexing_lag: HistogramVec,
    pub cdc_reader_up: GaugeVec,
    pub cdc_handler_errors_total: CounterVec,
    pub cdc_reader_restarts_total: CounterVec,
    pub cdc_last_processed_timestamp_seconds: GaugeVec,
    pub fts_index_size_bytes: GaugeVec,
    pub fts_segment_count: GaugeVec,
    dirty_indexes: Arc<DashSet<IndexKey>>,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        let registry = Registry::new();

        // Custom buckets: 0.1ms to 10s
        let buckets = vec![
            0.0001, // 0.1 ms
            0.0002, // 0.2 ms
            0.0005, // 0.5 ms
            0.001,  // 1 ms
            0.002,  // 2 ms
            0.005,  // 5 ms
            0.01,   // 10 ms
            0.02,   // 20 ms
            0.05,   // 50 ms
            0.1,    // 0.1 second
            0.2,    // 0.2 seconds
            0.5,    // 0.5 seconds
            1.0,    // 1 seconds
            2.0,    // 2 seconds
            5.0,    // 5 seconds
            10.0,   // 10 seconds
        ];

        let latency = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "request_latency_seconds",
                "Latency per index and operation (seconds)",
            )
            .buckets(buckets),
            &["keyspace", "index_name", "operation"],
        )
        .unwrap();

        let size = GaugeVec::new(
            prometheus::Opts::new("index_size", "Number of Vector per index"),
            &["keyspace", "index_name"],
        )
        .unwrap();

        let modified: CounterVec = CounterVec::new(
            prometheus::Opts::new("index_modified", "Number of modified items per index"),
            &["keyspace", "index_name", "operation"],
        )
        .unwrap();

        // Custom buckets spanning the realtime (sub-second) and consistent
        // (tens of seconds) CDC reader regimes, up to a few minutes to surface
        // stalled indexing.
        let lag_buckets = vec![
            0.05,  // 50 ms
            0.1,   // 0.1 second
            0.25,  // 0.25 seconds
            0.5,   // 0.5 seconds
            1.0,   // 1 second
            2.5,   // 2.5 seconds
            5.0,   // 5 seconds
            10.0,  // 10 seconds
            30.0,  // 30 seconds
            60.0,  // 1 minute
            120.0, // 2 minutes
            300.0, // 5 minutes
        ];

        let indexing_lag = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "indexing_lag_seconds",
                "Time in seconds between a CDC-recorded change in ScyllaDB and its indexing in the vector store",
            )
            .buckets(lag_buckets),
            &["keyspace", "index_name"],
        )
        .unwrap();

        let cdc_reader_up = GaugeVec::new(
            prometheus::Opts::new(
                "cdc_reader_up",
                "Whether the CDC reader for an index is currently running (1) or stopped (0)",
            ),
            &["keyspace", "index_name", "reader"],
        )
        .unwrap();

        let cdc_handler_errors_total = CounterVec::new(
            prometheus::Opts::new(
                "cdc_handler_errors_total",
                "Total number of CDC handler errors per index and reader",
            ),
            &["keyspace", "index_name", "reader"],
        )
        .unwrap();

        let cdc_reader_restarts_total = CounterVec::new(
            prometheus::Opts::new(
                "cdc_reader_restarts_total",
                "Total number of CDC reader restart attempts after an error, per index and reader",
            ),
            &["keyspace", "index_name", "reader"],
        )
        .unwrap();

        let cdc_last_processed_timestamp_seconds = GaugeVec::new(
            prometheus::Opts::new(
                "cdc_last_processed_timestamp_seconds",
                "Unix timestamp (seconds) up to which the CDC log has been fully consumed. \
                 This is the reader's checkpoint position, not the wall-clock time of the last mutation.",
            ),
            &["keyspace", "index_name", "reader"],
        )
        .unwrap();

        let fts_index_size_bytes = GaugeVec::new(
            prometheus::Opts::new(
                "fts_index_size_bytes",
                "Total size of a full-text search index (bytes)",
            ),
            &["keyspace", "index_name"],
        )
        .unwrap();

        let fts_segment_count = GaugeVec::new(
            prometheus::Opts::new(
                "fts_segment_count",
                "Number of segments in a full-text search index",
            ),
            &["keyspace", "index_name"],
        )
        .unwrap();

        registry.register(Box::new(latency.clone())).unwrap();
        registry.register(Box::new(size.clone())).unwrap();
        registry.register(Box::new(modified.clone())).unwrap();
        registry.register(Box::new(indexing_lag.clone())).unwrap();
        registry.register(Box::new(cdc_reader_up.clone())).unwrap();
        registry
            .register(Box::new(cdc_handler_errors_total.clone()))
            .unwrap();
        registry
            .register(Box::new(cdc_reader_restarts_total.clone()))
            .unwrap();
        registry
            .register(Box::new(cdc_last_processed_timestamp_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(fts_index_size_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(fts_segment_count.clone()))
            .unwrap();

        Self {
            registry,
            latency,
            size,
            modified,
            indexing_lag,
            cdc_reader_up,
            cdc_handler_errors_total,
            cdc_reader_restarts_total,
            cdc_last_processed_timestamp_seconds,
            fts_index_size_bytes,
            fts_segment_count,
            dirty_indexes: Arc::new(DashSet::new()),
        }
    }

    pub(crate) fn mark_dirty(&self, key: &IndexKey) {
        self.dirty_indexes.insert(key.clone());
    }

    pub(crate) fn take_dirty_indexes(&self) -> Vec<IndexKey> {
        // Collect, then remove.
        let keys: Vec<_> = self
            .dirty_indexes
            .iter()
            .map(|entry| entry.clone())
            .collect();
        for k in &keys {
            self.dirty_indexes.remove(k);
        }
        keys
    }

    pub(crate) fn remove_index_labels(&self, keyspace: &str, index_name: &str) {
        for op in REQUEST_OPERATIONS {
            let _ = self
                .latency
                .remove_label_values(&[keyspace, index_name, op]);
        }
        let _ = self.size.remove_label_values(&[keyspace, index_name]);
        let _ = self
            .indexing_lag
            .remove_label_values(&[keyspace, index_name]);
        let _ = self
            .fts_index_size_bytes
            .remove_label_values(&[keyspace, index_name]);
        let _ = self
            .fts_segment_count
            .remove_label_values(&[keyspace, index_name]);
        for op in OPERATIONS {
            let _ = self
                .modified
                .remove_label_values(&[keyspace, index_name, op]);
        }
        let key = IndexKey::new(&KeyspaceName::from(keyspace), &IndexName::from(index_name));
        self.dirty_indexes.remove(&key);
    }

    pub(crate) fn remove_reader_labels(&self, keyspace: &str, index_name: &str, reader: &str) {
        let _ = self
            .cdc_reader_up
            .remove_label_values(&[keyspace, index_name, reader]);
        let _ = self
            .cdc_handler_errors_total
            .remove_label_values(&[keyspace, index_name, reader]);
        let _ = self
            .cdc_reader_restarts_total
            .remove_label_values(&[keyspace, index_name, reader]);
        let _ = self
            .cdc_last_processed_timestamp_seconds
            .remove_label_values(&[keyspace, index_name, reader]);
    }

    /// Refreshes the gauges of the indexes marked dirty with `refresh`, which is true once the
    /// gauges of an index are set or the index is gone. The indexes are refreshed all at once, each
    /// for at most `index_timeout`, so that a busy index delays no other. An index whose refresh
    /// fails or times out is marked dirty again.
    async fn refresh_dirty_indexes<Fut>(
        &self,
        index_timeout: Duration,
        refresh: impl Fn(IndexKey) -> Fut,
    ) where
        Fut: std::future::Future<Output = bool>,
    {
        let refreshes = self.take_dirty_indexes().into_iter().map(|key| {
            let refreshed = time::timeout(index_timeout, refresh(key.clone()));
            async move {
                if !matches!(refreshed.await, Ok(true)) {
                    self.mark_dirty(&key);
                }
            }
        });
        future::join_all(refreshes).await;
    }
}

/// Refreshes the gauges of the indexes marked dirty every `REFRESH_INTERVAL`, until the engine is
/// gone. A scrape of /metrics then reads the gauges without waiting for an index.
pub(crate) fn spawn_refresh(metrics: Arc<Metrics>, engine: WeakSender<Engine>) {
    tokio::spawn(
        async move {
            let mut interval = time::interval(REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let Some(engine) = engine.upgrade() else {
                    break;
                };
                metrics
                    .refresh_dirty_indexes(INDEX_REFRESH_TIMEOUT, |key| {
                        refresh_index(&metrics, &engine, key)
                    })
                    .await;
            }
        }
        .instrument(debug_span!("metrics_refresh")),
    );
}

/// Sets the gauges of the index `key`: true once they are set or the index is gone, false when its
/// count or stats failed.
async fn refresh_index(metrics: &Metrics, engine: &Sender<Engine>, key: IndexKey) -> bool {
    let keyspace = key.keyspace();
    let index_name = key.index();
    let labels = [keyspace.as_ref(), index_name.as_ref()];

    if let Some((index, _)) = engine.get_vs_index(key.clone()).await {
        let Ok(count) = index.count(key).await else {
            return false;
        };
        metrics.size.with_label_values(&labels).set(count as f64);
        return true;
    }

    if let Some((index, _)) = engine.get_fts_index(key.clone()).await {
        let Ok(stats) = index.stats(key).await else {
            return false;
        };
        metrics
            .size
            .with_label_values(&labels)
            .set(stats.num_docs as f64);
        metrics
            .fts_index_size_bytes
            .with_label_values(&labels)
            .set(stats.size_bytes as f64);
        metrics
            .fts_segment_count
            .with_label_values(&labels)
            .set(stats.segment_count as f64);
        return true;
    }

    if let Some((index, _)) = engine.get_pattern_index(key.clone()).await {
        let Ok(count) = index.count(key).await else {
            return false;
        };
        metrics.size.with_label_values(&labels).set(count as f64);
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_cdc::READER_FINE;
    use crate::db_cdc::READER_WIDE;
    use prometheus::Encoder;
    use prometheus::TextEncoder;
    use std::collections::BTreeSet;

    fn index_key(index_name: &str) -> IndexKey {
        IndexKey::new(&KeyspaceName::from("ks"), &IndexName::from(index_name))
    }

    /// Refreshes "fast" at once, fails "failing", and takes a minute for any other index.
    async fn refresh_by_name(key: IndexKey) -> bool {
        match key.index().as_ref() {
            "fast" => true,
            "failing" => false,
            _ => {
                time::sleep(Duration::from_secs(60)).await;
                true
            }
        }
    }

    fn metric_families_text(metrics: &Metrics) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&metrics.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn remove_index_labels_clears_all_metric_series_for_index() {
        let metrics = Metrics::new();

        metrics.size.with_label_values(&["ks", "idx"]).set(10.0);
        metrics
            .modified
            .with_label_values(&["ks", "idx", OP_INSERT])
            .inc();
        metrics
            .latency
            .with_label_values(&["ks", "idx", OP_BM25])
            .observe(0.001);
        metrics
            .indexing_lag
            .with_label_values(&["ks", "idx"])
            .observe(0.001);
        metrics
            .latency
            .with_label_values(&["ks", "idx", OP_HIGHLIGHT])
            .observe(0.001);

        metrics.remove_index_labels("ks", "idx");

        let output = metric_families_text(&metrics);
        assert!(
            !output.contains(r#"keyspace="ks""#),
            "metric output should not contain labels for deleted index, got:\n{output}"
        );
    }

    #[test]
    fn remove_index_labels_is_noop_when_index_has_no_metrics() {
        let metrics = Metrics::new();
        metrics.remove_index_labels("ks", "nonexistent");
        assert!(metric_families_text(&metrics).is_empty());
    }

    #[test]
    fn remove_index_labels_only_removes_target_index() {
        let metrics = Metrics::new();

        metrics.size.with_label_values(&["ks", "idx1"]).set(1.0);
        metrics.size.with_label_values(&["ks", "idx2"]).set(2.0);

        metrics.remove_index_labels("ks", "idx1");

        let output = metric_families_text(&metrics);
        assert!(
            !output.contains(r#"index_name="idx1""#),
            "idx1 labels should be removed"
        );
        assert!(
            output.contains(r#"index_name="idx2""#),
            "idx2 labels should remain"
        );
    }

    #[test]
    fn remove_index_labels_clears_dirty_index_entry() {
        let metrics = Metrics::new();

        metrics.mark_dirty(&index_key("idx"));
        assert_eq!(metrics.dirty_indexes.len(), 1);

        metrics.remove_index_labels("ks", "idx");
        assert!(metrics.dirty_indexes.is_empty());
    }

    #[tokio::test]
    async fn a_refresh_leaves_dirty_the_indexes_it_could_not_refresh() {
        let metrics = Metrics::new();
        let mut left: BTreeSet<_> = (0..20).map(|n| index_key(&format!("slow{n}"))).collect();
        left.insert(index_key("failing"));
        for key in left.iter().chain([&index_key("fast")]) {
            metrics.mark_dirty(key);
        }
        let start = time::Instant::now();

        metrics
            .refresh_dirty_indexes(Duration::from_millis(100), refresh_by_name)
            .await;

        // Refreshed at once, the twenty slow indexes take their 100ms timeout once, not twenty times.
        let took = start.elapsed();
        assert!(took < Duration::from_secs(1), "took {took:?}");
        let dirty: BTreeSet<_> = metrics.take_dirty_indexes().into_iter().collect();
        assert_eq!(dirty, left);
    }

    #[test]
    fn indexing_lag_is_observed_and_exported() {
        use crate::AsyncInProgress;
        use crate::Timestamp;

        let metrics = Metrics::new();

        // Create and immediately drop a Cdc marker to trigger observation.
        let histogram = metrics.indexing_lag.with_label_values(&["ks", "idx"]);
        drop(AsyncInProgress::cdc(histogram.clone(), Timestamp::MIN));

        assert_eq!(histogram.get_sample_count(), 1);
        assert!(histogram.get_sample_sum() > 0.0);

        // The metric must be exported through the same registry the `/metrics`
        // endpoint scrapes, with the expected name, type, and labels.
        let output = metric_families_text(&metrics);
        assert!(
            output.contains("# TYPE indexing_lag_seconds histogram"),
            "indexing_lag_seconds histogram missing from export:\n{output}"
        );
        assert!(
            output.contains(r#"keyspace="ks""#) && output.contains(r#"index_name="idx""#),
            "expected keyspace/index_name labels in export:\n{output}"
        );
        assert!(
            output.contains(r#"indexing_lag_seconds_count{index_name="idx",keyspace="ks"} 1"#),
            "expected a single observed sample in export:\n{output}"
        );
    }

    #[test]
    fn indexing_lag_not_exported_until_observed() {
        let metrics = Metrics::new();

        // A histogram with no observations is still registered, but reports a
        // zero sample count for any label set.
        assert_eq!(
            metrics
                .indexing_lag
                .with_label_values(&["ks", "idx"])
                .get_sample_count(),
            0
        );
    }

    #[test]
    fn cdc_reader_up_is_exported() {
        let metrics = Metrics::new();

        metrics
            .cdc_reader_up
            .with_label_values(&["ks", "idx", READER_WIDE])
            .set(1.0);

        let output = metric_families_text(&metrics);
        assert!(
            output.contains("# TYPE cdc_reader_up gauge"),
            "cdc_reader_up gauge missing from export:\n{output}"
        );
        assert!(
            output.contains(r#"cdc_reader_up{index_name="idx",keyspace="ks",reader="wide"} 1"#),
            "expected cdc_reader_up=1 for wide reader in export:\n{output}"
        );
    }

    #[test]
    fn cdc_handler_errors_total_is_exported() {
        let metrics = Metrics::new();

        metrics
            .cdc_handler_errors_total
            .with_label_values(&["ks", "idx", READER_FINE])
            .inc();

        let output = metric_families_text(&metrics);
        assert!(
            output.contains("# TYPE cdc_handler_errors_total counter"),
            "cdc_handler_errors_total counter missing from export:\n{output}"
        );
        assert!(
            output.contains(
                r#"cdc_handler_errors_total{index_name="idx",keyspace="ks",reader="fine"} 1"#
            ),
            "expected a single observed handler error in export:\n{output}"
        );
    }

    #[test]
    fn cdc_reader_restarts_total_is_exported() {
        let metrics = Metrics::new();

        metrics
            .cdc_reader_restarts_total
            .with_label_values(&["ks", "idx", READER_FINE])
            .inc_by(2.0);

        let output = metric_families_text(&metrics);
        assert!(
            output.contains("# TYPE cdc_reader_restarts_total counter"),
            "cdc_reader_restarts_total counter missing from export:\n{output}"
        );
        assert!(
            output.contains(
                r#"cdc_reader_restarts_total{index_name="idx",keyspace="ks",reader="fine"} 2"#
            ),
            "expected two observed restarts in export:\n{output}"
        );
    }

    #[test]
    fn remove_index_labels_does_not_clear_cdc_reader_metrics() {
        let metrics = Metrics::new();

        // Non-CDC index-scoped metric; must be cleared.
        metrics.size.with_label_values(&["ks", "idx"]).set(1.0);

        // CDC reader metrics are owned by the CDC actor; remove_index_labels must not touch them.
        metrics
            .cdc_reader_up
            .with_label_values(&["ks", "idx", READER_WIDE])
            .set(1.0);

        metrics.remove_index_labels("ks", "idx");

        let output = metric_families_text(&metrics);
        assert!(
            !output.contains(r#"index_size{index_name="idx",keyspace="ks"}"#),
            "index_size should be removed, got:\n{output}"
        );
        assert!(
            output.contains(r#"cdc_reader_up{index_name="idx",keyspace="ks",reader="wide"} 1"#),
            "cdc_reader_up must survive remove_index_labels, got:\n{output}"
        );
    }

    #[test]
    fn cdc_last_processed_timestamp_seconds_is_exported() {
        let metrics = Metrics::new();

        metrics
            .cdc_last_processed_timestamp_seconds
            .with_label_values(&["ks", "idx", READER_WIDE])
            .set(1_700_000_000.0);

        let output = metric_families_text(&metrics);
        assert!(
            output.contains("# TYPE cdc_last_processed_timestamp_seconds gauge"),
            "cdc_last_processed_timestamp_seconds gauge missing from export:\n{output}"
        );
        assert!(
            output.contains(
                r#"cdc_last_processed_timestamp_seconds{index_name="idx",keyspace="ks",reader="wide"} 1700000000"#
            ),
            "expected observed value in export:\n{output}"
        );
    }

    #[test]
    fn remove_reader_labels_clears_only_the_given_readers_cdc_metrics() {
        let metrics = Metrics::new();

        for reader in [READER_WIDE, READER_FINE] {
            metrics
                .cdc_reader_up
                .with_label_values(&["ks", "idx", reader])
                .set(1.0);
            metrics
                .cdc_handler_errors_total
                .with_label_values(&["ks", "idx", reader])
                .inc();
            metrics
                .cdc_reader_restarts_total
                .with_label_values(&["ks", "idx", reader])
                .inc();
            metrics
                .cdc_last_processed_timestamp_seconds
                .with_label_values(&["ks", "idx", reader])
                .set(1_700_000_000.0);
        }

        metrics.remove_reader_labels("ks", "idx", READER_WIDE);

        let output = metric_families_text(&metrics);
        assert!(
            !output.contains(r#"reader="wide""#),
            "the wide reader's CDC metric series should be removed, got:\n{output}"
        );
        assert!(
            output.contains(r#"reader="fine""#),
            "the fine reader's CDC metric series must be unaffected, got:\n{output}"
        );
    }

    #[test]
    fn fts_index_size_bytes_is_exported() {
        let metrics = Metrics::new();

        metrics
            .fts_index_size_bytes
            .with_label_values(&["ks", "idx"])
            .set(1024.0);

        let output = metric_families_text(&metrics);
        assert!(
            output.contains("# TYPE fts_index_size_bytes gauge"),
            "fts_index_size_bytes gauge missing from export:\n{output}"
        );
        assert!(
            output.contains(r#"fts_index_size_bytes{index_name="idx",keyspace="ks"} 1024"#),
            "expected observed value in export:\n{output}"
        );
    }

    #[test]
    fn fts_segment_count_is_exported() {
        let metrics = Metrics::new();

        metrics
            .fts_segment_count
            .with_label_values(&["ks", "idx"])
            .set(3.0);

        let output = metric_families_text(&metrics);
        assert!(
            output.contains("# TYPE fts_segment_count gauge"),
            "fts_segment_count gauge missing from export:\n{output}"
        );
        assert!(
            output.contains(r#"fts_segment_count{index_name="idx",keyspace="ks"} 3"#),
            "expected observed value in export:\n{output}"
        );
    }

    #[test]
    fn request_latency_is_exported() {
        let metrics = Metrics::new();

        for op in REQUEST_OPERATIONS {
            metrics
                .latency
                .with_label_values(&["ks", "idx", op])
                .observe(0.001);
        }

        let output = metric_families_text(&metrics);

        assert!(
            output.contains("# TYPE request_latency_seconds histogram"),
            "request_latency_seconds histogram missing from export:\n{output}"
        );
        // Listed explicitly so that dropping an operation from REQUEST_OPERATIONS fails the test.
        for op in ["ann", "bm25", "highlight", "like"] {
            assert!(
                output.contains(&format!(
                    r#"request_latency_seconds_count{{index_name="idx",keyspace="ks",operation="{op}"}} 1"#
                )),
                "expected a single {op} latency sample in export:\n{output}"
            );
        }
    }

    #[test]
    fn remove_index_labels_clears_fts_metrics() {
        let metrics = Metrics::new();

        metrics
            .fts_index_size_bytes
            .with_label_values(&["ks", "idx"])
            .set(1024.0);
        metrics
            .fts_segment_count
            .with_label_values(&["ks", "idx"])
            .set(3.0);

        metrics.remove_index_labels("ks", "idx");

        let output = metric_families_text(&metrics);
        assert!(
            !output.contains(r#"keyspace="ks""#),
            "metric output should not contain labels for deleted index, got:\n{output}"
        );
    }
}
