/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
use dashmap::DashSet;
use prometheus::CounterVec;
use prometheus::GaugeVec;
use prometheus::HistogramVec;
use prometheus::Registry;
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub latency: HistogramVec,
    pub size: GaugeVec,
    pub modified: CounterVec,
    dirty_indexes: Arc<DashSet<(String, String)>>,
}

impl Metrics {
    pub fn new() -> Self {
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
                "Latency per index (seconds)",
            )
            .buckets(buckets),
            &["keyspace", "index_name"],
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

        registry.register(Box::new(latency.clone())).unwrap();
        registry.register(Box::new(size.clone())).unwrap();
        registry.register(Box::new(modified.clone())).unwrap();

        Self {
            registry,
            latency,
            size,
            modified,
            dirty_indexes: Arc::new(DashSet::new()),
        }
    }
    pub fn mark_dirty(&self, keyspace: &str, index_name: &str) {
        self.dirty_indexes
            .insert((keyspace.to_owned(), index_name.to_owned()));
    }
    pub fn take_dirty_indexes(&self) -> Vec<(String, String)> {
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
}
