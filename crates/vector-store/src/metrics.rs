/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
use prometheus::HistogramVec;
use prometheus::Registry;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub latency: HistogramVec,
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

        registry.register(Box::new(latency.clone())).unwrap();

        Self { registry, latency }
    }
}
