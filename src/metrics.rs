use prometheus::{Encoder, TextEncoder, Registry, HistogramVec};
use axum::{response::IntoResponse, http::{StatusCode, header, HeaderMap}};
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub latency: HistogramVec,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let latency = HistogramVec::new(
            prometheus::HistogramOpts::new("request_latency_seconds", "Latency per index (seconds)"),
            &["keyspace", "index_name"],
        ).unwrap();

        registry.register(Box::new(latency.clone())).unwrap();

        Arc::new(Self {
            registry,
            latency,
        })
    }
     pub async fn handler(self: Arc<Self>) -> impl IntoResponse {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();

        match encoder.encode(&metric_families, &mut buffer) {
            Ok(_) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8".parse().unwrap(),
                );
                (StatusCode::OK, headers, buffer)
            }
            Err(_) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    header::CONTENT_TYPE,
                    "text/plain; charset=utf-8".parse().unwrap(),
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    headers,
                    b"failed to encode metrics".to_vec(),
                )
            }
        }
    }
}
