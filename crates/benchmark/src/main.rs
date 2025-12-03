/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod data;
mod db;
mod vs;

use crate::data::Query;
use crate::db::Scylla;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use futures::future;
use itertools::Itertools;
use std::cmp;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::sync::Notify;
use tokio::task;
use tokio::time;
use tokio::time::Instant;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

const KEYSPACE: &str = "vsb_keyspace";
const TABLE: &str = "vsb_table";
const INDEX: &str = "vsb_index";

#[derive(Parser)]
#[clap(version)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum MetricType {
    Cosine,
    Euclidean,
    DotProduct,
}

#[derive(Subcommand)]
enum Command {
    BuildTable {
        #[clap(long)]
        data_dir: PathBuf,

        #[clap(long)]
        scylla: SocketAddr,

        #[clap(long)]
        user: Option<String>,

        #[clap(long)]
        passwd_path: Option<PathBuf>,

        #[clap(long)]
        rf: usize,

        #[clap(long, value_parser = clap::value_parser!(u32).range(1..=1_000_000))]
        concurrency: u32,
    },

    BuildIndex {
        #[clap(long)]
        scylla: SocketAddr,

        #[clap(long)]
        user: Option<String>,

        #[clap(long)]
        passwd_path: Option<PathBuf>,

        #[clap(long, required = true)]
        vector_store: Vec<SocketAddr>,

        #[clap(long)]
        metric_type: MetricType,

        #[clap(long)]
        m: usize,

        #[clap(long)]
        ef_construction: usize,

        #[clap(long)]
        ef_search: usize,
    },

    DropTable {
        #[clap(long)]
        scylla: SocketAddr,

        #[clap(long)]
        user: Option<String>,

        #[clap(long)]
        passwd_path: Option<PathBuf>,
    },

    DropIndex {
        #[clap(long)]
        scylla: SocketAddr,

        #[clap(long)]
        user: Option<String>,

        #[clap(long)]
        passwd_path: Option<PathBuf>,
    },

    SearchCql {
        #[clap(long)]
        data_dir: PathBuf,

        #[clap(long)]
        scylla: SocketAddr,

        #[clap(long)]
        user: Option<String>,

        #[clap(long)]
        passwd_path: Option<PathBuf>,

        #[clap(long, value_parser = clap::value_parser!(u32).range(1..=100))]
        limit: u32,

        #[clap(long)]
        duration: humantime::Duration,

        #[clap(long, value_parser = clap::value_parser!(u32).range(1..=1_000_000))]
        concurrency: u32,

        #[clap(long)]
        from: Option<humantime::Timestamp>,

        #[clap(long)]
        delay: Option<humantime::Duration>,
    },

    SearchHttp {
        #[clap(long)]
        data_dir: PathBuf,

        #[clap(long)]
        vector_store: Vec<SocketAddr>,

        #[clap(long, value_parser = clap::value_parser!(u32).range(1..=100))]
        limit: u32,

        #[clap(long)]
        duration: humantime::Duration,

        #[clap(long, value_parser = clap::value_parser!(u32).range(1..=1_000_000))]
        concurrency: u32,

        #[clap(long)]
        from: Option<humantime::Timestamp>,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap(),
        )
        .with(fmt::layer().with_target(false))
        .init();

    match Args::parse().command {
        Command::BuildTable {
            data_dir,
            scylla,
            user,
            passwd_path,
            rf,
            concurrency,
        } => {
            let dataset = data::new(data_dir).await;
            let dimension = dataset.dimension().await;
            let stream = dataset.vector_stream().await;
            let scylla = Scylla::new(scylla, user, passwd_path).await;
            let (duration, _) = measure_duration(async move {
                scylla.create_table(dimension, rf).await;
                scylla.upload_vectors(stream, concurrency as usize).await;
            })
            .await;
            info!("Build table took {duration:.2?}");
        }

        Command::BuildIndex {
            scylla,
            user,
            passwd_path,
            vector_store,
            metric_type,
            m,
            ef_construction,
            ef_search,
        } => {
            let scylla = Scylla::new(scylla, user, passwd_path).await;
            let clients = vs::new_http_clients(vector_store);
            let (duration, _) = measure_duration(async move {
                scylla
                    .create_index(metric_type, m, ef_construction, ef_search)
                    .await;
                vs::wait_for_indexes_ready(&clients).await;
            })
            .await;
            info!("Build Index took {duration:.2?}");
        }

        Command::DropTable {
            scylla,
            user,
            passwd_path,
        } => {
            let scylla = Scylla::new(scylla, user, passwd_path).await;
            let (duration, _) = measure_duration(async move {
                scylla.drop_table().await;
            })
            .await;
            info!("Drop Table took {duration:.2?}");
        }

        Command::DropIndex {
            scylla,
            user,
            passwd_path,
        } => {
            let scylla = Scylla::new(scylla, user, passwd_path).await;
            let (duration, _) = measure_duration(async move {
                scylla.drop_index().await;
            })
            .await;
            info!("Drop Index took {duration:.2?}");
        }

        Command::SearchCql {
            data_dir,
            scylla,
            user,
            passwd_path,
            limit,
            duration,
            concurrency,
            from,
            delay,
        } => {
            let dataset = data::new(data_dir).await;
            let queries = Arc::new(dataset.queries(limit as usize).await);
            let notify = Arc::new(Notify::new());
            let scylla = Scylla::new(scylla, user, passwd_path).await;

            let start = from
                .map(SystemTime::from)
                .unwrap_or_else(|| SystemTime::now() + Duration::from_secs(2));
            let stop = start + Duration::from(duration);

            let tasks = (0..concurrency)
                .map(|_| {
                    let queries = Arc::clone(&queries);
                    let notify = Arc::clone(&notify);
                    let scylla = scylla.clone();
                    tokio::spawn(async move {
                        let mut measurement = SearchMeasure::new();
                        notify.notified().await;
                        while SystemTime::now() < stop {
                            let query = random(&queries);
                            let (duration, recall) = measure_duration(scylla.search(query)).await;
                            measurement.record(duration, recall);
                            if let Some(delay) = delay {
                                let start = Instant::now();
                                let duration = Duration::from(delay);
                                while start.elapsed() < duration {
                                    task::yield_now().await;
                                }
                            }
                        }
                        measurement
                    })
                })
                .collect_vec();

            let wait_for_start = start.duration_since(SystemTime::now()).unwrap();
            info!("Synchronizing search cql tasks to start after {wait_for_start:.2?}");
            time::sleep_until(Instant::now() + wait_for_start).await;

            info!("Starting search cql tasks");
            let (duration, measurements) = measure_duration(async {
                notify.notify_waiters();
                future::join_all(tasks).await
            })
            .await;

            info!("Gathering measurements");
            let mut measurement = SearchMeasure::new();
            measurements.into_iter().for_each(|measure| {
                measurement.append(&measure.unwrap());
            });

            measurement.log(duration, None);
        }

        Command::SearchHttp {
            data_dir,
            vector_store,
            limit,
            duration,
            concurrency,
            from,
        } => {
            let dataset = data::new(data_dir).await;
            let queries = Arc::new(dataset.queries(limit as usize).await);
            let notify = Arc::new(Notify::new());
            assert!(!vector_store.is_empty());
            let clients = Arc::new(vs::new_http_clients(vector_store));
            let clients_len = clients.len();

            let start = from
                .map(SystemTime::from)
                .unwrap_or_else(|| SystemTime::now() + Duration::from_secs(2));
            let stop = start + Duration::from(duration);

            let tasks: Vec<_> = (0..concurrency)
                .map(|no| {
                    let queries = Arc::clone(&queries);
                    let notify = Arc::clone(&notify);
                    let clients = Arc::clone(&clients);
                    tokio::spawn(async move {
                        let mut count = 0;
                        let mut measurements = vec![SearchMeasure::without_recall(); clients_len];
                        notify.notified().await;
                        while SystemTime::now() < stop {
                            let query = random(&queries);
                            let idx = (no as usize + count) % clients_len;
                            let client = &clients[idx];
                            let (duration, _) = measure_duration(async {
                                client
                                    .ann(
                                        &KEYSPACE.to_string().into(),
                                        &INDEX.to_string().into(),
                                        query.query.clone().into(),
                                        NonZeroUsize::new(query.neighbors.len()).unwrap().into(),
                                    )
                                    .await
                            })
                            .await;
                            count += 1;
                            measurements[idx].record_without_recall(duration);
                        }
                        measurements
                    })
                })
                .collect_vec();

            let wait_for_start = start.duration_since(SystemTime::now()).unwrap();
            info!("Synchronizing search http tasks to start after {wait_for_start:.2?}");
            time::sleep_until(Instant::now() + wait_for_start).await;

            info!("Starting search http tasks");
            let (duration, measurements) = measure_duration(async {
                notify.notify_waiters();
                future::join_all(tasks).await
            })
            .await;

            info!("Gathering measurements");
            let mut measure_overall = SearchMeasure::without_recall();
            let mut measure_clients = vec![SearchMeasure::without_recall(); clients_len];
            measurements
                .into_iter()
                .flat_map(|measurements| measurements.unwrap().into_iter().enumerate())
                .for_each(|(idx, measurement)| {
                    measure_overall.append(&measurement);
                    measure_clients[idx].append(&measurement);
                });

            measure_overall.log(duration, None);
            if clients_len > 1 {
                measure_clients
                    .into_iter()
                    .enumerate()
                    .for_each(|(i, measure)| {
                        measure.log(duration, Some(&i.to_string()));
                    });
            }
        }
    };
}

async fn measure_duration<T>(f: impl Future<Output = T>) -> (Duration, T) {
    let start = Instant::now();
    let t = f.await;
    (start.elapsed(), t)
}

fn random(data: &[Query]) -> &Query {
    &data[rand::random_range(0..data.len())]
}

const BUCKETS: usize = 10_000;
const MIN_DURATION: Duration = Duration::from_millis(1);
const MAX_DURATION: Duration = Duration::from_millis(100);
const STEP_DURATION: Duration = MAX_DURATION
    .checked_sub(MIN_DURATION)
    .unwrap()
    .checked_div(BUCKETS as u32)
    .unwrap();

#[derive(Clone)]
struct Histogram {
    buckets: [u64; BUCKETS + 2],
    count: u64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: [0; BUCKETS + 2],
            count: 0,
        }
    }

    fn record(&mut self, value: Duration) {
        let idx = if value < MIN_DURATION {
            0
        } else if value > MAX_DURATION {
            BUCKETS + 1
        } else {
            (value - MIN_DURATION)
                .div_duration_f64(STEP_DURATION)
                .round() as usize
                + 1
        };
        self.buckets[idx] += 1;
        self.count += 1;
    }

    fn percentile(&self, percentile: f64) -> Duration {
        let percentile = (self.count as f64 * percentile / 100.0) as u64;
        let mut sum = 0;
        let Some(idx) = self
            .buckets
            .iter()
            .enumerate()
            .map(|(idx, &count)| {
                sum += count;
                (idx, sum)
            })
            .find_map(|(idx, sum)| (sum >= percentile).then_some(idx))
        else {
            return Duration::MAX;
        };
        if idx == self.buckets.len() - 1 {
            return Duration::MAX;
        }
        MIN_DURATION + STEP_DURATION * idx as u32
    }

    fn append(&mut self, other: &Self) {
        for (a, b) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *a += b;
        }
        self.count += other.count;
    }
}

#[derive(Clone)]
struct SearchMeasure {
    count: usize,
    histogram: Histogram,
    latency_min: Duration,
    latency_max: Duration,
    with_recall: bool,
    recall_min: f64,
    recall_max: f64,
    recall_sum: f64,
}

impl SearchMeasure {
    fn new() -> Self {
        Self {
            count: 0,
            histogram: Histogram::new(),
            latency_min: Duration::MAX,
            latency_max: Duration::ZERO,
            with_recall: true,
            recall_min: 0.0,
            recall_max: 0.0,
            recall_sum: 0.0,
        }
    }

    fn without_recall() -> Self {
        let mut sm = Self::new();
        sm.with_recall = false;
        sm
    }

    fn record_without_recall(&mut self, latency: Duration) {
        self.count += 1;
        self.histogram.record(latency);
        self.latency_min = cmp::min(self.latency_min, latency);
        self.latency_max = cmp::max(self.latency_max, latency);
    }

    fn record(&mut self, latency: Duration, recall: f64) {
        self.count += 1;
        self.histogram.record(latency);
        self.latency_min = cmp::min(self.latency_min, latency);
        self.latency_max = cmp::max(self.latency_max, latency);
        self.recall_min = self.recall_min.min(recall);
        self.recall_max = self.recall_max.max(recall);
        self.recall_sum += recall;
    }

    fn append(&mut self, other: &Self) {
        self.count += other.count;
        self.histogram.append(&other.histogram);
        self.latency_min = cmp::min(self.latency_min, other.latency_min);
        self.latency_max = cmp::max(self.latency_max, other.latency_max);
        if self.with_recall && other.with_recall {
            self.recall_min = self.recall_min.min(other.recall_min);
            self.recall_max = self.recall_max.max(other.recall_max);
            self.recall_sum += other.recall_sum;
        }
    }

    fn log(&self, duration: Duration, label: Option<&str>) {
        let label = label
            .map(|label| format!(" for {label}"))
            .unwrap_or_default();
        if label.is_empty() {
            info!("duration: {:.1?}", duration);
        }
        info!("queries{label}: {}", self.count);
        info!(
            "QPS{label}: {:.1}",
            self.count as f64 / duration.as_secs_f64()
        );
        info!("latency min{label}: {:.1?}", self.latency_min);
        for percentile in [1, 10, 25, 50, 75, 90, 99] {
            info!(
                "latency P{:.02}{label}: {:.1?}",
                percentile,
                self.histogram.percentile(percentile as f64)
            );
        }
        info!("latency max{label}: {:.1?}", self.latency_max);

        if self.with_recall {
            info!("recall min{label}: {:.1}", self.recall_min * 100.0);
            info!(
                "recall avg{label}: {:.1}",
                self.recall_sum * 100.0 / self.count as f64
            );
            info!("recall max{label}: {:.1}", self.recall_max * 100.0);
        }
    }
}
