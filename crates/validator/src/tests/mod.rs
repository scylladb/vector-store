/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod crud;

use crate::ServicesSubnet;
use crate::dns::Dns;
use crate::scylla_cluster::ScyllaCluster;
use crate::vector_store_cluster::VectorStoreCluster;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::Instrument;
use tracing::Span;
use tracing::error;
use tracing::info;
use tracing::info_span;

#[derive(Clone)]
pub(crate) struct TestActors {
    pub(crate) services_subnet: Arc<ServicesSubnet>,
    pub(crate) dns: mpsc::Sender<Dns>,
    pub(crate) db: mpsc::Sender<ScyllaCluster>,
    pub(crate) vs: mpsc::Sender<VectorStoreCluster>,
}

type TestFuture = BoxFuture<'static, ()>;

type TestFn = Box<dyn Fn(TestActors) -> TestFuture>;

#[derive(Debug)]
/// Statistics for a test run, including total tests, launched, successful, and failed.
pub(crate) struct Statistics {
    total: usize,
    launched: usize,
    ok: usize,
    failed: usize,
}

impl Statistics {
    fn new(total: usize) -> Self {
        Self {
            total,
            launched: 0,
            ok: 0,
            failed: 0,
        }
    }

    fn append(&mut self, other: &Self) {
        self.total += other.total;
        self.launched += other.launched;
        self.ok += other.ok;
        self.failed += other.failed;
    }
}

/// Represents a single test case, which can include initialization, multiple tests, and cleanup.
pub(crate) struct TestCase {
    init: Option<(Duration, TestFn)>,
    tests: Vec<(String, Duration, TestFn)>,
    cleanup: Option<(Duration, TestFn)>,
}

impl TestCase {
    /// Creates a new empty test case.
    fn empty() -> Self {
        Self {
            init: None,
            tests: vec![],
            cleanup: None,
        }
    }

    /// Add an initialization function to the test case.
    fn with_init<F, R>(mut self, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.init = Some((timeout, wrap_test_fn(test_fn)));
        self
    }

    /// Add a test to the test case.
    fn with_test<F, R>(mut self, name: impl ToString, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.tests
            .push((name.to_string(), timeout, wrap_test_fn(test_fn)));
        self
    }

    /// Add a cleanup function to the test case.
    fn with_cleanup<F, R>(mut self, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.cleanup = Some((timeout, wrap_test_fn(test_fn)));
        self
    }

    /// Run initialization, all tests, and cleanup functions in the test case.
    async fn run(&self, actors: TestActors, filter: &HashSet<String>) -> Statistics {
        let mut stats = Statistics::new(
            self.tests.len() + self.init.is_some() as usize + self.cleanup.is_some() as usize,
        );

        if let Some((timeout, init)) = &self.init {
            stats.launched += 1;
            if !run_single(info_span!("init"), *timeout, init(actors.clone())).await {
                stats.failed += 1;
                return stats;
            }
            stats.ok += 1;
        }

        stream::iter(self.tests.iter())
            .filter(|(name, _, _)| future::ready(filter.is_empty() || filter.contains(name)))
            .then(|(name, timeout, test)| {
                let actors = actors.clone();
                stats.launched += 1;
                async move { run_single(info_span!("test", name), *timeout, test(actors)).await }
            })
            .for_each(|ok| {
                if ok {
                    stats.ok += 1;
                } else {
                    stats.failed += 1;
                };
                future::ready(())
            })
            .await;

        if let Some((timeout, cleanup)) = &self.cleanup {
            stats.launched += 1;
            if !run_single(info_span!("cleanup"), *timeout, cleanup(actors.clone())).await {
                stats.failed += 1;
            } else {
                stats.ok += 1;
            }
        }

        stats
    }
}

/// Wraps a test function into a `TestFn` type, which is a boxed future that can be stored in a
/// container.
fn wrap_test_fn<F, R>(test_fn: F) -> TestFn
where
    F: Fn(TestActors) -> R + 'static,
    R: Future<Output = ()> + Send + 'static,
{
    Box::new(move |actors: TestActors| {
        let future = test_fn(actors);
        future.boxed()
    })
}

/// Runs a single test with a timeout, logging the result in the provided span.
async fn run_single(span: Span, timeout: Duration, future: TestFuture) -> bool {
    let task = tokio::spawn({
        async move {
            time::timeout(timeout, future)
                .await
                .expect("test timed out");
        }
        .instrument(span.clone())
    });
    if task.await.is_ok() {
        info!(parent: &span, "test ok");
        return true;
    }
    error!(parent: &span, "test failed");
    false
}

/// Returns a vector of all known test cases to be run. Each test case is registered with a name
pub(crate) async fn register() -> Vec<(String, TestCase)> {
    vec![("crud", crud::new().await)]
        .into_iter()
        .map(|(name, test_case)| (name.to_string(), test_case))
        .collect::<Vec<_>>()
}

/// Runs all test cases, filtering them based on the provided filter map.
pub(crate) async fn run(
    actors: TestActors,
    test_cases: Vec<(String, TestCase)>,
    filter: Arc<HashMap<String, HashSet<String>>>,
) -> bool {
    let stats = stream::iter(test_cases.into_iter())
        .filter(|(name, _)| {
            let process = filter.is_empty() || filter.contains_key(name);
            async move { process }
        })
        .then(|(name, test_case)| {
            let actors = actors.clone();
            let filter = filter.clone();
            let filter_name = name.clone();
            async move {
                let stats = test_case
                    .run(actors, filter.get(&filter_name).unwrap_or(&HashSet::new()))
                    .instrument(info_span!("test-case", name))
                    .await;
                if stats.failed > 0 {
                    error!("test case failed: {stats:?}");
                } else {
                    info!("test case ok: {stats:?}");
                }
                stats
            }
        })
        .fold(Statistics::new(0), |mut acc, stats| async move {
            acc.append(&stats);
            acc
        })
        .await;
    if stats.failed > 0 {
        error!("test run failed: {stats:?}");
        return false;
    }
    info!("test run ok: {stats:?}");
    true
}
