/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Config;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::info;
use tracing::trace;

const MEMORY_SAFETY_BUFFER_RATIO: f64 = 0.01; // 1% of total RAM
const MEMORY_SAFETY_BUFFER_MIN: u64 = 200 * 1024 * 1024; // 200 MB
const MEMORY_INFO_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum Allocate {
    Can,
    Cannot,
}

pub(crate) type AllocateR = watch::Receiver<Allocate>;

pub enum Memory {
    SubscribeAllocate { tx: oneshot::Sender<AllocateR> },
}

pub(crate) trait MemoryExt {
    async fn subscribe_allocate(&self) -> AllocateR;
}

impl MemoryExt for mpsc::Sender<Memory> {
    async fn subscribe_allocate(&self) -> AllocateR {
        let (tx, rx) = oneshot::channel();
        self.send(Memory::SubscribeAllocate { tx })
            .await
            .expect("MemoryExt::can_allocate: internal actor should receive request");
        rx.await
            .expect("MemoryExt::can_allocate: internal actor should send response")
    }
}

pub(crate) fn new(mut config_rx: watch::Receiver<Arc<Config>>) -> mpsc::Sender<Memory> {
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(async move {
        debug!("starting");


        let mut config = config_rx.borrow_and_update().clone();

        let mut system_info = System::new_all();
        let mut memory_limit = calculate_memory_limit(available_memory(&system_info), &config);
        info!("Memory limit set to {memory_limit} bytes");

        let mut interval = tokio::time::interval(
            config
                .memory_usage_check_interval
                .unwrap_or(MEMORY_INFO_REFRESH_INTERVAL),
        );
        info!("Memory usage check interval set to {:?}", interval.period());

        let (allocate_tx, allocate_rx) = watch::channel(
            can_allocate(used_memory(&system_info), memory_limit, Allocate::Can));

        loop {
            select! {
                _ = config_rx.changed() => {
                    let config_new = config_rx.borrow_and_update().clone();
                    if config.memory_limit != config_new.memory_limit {
                        memory_limit = calculate_memory_limit(
                            available_memory(&system_info), &config_new);
                        info!("Memory limit updated to {memory_limit} bytes");
                    }
                    if config.memory_usage_check_interval != config_new.memory_usage_check_interval {
                        interval = tokio::time::interval(
                            config_new
                            .memory_usage_check_interval
                            .unwrap_or(MEMORY_INFO_REFRESH_INTERVAL),
                        );
                        info!("Memory usage check interval updated to {:?}", interval.period());
                    }
                    config = config_new;
                }

                _ = interval.tick() => {
                    system_info.refresh_memory();
                    let allocate = *allocate_tx.borrow();
                    _ = allocate_tx.send(
                        can_allocate(used_memory(&system_info), memory_limit, allocate));
                }

                msg = rx.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };
                    match msg {
                        Memory::SubscribeAllocate { tx } => {
                            tx
                                .send(allocate_rx.clone())
                                .unwrap_or_else(|_|
                                    trace!("can_allocate: unable to send response"));
                        }
                    }
                }
            }
        }

        debug!("finished");
    }.instrument(debug_span!("memory")));

    tx
}

fn available_memory(system_info: &System) -> u64 {
    let cgroup = system_info.cgroup_limits();

    if let Some(cgroup) = cgroup {
        cgroup.total_memory
    } else {
        system_info.total_memory()
    }
}

fn used_memory(system_info: &System) -> u64 {
    let cgroup = system_info.cgroup_limits();

    if let Some(cgroup) = cgroup {
        cgroup.rss
    } else {
        system_info.used_memory()
    }
}

fn calculate_memory_limit(available_memory: u64, config: &Config) -> u64 {
    let safety_buffer = (available_memory as f64 * MEMORY_SAFETY_BUFFER_RATIO) as u64;
    let safety_buffer = safety_buffer.max(MEMORY_SAFETY_BUFFER_MIN);
    let system_limit = available_memory.saturating_sub(safety_buffer);

    if let Some(memory_limit) = config.memory_limit {
        memory_limit.min(system_limit)
    } else {
        system_limit
    }
}

fn can_allocate(used_memory: u64, memory_limit: u64, current: Allocate) -> Allocate {
    if used_memory < memory_limit {
        if current == Allocate::Cannot {
            info!("Memory usage below limit ({used_memory}), can allocate more memory");
        }
        Allocate::Can
    } else {
        if current == Allocate::Can {
            info!("Memory usage above limit ({used_memory}), cannot allocate more memory");
        }
        Allocate::Cannot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_calculate_memory_limit() {
        const MB: u64 = 1024 * 1024;
        const GB: u64 = 1024 * MB;

        let no_limit = Config {
            memory_limit: None,
            ..Default::default()
        };

        // 4 GB RAM: 1% = ~41 MB < 200 MB floor, so safety buffer = 200 MB.
        let total_ram = 4 * GB;
        assert_eq!(
            calculate_memory_limit(total_ram, &no_limit),
            total_ram - 200 * MB,
        );

        // 32 GB RAM: 1% = ~328 MB > 200 MB floor, so safety buffer = 1% of RAM.
        let total_ram = 32 * GB;
        let expected_buffer = (total_ram as f64 * MEMORY_SAFETY_BUFFER_RATIO) as u64;
        assert_eq!(
            calculate_memory_limit(total_ram, &no_limit),
            total_ram - expected_buffer,
        );

        // Config limit below system limit: config limit applies.
        let total_ram = 4 * GB;
        let config_low = Config {
            memory_limit: Some(GB),
            ..Default::default()
        };
        assert_eq!(calculate_memory_limit(total_ram, &config_low), GB);

        // Config limit above system limit: system limit applies.
        let config_high = Config {
            memory_limit: Some(total_ram),
            ..Default::default()
        };
        assert_eq!(
            calculate_memory_limit(total_ram, &config_high),
            total_ram - 200 * MB,
        );
    }

    #[test]
    fn check_can_allocate() {
        assert_eq!(can_allocate(99, 100, Allocate::Can), Allocate::Can);
        assert_eq!(can_allocate(99, 100, Allocate::Cannot), Allocate::Can);
        assert_eq!(can_allocate(100, 100, Allocate::Can), Allocate::Cannot);
        assert_eq!(can_allocate(100, 100, Allocate::Cannot), Allocate::Cannot);
        assert_eq!(can_allocate(101, 100, Allocate::Can), Allocate::Cannot);
        assert_eq!(can_allocate(101, 100, Allocate::Cannot), Allocate::Cannot);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn check_configuration_change() {
        let (config_tx, config_rx) = watch::channel(Arc::new(Config {
            memory_usage_check_interval: Some(Duration::from_secs(3600)),
            ..Config::default()
        }));
        let memory_actor = new(config_rx);
        let allocator_rx = memory_actor.subscribe_allocate().await;

        // be sure the actor has started
        assert_eq!(*allocator_rx.borrow(), Allocate::Can);

        let available_mem = available_memory(&System::new_all());
        let default_limit = calculate_memory_limit(available_mem, &Config::default());
        assert!(logs_contain(&format!(
            "Memory limit set to {default_limit} bytes"
        )));
        assert!(logs_contain(&format!(
            "Memory usage check interval set to {:?}",
            Duration::from_secs(3600)
        )));

        config_tx
            .send(Arc::new(Config {
                memory_limit: Some(default_limit - 1),
                memory_usage_check_interval: Some(Duration::from_millis(1)),
                ..Config::default()
            }))
            .unwrap();

        // be sure the configuration reload has taken place
        let allocator_rx = memory_actor.subscribe_allocate().await;
        assert_eq!(*allocator_rx.borrow(), Allocate::Can);
        assert!(logs_contain(&format!(
            "Memory limit updated to {} bytes",
            { default_limit - 1 }
        )));
        assert!(logs_contain(&format!(
            "Memory usage check interval updated to {:?}",
            Duration::from_millis(1)
        )));
    }
}
