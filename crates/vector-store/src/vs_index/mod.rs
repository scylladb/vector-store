/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

mod actor;
mod diskann;
mod factory;
mod opensearch;
mod usearch;
mod validator;

use crate::Config;
use crate::ModifyAdmission;
use crate::memory::Memory;
use crate::worker::Worker;
use actor::AnnR;
pub(crate) use actor::CountR;
use actor::Message;
pub(crate) use actor::VsIndexModify;
pub(crate) use actor::VsIndexModifyExt;
pub(crate) use actor::VsIndexSearch;
pub(crate) use actor::VsIndexSearchExt;
pub(crate) use factory::VsIndexConfiguration;
pub(crate) use factory::VsIndexFactory;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::Instant;
pub(crate) use validator::Error;

async fn recv(
    rx_search: &mut mpsc::Receiver<VsIndexSearch>,
    rx_modify: &mut mpsc::Receiver<VsIndexModify>,
) -> Option<Message> {
    tokio::select! {
        // The order of the select branches is important: search messages go first, so a
        // modification is taken only when no search is queued. Under a busy search load that
        // starves modifications down to ~100 a second (VECTOR-951); the usearch index wraps this
        // in `Admission`, the DiskANN and OpenSearch indexes still use it alone.
        biased;

        Some(msg) = rx_search.recv() => Some(Message::Search(msg)),
        Some(msg) = rx_modify.recv() => Some(Message::Modify(msg)),
        else => None,
    }
}

/// The messages of an index actor, searches first, with modifications admitted as
/// `Config::modify_admission` says; it follows that setting as the configuration reloads.
struct Admission {
    config: watch::Receiver<Arc<Config>>,
    policy: ModifyAdmission,
    batch_left: usize,
    last_modify: Instant,
}

impl Admission {
    fn new(mut config: watch::Receiver<Arc<Config>>) -> Self {
        let policy = config.borrow_and_update().modify_admission;
        Self {
            config,
            policy,
            batch_left: 0,
            last_modify: Instant::now(),
        }
    }

    async fn recv(
        &mut self,
        rx_search: &mut mpsc::Receiver<VsIndexSearch>,
        rx_modify: &mut mpsc::Receiver<VsIndexModify>,
    ) -> Option<Message> {
        if self.config.has_changed().unwrap_or(false) {
            self.policy = self.config.borrow_and_update().modify_admission;
            // a smaller batch size applies to the batch under way too
            let size = self.policy.batch_size.unwrap_or(rx_modify.max_capacity());
            self.batch_left = self.batch_left.min(size);
        }
        if let Some(msg) = self.next_in_batch(rx_modify) {
            return Some(Message::Modify(msg));
        }
        if let Some(msg) = self.overdue(rx_modify) {
            self.start_batch(rx_modify.max_capacity());
            return Some(Message::Modify(msg));
        }
        let msg = recv(rx_search, rx_modify).await?;
        if matches!(msg, Message::Modify(_)) {
            self.start_batch(rx_modify.max_capacity());
        }
        Some(msg)
    }

    /// Starts a batch after a modification; with no `batch_size` it is as deep as the queue.
    fn start_batch(&mut self, queue_depth: usize) {
        self.batch_left = self.policy.batch_size.unwrap_or(queue_depth);
        self.last_modify = Instant::now();
    }

    /// The next queued modification of the current batch, which ends at its size or when the queue
    /// runs empty.
    fn next_in_batch(
        &mut self,
        rx_modify: &mut mpsc::Receiver<VsIndexModify>,
    ) -> Option<VsIndexModify> {
        if self.batch_left == 0 {
            return None;
        }
        let Ok(msg) = rx_modify.try_recv() else {
            self.batch_left = 0;
            return None;
        };
        self.batch_left -= 1;
        self.last_modify = Instant::now();
        Some(msg)
    }

    /// A queued modification taken ahead of waiting searches: none was taken for `max_wait`. It
    /// starts a batch like any other, so a whole batch goes before the searches.
    fn overdue(&mut self, rx_modify: &mut mpsc::Receiver<VsIndexModify>) -> Option<VsIndexModify> {
        let max_wait = self
            .policy
            .max_wait
            .filter(|max_wait| !max_wait.is_zero())?;
        if self.last_modify.elapsed() < max_wait {
            return None;
        }
        rx_modify.try_recv().ok()
    }
}

pub(crate) fn new_index_factory_usearch(
    config_tx: watch::Receiver<Arc<Config>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> anyhow::Result<Box<dyn VsIndexFactory + Send + Sync>> {
    Ok(Box::new(usearch::new_usearch(config_tx, worker, memory)?))
}

pub(crate) fn new_index_factory_opensearch(
    addr: String,
    config_rx: watch::Receiver<Arc<Config>>,
) -> anyhow::Result<Box<dyn VsIndexFactory + Send + Sync>> {
    Ok(Box::new(opensearch::new_opensearch(&addr, config_rx)?))
}

pub(crate) fn new_index_factory_diskann(
    config_rx: watch::Receiver<Arc<Config>>,
    worker: async_channel::Sender<Worker>,
    memory: mpsc::Sender<Memory>,
) -> anyhow::Result<Box<dyn VsIndexFactory + Send + Sync>> {
    Ok(Box::new(diskann::new_diskann(config_rx, worker, memory)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DEFAULT_MODIFY_MAX_WAIT;
    use crate::IndexKey;
    use crate::table::PartitionId;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn modify() -> VsIndexModify {
        VsIndexModify::RemovePartition {
            partition_id: PartitionId::from(0),
        }
    }

    fn search() -> VsIndexSearch {
        let (tx, _rx) = oneshot::channel();
        VsIndexSearch::Count {
            index_key: IndexKey::new(&"ks".into(), &"idx".into()),
            tx,
        }
    }

    fn policy(batch_size: Option<usize>, max_wait: Option<Duration>) -> ModifyAdmission {
        ModifyAdmission {
            batch_size,
            max_wait,
        }
    }

    struct Actor {
        admission: Admission,
        config: watch::Sender<Arc<Config>>,
        rx_search: mpsc::Receiver<VsIndexSearch>,
        rx_modify: mpsc::Receiver<VsIndexModify>,
        tx_search: mpsc::Sender<VsIndexSearch>,
        tx_modify: mpsc::Sender<VsIndexModify>,
    }

    impl Actor {
        fn new(policy: ModifyAdmission, queue_depth: usize) -> Self {
            let (config, config_rx) = watch::channel(Arc::new(Config {
                modify_admission: policy,
                ..Config::default()
            }));
            let (tx_search, rx_search) = mpsc::channel(100);
            let (tx_modify, rx_modify) = mpsc::channel(queue_depth);
            Self {
                admission: Admission::new(config_rx),
                config,
                rx_search,
                rx_modify,
                tx_search,
                tx_modify,
            }
        }

        async fn queue(&self, searches: usize, modifies: usize) {
            for _ in 0..searches {
                self.tx_search.send(search()).await.unwrap();
            }
            for _ in 0..modifies {
                self.tx_modify.send(modify()).await.unwrap();
            }
        }

        /// The kinds of the next `count` messages: 'S' for a search, 'M' for a modification.
        async fn take(&mut self, count: usize) -> String {
            let mut kinds = String::new();
            for _ in 0..count {
                let msg = self
                    .admission
                    .recv(&mut self.rx_search, &mut self.rx_modify)
                    .await
                    .unwrap();
                kinds.push(if matches!(msg, Message::Search(_)) {
                    'S'
                } else {
                    'M'
                });
            }
            kinds
        }
    }

    #[tokio::test]
    async fn by_default_a_batch_is_as_deep_as_the_queue() {
        let default = policy(None, Some(DEFAULT_MODIFY_MAX_WAIT));
        assert_eq!(ModifyAdmission::default(), default);
        assert_eq!(DEFAULT_MODIFY_MAX_WAIT, Duration::from_millis(100));
        let mut actor = Actor::new(policy(None, None), 2);
        actor.queue(1, 2).await;
        assert_eq!(actor.take(2).await, "SM"); // the gap starts a batch of two more
        actor.queue(1, 1).await;
        assert_eq!(actor.take(1).await, "M"); // ahead of the search queued meanwhile
        actor.queue(0, 1).await;
        assert_eq!(actor.take(1).await, "M"); // the batch ends at the queue's depth...
        assert_eq!(actor.take(2).await, "SM"); // ...so the search goes before the modify still queued
    }

    #[tokio::test]
    async fn with_batch_size_zero_one_modify_fills_a_gap() {
        let mut actor = Actor::new(policy(Some(0), None), 100);
        actor.queue(2, 3).await;
        assert_eq!(actor.take(3).await, "SSM");
        actor.queue(2, 0).await;
        assert_eq!(actor.take(3).await, "SSM");
    }

    #[tokio::test]
    async fn a_batch_takes_queued_modifies_ahead_of_searches() {
        let mut actor = Actor::new(policy(Some(2), None), 100);
        actor.queue(1, 5).await;
        assert_eq!(actor.take(1).await, "S");
        actor.queue(2, 0).await;
        assert_eq!(actor.take(4).await, "SSMM"); // the gap after the searches starts a batch
        actor.queue(1, 0).await;
        assert_eq!(actor.take(3).await, "MSM"); // the batch of three ends at its size
    }

    #[tokio::test]
    async fn a_batch_ends_when_the_queue_runs_empty() {
        let mut actor = Actor::new(policy(Some(10), None), 100);
        actor.queue(0, 2).await;
        assert_eq!(actor.take(2).await, "MM");
        actor.queue(1, 0).await;
        assert_eq!(actor.take(1).await, "S"); // the batch found the queue empty and ended
        actor.queue(1, 1).await;
        assert_eq!(actor.take(2).await, "SM"); // so searches go first again
    }

    #[tokio::test(start_paused = true)]
    async fn an_overdue_modify_goes_ahead_of_queued_searches() {
        let mut actor = Actor::new(policy(Some(0), Some(Duration::from_millis(50))), 100);
        actor.queue(0, 1).await;
        assert_eq!(actor.take(1).await, "M");
        actor.queue(3, 1).await;
        assert_eq!(actor.take(1).await, "S");
        tokio::time::advance(Duration::from_millis(60)).await;
        assert_eq!(actor.take(1).await, "M");
    }

    #[tokio::test]
    async fn a_zero_max_wait_never_puts_a_modify_first() {
        let mut actor = Actor::new(policy(Some(0), Some(Duration::ZERO)), 100);
        actor.queue(2, 1).await;
        assert_eq!(actor.take(3).await, "SSM");
    }

    #[tokio::test]
    async fn a_reload_to_a_smaller_batch_cuts_the_batch_under_way() {
        let mut actor = Actor::new(policy(Some(5), None), 100);
        actor.queue(0, 1).await;
        assert_eq!(actor.take(1).await, "M"); // a batch of five more starts
        actor.config.send_modify(|config| {
            *config = Arc::new(Config {
                modify_admission: policy(Some(0), None),
                ..Config::clone(config)
            })
        });
        actor.queue(1, 2).await;
        assert_eq!(actor.take(3).await, "SMM"); // the search goes first: the batch is cut to zero
    }

    #[tokio::test]
    async fn the_policy_follows_a_config_reload() {
        let mut actor = Actor::new(policy(Some(0), None), 100);
        actor.config.send_modify(|config| {
            *config = Arc::new(Config {
                modify_admission: policy(Some(3), None),
                ..Config::clone(config)
            })
        });
        actor.queue(0, 4).await;
        assert_eq!(actor.take(1).await, "M");
        actor.queue(1, 0).await;
        assert_eq!(actor.take(4).await, "MMMS"); // the old policy would have taken the search second
    }
}
