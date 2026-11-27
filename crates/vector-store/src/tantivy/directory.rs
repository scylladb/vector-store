/*
 * Copyright 2026-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//! Tantivy's RAM directory with a meta lock that wakes a waiter when it is released.
//!
//! The meta lock keeps the files listed in the index's `meta.json` from being deleted while a
//! reload opens them: reloads and Tantivy's removal of unused files both take it briefly, often
//! at once. The RAM directory's locks are files, and a blocking take of a held one retries every
//! 100 ms, so one of the two slept 100 ms. Like Tantivy's file system directory with its OS lock,
//! this one waits on a condition variable; it still gives up after 10 s.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use tantivy::directory::Directory;
use tantivy::directory::DirectoryLock;
use tantivy::directory::FileHandle;
use tantivy::directory::FileSlice;
use tantivy::directory::Lock;
use tantivy::directory::META_LOCK;
use tantivy::directory::RamDirectory;
use tantivy::directory::WatchCallback;
use tantivy::directory::WatchHandle;
use tantivy::directory::WritePtr;
use tantivy::directory::error::DeleteError;
use tantivy::directory::error::LockError;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::error::OpenWriteError;

/// As long as Tantivy's own wait: 100 tries, 100 ms apart.
const META_LOCK_TIMEOUT: Duration = Duration::from_secs(10);

/// `RamDirectory` with an in-memory meta lock; clones share the files and the lock.
#[derive(Clone, Debug, Default)]
pub(crate) struct MemoryDirectory {
    files: RamDirectory,
    meta_lock: Arc<MetaLock>,
}

#[cfg(test)]
impl MemoryDirectory {
    /// Blocking takes of the meta lock that found it held.
    pub(crate) fn meta_lock_waits(&self) -> usize {
        self.meta_lock
            .waits
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Whether the meta lock is held, and the condition variable its waiters wait on.
#[derive(Debug, Default)]
struct MetaLock {
    held: Mutex<bool>,
    released: Condvar,
    // Counted under the mutex before waiting, so a test can release once a waiter waits.
    #[cfg(test)]
    waits: std::sync::atomic::AtomicUsize,
}

impl MetaLock {
    // The mutex guards a single flag, so a poisoned one is safe to use.
    fn acquire(self: &Arc<Self>, lock: &Lock) -> Result<DirectoryLock, LockError> {
        let held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let mut held = if lock.is_blocking {
            #[cfg(test)]
            if *held {
                self.waits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            let (held, timeout) = self
                .released
                .wait_timeout_while(held, META_LOCK_TIMEOUT, |held| *held)
                .unwrap_or_else(PoisonError::into_inner);
            if timeout.timed_out() && *held {
                return Err(LockError::LockBusy);
            }
            held
        } else if *held {
            return Err(LockError::LockBusy);
        } else {
            held
        };
        *held = true;
        Ok(DirectoryLock::from(Box::new(MetaLockGuard(Arc::clone(
            self,
        )))))
    }
}

struct MetaLockGuard(Arc<MetaLock>);

impl Drop for MetaLockGuard {
    fn drop(&mut self) {
        *self.0.held.lock().unwrap_or_else(PoisonError::into_inner) = false;
        self.0.released.notify_one();
    }
}

impl Directory for MemoryDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.files.get_file_handle(path)
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        self.files.open_read(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.files.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.files.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.files.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.files.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.files.atomic_write(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.files.sync_directory()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.files.watch(watch_callback)
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        if lock.filepath == META_LOCK.filepath {
            self.meta_lock.acquire(lock)
        } else {
            self.files.acquire_lock(lock)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Instant;

    use super::*;

    fn non_blocking_meta_lock() -> Lock {
        Lock {
            filepath: META_LOCK.filepath.clone(),
            is_blocking: false,
        }
    }

    /// A blocking take of a held meta lock is woken by its release.
    #[test]
    fn a_held_meta_lock_wakes_its_waiter_when_released() {
        let directory = MemoryDirectory::default();
        let held = directory.acquire_lock(&META_LOCK).unwrap();
        let waiter = directory.clone();
        let release = thread::spawn(move || {
            // Counted under the mutex the release takes, so the waiter is already waiting.
            let deadline = Instant::now() + Duration::from_secs(5);
            while waiter.meta_lock_waits() == 0 {
                assert!(Instant::now() < deadline, "the take did not wait");
                thread::yield_now();
            }
            let released = Instant::now();
            drop(held);
            released
        });
        let _lock = directory.acquire_lock(&META_LOCK).unwrap();
        let acquired = Instant::now();
        let released = release.join().unwrap();
        assert_eq!(directory.meta_lock_waits(), 1);
        assert!(acquired >= released);
        // Not by the 10 s timeout.
        let after_release = acquired - released;
        assert!(after_release < Duration::from_secs(5), "{after_release:?}");
    }

    /// A non-blocking take fails while a clone of the directory holds the meta lock.
    #[test]
    fn a_non_blocking_take_fails_while_the_meta_lock_is_held() {
        let directory = MemoryDirectory::default();
        let clone = directory.clone();
        let held = directory.acquire_lock(&META_LOCK).unwrap();
        assert!(matches!(
            clone.acquire_lock(&non_blocking_meta_lock()),
            Err(LockError::LockBusy)
        ));
        drop(held);
        assert!(clone.acquire_lock(&non_blocking_meta_lock()).is_ok());
    }

    /// Other locks, such as the writer's, stay files of the RAM directory.
    #[test]
    fn other_locks_are_unchanged() {
        let directory = MemoryDirectory::default();
        let writer_lock = || Lock {
            filepath: "writer.lock".into(),
            is_blocking: false,
        };
        let held = directory.acquire_lock(&writer_lock()).unwrap();
        assert!(matches!(
            directory.acquire_lock(&writer_lock()),
            Err(LockError::LockBusy)
        ));
        let _meta = directory.acquire_lock(&META_LOCK).unwrap();
        drop(held);
        assert!(directory.acquire_lock(&writer_lock()).is_ok());
    }
}
