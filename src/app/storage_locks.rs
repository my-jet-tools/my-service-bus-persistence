use std::sync::Arc;

use ahash::AHashMap;
use parking_lot::Mutex;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::topic_key::{TopicKey, TopicKeyRef};

/// One `RwLock` per topic, guarding that topic's files against being deleted out from under a
/// reader.
///
/// The pattern the background archiver follows:
///
/// 1. take the **read** lock and move the file to the cold tier - the upload is slow and readers
///    keep working through it, since the file is still on disk and unchanged;
/// 2. drop it, take the **write** lock and only then delete the local copy and drop the cached
///    handle. Nothing can be mid-read at that point, so no reader is left holding a handle to a
///    file that is about to vanish.
///
/// `tokio::sync::RwLock` rather than `parking_lot`, deliberately: the guard is held across the
/// upload and across a ranged read, both of which are `.await`s.
pub struct StorageLocks {
    /// `parking_lot` here on purpose - this only ever does a map lookup, never an await.
    locks: Mutex<AHashMap<TopicKey, Arc<RwLock<()>>>>,
}

impl StorageLocks {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(AHashMap::new()),
        }
    }

    fn get(&self, topic_key: TopicKeyRef<'_>) -> Arc<RwLock<()>> {
        let mut write_access = self.locks.lock();

        if let Some(lock) = write_access.get(&topic_key.to_owned_key()) {
            return lock.clone();
        }

        let lock = Arc::new(RwLock::new(()));
        write_access.insert(topic_key.to_owned_key(), lock.clone());
        lock
    }

    /// Held while a file is being read or uploaded. Several readers pass at once.
    pub async fn read(&self, topic_key: TopicKeyRef<'_>) -> OwnedRwLockReadGuard<()> {
        self.get(topic_key).read_owned().await
    }

    /// Held while a file is being deleted. Waits out every reader first.
    pub async fn write(&self, topic_key: TopicKeyRef<'_>) -> OwnedRwLockWriteGuard<()> {
        self.get(topic_key).write_owned().await
    }

    /// Drops the lock of a topic that is gone. Safe because it only forgets the map entry: anyone
    /// still holding a guard holds it through their own `Arc`.
    pub fn forget(&self, topic_key: TopicKeyRef<'_>) {
        self.locks.lock().remove(&topic_key.to_owned_key());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_same_topic_shares_one_lock() {
        let locks = StorageLocks::new();

        let key = TopicKeyRef::new("default", "orders");

        let first = locks.get(key);
        let second = locks.get(key);

        assert!(Arc::ptr_eq(&first, &second));
    }

    /// The same topic name in two namespaces must not share a lock, or one namespace would stall
    /// the other.
    #[tokio::test]
    async fn two_namespaces_do_not_share_a_lock() {
        let locks = StorageLocks::new();

        let default_ns = locks.get(TopicKeyRef::new("default", "orders"));
        let alpha_ns = locks.get(TopicKeyRef::new("alpha", "orders"));

        assert!(!Arc::ptr_eq(&default_ns, &alpha_ns));
    }

    #[tokio::test]
    async fn readers_do_not_block_each_other() {
        let locks = StorageLocks::new();
        let key = TopicKeyRef::new("default", "orders");

        let first = locks.read(key).await;
        let second = locks.read(key).await;

        drop(first);
        drop(second);
    }

    /// The point of the whole thing: a delete waits until every reader is done.
    #[tokio::test]
    async fn a_delete_waits_for_the_readers() {
        let locks = Arc::new(StorageLocks::new());
        let key = TopicKeyRef::new("default", "orders");

        let reader = locks.read(key).await;

        let waiting = {
            let locks = locks.clone();
            tokio::spawn(async move {
                let _guard = locks.write(TopicKeyRef::new("default", "orders")).await;
                true
            })
        };

        // Still held by the reader
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        drop(reader);

        assert!(waiting.await.unwrap());
    }
}
