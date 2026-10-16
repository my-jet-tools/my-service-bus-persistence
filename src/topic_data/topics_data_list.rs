use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use rust_extensions::SortedVecOfArcWith2StrKey;

use crate::topic_key::TopicKeyRef;

use super::TopicData;

struct TopicsDataInner {
    data: SortedVecOfArcWith2StrKey<TopicData>,
    as_vec: Arc<Vec<Arc<TopicData>>>,
}

impl TopicsDataInner {
    fn empty() -> Self {
        Self {
            data: SortedVecOfArcWith2StrKey::new(),
            as_vec: Arc::new(Vec::new()),
        }
    }

    fn from_data(data: SortedVecOfArcWith2StrKey<TopicData>) -> Self {
        let as_vec = Arc::new(data.iter().cloned().collect());
        Self { data, as_vec }
    }
}

/// `SortedVecOfArcWith2StrKey` derives `Clone` with a `TValue: Clone` bound it does not actually
/// need (only the `Arc`s are cloned), and `TopicData` is not `Clone`. Copy-on-write writers are
/// rare - topic creation / removal - so we rebuild the container from the existing `Arc`s, which
/// are already in key order.
fn copy_of(src: &SortedVecOfArcWith2StrKey<TopicData>) -> SortedVecOfArcWith2StrKey<TopicData> {
    let mut result = SortedVecOfArcWith2StrKey::new();

    for topic_data in src.iter() {
        result.insert_or_replace(topic_data.clone());
    }

    result
}

pub struct TopicsDataList {
    inner: ArcSwap<TopicsDataInner>,
    write_lock: Mutex<()>,
}

impl TopicsDataList {
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(TopicsDataInner::empty()),
            write_lock: Mutex::new(()),
        }
    }

    pub fn get(&self, topic_key: TopicKeyRef<'_>) -> Option<Arc<TopicData>> {
        self.inner
            .load()
            .data
            .get(topic_key.namespace, topic_key.topic_id)
            .cloned()
    }

    pub fn get_all(&self) -> Arc<Vec<Arc<TopicData>>> {
        self.inner.load().as_vec.clone()
    }

    pub fn create_topic_data(&self, topic_key: TopicKeyRef<'_>) -> bool {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        if current
            .data
            .get(topic_key.namespace, topic_key.topic_id)
            .is_some()
        {
            return false;
        }

        let mut new_data = copy_of(&current.data);
        new_data.insert_or_replace(Arc::new(TopicData::new(topic_key)));

        self.inner
            .store(Arc::new(TopicsDataInner::from_data(new_data)));

        true
    }

    pub fn init_topic_data(&self, topic_key: TopicKeyRef<'_>) -> Arc<TopicData> {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        if let Some(existing) = current.data.get(topic_key.namespace, topic_key.topic_id) {
            return existing.clone();
        }

        let topic_data = Arc::new(TopicData::new(topic_key));

        let mut new_data = copy_of(&current.data);
        new_data.insert_or_replace(topic_data.clone());

        self.inner
            .store(Arc::new(TopicsDataInner::from_data(new_data)));

        topic_data
    }

    /// Rebuilt without the removed topic rather than rebuilt-then-removed, and that is not a
    /// stylistic choice.
    ///
    /// `SortedVecOfArcWith2StrKey::remove` takes the row out of its namespace partition but leaves
    /// the partition behind, empty. `PartitionOfArc::get_key` is `rows.get(0).unwrap()`, so the
    /// next lookup of *any* topic panics on it - deleting the last topic of a namespace would
    /// otherwise take the whole service down. Building the copy without the item never creates an
    /// empty partition, because a partition only comes into existence when something is inserted
    /// into it.
    pub fn remove(&self, topic_key: TopicKeyRef<'_>) {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        let mut new_data = SortedVecOfArcWith2StrKey::new();
        let mut removed = false;

        for topic_data in current.data.iter() {
            if topic_data.get_topic_key() == topic_key {
                removed = true;
                continue;
            }

            new_data.insert_or_replace(topic_data.clone());
        }

        if !removed {
            return;
        }

        self.inner
            .store(Arc::new(TopicsDataInner::from_data(new_data)));
    }

    // TODO: soft-delete (`delete`) is being reworked. See `TODO.md`.
    // pub fn delete(&self, topic_key: TopicKeyRef<'_>) -> Option<Arc<TopicData>> {
    //     let _guard = self.write_lock.lock().unwrap();
    //     let current = self.inner.load_full();
    //
    //     let mut new_data = copy_of(&current.data);
    //     let removed = new_data.remove(topic_key.namespace, topic_key.topic_id)?;
    //
    //     let mut new_deleted = current.deleted.clone();
    //     new_deleted.insert(topic_key.to_owned_key());
    //
    //     self.inner
    //         .store(Arc::new(TopicsDataInner::from_parts(new_data, new_deleted)));
    //
    //     Some(removed)
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Removing the last topic of a namespace must leave the list usable. It used not to: the
    /// namespace partition survived empty and the next lookup panicked inside rust-extensions.
    #[test]
    fn removing_the_last_topic_of_a_namespace_does_not_brick_the_list() {
        let list = TopicsDataList::new();

        list.create_topic_data(TopicKeyRef::new("default", "orders"));
        list.create_topic_data(TopicKeyRef::new("alpha", "orders"));

        list.remove(TopicKeyRef::new("alpha", "orders"));

        // The lookup that used to panic
        assert!(list.get(TopicKeyRef::new("default", "orders")).is_some());
        assert!(list.get(TopicKeyRef::new("alpha", "orders")).is_none());
        assert!(list.get(TopicKeyRef::new("beta", "whatever")).is_none());
        assert_eq!(1, list.get_all().len());

        // and the namespace can come back
        list.create_topic_data(TopicKeyRef::new("alpha", "orders"));
        assert!(list.get(TopicKeyRef::new("alpha", "orders")).is_some());
        assert_eq!(2, list.get_all().len());
    }

    #[test]
    fn removing_one_of_several_leaves_the_rest() {
        let list = TopicsDataList::new();

        list.create_topic_data(TopicKeyRef::new("default", "a"));
        list.create_topic_data(TopicKeyRef::new("default", "b"));

        list.remove(TopicKeyRef::new("default", "a"));

        assert!(list.get(TopicKeyRef::new("default", "a")).is_none());
        assert!(list.get(TopicKeyRef::new("default", "b")).is_some());
        assert_eq!(1, list.get_all().len());
    }

    #[test]
    fn removing_something_that_is_not_there_changes_nothing() {
        let list = TopicsDataList::new();
        list.create_topic_data(TopicKeyRef::new("default", "a"));

        list.remove(TopicKeyRef::new("default", "nope"));
        list.remove(TopicKeyRef::new("nope", "a"));

        assert_eq!(1, list.get_all().len());
    }
}
