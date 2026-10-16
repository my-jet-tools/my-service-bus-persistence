use std::{collections::BTreeMap, sync::Arc};

use parking_lot::Mutex;

use crate::topic_key::TopicKeyRef;

use super::{ArchiveFileNo, ArchiveStorage};

/// Opens archive files. Implemented by `AppContext`, which is the only thing that knows the data
/// folder and whether a cold tier is configured - the list itself stays free of storage details.
#[async_trait::async_trait]
pub trait ArchiveFileOpener {
    /// Opens the local file, creating it if needed. Only ever used for the file being written.
    async fn open_or_create_archive(
        &self,
        topic_key: TopicKeyRef<'_>,
        archive_file_no: ArchiveFileNo,
    ) -> ArchiveStorage;

    /// Local file first, cold tier on miss. `None` when the archive exists in neither.
    async fn try_open_archive(
        &self,
        topic_key: TopicKeyRef<'_>,
        archive_file_no: ArchiveFileNo,
    ) -> Option<ArchiveStorage>;
}

/// archive_file_no -> storage, within one topic of one namespace.
type ArchivesOfTopic = BTreeMap<i64, Arc<ArchiveStorage>>;
/// topic_id -> its archives, within one namespace.
type TopicsOfNamespace = BTreeMap<String, ArchivesOfTopic>;

/// Keyed by the full topic key, so the same topic name in two namespaces never shares an archive.
pub struct ArchiveStorageList {
    items: Mutex<BTreeMap<String, TopicsOfNamespace>>,
}

impl ArchiveStorageList {
    pub fn new() -> Self {
        Self {
            items: Mutex::new(BTreeMap::new()),
        }
    }

    fn get_existing(
        &self,
        topic_key: TopicKeyRef<'_>,
        archive_file_no: ArchiveFileNo,
    ) -> Option<Arc<ArchiveStorage>> {
        let read_access = self.items.lock();

        let archive_storage = read_access
            .get(topic_key.namespace)?
            .get(topic_key.topic_id)?
            .get(&archive_file_no.get_value())?;

        Some(archive_storage.clone())
    }

    fn insert(
        &self,
        topic_key: TopicKeyRef<'_>,
        archive_file_no: ArchiveFileNo,
        storage: Arc<ArchiveStorage>,
    ) {
        let mut write_access = self.items.lock();

        write_access
            .entry(topic_key.namespace.to_string())
            .or_default()
            .entry(topic_key.topic_id.to_string())
            .or_default()
            .insert(archive_file_no.get_value(), storage);
    }

    /// Drops the cached handle of **one** archive - used after it was uploaded and its local file
    /// removed, so the next read reopens it against the cold tier.
    ///
    /// Deliberately not "forget the whole topic": the file currently being written must keep its
    /// handle. Dropping it would let a reopen hand out a second `FileStorage` for the same path
    /// while an in-flight `save_sub_page` still holds the first one, and two independent
    /// `seek(End) + write` pairs can interleave into the same offset.
    pub fn forget_archive(&self, topic_key: TopicKeyRef<'_>, archive_file_no: ArchiveFileNo) {
        let mut write_access = self.items.lock();

        let Some(topics) = write_access.get_mut(topic_key.namespace) else {
            return;
        };

        let Some(archives) = topics.get_mut(topic_key.topic_id) else {
            return;
        };

        archives.remove(&archive_file_no.get_value());
    }

    /// Drops every cached handle of a topic. Only for hard delete, where the topic is already
    /// gone from `TopicsDataList` and nothing is supposed to be writing to it any more.
    pub fn forget_topic(&self, topic_key: TopicKeyRef<'_>) {
        let mut write_access = self.items.lock();

        let Some(topics) = write_access.get_mut(topic_key.namespace) else {
            return;
        };

        topics.remove(topic_key.topic_id);
    }

    pub async fn get_or_create(
        &self,
        archive_file_no: ArchiveFileNo,
        topic_key: TopicKeyRef<'_>,
        opener: &impl ArchiveFileOpener,
    ) -> Arc<ArchiveStorage> {
        if let Some(archive_storage) = self.get_existing(topic_key, archive_file_no) {
            return archive_storage;
        }

        let archive_storage = opener
            .open_or_create_archive(topic_key, archive_file_no)
            .await;

        let archive_storage = Arc::new(archive_storage);

        self.insert(topic_key, archive_file_no, archive_storage.clone());

        archive_storage
    }

    pub async fn try_get_or_open(
        &self,
        archive_file_no: ArchiveFileNo,
        topic_key: TopicKeyRef<'_>,
        opener: &impl ArchiveFileOpener,
    ) -> Option<Arc<ArchiveStorage>> {
        if let Some(archive_storage) = self.get_existing(topic_key, archive_file_no) {
            return Some(archive_storage);
        }

        let archive_storage = opener.try_open_archive(topic_key, archive_file_no).await?;

        let archive_storage = Arc::new(archive_storage);

        self.insert(topic_key, archive_file_no, archive_storage.clone());

        Some(archive_storage)
    }
}
