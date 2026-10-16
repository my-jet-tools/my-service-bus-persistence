use my_logger::LogEventCtx;
use parking_lot::RwLock;

use crate::topic_key::TopicKey;

use super::{file_storage::TopicsSnapshotStorage, protobuf_model::*};

#[derive(Clone)]
pub struct TopicsSnapshotData {
    pub snapshot_id: i64,
    pub last_saved_snapshot_id: i64,
    pub snapshot: TopicsSnapshotProtobufModelV3,
}

impl TopicsSnapshotData {
    pub fn new(
        data: Vec<TopicSnapshotProtobufModel>,
        deleted_topics: Vec<DeletedTopicProtobufModel>,
    ) -> Self {
        Self {
            snapshot: TopicsSnapshotProtobufModelV3 {
                data,
                deleted_topics,
            },
            snapshot_id: 0,
            last_saved_snapshot_id: 0,
        }
    }

    pub fn update(&mut self, data: Vec<TopicSnapshotProtobufModel>) {
        self.snapshot.data = data;
        self.snapshot_id += 1;
    }

    pub fn update_snapshot_id(&mut self, saved_id: i64) {
        self.last_saved_snapshot_id = saved_id;
    }
}

/// Flat in memory - every namespace in one list, because `GetQueueSnapshot` is deliberately a
/// single stream for all of them. On disk it is split into one YAML file per namespace.
pub struct CurrentTopicsSnapshot {
    data: RwLock<TopicsSnapshotData>,
    pub storage: TopicsSnapshotStorage,
}

impl CurrentTopicsSnapshot {
    pub async fn read_or_create(data_folder: String) -> Self {
        let storage = TopicsSnapshotStorage::new(data_folder);
        let loaded = storage.read().await;

        Self {
            data: RwLock::new(TopicsSnapshotData::new(
                loaded.topics,
                loaded.deleted_topics,
            )),
            storage,
        }
    }

    pub async fn get(&self) -> TopicsSnapshotData {
        let read_access = self.data.read();
        read_access.clone()
    }

    pub async fn get_topics_list(&self) -> Vec<TopicKey> {
        let read_access = self.data.read();
        read_access
            .snapshot
            .data
            .iter()
            .map(|itm| itm.get_topic_key().to_owned_key())
            .collect()
    }

    pub async fn update(&self, snapshot: Vec<TopicSnapshotProtobufModel>) {
        let mut write_access = self.data.write();
        write_access.update(snapshot);
    }

    pub async fn update_snapshot_id_as_saved(&self, saved_id: i64) {
        let mut write_access = self.data.write();
        write_access.update_snapshot_id(saved_id);
    }

    pub async fn get_snapshot_if_there_are_changes(&self) -> Option<TopicsSnapshotData> {
        let read_access = self.data.read();
        if read_access.snapshot_id == read_access.last_saved_snapshot_id {
            return None;
        }

        Some(read_access.clone())
    }

    pub async fn flush_topics_snapshot_to_blob(&self) {
        let topics_snapshot = self.get_snapshot_if_there_are_changes().await;

        let Some(topics_snapshot) = topics_snapshot else {
            return;
        };

        let mut attempt_no = 0;

        loop {
            let result = self
                .storage
                .write(
                    topics_snapshot.snapshot.data.as_slice(),
                    topics_snapshot.snapshot.deleted_topics.as_slice(),
                )
                .await;

            if let Err(err) = result {
                my_logger::LOGGER.write_error(
                    "Write Topics Snapshot".to_string(),
                    format!(
                        "Can not write snapshot with ID #{}. Attempt:{}. Err: {}",
                        topics_snapshot.snapshot_id, attempt_no, err
                    ),
                    LogEventCtx::new(),
                );

                if attempt_no >= 5 {
                    return;
                }

                attempt_no += 1;

                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            } else {
                self.update_snapshot_id_as_saved(topics_snapshot.snapshot_id)
                    .await;
                return;
            }
        }
    }
}
