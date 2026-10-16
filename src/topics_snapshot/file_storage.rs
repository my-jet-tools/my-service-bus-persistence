use std::path::PathBuf;

use crate::{app::storage_layout, topic_key::Namespace};

use super::{protobuf_model::*, yaml_model::TopicsAndQueuesSnapshotYamlModel};

pub struct LoadedTopicsSnapshot {
    pub topics: Vec<TopicSnapshotProtobufModel>,
    pub deleted_topics: Vec<DeletedTopicProtobufModel>,
}

/// The topics + queues snapshot, stored as one readable YAML file per namespace at
/// `{data_folder}/{namespace}/topics-and-queue.yaml`.
///
/// In memory it stays flat - every namespace in one list - because `GetQueueSnapshot` is
/// deliberately a single stream for all of them: that is how the bus node restores everything in
/// one call and learns which namespaces exist. Splitting happens only on the way to disk.
pub struct TopicsSnapshotStorage {
    data_folder: String,
}

impl TopicsSnapshotStorage {
    pub fn new(data_folder: String) -> Self {
        Self { data_folder }
    }

    pub async fn read(&self) -> LoadedTopicsSnapshot {
        let mut topics = Vec::new();
        let mut deleted_topics = Vec::new();

        for namespace in scan_namespaces(self.data_folder.as_str()).await {
            let path = storage_layout::get_namespace_snapshot_file(
                self.data_folder.as_str(),
                namespace.as_str(),
            );

            let Some(content) = read_to_string_if_exists(&path).await else {
                continue;
            };

            let model: TopicsAndQueuesSnapshotYamlModel = match serde_yaml::from_str(&content) {
                Ok(model) => model,
                Err(err) => panic!("Can not parse {:?}: {}", path, err),
            };

            let (namespace_topics, namespace_deleted) = model.into_domain(&namespace);

            println!(
                "Loaded snapshot of namespace '{}'. Topics amount is: {}",
                namespace,
                namespace_topics.len()
            );

            topics.extend(namespace_topics);
            deleted_topics.extend(namespace_deleted);
        }

        LoadedTopicsSnapshot {
            topics,
            deleted_topics,
        }
    }

    pub async fn write(
        &self,
        topics: &[TopicSnapshotProtobufModel],
        deleted_topics: &[DeletedTopicProtobufModel],
    ) -> Result<(), String> {
        let mut namespaces: Vec<String> = Vec::new();

        for topic in topics {
            if !namespaces.iter().any(|itm| itm == topic.get_namespace()) {
                namespaces.push(topic.get_namespace().to_string());
            }
        }

        for topic in deleted_topics {
            if !namespaces.iter().any(|itm| itm == topic.get_namespace()) {
                namespaces.push(topic.get_namespace().to_string());
            }
        }

        // A namespace whose last topic is gone keeps its file, emptied - leaving stale content
        // behind would resurrect topics on the next start.
        for namespace in scan_namespaces(self.data_folder.as_str()).await {
            let namespace = namespace.as_str().to_string();
            if !namespaces.contains(&namespace) {
                namespaces.push(namespace);
            }
        }

        for namespace in namespaces {
            let model = TopicsAndQueuesSnapshotYamlModel::from_domain(
                namespace.as_str(),
                topics,
                deleted_topics,
            );

            let content = serde_yaml::to_string(&model).map_err(|err| {
                format!("Can not serialize the snapshot of '{}': {}", namespace, err)
            })?;

            let path = storage_layout::get_namespace_snapshot_file(
                self.data_folder.as_str(),
                namespace.as_str(),
            );

            write_atomically(&path, content.as_bytes()).await?;
        }

        Ok(())
    }
}

/// Truncate-then-write would leave a half-written snapshot after a crash, and this file is the
/// source of truth for which topics exist. Write next to it and rename - on POSIX the rename is
/// atomic, so a reader sees either the old file or the new one.
async fn write_atomically(path: &PathBuf, content: &[u8]) -> Result<(), String> {
    if let Some(folder) = path.parent() {
        tokio::fs::create_dir_all(folder)
            .await
            .map_err(|err| format!("Can not create {:?}: {}", folder, err))?;
    }

    let mut tmp_path = path.clone();
    tmp_path.set_extension("yaml.tmp");

    tokio::fs::write(&tmp_path, content)
        .await
        .map_err(|err| format!("Can not write {:?}: {}", tmp_path, err))?;

    tokio::fs::rename(&tmp_path, path)
        .await
        .map_err(|err| format!("Can not rename {:?} to {:?}: {}", tmp_path, path, err))?;

    Ok(())
}

async fn read_to_string_if_exists(path: &PathBuf) -> Option<String> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => Some(content),
        Err(err) => {
            if let std::io::ErrorKind::NotFound = err.kind() {
                return None;
            }
            panic!("Can not read {:?}: {}", path, err);
        }
    }
}

/// Top-level folders of the data folder. Anything that is not a valid namespace name is skipped
/// with a warning rather than silently - a stray folder there is worth noticing.
pub async fn scan_namespaces(data_folder: &str) -> Vec<Namespace> {
    let mut result = Vec::new();

    let Ok(mut entries) = tokio::fs::read_dir(data_folder).await else {
        return result;
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        if !path.is_dir() {
            continue;
        }

        let Some(name) = path.file_name().and_then(|itm| itm.to_str()) else {
            continue;
        };

        match Namespace::parse(Some(name)) {
            Ok(namespace) => result.push(namespace),
            Err(err) => {
                println!(
                    "Skipping folder '{}' in the data folder: it is not a valid namespace. {}",
                    name,
                    err.as_string()
                );
            }
        }
    }

    if result.is_empty() {
        result.push(Namespace::default_namespace());
    }

    result
}

/// The pre-YAML format: one protobuf blob at `{data_folder}/topicsdata`, all namespaces at once,
/// prefixed with its length because a page blob was padded to whole 512-byte pages. Read once by
/// the migration, then the file is gone.
pub fn deserialize_legacy_protobuf(content: &[u8]) -> LoadedTopicsSnapshot {
    let empty = LoadedTopicsSnapshot {
        topics: Vec::new(),
        deleted_topics: Vec::new(),
    };

    if content.len() < 4 {
        return empty;
    }

    let mut array = [0u8; 4];
    array.copy_from_slice(&content[..4]);

    let data_size = u32::from_le_bytes(array) as usize;

    if data_size == 0 {
        return empty;
    }

    if content.len() < data_size + 4 {
        panic!(
            "The legacy topics snapshot is truncated: the header says {} bytes, the file holds {}",
            data_size,
            content.len() - 4
        );
    }

    let data = &content[4..data_size + 4];

    // V3 carries the namespace at tag 7; V2 and V1 do not, and an absent namespace decodes as
    // empty, which reads back as `default`.
    let as_v3: Result<TopicsSnapshotProtobufModelV3, _> = prost::Message::decode(data);
    if let Ok(msg) = as_v3 {
        println!(
            "Loaded legacy topic snapshot V3. Topics amount is: {}",
            msg.data.len()
        );
        return LoadedTopicsSnapshot {
            topics: msg.data,
            deleted_topics: msg.deleted_topics,
        };
    }

    let as_v2: Result<TopicsSnapshotProtobufModelV2, _> = prost::Message::decode(data);
    if let Ok(msg) = as_v2 {
        println!(
            "Loaded legacy topic snapshot V2. Topics amount is: {}",
            msg.data.len()
        );
        return LoadedTopicsSnapshot {
            topics: msg.data,
            deleted_topics: msg.deleted_topics,
        };
    }

    let as_v1: Result<TopicsSnapshotProtobufModel, _> = prost::Message::decode(data);
    match as_v1 {
        Ok(msg) => {
            println!(
                "Loaded legacy topic snapshot V1. Topics amount is: {}",
                msg.data.len()
            );
            LoadedTopicsSnapshot {
                topics: msg.data,
                deleted_topics: Vec::new(),
            }
        }
        Err(_) => panic!("Can not deserialize the legacy topics snapshot as V3, V2 or V1"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use my_service_bus::abstractions::AsMessageId;

    fn temp_folder(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-snapshot-{}", name));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn topic(namespace: &str, topic_id: &str, message_id: i64) -> TopicSnapshotProtobufModel {
        TopicSnapshotProtobufModel::new(
            &Namespace::parse(Some(namespace)).unwrap(),
            topic_id.to_string(),
            message_id.as_message_id(),
            vec![],
            Some(true),
            0,
        )
    }

    #[tokio::test]
    async fn one_file_per_namespace() {
        let root = temp_folder("per_namespace");
        let storage = TopicsSnapshotStorage::new(root.to_str().unwrap().to_string());

        let topics = vec![
            topic("default", "orders", 1),
            topic("alpha", "orders", 2),
            topic("alpha", "payments", 3),
        ];

        storage.write(&topics, &[]).await.unwrap();

        assert!(root.join("default").join("topics-and-queue.yaml").is_file());
        assert!(root.join("alpha").join("topics-and-queue.yaml").is_file());

        let loaded = storage.read().await;
        assert_eq!(3, loaded.topics.len());

        // The same topic name in two namespaces stays two records with their own message ids
        let default_orders = loaded
            .topics
            .iter()
            .find(|itm| itm.get_namespace() == "default" && itm.topic_id == "orders")
            .unwrap();
        let alpha_orders = loaded
            .topics
            .iter()
            .find(|itm| itm.get_namespace() == "alpha" && itm.topic_id == "orders")
            .unwrap();

        assert_eq!(1, default_orders.get_message_id().get_value());
        assert_eq!(2, alpha_orders.get_message_id().get_value());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_namespace_that_lost_every_topic_is_emptied_not_left_stale() {
        let root = temp_folder("emptied");
        let storage = TopicsSnapshotStorage::new(root.to_str().unwrap().to_string());

        storage
            .write(&[topic("alpha", "orders", 1)], &[])
            .await
            .unwrap();
        assert_eq!(1, storage.read().await.topics.len());

        storage.write(&[], &[]).await.unwrap();
        assert_eq!(0, storage.read().await.topics.len());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn no_temp_file_is_left_behind() {
        let root = temp_folder("no_tmp");
        let storage = TopicsSnapshotStorage::new(root.to_str().unwrap().to_string());

        storage
            .write(&[topic("default", "orders", 1)], &[])
            .await
            .unwrap();

        let leftovers: Vec<_> = std::fs::read_dir(root.join("default"))
            .unwrap()
            .filter_map(|itm| itm.ok())
            .filter(|itm| itm.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();

        assert!(leftovers.is_empty());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn an_empty_data_folder_reads_as_an_empty_snapshot() {
        let root = temp_folder("empty");
        let storage = TopicsSnapshotStorage::new(root.to_str().unwrap().to_string());

        assert!(storage.read().await.topics.is_empty());

        let _ = std::fs::remove_dir_all(&root);
    }
}
