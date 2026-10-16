use serde::{Deserialize, Serialize};

use crate::topic_key::Namespace;

use super::protobuf_model::*;

/// How the topics + queues snapshot is stored: one readable YAML file per namespace, at
/// `{data_folder}/{namespace}/topics-and-queue.yaml`.
///
/// The namespace is **not** a field here - the path carries it, exactly like every other file in
/// the layout. That also makes a namespace folder self-contained: copy `alpha/` somewhere else and
/// its snapshot travels with it.
///
/// The in-memory model stays the protobuf one (`TopicSnapshotProtobufModel`), which is what the
/// gRPC mappers and the rest of the service speak; this is purely the persisted representation.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TopicsAndQueuesSnapshotYamlModel {
    #[serde(default)]
    pub topics: Vec<TopicYamlModel>,

    /// Kept for the soft-delete + GC flow that is currently disabled - see TODO.md.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted_topics: Vec<DeletedTopicYamlModel>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TopicYamlModel {
    pub topic_id: String,
    pub message_id: i64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persist: Option<bool>,

    #[serde(default, skip_serializing_if = "is_zero")]
    pub deleted: i64,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queues: Vec<QueueYamlModel>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueYamlModel {
    pub queue_id: String,
    pub queue_type: i32,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<QueueRangeYamlModel>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueueRangeYamlModel {
    pub from_id: i64,
    pub to_id: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeletedTopicYamlModel {
    pub topic_id: String,
    pub message_id: i64,
    pub gc_after: i64,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

impl TopicsAndQueuesSnapshotYamlModel {
    /// Everything of one namespace, taken out of the flat in-memory snapshot.
    pub fn from_domain(
        namespace: &str,
        topics: &[TopicSnapshotProtobufModel],
        deleted_topics: &[DeletedTopicProtobufModel],
    ) -> Self {
        Self {
            topics: topics
                .iter()
                .filter(|itm| itm.get_namespace() == namespace)
                .map(TopicYamlModel::from_domain)
                .collect(),
            deleted_topics: deleted_topics
                .iter()
                .filter(|itm| itm.get_namespace() == namespace)
                .map(DeletedTopicYamlModel::from_domain)
                .collect(),
        }
    }

    pub fn into_domain(
        self,
        namespace: &Namespace,
    ) -> (
        Vec<TopicSnapshotProtobufModel>,
        Vec<DeletedTopicProtobufModel>,
    ) {
        let topics = self
            .topics
            .into_iter()
            .map(|itm| itm.into_domain(namespace))
            .collect();

        let deleted_topics = self
            .deleted_topics
            .into_iter()
            .map(|itm| itm.into_domain(namespace))
            .collect();

        (topics, deleted_topics)
    }
}

impl TopicYamlModel {
    fn from_domain(src: &TopicSnapshotProtobufModel) -> Self {
        Self {
            topic_id: src.topic_id.clone(),
            message_id: src.get_message_id().get_value(),
            persist: src.persist,
            deleted: src.deleted,
            queues: src.queues.iter().map(QueueYamlModel::from_domain).collect(),
        }
    }

    fn into_domain(self, namespace: &Namespace) -> TopicSnapshotProtobufModel {
        TopicSnapshotProtobufModel::new(
            namespace,
            self.topic_id,
            self.message_id.into(),
            self.queues
                .into_iter()
                .map(|itm| itm.into_domain())
                .collect(),
            self.persist,
            self.deleted,
        )
    }
}

impl QueueYamlModel {
    fn from_domain(src: &QueueSnapshotProtobufModel) -> Self {
        Self {
            queue_id: src.queue_id.clone(),
            queue_type: src.queue_type,
            ranges: src
                .ranges
                .iter()
                .map(|itm| QueueRangeYamlModel {
                    from_id: itm.get_from_id().get_value(),
                    to_id: itm.get_to_id().get_value(),
                })
                .collect(),
        }
    }

    fn into_domain(self) -> QueueSnapshotProtobufModel {
        QueueSnapshotProtobufModel {
            queue_id: self.queue_id,
            queue_type: self.queue_type,
            ranges: self
                .ranges
                .into_iter()
                .map(|itm| QueueRangeProtobufModel::new(itm.from_id, itm.to_id))
                .collect(),
        }
    }
}

impl DeletedTopicYamlModel {
    fn from_domain(src: &DeletedTopicProtobufModel) -> Self {
        Self {
            topic_id: src.topic_id.clone(),
            message_id: src.message_id,
            gc_after: src.gc_after,
        }
    }

    fn into_domain(self, namespace: &Namespace) -> DeletedTopicProtobufModel {
        DeletedTopicProtobufModel::new(
            namespace,
            self.topic_id,
            self.message_id.into(),
            self.gc_after,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use my_service_bus::abstractions::AsMessageId;

    fn topic(namespace: &str, topic_id: &str, message_id: i64) -> TopicSnapshotProtobufModel {
        TopicSnapshotProtobufModel::new(
            &Namespace::parse(Some(namespace)).unwrap(),
            topic_id.to_string(),
            message_id.as_message_id(),
            vec![QueueSnapshotProtobufModel {
                queue_id: "consumer".to_string(),
                queue_type: 1,
                ranges: vec![QueueRangeProtobufModel::new(5, 9)],
            }],
            Some(true),
            0,
        )
    }

    #[test]
    fn only_the_topics_of_that_namespace_land_in_the_file() {
        let topics = vec![
            topic("default", "orders", 1),
            topic("alpha", "orders", 2),
            topic("default", "payments", 3),
        ];

        let model = TopicsAndQueuesSnapshotYamlModel::from_domain("default", &topics, &[]);

        assert_eq!(2, model.topics.len());
        assert_eq!("orders", model.topics[0].topic_id);
        assert_eq!(1, model.topics[0].message_id);
        assert_eq!("payments", model.topics[1].topic_id);

        let model = TopicsAndQueuesSnapshotYamlModel::from_domain("alpha", &topics, &[]);
        assert_eq!(1, model.topics.len());
        assert_eq!(2, model.topics[0].message_id);
    }

    /// The namespace comes back from the path, not from the file.
    #[test]
    fn round_trip_through_yaml() {
        let topics = vec![topic("alpha", "orders", 42)];

        let model = TopicsAndQueuesSnapshotYamlModel::from_domain("alpha", &topics, &[]);
        let yaml = serde_yaml::to_string(&model).unwrap();

        let parsed: TopicsAndQueuesSnapshotYamlModel = serde_yaml::from_str(&yaml).unwrap();
        let (restored, _) = parsed.into_domain(&Namespace::parse(Some("alpha")).unwrap());

        assert_eq!(1, restored.len());
        assert_eq!("orders", restored[0].topic_id);
        assert_eq!("alpha", restored[0].get_namespace());
        assert_eq!(42, restored[0].get_message_id().get_value());
        assert_eq!(Some(true), restored[0].persist);
        assert_eq!(1, restored[0].queues.len());
        assert_eq!("consumer", restored[0].queues[0].queue_id);
        assert_eq!(1, restored[0].queues[0].queue_type);
        assert_eq!(5, restored[0].queues[0].ranges[0].get_from_id().get_value());
        assert_eq!(9, restored[0].queues[0].ranges[0].get_to_id().get_value());
    }

    #[test]
    fn the_yaml_is_meant_to_be_read_by_a_human() {
        let topics = vec![topic("default", "orders", 7)];
        let model = TopicsAndQueuesSnapshotYamlModel::from_domain("default", &topics, &[]);

        let yaml = serde_yaml::to_string(&model).unwrap();

        assert!(yaml.contains("topic_id: orders"));
        assert!(yaml.contains("message_id: 7"));
        assert!(yaml.contains("queue_id: consumer"));
        // No namespace inside - the path carries it
        assert!(!yaml.contains("namespace"));
        // Defaults are not written out
        assert!(!yaml.contains("deleted"));
    }

    #[test]
    fn an_empty_file_is_an_empty_snapshot() {
        let parsed: TopicsAndQueuesSnapshotYamlModel = serde_yaml::from_str("topics: []").unwrap();
        let (topics, deleted) = parsed.into_domain(&Namespace::default_namespace());

        assert!(topics.is_empty());
        assert!(deleted.is_empty());
    }
}
