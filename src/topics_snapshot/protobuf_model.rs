use my_service_bus::abstractions::MessageId;

use crate::topic_key::{namespace_from_persisted, namespace_to_persist, Namespace, TopicKeyRef};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicsSnapshotProtobufModel {
    #[prost(message, repeated, tag = "1")]
    pub data: Vec<TopicSnapshotProtobufModel>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicsSnapshotProtobufModelV2 {
    #[prost(message, repeated, tag = "1")]
    pub data: Vec<TopicSnapshotProtobufModel>,
    #[prost(message, repeated, tag = "2")]
    pub deleted_topics: Vec<DeletedTopicProtobufModel>,
}

/// V3 adds the namespace to every topic record (`TopicSnapshotProtobufModel` tag 7,
/// `DeletedTopicProtobufModel` tag 4). The added fields are optional on the wire, so a V2 or V1
/// blob decodes as V3 with an empty namespace, which reads back as `default` - old snapshots are
/// therefore readable as-is and no file migration is needed.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicsSnapshotProtobufModelV3 {
    #[prost(message, repeated, tag = "1")]
    pub data: Vec<TopicSnapshotProtobufModel>,
    #[prost(message, repeated, tag = "2")]
    pub deleted_topics: Vec<DeletedTopicProtobufModel>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicSnapshotProtobufModel {
    #[prost(string, tag = "1")]
    pub topic_id: String,

    #[prost(int64, tag = "2")]
    message_id: i64,

    #[prost(int32, tag = "3")]
    pub not_used: i32,

    #[prost(message, repeated, tag = "4")]
    pub queues: Vec<QueueSnapshotProtobufModel>,

    #[prost(bool, optional, tag = "5")]
    pub persist: Option<bool>,

    #[prost(int64, tag = "6")]
    pub deleted: i64,

    /// Empty means the `default` namespace - see `namespace_to_persist`.
    #[prost(string, tag = "7")]
    namespace: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletedTopicProtobufModel {
    #[prost(string, tag = "1")]
    pub topic_id: String,
    #[prost(int64, tag = "2")]
    pub message_id: i64,
    #[prost(int64, tag = "3")]
    pub gc_after: i64,
    /// Empty means the `default` namespace - see `namespace_to_persist`.
    #[prost(string, tag = "4")]
    namespace: String,
}

// TODO: used by the soft-delete + GC flow when it is reimplemented (see TODO.md). The persisted
// record is namespace-aware already, so GC can never delete a same-named topic in another
// namespace.
#[allow(dead_code)]
impl DeletedTopicProtobufModel {
    pub fn new(
        namespace: &Namespace,
        topic_id: String,
        message_id: MessageId,
        gc_after: i64,
    ) -> Self {
        Self {
            topic_id,
            message_id: message_id.get_value(),
            gc_after,
            namespace: namespace_to_persist(namespace.as_str()),
        }
    }

    pub fn get_namespace(&self) -> &str {
        namespace_from_persisted(self.namespace.as_str())
    }

    pub fn get_topic_key(&self) -> TopicKeyRef<'_> {
        TopicKeyRef::new(self.get_namespace(), self.topic_id.as_str())
    }
}

impl TopicSnapshotProtobufModel {
    pub fn new(
        namespace: &Namespace,
        topic_id: String,
        message_id: MessageId,
        queues: Vec<QueueSnapshotProtobufModel>,
        persist: Option<bool>,
        deleted: i64,
    ) -> Self {
        Self {
            topic_id,
            message_id: message_id.get_value(),
            not_used: 0,
            queues,
            persist,
            deleted,
            namespace: namespace_to_persist(namespace.as_str()),
        }
    }
    pub fn get_message_id(&self) -> MessageId {
        self.message_id.into()
    }

    pub fn get_namespace(&self) -> &str {
        namespace_from_persisted(self.namespace.as_str())
    }

    pub fn get_topic_key(&self) -> TopicKeyRef<'_> {
        TopicKeyRef::new(self.get_namespace(), self.topic_id.as_str())
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueueSnapshotProtobufModel {
    #[prost(string, tag = "1")]
    pub queue_id: ::prost::alloc::string::String,

    #[prost(message, repeated, tag = "2")]
    pub ranges: Vec<QueueRangeProtobufModel>,

    #[prost(int32, tag = "3")]
    pub queue_type: i32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueueRangeProtobufModel {
    #[prost(int64, tag = "1")]
    from_id: i64,

    #[prost(int64, tag = "2")]
    to_id: i64,
}

impl QueueRangeProtobufModel {
    pub fn new(from_id: i64, to_id: i64) -> Self {
        Self {
            from_id: from_id.into(),
            to_id: to_id.into(),
        }
    }

    pub fn get_from_id(&self) -> MessageId {
        self.from_id.into()
    }

    pub fn get_to_id(&self) -> MessageId {
        self.to_id.into()
    }
}
