use my_service_bus::{
    abstractions::AsMessageId,
    shared::protobuf_models::{MessageMetaDataProtobufModel, MessageProtobufModel},
};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    persistence_grpc::*,
    topic_key::{Namespace, NamespaceError},
    topics_snapshot::{
        QueueRangeProtobufModel, QueueSnapshotProtobufModel, TopicSnapshotProtobufModel,
    },
};

/// Fallible because the namespace arrives from the wire: it is validated on the node, and
/// re-validated here before it can become part of a storage key.
impl TryFrom<TopicAndQueuesSnapshotGrpcModel> for TopicSnapshotProtobufModel {
    type Error = NamespaceError;

    fn try_from(value: TopicAndQueuesSnapshotGrpcModel) -> Result<Self, Self::Error> {
        let namespace = Namespace::parse(value.namespace.as_deref())?;

        let queues = value
            .queue_snapshots
            .into_iter()
            .map(|itm| itm.into())
            .collect();

        Ok(Self::new(
            &namespace,
            value.topic_id.to_string(),
            value.message_id.as_message_id(),
            queues,
            value.persist,
            value.deleted,
        ))
    }
}

impl Into<QueueSnapshotProtobufModel> for QueueSnapshotGrpcModel {
    fn into(self) -> QueueSnapshotProtobufModel {
        let queue_type = self.queue_type() as i32;
        let ranges = self.ranges.into_iter().map(|itm| itm.into()).collect();

        QueueSnapshotProtobufModel {
            queue_id: self.queue_id,
            ranges,
            queue_type,
        }
    }
}

impl Into<QueueRangeProtobufModel> for QueueIndexRangeGrpcModel {
    fn into(self) -> QueueRangeProtobufModel {
        QueueRangeProtobufModel::new(self.from_id, self.to_id)
    }
}

impl Into<MessageProtobufModel> for MessageContentGrpcModel {
    fn into(self) -> MessageProtobufModel {
        MessageProtobufModel::new(
            self.message_id.into(),
            DateTimeAsMicroseconds::new(self.created),
            self.data,
            self.meta_data
                .into_iter()
                .map(|itm| MessageMetaDataProtobufModel {
                    key: itm.key,
                    value: itm.value,
                })
                .collect(),
        )
    }
}
