// TODO: Restore soft-deleted topic. See `TODO.md`.
// Should: clear the soft-delete entry from `deleted_topics` so the topic stops
// being treated as scheduled for GC and remains available.
//
// use crate::{app::AppContext, topics_snapshot::DeletedTopicProtobufModel};
//
// pub async fn restore_topic(app: &AppContext, topic_id: &str) -> Option<DeletedTopicProtobufModel> {
//     app.topics_snapshot.remove_deleted_topic(topic_id).await
// }
