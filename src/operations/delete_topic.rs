// TODO: Soft-delete topic flow.
// See `TODO.md` for the design we want here:
//   - mark topic with an expiration date (`gc_after`),
//   - keep its data on disk until the timer triggers permanent removal.
// The previous in-progress implementation is preserved below for reference.
//
// use rust_extensions::date_time::DateTimeAsMicroseconds;
// use serde::*;
//
// use crate::app::AppContext;
//
// use super::OperationError;
//
// #[derive(Serialize, Deserialize, Debug)]
// pub struct TopicSoftDeleteMetadataBlobModel {
//     pub message_id: i64,
//     pub delete_after: String,
// }
//
// pub async fn delete_topic(
//     app: &AppContext,
//     topic_id: &str,
//     delete_after: DateTimeAsMicroseconds,
// ) -> Result<(), OperationError> {
//     // Get current snapshot and find the topic
//     let mut topics_snapshot = app.topics_snapshot.get().await;
//
//     let index = topics_snapshot
//         .snapshot
//         .data
//         .iter()
//         .position(|topic| topic.topic_id.as_str() == topic_id);
//
//     if index.is_none() {
//         return Err(OperationError::TopicNotFound(topic_id.to_string()));
//     }
//
//     let index = index.unwrap();
//
//     let message_id = topics_snapshot.snapshot.data[index].get_message_id();
//
//     // Remove topic from active snapshot data
//     topics_snapshot.snapshot.data.remove(index);
//     app.topics_snapshot
//         .update(topics_snapshot.snapshot.data)
//         .await;
//
//     // Drop in-memory topic data
//     app.topics_list.delete(topic_id);
//
//     // Register soft-delete with GC deadline
//     app.topics_snapshot
//         .add_deleted_topic(topic_id, message_id, delete_after)
//         .await;
//
//     Ok(())
// }
