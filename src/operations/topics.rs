use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData, topic_key::TopicKeyRef};

use super::OperationError;

pub async fn get_topic(
    app: &AppContext,
    topic_key: TopicKeyRef<'_>,
) -> Result<Arc<TopicData>, OperationError> {
    let result = app.topics_list.get(topic_key);

    match result {
        Some(topic) => Ok(topic),
        None => Err(OperationError::TopicNotFound(topic_key.to_string())),
    }
}
