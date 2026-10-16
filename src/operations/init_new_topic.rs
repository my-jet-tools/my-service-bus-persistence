use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData, topic_key::TopicKeyRef};

pub async fn init_new_topic(
    app: &AppContext,
    topic_key: TopicKeyRef<'_>,
) -> Option<Arc<TopicData>> {
    if app.topics_list.create_topic_data(topic_key) {
        app.create_topic_folder(topic_key).await;
    }

    app.topics_list.get(topic_key)
}
