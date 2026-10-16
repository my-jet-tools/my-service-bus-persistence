use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData, topic_key::TopicKeyRef};

pub async fn get_topic_data_to_write(
    app: &AppContext,
    topic_key: TopicKeyRef<'_>,
) -> Arc<TopicData> {
    loop {
        if let Some(topic_data) = app.topics_list.get(topic_key) {
            return topic_data;
        }

        super::init_new_topic(app, topic_key).await;
    }
}
