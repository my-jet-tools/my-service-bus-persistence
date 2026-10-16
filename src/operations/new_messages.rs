use std::collections::BTreeMap;

use my_service_bus::shared::{protobuf_models::MessageProtobufModel, sub_page::SubPageId};

use crate::{app::AppContext, topic_key::TopicKeyRef};

pub async fn new_messages(
    app: &AppContext,
    topic_key: TopicKeyRef<'_>,
    messages: impl Iterator<Item = MessageProtobufModel>,
) {
    let mut messages_by_sub_page: BTreeMap<SubPageId, Vec<MessageProtobufModel>> = BTreeMap::new();

    for message in messages {
        let sub_page_id: SubPageId = message.get_message_id().into();

        match messages_by_sub_page.get_mut(&sub_page_id) {
            Some(items) => {
                items.push(message);
            }
            None => {
                messages_by_sub_page.insert(sub_page_id, vec![message]);
            }
        }
    }

    let topic_data = crate::operations::get_topic_data_to_write(app, topic_key).await;
    for (sub_page_id, messages) in messages_by_sub_page {
        let page = topic_data
            .get_sub_page_to_publish_messages(sub_page_id)
            .await;

        crate::operations::index_by_minute::new_messages(app, &topic_data, messages.as_slice())
            .await;

        page.new_messages(messages).await;
    }
}
