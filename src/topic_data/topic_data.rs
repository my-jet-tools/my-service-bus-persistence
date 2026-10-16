use std::sync::Arc;

use my_service_bus::shared::sub_page::SubPageId;
use rust_extensions::EntityWith2StrKey;

use crate::{
    index_by_minute::IndexByMinuteList,
    message_pages::{PagesList, SubPage, SubPageInner},
    topic_key::TopicKeyRef,
};

use super::topic_data_metrics::TopicDataMetrics;

pub struct TopicData {
    pub namespace: String,
    pub topic_id: String,
    pub pages_list: PagesList,
    pub metrics: TopicDataMetrics,
    pub yearly_index_by_minute: IndexByMinuteList,
}

/// Topics are keyed by the `(namespace, topic_id)` pair - the same topic name in two namespaces
/// are two independent topics.
impl EntityWith2StrKey for TopicData {
    fn get_primary_key(&self) -> &str {
        &self.namespace
    }

    fn get_secondary_key(&self) -> &str {
        &self.topic_id
    }
}

impl TopicData {
    pub fn new(topic_key: TopicKeyRef<'_>) -> Self {
        Self {
            namespace: topic_key.namespace.to_string(),
            topic_id: topic_key.topic_id.to_string(),
            pages_list: PagesList::new(),
            metrics: TopicDataMetrics::new(),
            yearly_index_by_minute: IndexByMinuteList::new(),
        }
    }

    pub fn get_topic_key(&self) -> TopicKeyRef<'_> {
        TopicKeyRef::new(self.namespace.as_str(), self.topic_id.as_str())
    }

    pub async fn get_sub_page_to_publish_messages(&self, sub_page_id: SubPageId) -> Arc<SubPage> {
        loop {
            let sub_page = self.pages_list.get(sub_page_id).await;

            if let Some(sub_page) = sub_page {
                return sub_page;
            };

            let sub_page_inner = SubPageInner::new(sub_page_id);
            self.pages_list.insert(sub_page_inner).await;
        }
    }
}
