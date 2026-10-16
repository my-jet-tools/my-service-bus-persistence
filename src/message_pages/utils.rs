use std::collections::BTreeMap;

use my_service_bus::shared::page_id::PageId;

use crate::topics_snapshot::TopicSnapshotProtobufModel;

pub fn get_active_pages(snapshot: &TopicSnapshotProtobufModel) -> BTreeMap<i64, PageId> {
    let mut result: BTreeMap<i64, PageId> = BTreeMap::new();

    let page_id: PageId = snapshot.get_message_id().into();
    result.insert(page_id.get_value(), page_id);

    for topic_queue in &snapshot.queues {
        for range in &topic_queue.ranges {
            let page_id: PageId = range.get_from_id().into();
            result.insert(page_id.get_value(), page_id);
        }
    }

    result
}
