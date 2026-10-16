use my_service_bus::abstractions::MessageId;
use parking_lot::Mutex;
use rust_extensions::sorted_vec::{EntityWithKey, SortedVec};

use super::MinuteWithinYear;

#[derive(Clone)]
pub struct UpdateQueueItem {
    pub minute_within_year: MinuteWithinYear,
    pub message_id: MessageId,
}

impl EntityWithKey<MinuteWithinYear> for UpdateQueueItem {
    fn get_key(&self) -> &MinuteWithinYear {
        &self.minute_within_year
    }
}

pub struct UpdateQueue {
    data: Mutex<SortedVec<MinuteWithinYear, UpdateQueueItem>>,
}

impl UpdateQueue {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(SortedVec::new()),
        }
    }

    pub async fn update(&self, minute_within_year: MinuteWithinYear, message_id: MessageId) {
        let mut update_queue = self.data.lock();
        match update_queue.insert_or_if_not_exists(&minute_within_year) {
            rust_extensions::sorted_vec::InsertIfNotExists::Insert(entry) => {
                let item = UpdateQueueItem {
                    minute_within_year,
                    message_id,
                };
                entry.insert(item);
            }
            rust_extensions::sorted_vec::InsertIfNotExists::Exists(_) => {}
        }
    }

    pub async fn get(&self, minute_within_year: MinuteWithinYear) -> Option<MessageId> {
        let read_access = self.data.lock();
        let result = read_access.get(&minute_within_year)?;
        Some(result.message_id)
    }

    pub async fn get_items_ready_to_be_gc(&self) -> Option<Vec<MinuteWithinYear>> {
        let read_access = self.data.lock();
        if read_access.len() <= 1 {
            return None;
        }

        let last = read_access.last().unwrap().minute_within_year;

        let result: Vec<MinuteWithinYear> = read_access
            .iter()
            .filter(|itm| itm.minute_within_year.get_value() != last.get_value())
            .map(|itm| itm.minute_within_year)
            .collect();
        Some(result)
    }

    pub async fn remove_first_element(&self) -> Option<UpdateQueueItem> {
        let mut write_access = self.data.lock();
        write_access.remove_at(0)
    }

    pub async fn remove(&self, minute_within_year: MinuteWithinYear) -> Option<MessageId> {
        let mut write_access = self.data.lock();
        let result = write_access.remove(&minute_within_year)?;
        Some(result.message_id)
    }
}
