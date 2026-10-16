use std::path::PathBuf;

use my_service_bus::abstractions::MessageId;
use rust_extensions::date_time::AtomicDateTimeAsMicroseconds;

use super::{IndexByMinuteFile, MinuteWithinYear, UpdateQueue};

pub struct YearlyIndexByMinute {
    file: IndexByMinuteFile,
    update_queue: UpdateQueue,
    pub last_access: AtomicDateTimeAsMicroseconds,
}

impl YearlyIndexByMinute {
    pub async fn open_or_create(path: impl Into<PathBuf>) -> Self {
        Self {
            file: IndexByMinuteFile::open_or_create(path).await,
            update_queue: UpdateQueue::new(),
            last_access: AtomicDateTimeAsMicroseconds::now(),
        }
    }

    #[cfg(test)]
    pub fn get_file(&self) -> &crate::file_storage::FileStorage {
        self.file.get_file()
    }

    pub async fn load_if_exists(path: impl Into<PathBuf>) -> Option<Self> {
        let file = IndexByMinuteFile::open_if_exists(path).await?;

        Self {
            file,
            update_queue: UpdateQueue::new(),
            last_access: AtomicDateTimeAsMicroseconds::now(),
        }
        .into()
    }

    pub async fn update_minute_index_if_new(
        &self,
        minute_within_year: MinuteWithinYear,
        message_id: MessageId,
    ) {
        self.update_queue
            .update(minute_within_year, message_id)
            .await;
    }

    pub async fn get_message_id(&self, minute_within_year: MinuteWithinYear) -> Option<MessageId> {
        if let Some(result) = self.update_queue.get(minute_within_year).await {
            return Some(result);
        }

        self.file
            .read_message_id_from_minute_index(minute_within_year)
            .await
    }

    pub async fn flush_to_storage(&self) {
        let items_to_write = self.update_queue.get_items_ready_to_be_gc().await;

        if items_to_write.is_none() {
            return;
        }

        for minute in items_to_write.unwrap() {
            let minute = (minute).into();

            if let Some(message_id) = self.update_queue.remove(minute).await {
                self.write_to_blob(minute, message_id).await;
            }
        }
    }

    pub async fn write_everything_before_gc(&self) {
        while let Some(item) = self.update_queue.remove_first_element().await {
            self.write_to_blob(item.minute_within_year, item.message_id)
                .await;
        }
    }

    async fn write_to_blob(&self, minute: MinuteWithinYear, message_id: MessageId) {
        let value = self.file.read_message_id_from_minute_index(minute).await;

        if value.is_some() {
            return;
        }

        self.file
            .write_message_id_to_minute_index(minute, message_id)
            .await;
    }
}

#[cfg(test)]
mod tests {

    use my_service_bus::abstractions::MessageId;

    use crate::index_by_minute::{MinuteWithinYear, YearlyIndexByMinute};

    fn temp_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-yearly-{}", name));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[tokio::test]
    async fn test_open_not_existing() {
        let path = temp_path("not_existing");

        let index = YearlyIndexByMinute::load_if_exists(&path).await;

        assert_eq!(index.is_none(), true);
    }

    #[tokio::test]
    async fn test_we_already_written() {
        let path = temp_path("already_written");

        let index = YearlyIndexByMinute::open_or_create(&path).await;

        //Writing First element
        let minute = MinuteWithinYear::new(0);

        let message_id = MessageId::new(15);

        index.update_minute_index_if_new(minute, message_id).await;

        let minute = MinuteWithinYear::new(1);

        //Writing Second element

        let message_id = MessageId::new(16);

        index.update_minute_index_if_new(minute, message_id).await;

        index.flush_to_storage().await;

        let result = index.get_file().read(0, 8).await.unwrap();

        let mut value = [0u8; 8];
        value.copy_from_slice(result.as_slice());

        assert_eq!(i64::from_le_bytes(value), 15);

        let _ = std::fs::remove_file(&path);
    }
}
