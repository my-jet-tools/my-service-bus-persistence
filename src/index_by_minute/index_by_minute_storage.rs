use std::path::PathBuf;

use my_service_bus::abstractions::MessageId;

use crate::file_storage::FileStorage;

use super::{utils::MINUTE_INDEX_FILE_SIZE, MinuteWithinYear};

/// One file per topic per year: a flat array of 527 040 slots, 8 bytes each, addressed at
/// `minute * 8`. A slot holds the id of the first message of that minute, or zero when the
/// minute saw no traffic - which is why a read past the end has to come back zero-filled rather
/// than fail.
pub struct IndexByMinuteFile {
    file: FileStorage,
}

impl IndexByMinuteFile {
    pub async fn open_or_create(path: impl Into<PathBuf>) -> Self {
        let file = FileStorage::open_or_create(path)
            .await
            .expect("Can not open the year index file");

        file.ensure_size(MINUTE_INDEX_FILE_SIZE as u64)
            .await
            .expect("Can not size the year index file");

        Self { file }
    }

    pub async fn open_if_exists(path: impl Into<PathBuf>) -> Option<Self> {
        let file = FileStorage::open_if_exists(path)
            .await
            .expect("Can not open the year index file")?;

        let size = file
            .get_size()
            .await
            .expect("Can not read the year index size");

        if (size as usize) < MINUTE_INDEX_FILE_SIZE {
            return None;
        }

        Some(Self { file })
    }

    pub async fn write_message_id_to_minute_index(
        &self,
        minute: MinuteWithinYear,
        message_id: MessageId,
    ) {
        let payload = message_id.get_value().to_le_bytes();

        self.file
            .write(minute.get_position_in_file(), payload.as_slice())
            .await
            .expect("Can not write into the year index");
    }

    pub async fn read_message_id_from_minute_index(
        &self,
        minute: MinuteWithinYear,
    ) -> Option<MessageId> {
        let payload = self
            .file
            .read(minute.get_position_in_file(), 8)
            .await
            .expect("Can not read the year index");

        let mut value = [0u8; 8];
        value.copy_from_slice(payload.as_slice());

        let result = i64::from_le_bytes(value);

        if result == 0 {
            return None;
        }

        Some(MessageId::new(result))
    }

    #[cfg(test)]
    pub fn get_file(&self) -> &FileStorage {
        &self.file
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::index_by_minute::MinuteWithinYear;
    use my_service_bus::abstractions::MessageId;

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-yearindex-{}", name));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[tokio::test]
    async fn test_create_new_file_then_write_and_read() {
        let path = temp_path("write_and_read");
        let storage = IndexByMinuteFile::open_or_create(&path).await;

        let minute = MinuteWithinYear::new(15);
        let message_id = MessageId::new(123);

        storage
            .write_message_id_to_minute_index(minute, message_id)
            .await;

        let result = storage.read_message_id_from_minute_index(minute).await;

        assert_eq!(result.unwrap(), message_id);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_create_new_file_then_read() {
        let path = temp_path("read_empty");
        let storage = IndexByMinuteFile::open_or_create(&path).await;

        let result = storage
            .read_message_id_from_minute_index(MinuteWithinYear::new(15))
            .await;

        assert!(result.is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_position_in_file() {
        let path = temp_path("position");
        let storage = IndexByMinuteFile::open_or_create(&path).await;

        let minute = MinuteWithinYear::new(15);

        storage
            .write_message_id_to_minute_index(minute, MessageId::new(123))
            .await;

        let payload = storage.get_file().read_all().await.unwrap();

        assert_eq!(
            &payload[minute.get_value() as usize * 8..minute.get_value() as usize * 8 + 8],
            [123u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn open_if_exists_returns_none_for_a_missing_file() {
        let path = temp_path("missing");

        assert!(IndexByMinuteFile::open_if_exists(&path).await.is_none());

        IndexByMinuteFile::open_or_create(&path).await;

        assert!(IndexByMinuteFile::open_if_exists(&path).await.is_some());

        let _ = std::fs::remove_file(&path);
    }
}
