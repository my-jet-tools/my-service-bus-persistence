use my_logger::LogEventCtx;
use my_service_bus::shared::sub_page::SubPageId;
use rust_extensions::{date_time::DateTimeAsMicroseconds, StopWatch};

use crate::{
    app::AppContext,
    message_pages::{SubPage, SubPageInner},
    topic_data::TopicData,
};

#[derive(Debug)]
#[allow(dead_code)]
pub enum RestoreSubPageError {
    NotFound,
    ArchiveStorageError(crate::archive_storage::ArchiveStorageError),
}

impl From<crate::archive_storage::ArchiveStorageError> for RestoreSubPageError {
    fn from(err: crate::archive_storage::ArchiveStorageError) -> Self {
        Self::ArchiveStorageError(err)
    }
}

pub async fn restore_sub_page(
    app: &AppContext,
    topic_data: &TopicData,
    sub_page_id: SubPageId,
) -> Result<SubPage, RestoreSubPageError> {
    // Held across the open AND the read: the background archiver takes it exclusively before it
    // deletes a local file, so nothing can vanish between the two.
    let _guard = app.archive_locks.read(topic_data.get_topic_key()).await;

    // The app-wide list, the same one `save_sub_page` writes through: a second cache would hand
    // out a second `FileStorage` for the same file, and two independent `seek(End) + write` pairs
    // can interleave into one offset.
    let page_blob_storage = app
        .archive_storage_list
        .try_get_or_open(sub_page_id.into(), topic_data.get_topic_key(), app)
        .await;

    if page_blob_storage.is_none() {
        return Err(RestoreSubPageError::NotFound);
    }

    let compressed_payload = page_blob_storage
        .unwrap()
        .read_sub_page_payload(sub_page_id)
        .await?;

    if compressed_payload.is_none() {
        return Err(RestoreSubPageError::NotFound);
    }

    let compressed_payload = compressed_payload.unwrap();

    // A payload that can not be decompressed carries no data: the page is missing, not broken.
    let result =
        match SubPageInner::from_compressed_payload(sub_page_id, compressed_payload.as_slice()) {
            Ok(result) => result,
            Err(err) => {
                my_logger::LOGGER.write_warning(
                    "restore_sub_page",
                    format!("Can not decompress the sub page. Err: {:?}", err),
                    LogEventCtx::new()
                        .add("topicId", topic_data.get_topic_key().to_string())
                        .add("subPageId", sub_page_id.get_value().to_string()),
                );
                return Err(RestoreSubPageError::NotFound);
            }
        };

    Ok(SubPage::restore_from_archive(result))
}

pub async fn save_sub_page(app: &AppContext, topic_data: &TopicData, sub_page: &SubPage) {
    let sub_page_id = sub_page.get_id();
    if let Some(zip_payload) = sub_page.to_compressed_payload().await {
        let _guard = app.archive_locks.read(topic_data.get_topic_key()).await;

        let storage = app
            .archive_storage_list
            .get_or_create(sub_page_id.into(), topic_data.get_topic_key(), app)
            .await;

        let sw = StopWatch::new();

        if let Err(err) = storage
            .write_payload(sub_page_id, zip_payload.as_slice())
            .await
        {
            panic!(
                "Can not archive sub page {} of topic {}: {:?}",
                sub_page_id.get_value(),
                topic_data.get_topic_key(),
                err
            );
        }

        topic_data.metrics.update_last_saved_duration(sw.duration());

        topic_data
            .metrics
            .update_last_saved_moment(DateTimeAsMicroseconds::now());
    }
}
