use std::{sync::Arc, time::Duration};

use chrono::Datelike;
use my_logger::LogEventCtx;
use my_service_bus::shared::sub_page::SubPageId;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    app::{storage_layout, AppContext},
    archive_storage::ArchiveFileNo,
    file_storage::delete_folder_if_exists,
    topic_key::{TopicKey, TopicKeyRef},
    typing::Year,
};

/// The cold tier can not be listed, so the years a topic spans have to be guessed. Deleting is a
/// background job with no deadline, so a generous range costs nothing: a few dozen requests that
/// mostly answer "not there", which is not an error.
const OLDEST_POSSIBLE_YEAR: u32 = 2000;

const DELETE_ATTEMPTS: usize = 3;

/// Deletes a topic within one namespace only: the same topic name in any other namespace is a
/// different topic and must stay untouched.
///
/// Returns as soon as the topic stops being served - dropping it from the in-memory list is the
/// only part that has to be immediate. Wiping the data can take a while (a topic folder is
/// gigabytes, and the cold tier is deleted key by key), so it runs as a background job at
/// whatever pace it manages. Failures are logged, not surfaced: there is no caller left to tell.
pub fn hard_delete_topic(app: &Arc<AppContext>, topic_key: TopicKeyRef<'_>) {
    app.topics_list.remove(topic_key);
    app.archive_storage_list.forget_topic(topic_key);

    tokio::spawn(delete_topic_data(app.clone(), topic_key.to_owned_key()));
}

async fn delete_topic_data(app: Arc<AppContext>, topic_key: TopicKey) {
    let topic_key_ref = topic_key.to_ref();

    let highest_archive_file_no = get_highest_archive_file_no(app.as_ref(), topic_key_ref).await;

    let folder = storage_layout::get_topic_folder(app.get_data_folder(), topic_key_ref);

    {
        // Exclusive on both: the whole folder is going, so no read of any archive or index of this
        // topic may be in flight.
        let _archives = app.archive_locks.write(topic_key_ref).await;
        let _indexes = app.index_locks.write(topic_key_ref).await;

        if let Err(err) = delete_folder_if_exists(folder.as_path()).await {
            write_error(
                topic_key_ref,
                format!("Can not delete {:?}. Err: {}", folder, err),
            );
        }
    }

    app.archive_locks.forget(topic_key_ref);
    app.index_locks.forget(topic_key_ref);

    delete_from_cold_storage(app.as_ref(), topic_key_ref, highest_archive_file_no).await;

    println!("Topic {} is deleted", topic_key_ref);
}

/// The highest archive file the topic can possibly have, derived from the message id the snapshot
/// last recorded for it.
async fn get_highest_archive_file_no(
    app: &AppContext,
    topic_key: TopicKeyRef<'_>,
) -> Option<ArchiveFileNo> {
    let snapshot = app.topics_snapshot.get().await;

    let topic = snapshot
        .snapshot
        .data
        .iter()
        .find(|itm| itm.get_topic_key() == topic_key)?;

    let sub_page_id: SubPageId = topic.get_message_id().into();

    Some(sub_page_id.into())
}

async fn delete_from_cold_storage(
    app: &AppContext,
    topic_key: TopicKeyRef<'_>,
    highest_archive_file_no: Option<ArchiveFileNo>,
) {
    if app.get_cold_storage().is_none() {
        return;
    }

    // Archive numbering follows from the topic's message id, so the range is exact.
    if let Some(highest) = highest_archive_file_no {
        for file_no in 0..=highest.get_value() {
            let file_name = storage_layout::get_archive_file_name(ArchiveFileNo::new(file_no));
            delete_key(app, topic_key, file_name.as_str()).await;
        }
    }

    let current_year = DateTimeAsMicroseconds::now().to_chrono_utc().year() as u32;

    for year in OLDEST_POSSIBLE_YEAR..=current_year + 1 {
        let file_name = storage_layout::get_year_index_file_name(Year::new(year));
        delete_key(app, topic_key, file_name.as_str()).await;
    }

    delete_key(app, topic_key, storage_layout::ACTIVE_FILE_NAME).await;
}

/// A missing key is not an error - `ColdStorage::delete` already treats a 404 as success. Only a
/// real failure is retried, and only a handful of times: an orphaned object is worse than a slow
/// delete, but not worth blocking the job forever.
async fn delete_key(app: &AppContext, topic_key: TopicKeyRef<'_>, file_name: &str) {
    let Some(cold_storage) = app.get_cold_storage() else {
        return;
    };

    for attempt_no in 1..=DELETE_ATTEMPTS {
        match cold_storage.delete(topic_key, file_name).await {
            Ok(_) => return,
            Err(err) => {
                if attempt_no == DELETE_ATTEMPTS {
                    write_error(
                        topic_key,
                        format!(
                            "Can not delete {}/{} from the cold storage after {} attempts. It stays orphaned. Err: {}",
                            topic_key, file_name, DELETE_ATTEMPTS, err
                        ),
                    );
                    return;
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn write_error(topic_key: TopicKeyRef<'_>, message: String) {
    my_logger::LOGGER.write_error(
        "hard_delete_topic",
        message,
        LogEventCtx::new().add("topic", topic_key.to_string()),
    );
}
