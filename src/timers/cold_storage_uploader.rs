use std::{path::PathBuf, sync::Arc};

use my_logger::LogEventCtx;
use rust_extensions::{MyTimerTick, RepeatTimerIteration};

use crate::{
    app::{storage_layout, AppContext, StorageLocks},
    archive_storage::ArchiveFileNo,
    file_storage::delete_file_if_exists,
    topic_key::{TopicKey, TopicKeyRef},
};

/// Moves sealed files to the cold tier.
///
/// Per topic, per tick: whichever archive carries the highest number is the one being written, so
/// it stays; **every** other archive is sealed and goes up - not just the one below the current,
/// since a backlog of two or three is normal after the cold tier was unreachable, after a restart
/// before the upload ran, or on a topic busy enough to roll through several files quickly. Year
/// indexes follow the same rule: the highest year is live, every earlier one is sealed.
///
/// Nothing is persisted and nothing needs to be. "The highest number on disk is the current one"
/// stays true by itself: a rollover turns the previous current into a sealed file that the next
/// tick picks up, and after a restart the same listing yields the same answer.
pub struct ColdStorageUploaderTimer {
    app: Arc<AppContext>,
}

impl ColdStorageUploaderTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for ColdStorageUploaderTimer {
    async fn tick(&self) -> RepeatTimerIteration {
        if self.app.get_cold_storage().is_none() {
            return RepeatTimerIteration::WithInterval;
        }

        for topic_folder in get_topic_folders(self.app.get_data_folder()).await {
            upload_sealed_files(self.app.as_ref(), &topic_folder).await;
        }

        RepeatTimerIteration::WithInterval
    }
}

struct TopicFolder {
    topic_key: TopicKey,
    path: PathBuf,
}

impl TopicFolder {
    fn get_topic_key(&self) -> TopicKeyRef<'_> {
        self.topic_key.to_ref()
    }
}

async fn get_topic_folders(data_folder: &str) -> Vec<TopicFolder> {
    crate::operations::scan_topic_folders(data_folder)
        .await
        .into_iter()
        .map(|topic_key| {
            let path = storage_layout::get_topic_folder(data_folder, topic_key.to_ref());
            TopicFolder { topic_key, path }
        })
        .collect()
}

async fn upload_sealed_files(app: &AppContext, topic_folder: &TopicFolder) {
    let mut archives: Vec<(i64, String)> = Vec::new();
    let mut year_indexes: Vec<(u32, String)> = Vec::new();

    let Ok(mut entries) = tokio::fs::read_dir(topic_folder.path.as_path()).await else {
        return;
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|itm| itm.to_str()) else {
            continue;
        };

        if let Some(archive_file_no) = storage_layout::parse_archive_file_name(file_name) {
            archives.push((archive_file_no.get_value(), file_name.to_string()));
            continue;
        }

        if let Some(year) = storage_layout::parse_year_index_file_name(file_name) {
            year_indexes.push((year.get_value(), file_name.to_string()));
        }
    }

    for (archive_file_no, file_name) in take_all_but_the_highest(archives) {
        upload_and_drop(
            app,
            topic_folder,
            file_name.as_str(),
            &app.archive_locks,
            Some(ArchiveFileNo::new(archive_file_no)),
        )
        .await;
    }

    for (_, file_name) in take_all_but_the_highest(year_indexes) {
        upload_and_drop(
            app,
            topic_folder,
            file_name.as_str(),
            &app.index_locks,
            None,
        )
        .await;
    }
}

/// The highest-numbered file is the live one; everything below it is sealed.
fn take_all_but_the_highest<TOrder: Ord>(
    mut items: Vec<(TOrder, String)>,
) -> Vec<(TOrder, String)> {
    if items.len() < 2 {
        return Vec::new();
    }

    items.sort_by(|left, right| left.0.cmp(&right.0));
    items.pop();

    items
}

/// The two-lock pattern, and the reason this is not just "upload then delete":
///
/// 1. the **read** lock is held while the file is read and uploaded. That is the slow part, and
///    readers pass through it freely - the file is still on disk and unchanged;
/// 2. the lock is released and the **write** lock taken to delete the local copy and drop the
///    cached handle. It waits out every reader, so nothing is left holding a handle to a file
///    that is about to disappear.
///
/// Upload strictly before delete, so a crash in between costs a repeated upload - which is
/// idempotent - rather than the file.
async fn upload_and_drop(
    app: &AppContext,
    topic_folder: &TopicFolder,
    file_name: &str,
    locks: &StorageLocks,
    archive_file_no: Option<ArchiveFileNo>,
) {
    let Some(cold_storage) = app.get_cold_storage() else {
        return;
    };

    let topic_key = topic_folder.get_topic_key();
    // The namespace is the bucket, so the key is what is left of the path.

    let mut path = topic_folder.path.clone();
    path.push(file_name);

    // Phase 1 - shared: upload while everyone else keeps reading. Streamed from the file, so
    // peak memory is a chunk rather than the whole archive.
    {
        let _guard = locks.read(topic_key).await;

        if !path.is_file() {
            return;
        }

        if let Err(err) = cold_storage
            .upload_file(topic_key, file_name, path.as_path())
            .await
        {
            write_error(
                format!("{}/{}", topic_key, file_name).as_str(),
                format!("Can not upload. Err: {}", err),
            );
            return;
        }
    }

    // Phase 2 - exclusive: nothing is mid-read, so the local copy can go.
    let _guard = locks.write(topic_key).await;

    if let Err(err) = delete_file_if_exists(&path).await {
        write_error(
            format!("{}/{}", topic_key, file_name).as_str(),
            format!("Uploaded, but can not delete the local copy. Err: {}", err),
        );
        return;
    }

    // The cached handle points at a file that is gone - the next read reopens it against the cold
    // tier. Only this one: the handle of the archive still being written must survive.
    if let Some(archive_file_no) = archive_file_no {
        app.archive_storage_list
            .forget_archive(topic_key, archive_file_no);
    }

    println!("Moved {}/{} to the cold storage", topic_key, file_name);
}

fn write_error(key: &str, message: String) {
    my_logger::LOGGER.write_error(
        "ColdStorageUploader",
        message,
        LogEventCtx::new().add("key", key.to_string()),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_highest_one_stays() {
        let items = vec![
            (2i64, "2".to_string()),
            (10, "10".to_string()),
            (1, "1".to_string()),
        ];

        assert_eq!(
            vec![(1i64, "1".to_string()), (2, "2".to_string())],
            take_all_but_the_highest(items)
        );
    }

    /// A backlog of several sealed files all goes, not only the one below the current.
    #[test]
    fn a_backlog_goes_in_full() {
        let items = vec![
            (1i64, "1".to_string()),
            (2, "2".to_string()),
            (3, "3".to_string()),
            (4, "4".to_string()),
        ];

        assert_eq!(
            vec![
                (1i64, "1".to_string()),
                (2, "2".to_string()),
                (3, "3".to_string())
            ],
            take_all_but_the_highest(items)
        );
    }

    #[test]
    fn a_lone_file_is_the_current_one_and_stays() {
        let items = vec![(7i64, "7".to_string())];
        assert!(take_all_but_the_highest(items).is_empty());

        let items: Vec<(i64, String)> = Vec::new();
        assert!(take_all_but_the_highest(items).is_empty());
    }
}
