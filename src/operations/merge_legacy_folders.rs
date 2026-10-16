use std::path::{Path, PathBuf};

use crate::{
    app::storage_layout,
    cold_storage::ColdStorage,
    operations::current_sub_pages_io::LEGACY_ACTIVE_PAGES_FILE_NAME,
    settings::LegacyFoldersSettingsModel,
    topic_key::{TopicKeyRef, DEFAULT_NAMESPACE},
};

/// What the pre-single-root layout looked like, derived from what the code addressed:
///
/// ```text
/// {topics}/topics/topicsdata            the snapshot
/// {topics}/topics/.active-pages         the open tail of every topic
/// {topics}/{topic}/                     empty container folders, nothing was ever written there
/// {messages}/{topic}/.{year}.yearindex
/// {archive}/{topic}/{:019}.archive
/// ```
///
/// All of it predates namespaces, so all of it belongs to `default`. Both phases write straight to
/// `{data}/default/{topic}/` - there is no intermediate shape for the namespace step to lift.
///
/// Files are **moved**: written to their new home, then removed from the legacy folder. What is
/// still in the legacy folder is therefore exactly what has not been migrated yet, which is also
/// what makes an interrupted run resumable without any bookkeeping.
pub struct LegacyMigration {
    pub data_folder: String,
    pub legacy: LegacyFoldersSettingsModel,
}

impl LegacyMigration {
    pub fn new(data_folder: String, legacy: LegacyFoldersSettingsModel) -> Self {
        Self {
            data_folder,
            legacy,
        }
    }

    /// Everything the service needs to start serving, and nothing more: the snapshot, the shared
    /// tail, and per topic the one archive and the one year index that are still being written.
    ///
    /// That is the same "the highest number is the live one" rule the cold-storage uploader uses,
    /// so a topic is writable immediately while its history is still on its way over.
    pub async fn move_working_files(&self) {
        println!("Migrating the working set out of the legacy folders");

        // First of all, and before `default/` is created: from here on a root `default/` is the
        // namespace folder we made, not a pre-namespace topic that happened to be called that.
        // Without this the namespace step would park it and nest everything under
        // `default/default` - see `migrate_legacy_layout`.
        mark_migration_in_progress(self.data_folder.as_str()).await;

        for file_name in [
            storage_layout::LEGACY_TOPICS_SNAPSHOT_FILE_NAME,
            LEGACY_ACTIVE_PAGES_FILE_NAME,
        ] {
            let from = PathBuf::from(self.legacy.topics.as_str())
                .join("topics")
                .join(file_name);
            let to = PathBuf::from(self.data_folder.as_str()).join(file_name);

            move_file(from.as_path(), to.as_path()).await;
        }

        for topic_id in self.get_legacy_topics().await {
            // A topic with no files at all still needs its folder, or it vanishes from the layout.
            let destination = self.get_destination_folder(topic_id.as_str());
            let _ = tokio::fs::create_dir_all(destination.as_path()).await;

            for (folder, parse) in self.data_folders() {
                if let Some(live) =
                    get_highest_numbered_file(folder.as_str(), topic_id.as_str(), parse).await
                {
                    self.move_one(folder.as_str(), topic_id.as_str(), live.as_str())
                        .await;
                }
            }
        }
    }

    /// Everything else, one file at a time so the live traffic keeps the disk to itself.
    ///
    /// With a cold tier configured a sealed file goes there straight from the legacy folder - it
    /// never lands on the new local disk at all, which is the whole point of doing it this way
    /// round.
    pub async fn move_the_rest(&self, cold_storage: Option<&ColdStorage>) {
        println!("Migrating the remaining legacy files");

        let mut moved = 0usize;

        for topic_id in self.get_legacy_topics().await {
            for (folder, _) in self.data_folders() {
                for file_name in get_files(folder.as_str(), topic_id.as_str()).await {
                    match cold_storage {
                        Some(cold_storage) => {
                            self.upload_one(
                                cold_storage,
                                folder.as_str(),
                                topic_id.as_str(),
                                file_name.as_str(),
                            )
                            .await
                        }
                        None => {
                            self.move_one(folder.as_str(), topic_id.as_str(), file_name.as_str())
                                .await
                        }
                    }

                    moved += 1;
                }
            }
        }

        println!("Migrated {} remaining legacy files", moved);
    }

    /// `(folder, how to read a file name as a number)` - the two kinds of numbered file a topic
    /// has, each with its own "the highest one is live".
    fn data_folders(&self) -> [(String, fn(&str) -> Option<i64>); 2] {
        [
            (self.legacy.archive.clone(), parse_archive_no),
            (self.legacy.messages.clone(), parse_year),
        ]
    }

    fn get_destination_folder(&self, topic_id: &str) -> PathBuf {
        storage_layout::get_topic_folder(
            self.data_folder.as_str(),
            TopicKeyRef::new(DEFAULT_NAMESPACE, topic_id),
        )
    }

    async fn move_one(&self, legacy_folder: &str, topic_id: &str, file_name: &str) {
        let from = PathBuf::from(legacy_folder).join(topic_id).join(file_name);
        let to = self.get_destination_folder(topic_id).join(file_name);

        move_file(from.as_path(), to.as_path()).await;
    }

    async fn upload_one(
        &self,
        cold_storage: &ColdStorage,
        legacy_folder: &str,
        topic_id: &str,
        file_name: &str,
    ) {
        let from = PathBuf::from(legacy_folder).join(topic_id).join(file_name);

        // Everything legacy belongs to `default`.
        let topic_key = TopicKeyRef::new(DEFAULT_NAMESPACE, topic_id);

        if !from.is_file() {
            return;
        }

        println!("Uploading {:?} to the cold storage", from);

        if let Err(err) = cold_storage
            .upload_file(topic_key, file_name, from.as_path())
            .await
        {
            panic!("Can not upload {:?} to the cold storage: {}", from, err);
        }

        // Only once the upload is confirmed - a crash in between costs a repeated upload, which is
        // idempotent, rather than the file.
        tokio::fs::remove_file(from.as_path())
            .await
            .unwrap_or_else(|err| panic!("Can not remove {:?}: {}", from, err));

        println!("Moved {:?} -> cold storage", from);
    }

    /// Every topic that exists in any of the three legacy folders.
    async fn get_legacy_topics(&self) -> Vec<String> {
        let mut result: Vec<String> = Vec::new();

        for folder in [
            self.legacy.archive.as_str(),
            self.legacy.messages.as_str(),
            self.legacy.topics.as_str(),
        ] {
            for topic_id in get_folders(folder).await {
                // `topics` is not a topic - it is where the two global files lived.
                if topic_id == "topics" || result.contains(&topic_id) {
                    continue;
                }

                result.push(topic_id);
            }
        }

        result
    }
}

async fn mark_migration_in_progress(data_folder: &str) {
    let path = PathBuf::from(data_folder).join(super::MIGRATION_IN_PROGRESS_FILE_NAME);

    if path.exists() {
        return;
    }

    tokio::fs::create_dir_all(data_folder)
        .await
        .unwrap_or_else(|err| panic!("Can not create the data folder {}: {}", data_folder, err));

    tokio::fs::write(path.as_path(), super::CURRENT_LAYOUT_VERSION)
        .await
        .unwrap_or_else(|err| panic!("Can not write the migration marker: {}", err));
}

async fn move_file(from: &Path, to: &Path) {
    if !from.is_file() {
        return;
    }

    if let Some(folder) = to.parent() {
        tokio::fs::create_dir_all(folder)
            .await
            .unwrap_or_else(|err| panic!("Can not create {:?}: {}", folder, err));
    }

    if to.exists() {
        // Moved by an interrupted run - drop the leftover source rather than overwrite the copy
        // that is already in use.
        let _ = tokio::fs::remove_file(from).await;
        return;
    }

    // Copy next to the destination and rename: a crash mid-copy can then never leave a truncated
    // archive under the real name. Copy rather than rename, because the legacy folders may sit on
    // a different mount.
    let mut partial = to.to_path_buf();
    partial.set_extension("migrating");

    tokio::fs::copy(from, partial.as_path())
        .await
        .unwrap_or_else(|err| panic!("Can not copy {:?} to {:?}: {}", from, partial, err));

    tokio::fs::rename(partial.as_path(), to)
        .await
        .unwrap_or_else(|err| panic!("Can not rename {:?} to {:?}: {}", partial, to, err));

    tokio::fs::remove_file(from)
        .await
        .unwrap_or_else(|err| panic!("Can not remove {:?}: {}", from, err));

    println!("Moved {:?} -> {:?}", from, to);
}

async fn get_folders(root: &str) -> Vec<String> {
    let mut result = Vec::new();

    let Ok(mut entries) = tokio::fs::read_dir(root).await else {
        return result;
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        if !path.is_dir() {
            continue;
        }

        if let Some(name) = path.file_name().and_then(|itm| itm.to_str()) {
            result.push(name.to_string());
        }
    }

    result
}

async fn get_files(root: &str, topic_id: &str) -> Vec<String> {
    let mut result = Vec::new();

    let Ok(mut entries) = tokio::fs::read_dir(PathBuf::from(root).join(topic_id)).await else {
        return result;
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        if let Some(name) = path.file_name().and_then(|itm| itm.to_str()) {
            result.push(name.to_string());
        }
    }

    result
}

async fn get_highest_numbered_file(
    root: &str,
    topic_id: &str,
    parse: fn(&str) -> Option<i64>,
) -> Option<String> {
    let mut highest: Option<(i64, String)> = None;

    for file_name in get_files(root, topic_id).await {
        let Some(value) = parse(file_name.as_str()) else {
            continue;
        };

        let is_higher = match &highest {
            Some((current, _)) => value > *current,
            None => true,
        };

        if is_higher {
            highest = Some((value, file_name));
        }
    }

    highest.map(|(_, file_name)| file_name)
}

fn parse_archive_no(file_name: &str) -> Option<i64> {
    storage_layout::parse_archive_file_name(file_name).map(|itm| itm.get_value())
}

fn parse_year(file_name: &str) -> Option<i64> {
    storage_layout::parse_year_index_file_name(file_name).map(|itm| itm.get_value() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-legacy-{}", name));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn write(path: PathBuf, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn seed(root: &Path) -> LegacyMigration {
        let topics = root.join("Topics");
        let messages = root.join("Messages");
        let archive = root.join("Archive");

        write(topics.join("topics").join("topicsdata"), "snapshot");
        write(topics.join("topics").join(".active-pages"), "tails");
        std::fs::create_dir_all(topics.join("orders")).unwrap();
        std::fs::create_dir_all(topics.join("never-used")).unwrap();

        write(messages.join("orders").join(".2024.yearindex"), "idx-2024");
        write(messages.join("orders").join(".2025.yearindex"), "idx-2025");

        write(
            archive.join("orders").join("0000000000000000000.archive"),
            "a0",
        );
        write(
            archive.join("orders").join("0000000000000000001.archive"),
            "a1",
        );
        write(
            archive.join("orders").join("0000000000000000002.archive"),
            "a2",
        );

        LegacyMigration::new(
            root.join("sb-data").to_str().unwrap().to_string(),
            LegacyFoldersSettingsModel {
                topics: topics.to_str().unwrap().to_string(),
                messages: messages.to_str().unwrap().to_string(),
                archive: archive.to_str().unwrap().to_string(),
            },
        )
    }

    #[tokio::test]
    async fn the_working_set_is_only_the_live_files() {
        let root = temp_root("working_set");
        let migration = seed(&root);
        let data = PathBuf::from(migration.data_folder.as_str());

        migration.move_working_files().await;

        let topic = data.join("default").join("orders");

        // The archive being written and the current year index came over
        assert_eq!(
            "a2",
            std::fs::read_to_string(topic.join("0000000000000000002.archive")).unwrap()
        );
        assert_eq!(
            "idx-2025",
            std::fs::read_to_string(topic.join(".2025.yearindex")).unwrap()
        );
        // and the sealed ones stayed behind for the background phase
        assert!(!topic.join("0000000000000000000.archive").exists());
        assert!(!topic.join(".2024.yearindex").exists());

        // The two global files are needed to start
        assert_eq!(
            "snapshot",
            std::fs::read_to_string(data.join("topicsdata")).unwrap()
        );
        assert_eq!(
            "tails",
            std::fs::read_to_string(data.join(".active-pages")).unwrap()
        );

        // A topic that only ever had an empty container still exists
        assert!(data.join("default").join("never-used").is_dir());

        // The marker that stops the namespace step from mistaking `default/` for a topic
        assert!(data
            .join(super::super::MIGRATION_IN_PROGRESS_FILE_NAME)
            .is_file());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn the_background_phase_brings_the_rest_and_empties_legacy() {
        let root = temp_root("the_rest");
        let migration = seed(&root);
        let data = PathBuf::from(migration.data_folder.as_str());

        migration.move_working_files().await;
        migration.move_the_rest(None).await;

        let topic = data.join("default").join("orders");

        assert_eq!(
            "a0",
            std::fs::read_to_string(topic.join("0000000000000000000.archive")).unwrap()
        );
        assert_eq!(
            "a1",
            std::fs::read_to_string(topic.join("0000000000000000001.archive")).unwrap()
        );
        assert_eq!(
            "idx-2024",
            std::fs::read_to_string(topic.join(".2024.yearindex")).unwrap()
        );

        // Nothing is left behind - what stays in legacy is what is not migrated yet
        assert!(get_files(migration.legacy.archive.as_str(), "orders")
            .await
            .is_empty());
        assert!(get_files(migration.legacy.messages.as_str(), "orders")
            .await
            .is_empty());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn running_the_phases_twice_is_harmless() {
        let root = temp_root("twice");
        let migration = seed(&root);
        let data = PathBuf::from(migration.data_folder.as_str());

        migration.move_working_files().await;
        migration.move_working_files().await;
        migration.move_the_rest(None).await;
        migration.move_the_rest(None).await;

        let topic = data.join("default").join("orders");
        assert_eq!(
            "a2",
            std::fs::read_to_string(topic.join("0000000000000000002.archive")).unwrap()
        );
        assert_eq!(
            "a0",
            std::fs::read_to_string(topic.join("0000000000000000000.archive")).unwrap()
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn no_partial_files_are_left_behind() {
        let root = temp_root("partials");
        let migration = seed(&root);
        let data = PathBuf::from(migration.data_folder.as_str());

        migration.move_working_files().await;
        migration.move_the_rest(None).await;

        let leftovers: Vec<_> = std::fs::read_dir(data.join("default").join("orders"))
            .unwrap()
            .filter_map(|itm| itm.ok())
            .filter(|itm| itm.file_name().to_string_lossy().ends_with(".migrating"))
            .collect();

        assert!(leftovers.is_empty());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn missing_legacy_folders_are_not_an_error() {
        let root = temp_root("missing");

        let migration = LegacyMigration::new(
            root.join("sb-data").to_str().unwrap().to_string(),
            LegacyFoldersSettingsModel {
                topics: root.join("nope-1").to_str().unwrap().to_string(),
                messages: root.join("nope-2").to_str().unwrap().to_string(),
                archive: root.join("nope-3").to_str().unwrap().to_string(),
            },
        );

        migration.move_working_files().await;
        migration.move_the_rest(None).await;

        let _ = std::fs::remove_dir_all(&root);
    }
}
