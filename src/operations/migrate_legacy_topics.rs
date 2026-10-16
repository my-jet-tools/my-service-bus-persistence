use std::path::{Path, PathBuf};

use crate::{
    app::storage_layout,
    file_storage::delete_file_if_exists,
    topic_key::DEFAULT_NAMESPACE,
    topics_snapshot::file_storage::{deserialize_legacy_protobuf, TopicsSnapshotStorage},
};

/// Marks the data folder as laid out by namespace. Its **absence** is the marker that matters:
/// before namespaces there was no such concept, so every folder at the root was a topic. That is
/// what makes the migration exact instead of a guess - no sniffing what a folder contains, no
/// edge case around a topic that was created but never received a message, or one whose files
/// have all been uploaded and dropped locally.
pub const LAYOUT_MARKER_FILE_NAME: &str = ".layout-version";

/// Layout 2 is `{data_folder}/{namespace}/{topic}/`. Layout 1 - unmarked - was
/// `{data_folder}/{topic}/`.
pub const CURRENT_LAYOUT_VERSION: &str = "2";

/// Written before the first move and removed after the last one.
///
/// Without it a resumed migration can not tell the two meanings of a root `default/` apart: a
/// pre-namespace topic that happened to be called `default`, or the namespace folder a previous,
/// interrupted run already created. Guessing wrong nests the entire namespace under
/// `default/default` and orphans every topic already moved.
pub const MIGRATION_IN_PROGRESS_FILE_NAME: &str = ".layout-migrating";

/// Where a pre-namespace topic named `default` waits while the namespace folder is created - a
/// folder can not be renamed into itself.
pub const PARKED_DEFAULT_FOLDER_NAME: &str = ".default-being-migrated";

/// Brings a pre-namespace data folder up to the current layout, once:
///
/// 1. every folder at the root is a topic - move it into `default/`;
/// 2. the global protobuf `topicsdata` becomes one `topics-and-queue.yaml` per namespace.
///
/// The folder moves are renames within one mount: atomic, instant, nothing is copied. The marker
/// is written last, so a crash halfway simply means the next start finishes the job.
///
/// Runs before the snapshot is read, so it takes the folder rather than the `AppContext`.
pub async fn migrate_legacy_layout(data_folder: &str) {
    let root = PathBuf::from(data_folder);
    let marker_path = root.join(LAYOUT_MARKER_FILE_NAME);

    // Keyed on the file being there rather than on the marker: `move_working_files` imports
    // `topicsdata` whenever a legacy section is configured, which can happen long after the
    // marker was written - start once without the section, add it, restart. Gating this on the
    // marker would strand the imported snapshot in a file nothing ever opens again.
    convert_legacy_snapshot(data_folder).await;

    if marker_path.exists() {
        return;
    }

    let in_progress_path = root.join(MIGRATION_IN_PROGRESS_FILE_NAME);
    let parked_path = root.join(PARKED_DEFAULT_FOLDER_NAME);

    if !in_progress_path.exists() {
        // Park before the marker, never after: a crash in between leaves no root `default/`, so
        // the next run simply parks nothing and carries on.
        let legacy_default = root.join(DEFAULT_NAMESPACE);

        if legacy_default.is_dir() {
            rename(legacy_default.as_path(), parked_path.as_path()).await;
        }

        if root.exists() {
            tokio::fs::write(in_progress_path.as_path(), CURRENT_LAYOUT_VERSION)
                .await
                .unwrap_or_else(|err| panic!("Can not write the migration marker: {}", err));
        }
    }

    // From here on a root `default/` can only be the namespace folder - we created it.
    let mut topics = get_folders_at_root(data_folder).await;
    topics.retain(|itm| itm != DEFAULT_NAMESPACE && itm != PARKED_DEFAULT_FOLDER_NAME);

    if !topics.is_empty() || parked_path.is_dir() {
        move_into_default_namespace(data_folder, topics).await;
    }

    tokio::fs::create_dir_all(data_folder)
        .await
        .unwrap_or_else(|err| panic!("Can not create the data folder {}: {}", data_folder, err));

    tokio::fs::write(marker_path.as_path(), CURRENT_LAYOUT_VERSION)
        .await
        .unwrap_or_else(|err| panic!("Can not write the layout marker: {}", err));

    let _ = tokio::fs::remove_file(in_progress_path.as_path()).await;
}

/// The legacy `topicsdata` holds every namespace at once; it becomes one YAML file per namespace.
/// Topics written before namespaces existed carry no namespace field and land in `default`.
///
/// **Merges** rather than overwrites: this runs whenever the file is present, which can be long
/// after the service has been writing its own snapshot, and the live one is the newer of the two.
/// A legacy topic is taken only when the current snapshot does not already know it.
async fn convert_legacy_snapshot(data_folder: &str) {
    let path = storage_layout::get_legacy_topics_snapshot_file(data_folder);

    let content = match tokio::fs::read(path.as_path()).await {
        Ok(content) => content,
        Err(err) => {
            if let std::io::ErrorKind::NotFound = err.kind() {
                return;
            }
            panic!(
                "Can not read the legacy topics snapshot {:?}: {}",
                path, err
            );
        }
    };

    let legacy = deserialize_legacy_protobuf(content.as_slice());

    let storage = TopicsSnapshotStorage::new(data_folder.to_string());
    let current = storage.read().await;

    let mut topics = current.topics;
    let mut deleted_topics = current.deleted_topics;

    let mut taken = 0usize;

    for legacy_topic in legacy.topics {
        let already_known = topics
            .iter()
            .any(|itm| itm.get_topic_key() == legacy_topic.get_topic_key());

        if already_known {
            continue;
        }

        topics.push(legacy_topic);
        taken += 1;
    }

    for legacy_deleted in legacy.deleted_topics {
        let already_known = deleted_topics
            .iter()
            .any(|itm| itm.get_topic_key() == legacy_deleted.get_topic_key());

        if !already_known {
            deleted_topics.push(legacy_deleted);
        }
    }

    println!(
        "Converting the legacy topics snapshot: {} topics taken into per-namespace YAML",
        taken
    );

    storage
        .write(topics.as_slice(), deleted_topics.as_slice())
        .await
        .unwrap_or_else(|err| panic!("Can not write the converted topics snapshot: {}", err));

    delete_file_if_exists(path.as_path())
        .await
        .unwrap_or_else(|err| panic!("Can not delete the legacy topics snapshot: {}", err));
}

/// Every folder - files at the root (`topicsdata`, a legacy `.active-pages`) stay where they are.
async fn get_folders_at_root(data_folder: &str) -> Vec<String> {
    let mut result = Vec::new();

    let mut entries = match tokio::fs::read_dir(data_folder).await {
        Ok(entries) => entries,
        Err(err) => {
            if let std::io::ErrorKind::NotFound = err.kind() {
                return result;
            }
            panic!("Can not read the data folder {}: {}", data_folder, err);
        }
    };

    while let Some(entry) = entries
        .next_entry()
        .await
        .unwrap_or_else(|err| panic!("Can not read the data folder {}: {}", data_folder, err))
    {
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

async fn move_into_default_namespace(data_folder: &str, topics: Vec<String>) {
    let root = PathBuf::from(data_folder);
    let default_folder = root.join(DEFAULT_NAMESPACE);

    tokio::fs::create_dir_all(default_folder.as_path())
        .await
        .unwrap_or_else(|err| panic!("Can not create the default namespace folder: {}", err));

    for topic_id in topics {
        move_one(
            root.join(topic_id.as_str()),
            default_folder.join(topic_id.as_str()),
        )
        .await;
    }

    // The topic that was called `default` goes in last, under its own name.
    let parked = root.join(PARKED_DEFAULT_FOLDER_NAME);

    if parked.is_dir() {
        move_one(parked, default_folder.join(DEFAULT_NAMESPACE)).await;
    }
}

async fn move_one(from: PathBuf, to: PathBuf) {
    if !from.is_dir() {
        // Already moved by an interrupted run
        return;
    }

    if to.exists() {
        panic!(
            "Can not migrate {:?} into {:?}: the destination already exists",
            from, to
        );
    }

    println!("Migrating legacy topic {:?} into {:?}", from, to);

    rename(from.as_path(), to.as_path()).await;
}

async fn rename(from: &Path, to: &Path) {
    tokio::fs::rename(from, to)
        .await
        .unwrap_or_else(|err| panic!("Can not move {:?} to {:?}: {}", from, to, err));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_folder(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-migration-{}", name));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn as_str(path: &Path) -> &str {
        path.to_str().unwrap()
    }

    fn topic_with_data(root: &Path, name: &str) {
        std::fs::create_dir_all(root.join(name)).unwrap();
        std::fs::write(root.join(name).join("active"), name.as_bytes()).unwrap();
    }

    #[tokio::test]
    async fn every_folder_at_the_root_is_a_topic() {
        let root = temp_folder("every_folder");

        topic_with_data(&root, "orders");
        // Created but never wrote a message - the case a "does it hold files" heuristic misses
        std::fs::create_dir_all(root.join("empty-topic")).unwrap();
        // Files at the root are not topics
        std::fs::write(root.join("some-file"), [0u8; 4]).unwrap();

        migrate_legacy_layout(as_str(&root)).await;

        assert!(root.join("default").join("orders").join("active").is_file());
        assert!(root.join("default").join("empty-topic").is_dir());
        assert!(root.join("some-file").is_file());
        assert!(!root.join("orders").exists());
        assert!(root.join(LAYOUT_MARKER_FILE_NAME).is_file());
        assert!(!root.join(MIGRATION_IN_PROGRESS_FILE_NAME).exists());

        let _ = std::fs::remove_dir_all(&root);
    }

    /// A pre-namespace topic could be named `default`; it belongs at `default/default`.
    #[tokio::test]
    async fn a_topic_named_default_is_migrated_too() {
        let root = temp_folder("topic_named_default");

        topic_with_data(&root, "default");
        topic_with_data(&root, "orders");

        migrate_legacy_layout(as_str(&root)).await;

        assert_eq!(
            b"default".to_vec(),
            std::fs::read(root.join("default").join("default").join("active")).unwrap()
        );
        assert!(root.join("default").join("orders").join("active").is_file());
        assert!(!root.join(PARKED_DEFAULT_FOLDER_NAME).exists());

        let _ = std::fs::remove_dir_all(&root);
    }

    /// Running it again must be a no-op, not a second round of moves.
    #[tokio::test]
    async fn running_twice_changes_nothing() {
        let root = temp_folder("twice");

        topic_with_data(&root, "orders");

        migrate_legacy_layout(as_str(&root)).await;
        migrate_legacy_layout(as_str(&root)).await;

        assert!(root.join("default").join("orders").join("active").is_file());
        assert!(!root.join("default").join("default").exists());

        let _ = std::fs::remove_dir_all(&root);
    }

    /// The case the audit caught: a run that died after moving some topics but before writing the
    /// marker must NOT mistake the namespace folder it created for a topic named `default` and
    /// nest the whole thing under `default/default`.
    #[tokio::test]
    async fn a_crashed_run_is_resumed_not_restarted() {
        let root = temp_folder("resumed");

        // What an interrupted run leaves behind: default/ created, one topic moved, one still at
        // the root, the in-progress marker present and the final marker absent.
        std::fs::create_dir_all(root.join("default").join("moved")).unwrap();
        std::fs::write(root.join("default").join("moved").join("active"), b"moved").unwrap();
        topic_with_data(&root, "not-yet-moved");
        std::fs::write(root.join(MIGRATION_IN_PROGRESS_FILE_NAME), b"2").unwrap();

        migrate_legacy_layout(as_str(&root)).await;

        // The already-migrated topic stayed where it was
        assert_eq!(
            b"moved".to_vec(),
            std::fs::read(root.join("default").join("moved").join("active")).unwrap()
        );
        // and the rest caught up
        assert!(root
            .join("default")
            .join("not-yet-moved")
            .join("active")
            .is_file());
        // and nothing got nested
        assert!(!root.join("default").join("default").exists());
        assert!(root.join(LAYOUT_MARKER_FILE_NAME).is_file());

        let _ = std::fs::remove_dir_all(&root);
    }

    /// Dying between parking `default` and writing the in-progress marker is also resumable.
    #[tokio::test]
    async fn a_run_that_died_right_after_parking_is_resumed() {
        let root = temp_folder("parked");

        std::fs::create_dir_all(root.join(PARKED_DEFAULT_FOLDER_NAME)).unwrap();
        std::fs::write(
            root.join(PARKED_DEFAULT_FOLDER_NAME).join("active"),
            b"was-default",
        )
        .unwrap();
        topic_with_data(&root, "orders");

        migrate_legacy_layout(as_str(&root)).await;

        assert_eq!(
            b"was-default".to_vec(),
            std::fs::read(root.join("default").join("default").join("active")).unwrap()
        );
        assert!(root.join("default").join("orders").join("active").is_file());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_fresh_empty_folder_is_just_marked() {
        let root = temp_folder("fresh");

        migrate_legacy_layout(as_str(&root)).await;

        assert!(root.join(LAYOUT_MARKER_FILE_NAME).is_file());

        let _ = std::fs::remove_dir_all(&root);
    }
}
