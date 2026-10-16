use std::path::PathBuf;

use crate::{archive_storage::ArchiveFileNo, topic_key::TopicKeyRef, typing::Year};

/// Where a topic's data lives - the single place that turns a `(namespace, topic_id)` pair into
/// a physical location.
///
/// ```text
/// {data_folder}/
///     {namespace}/
///         topics-and-queue.yaml     that namespace's topics + queues, human readable
///         {topic}/
///             {:019}.archive        sealed sub pages: TOC + compressed blocks
///             .{year}.yearindex     527 040 minutes x 8 bytes, addressed at minute*8
///             active                the open tail - the sub page still being filled
/// ```
///
/// In the cold tier the same relative path is the key, under a fixed root in the one bucket:
/// `/{bucket}/my-sb-persistence/{namespace}/{topic}/{file}`.
///
/// `default` is not special: it gets its own folder like every other namespace, so the layout has
/// no exceptions. Data written before namespaces existed sits directly at the root and is moved
/// into `default/` once, at startup - see `operations::migrate_legacy_topics`.
/// One per namespace, next to that namespace's topic folders.
pub const NAMESPACE_SNAPSHOT_FILE_NAME: &str = "topics-and-queue.yaml";
/// The pre-YAML global protobuf blob - only the migration still knows about it.
pub const LEGACY_TOPICS_SNAPSHOT_FILE_NAME: &str = "topicsdata";
pub const ACTIVE_FILE_NAME: &str = "active";
pub const ARCHIVE_FILE_EXTENSION: &str = ".archive";
pub const YEAR_INDEX_FILE_EXTENSION: &str = ".yearindex";

/// `{namespace}/{topic}` - the local sub-folder.
pub fn get_topic_relative_path(topic_key: TopicKeyRef<'_>) -> String {
    format!("{}/{}", topic_key.namespace, topic_key.topic_id)
}

/// `{namespace}/{topic}/{file_name}` - the path under the data folder, and the S3 key under the
/// `my-sb-persistence/` root.
pub fn get_relative_path(topic_key: TopicKeyRef<'_>, file_name: &str) -> String {
    format!("{}/{}", get_topic_relative_path(topic_key), file_name)
}

pub fn get_local_path(data_folder: &str, relative_path: &str) -> PathBuf {
    let mut result = PathBuf::from(data_folder);

    for part in relative_path.split('/') {
        // The namespace and the topic id are validated at the API boundary, so this can not
        // trigger - but it is what physically stops a `..` from walking out of the data folder,
        // and it is cheap enough to keep as the last line of defence.
        if part.is_empty() || part == "." || part == ".." {
            panic!(
                "Refusing to build a path from '{}': the component '{}' escapes the data folder",
                relative_path, part
            );
        }

        result.push(part);
    }

    result
}

pub fn get_topic_folder(data_folder: &str, topic_key: TopicKeyRef<'_>) -> PathBuf {
    get_local_path(data_folder, get_topic_relative_path(topic_key).as_str())
}

pub fn get_namespace_snapshot_file(data_folder: &str, namespace: &str) -> PathBuf {
    let mut result = PathBuf::from(data_folder);
    result.push(namespace);
    result.push(NAMESPACE_SNAPSHOT_FILE_NAME);
    result
}

pub fn get_legacy_topics_snapshot_file(data_folder: &str) -> PathBuf {
    let mut result = PathBuf::from(data_folder);
    result.push(LEGACY_TOPICS_SNAPSHOT_FILE_NAME);
    result
}

pub fn get_archive_file_name(archive_file_no: ArchiveFileNo) -> String {
    format!(
        "{:019}{}",
        archive_file_no.get_value(),
        ARCHIVE_FILE_EXTENSION
    )
}

/// Zero-padded on purpose: lexicographic order equals numeric order, which keeps an S3 prefix
/// listing sorted without parsing.
pub fn get_archive_relative_path(
    topic_key: TopicKeyRef<'_>,
    archive_file_no: ArchiveFileNo,
) -> String {
    get_relative_path(topic_key, get_archive_file_name(archive_file_no).as_str())
}

pub fn get_year_index_file_name(year: Year) -> String {
    format!(".{}{}", year.get_value(), YEAR_INDEX_FILE_EXTENSION)
}

pub fn get_year_index_relative_path(topic_key: TopicKeyRef<'_>, year: Year) -> String {
    get_relative_path(topic_key, get_year_index_file_name(year).as_str())
}

pub fn get_active_relative_path(topic_key: TopicKeyRef<'_>) -> String {
    get_relative_path(topic_key, ACTIVE_FILE_NAME)
}

/// `0000000000000042.archive` -> `42`. `None` for anything that is not an archive file name.
pub fn parse_archive_file_name(file_name: &str) -> Option<ArchiveFileNo> {
    let value = file_name.strip_suffix(ARCHIVE_FILE_EXTENSION)?;
    let value: i64 = value.parse().ok()?;
    Some(ArchiveFileNo::new(value))
}

/// `.2024.yearindex` -> `2024`. `None` for anything that is not a year index file name.
pub fn parse_year_index_file_name(file_name: &str) -> Option<Year> {
    let value = file_name.strip_suffix(YEAR_INDEX_FILE_EXTENSION)?;
    let value = value.strip_prefix('.')?;
    let value: u32 = value.parse().ok()?;
    Some(Year::new(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_path_is_the_local_layout() {
        let key = TopicKeyRef::new("default", "orders");

        assert_eq!("default/orders", get_topic_relative_path(key));
        assert_eq!(
            "default/orders/0000000000000000007.archive",
            get_archive_relative_path(key, ArchiveFileNo::new(7))
        );
        assert_eq!(
            "default/orders/.2024.yearindex",
            get_year_index_relative_path(key, Year::new(2024))
        );
        assert_eq!("default/orders/active", get_active_relative_path(key));
    }

    #[test]
    fn same_topic_in_two_namespaces_never_collides() {
        let default_ns = get_topic_relative_path(TopicKeyRef::new("default", "orders"));
        let alpha_ns = get_topic_relative_path(TopicKeyRef::new("alpha", "orders"));

        assert_ne!(default_ns, alpha_ns);
    }

    #[test]
    fn local_path_is_built_from_the_relative_one() {
        let key = TopicKeyRef::new("alpha", "orders");

        let path = get_local_path(
            "/var/data",
            get_archive_relative_path(key, ArchiveFileNo::new(1)).as_str(),
        );

        assert_eq!(
            "/var/data/alpha/orders/0000000000000000001.archive",
            path.to_str().unwrap()
        );
    }

    #[test]
    fn file_names_round_trip() {
        let file_name = get_archive_file_name(ArchiveFileNo::new(42));
        assert_eq!(42, parse_archive_file_name(&file_name).unwrap().get_value());

        let file_name = get_year_index_file_name(Year::new(2031));
        assert_eq!(
            2031,
            parse_year_index_file_name(&file_name).unwrap().get_value()
        );

        assert!(parse_archive_file_name("active").is_none());
        assert!(parse_archive_file_name(".2024.yearindex").is_none());
        assert!(parse_year_index_file_name("active").is_none());
        assert!(parse_year_index_file_name("0000000000000000001.archive").is_none());
    }

    /// Zero padding is what keeps an S3 listing in numeric order.
    #[test]
    fn archive_names_sort_numerically() {
        let mut names = vec![
            get_archive_file_name(ArchiveFileNo::new(10)),
            get_archive_file_name(ArchiveFileNo::new(2)),
            get_archive_file_name(ArchiveFileNo::new(1)),
        ];

        names.sort();

        assert_eq!(
            vec![
                get_archive_file_name(ArchiveFileNo::new(1)),
                get_archive_file_name(ArchiveFileNo::new(2)),
                get_archive_file_name(ArchiveFileNo::new(10)),
            ],
            names
        );
    }
}
