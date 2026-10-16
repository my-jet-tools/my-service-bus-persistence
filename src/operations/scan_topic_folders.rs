use crate::topic_key::TopicKey;

/// Every topic that has a folder on disk, found by walking `{data_folder}/{namespace}/{topic}/`.
///
/// Deliberately not driven off the topics snapshot: the snapshot is what the bus node pushes over
/// `SaveQueueSnapshot`, so a topic that has received messages but has not made it into a saved
/// snapshot yet is missing from it. Anything that has to find *all* of a topic's files - restoring
/// the open tail, sweeping sealed files to the cold tier - has to look at the disk instead, or it
/// silently skips those topics.
pub async fn scan_topic_folders(data_folder: &str) -> Vec<TopicKey> {
    let mut result = Vec::new();

    let Ok(mut namespaces) = tokio::fs::read_dir(data_folder).await else {
        return result;
    };

    while let Ok(Some(namespace_entry)) = namespaces.next_entry().await {
        let namespace_path = namespace_entry.path();

        if !namespace_path.is_dir() {
            continue;
        }

        let Some(namespace) = namespace_path.file_name().and_then(|itm| itm.to_str()) else {
            continue;
        };

        let Ok(mut topics) = tokio::fs::read_dir(namespace_path.as_path()).await else {
            continue;
        };

        while let Ok(Some(topic_entry)) = topics.next_entry().await {
            let topic_path = topic_entry.path();

            if !topic_path.is_dir() {
                continue;
            }

            let Some(topic_id) = topic_path.file_name().and_then(|itm| itm.to_str()) else {
                continue;
            };

            result.push(TopicKey {
                namespace: namespace.to_string(),
                topic_id: topic_id.to_string(),
            });
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn walks_two_levels_and_ignores_files() {
        let mut root = std::env::temp_dir();
        root.push("my-sb-persistence-scan-topics");
        let _ = std::fs::remove_dir_all(&root);

        std::fs::create_dir_all(root.join("default").join("orders")).unwrap();
        std::fs::create_dir_all(root.join("alpha").join("orders")).unwrap();
        std::fs::write(root.join("topicsdata"), [0u8; 4]).unwrap();
        std::fs::write(root.join("default").join("stray-file"), [0u8; 4]).unwrap();

        let mut found = scan_topic_folders(root.to_str().unwrap()).await;
        found.sort_by(|left, right| left.to_string().cmp(&right.to_string()));

        assert_eq!(2, found.len());
        assert_eq!("alpha/orders", found[0].to_string());
        assert_eq!("default/orders", found[1].to_string());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_missing_folder_yields_nothing() {
        let mut root = std::env::temp_dir();
        root.push("my-sb-persistence-scan-topics-missing");
        let _ = std::fs::remove_dir_all(&root);

        assert!(scan_topic_folders(root.to_str().unwrap()).await.is_empty());
    }
}
