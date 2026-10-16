use my_logger::LogEventCtx;
use my_service_bus::shared::sub_page::SubPageId;

use crate::{
    app::{storage_layout, AppContext},
    file_storage::{delete_file_if_exists, FileStorage},
    message_pages::SubPageInner,
    topic_key::{namespace_from_persisted, TopicKey, TopicKeyRef},
};

/// The open tail of one topic: the sub page still being filled, which lives nowhere else but in
/// memory until the service shuts down.
///
/// The file sits at `{namespace}/{topic}/active`, so the path is the key - the record carries
/// neither the namespace nor the topic id.
#[derive(Clone, prost::Message)]
pub struct ActiveSubPageModel {
    #[prost(int64, tag = "1")]
    pub sub_page_id: i64,
    #[prost(bytes, tag = "2")]
    pub payload: Vec<u8>,
}

/// The pre-namespace format: one `.active-pages` holding the tail of *every* topic at once.
/// Read once, on the first start after the move to per-topic files, then deleted.
#[derive(Clone, prost::Message)]
pub struct LegacyActiveSubPageModel {
    #[prost(string, tag = "1")]
    pub topic_id: String,
    #[prost(int64, tag = "2")]
    pub sub_page_id: i64,
    #[prost(bytes, tag = "3")]
    pub payload: Vec<u8>,
    /// Empty - and always absent in data written before namespaces - means `default`.
    #[prost(string, tag = "4")]
    pub namespace: String,
}

#[derive(Clone, prost::Message)]
pub struct LegacyActivePages {
    #[prost(message, repeated, tag = "1")]
    pub sub_pages: Vec<LegacyActiveSubPageModel>,
}

pub struct RestoredSubPage {
    pub topic_key: TopicKey,
    pub sub_page: SubPageInner,
}

pub const LEGACY_ACTIVE_PAGES_FILE_NAME: &str = ".active-pages";

/// Restores every open tail: the per-topic `active` file of every topic that has a folder on
/// disk, plus the legacy global file if one is still lying around.
///
/// The topic list comes from the disk rather than from the topics snapshot: the snapshot is
/// pushed by the bus node, so a topic that received messages but never made it into a saved
/// snapshot is not in it - and its tail would be skipped and left orphaned on disk.
pub async fn restore(app: &AppContext) -> Vec<RestoredSubPage> {
    let mut result = restore_legacy(app).await;

    for topic_key in crate::operations::scan_topic_folders(app.get_data_folder()).await {
        if let Some(sub_page) = restore_topic(app, topic_key.to_ref()).await {
            result.push(sub_page);
        }
    }

    result
}

async fn restore_topic(app: &AppContext, topic_key: TopicKeyRef<'_>) -> Option<RestoredSubPage> {
    let path = storage_layout::get_local_path(
        app.get_data_folder(),
        storage_layout::get_active_relative_path(topic_key).as_str(),
    );

    let file = FileStorage::open_if_exists(&path)
        .await
        .expect("Can not open the active sub page file")?;

    let content = file
        .read_all()
        .await
        .expect("Can not read the active sub page file");

    if content.is_empty() {
        return None;
    }

    let model: ActiveSubPageModel = match prost::Message::decode(content.as_slice()) {
        Ok(model) => model,
        Err(err) => {
            println!(
                "Can not decode the active sub page of {}. Skipping it. Err: {:?}",
                topic_key, err
            );
            return None;
        }
    };

    let sub_page = SubPageInner::from_compressed_payload(
        SubPageId::new(model.sub_page_id),
        model.payload.as_slice(),
    );

    let sub_page = match sub_page {
        Ok(sub_page) => sub_page,
        Err(err) => {
            println!(
                "Can not decompress the active sub page of {}. Skipping it. Err: {:?}",
                topic_key, err
            );
            return None;
        }
    };

    // Restored into memory - the file has served its purpose and must not be replayed again.
    delete_file_if_exists(&path)
        .await
        .expect("Can not delete the active sub page file");

    Some(RestoredSubPage {
        topic_key: topic_key.to_owned_key(),
        sub_page,
    })
}

async fn restore_legacy(app: &AppContext) -> Vec<RestoredSubPage> {
    let mut path = std::path::PathBuf::from(app.get_data_folder());
    path.push(LEGACY_ACTIVE_PAGES_FILE_NAME);

    let Some(file) = FileStorage::open_if_exists(&path)
        .await
        .expect("Can not open the legacy active pages file")
    else {
        return Vec::new();
    };

    let content = file
        .read_all()
        .await
        .expect("Can not read the legacy active pages file");

    let mut result = Vec::new();

    let legacy: LegacyActivePages = match prost::Message::decode(content.as_slice()) {
        Ok(legacy) => legacy,
        Err(err) => {
            // Do NOT delete it. This one file holds the open tail of every topic and it is the
            // only copy - the legacy source was already removed when it was moved here. prost is
            // all-or-nothing, so a single truncated record loses the intact ones too; leaving the
            // file in place at least keeps it recoverable by hand. Same rule as `restore_topic`.
            my_logger::LOGGER.write_error(
                "restore_legacy_active_pages",
                format!(
                    "Can not decode {:?}. Leaving it in place - it holds the open tail of every topic. Err: {:?}",
                    path, err
                ),
                LogEventCtx::new(),
            );
            return Vec::new();
        }
    };

    for item in legacy.sub_pages {
        let sub_page = SubPageInner::from_compressed_payload(
            SubPageId::new(item.sub_page_id),
            item.payload.as_slice(),
        );

        match sub_page {
            Ok(sub_page) => result.push(RestoredSubPage {
                topic_key: TopicKey {
                    namespace: namespace_from_persisted(item.namespace.as_str()).to_string(),
                    topic_id: item.topic_id,
                },
                sub_page,
            }),
            Err(err) => {
                println!(
                    "Can not decompress a legacy active sub page of {}. Skipping it. Err: {:?}",
                    item.topic_id, err
                );
            }
        }
    }

    println!(
        "Restored {} sub pages from the legacy active pages file",
        result.len()
    );

    delete_file_if_exists(&path)
        .await
        .expect("Can not delete the legacy active pages file");

    result
}

/// Writes the open tail of every topic, one file per topic.
pub async fn write(app: &AppContext) {
    for topic_data in app.topics_list.get_all().iter() {
        let Some(sub_page) = topic_data.pages_list.get_active_sub_page().await else {
            continue;
        };

        let Some(payload) = sub_page.to_compressed_payload().await else {
            continue;
        };

        let model = ActiveSubPageModel {
            sub_page_id: sub_page.get_id().get_value(),
            payload,
        };

        let mut content = Vec::new();
        prost::Message::encode(&model, &mut content)
            .expect("Can not serialize the active sub page");

        let path = storage_layout::get_local_path(
            app.get_data_folder(),
            storage_layout::get_active_relative_path(topic_data.get_topic_key()).as_str(),
        );

        let file = FileStorage::open_or_create(&path)
            .await
            .expect("Can not open the active sub page file");

        file.write_all(content.as_slice())
            .await
            .expect("Can not write the active sub page file");

        file.sync()
            .await
            .expect("Can not flush the active sub page file");
    }
}
