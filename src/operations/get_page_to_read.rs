use std::sync::Arc;

use my_service_bus::shared::sub_page::SubPageId;

use crate::{app::AppContext, message_pages::SubPage, topic_data::TopicData};

use super::archive_io::RestoreSubPageError;

pub async fn get_page_to_read(
    app: &AppContext,
    topic_data: &TopicData,
    sub_page_id: SubPageId,
) -> Arc<SubPage> {
    loop {
        let page = topic_data.pages_list.get(sub_page_id).await;

        if let Some(page) = page {
            return page;
        };

        let sub_page =
            crate::operations::archive_io::restore_sub_page(app, topic_data, sub_page_id).await;

        match sub_page {
            Ok(sub_page) => {
                topic_data.pages_list.restore_from_archive(sub_page).await;
            }
            Err(err) => {
                match &err {
                    // An ordinary answer: nothing is stored for this sub page.
                    RestoreSubPageError::NotFound => {}
                    RestoreSubPageError::ArchiveStorageError(_) => {
                        println!(
                            "Can not restore sub page {}  for topic {}. Err: {:?}",
                            sub_page_id, topic_data.topic_id, err
                        );
                    }
                }

                // Handed to this reader only, never put into `pages_list`: that list is what the
                // writer and the GC work on. A placeholder ahead of the tail would be given to the
                // writer as the page to fill, which drops the messages, and being the newest page
                // it would make the GC archive the real tail while it is still being filled.
                return Arc::new(SubPage::create_missing(sub_page_id));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use my_service_bus::abstractions::MessageId;
    use my_service_bus::shared::{protobuf_models::MessageProtobufModel, sub_page::SubPageId};
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::{app::AppContext, settings::SettingsModel, topic_key::TopicKeyRef};

    fn temp_folder(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-page-to-read-{}", name));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    async fn app_over(data_folder: &std::path::Path) -> AppContext {
        AppContext::new(SettingsModel {
            data: data_folder.to_str().unwrap().to_string(),
            max_response_records_amount: 100,
            delete_topic_secret_key: String::new(),
            listen_unix_socket: None,
            s3_conn_string: None,
            legacy: None,
        })
        .await
    }

    fn orders() -> TopicKeyRef<'static> {
        TopicKeyRef::new("default", "orders")
    }

    async fn publish(app: &AppContext, message_id: i64) {
        let message = MessageProtobufModel::new(
            MessageId::new(message_id),
            DateTimeAsMicroseconds::now(),
            vec![1u8, 2, 3],
            vec![],
        );

        crate::operations::new_messages(app, orders(), [message].into_iter()).await;
    }

    async fn is_stored(app: &AppContext, message_id: i64) -> bool {
        crate::operations::get_message_by_id(app, orders(), MessageId::new(message_id))
            .await
            .unwrap()
            .is_some()
    }

    /// What `PagesGcTimer` does every second.
    async fn gc(app: &AppContext) {
        let topic_data = app.topics_list.get(orders()).unwrap();
        crate::operations::gc_pages(app, topic_data).await.unwrap();
    }

    /// A reader asks for a message of a sub page that has not been started yet. That used to
    /// leave a `Missing` placeholder in `pages_list`, which the writer then took for the page to
    /// fill - and every message arriving for that sub page was accepted and dropped.
    #[tokio::test]
    async fn a_read_ahead_of_the_tail_does_not_swallow_what_arrives_next() {
        let data_folder = temp_folder("swallow");
        let app = app_over(&data_folder).await;

        publish(&app, 1).await;

        assert!(!is_stored(&app, 1_500).await);

        publish(&app, 1_500).await;

        assert!(is_stored(&app, 1_500).await);

        let _ = std::fs::remove_dir_all(&data_folder);
    }

    /// The same placeholder, being the newest page in the list, made the GC archive the real tail
    /// while it was still being filled. A sub page is written once, so whatever arrived for it
    /// afterwards was dropped by the next GC pass.
    #[tokio::test]
    async fn a_read_ahead_of_the_tail_does_not_seal_the_tail_early() {
        let data_folder = temp_folder("early-seal");
        let app = app_over(&data_folder).await;

        publish(&app, 1).await;

        assert!(!is_stored(&app, 5_000_000).await);

        gc(&app).await;

        // Still the tail, still in memory
        let topic_data = app.topics_list.get(orders()).unwrap();
        assert!(topic_data
            .pages_list
            .get(SubPageId::new(0))
            .await
            .unwrap()
            .is_active());

        publish(&app, 2).await;

        // The next sub page starts, so this one is archived - with everything it received
        publish(&app, 1_000).await;
        gc(&app).await;

        assert!(topic_data.pages_list.get(SubPageId::new(0)).await.is_none());
        assert!(is_stored(&app, 1).await);
        assert!(is_stored(&app, 2).await);

        let _ = std::fs::remove_dir_all(&data_folder);
    }
}
