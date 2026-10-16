use std::{sync::Arc, time::Duration};

use crate::{app::AppContext, topic_data::TopicData};

pub async fn execute_before_shutdown(app: Arc<AppContext>) {
    let duration = Duration::from_secs(1);
    println!("Waiting until we flush all the queues and messages");
    tokio::time::sleep(duration).await;

    app.topics_snapshot.flush_topics_snapshot_to_blob().await;

    println!("Topic snapshot is flushed");

    let topics = app.topics_list.get_all();

    for topic_data in topics.iter() {
        println!("Flushing data for topic {} to blob ", topic_data.topic_id);
        topic_data
            .yearly_index_by_minute
            .save_before_shutdown()
            .await;

        save_topic_messages_to_be_archived(app.as_ref(), topic_data.as_ref()).await;
    }

    super::current_sub_pages_io::write(app.as_ref()).await;

    println!("Application can be closed now safely");
}

pub async fn save_topic_messages_to_be_archived(app: &AppContext, topic_data: &TopicData) {
    while let Some(sub_page) = topic_data.pages_list.gc().await {
        crate::operations::archive_io::save_sub_page(app, &topic_data, &sub_page).await;
    }
}
