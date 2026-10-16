use std::sync::Arc;

use my_service_bus::shared::protobuf_models::MessageProtobufModel;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    app::AppContext, index_by_minute::MinuteWithinYear, topic_data::TopicData, typing::Year,
};

pub async fn new_messages(
    app: &AppContext,
    topic_data: &TopicData,
    messages: &[MessageProtobufModel],
) {
    let now = DateTimeAsMicroseconds::now();
    for msg in messages {
        if let Some((minute_within_year, year)) = extract_year_and_minute_within_year(app, msg) {
            let mut yearly_index = topic_data.yearly_index_by_minute.get(year, Some(now)).await;

            if yearly_index.is_none() {
                let new_index = Arc::new(
                    app.open_or_create_index_by_minute(topic_data.get_topic_key(), year)
                        .await,
                );

                // Registering is what makes the write durable: `update_minute_index_if_new` only
                // queues in memory, and the only things that flush the queue - SaveMinIndexTimer,
                // the pages GC and shutdown - all iterate `IndexByMinuteList`. An unregistered
                // instance is dropped at the end of this iteration together with everything queued
                // on it, and it makes every message re-open the 4 MB index file.
                topic_data
                    .yearly_index_by_minute
                    .add(year, new_index.clone())
                    .await;

                yearly_index = Some(new_index);
            }

            yearly_index
                .unwrap()
                .update_minute_index_if_new(minute_within_year, msg.get_message_id())
                .await;
        }
    }
}

fn extract_year_and_minute_within_year(
    app: &AppContext,
    msg: &MessageProtobufModel,
) -> Option<(MinuteWithinYear, Year)> {
    return Some(
        app.index_by_minute_utils
            .get_minute_within_the_year(msg.get_created()),
    );
}
