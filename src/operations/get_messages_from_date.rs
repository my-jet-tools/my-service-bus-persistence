use std::sync::Arc;

use my_service_bus::shared::protobuf_models::MessageProtobufModel;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    app::AppContext,
    index_by_minute::{MinuteWithinYear, YearlyIndexByMinute},
    topic_data::TopicData,
    topic_key::TopicKeyRef,
};

use super::OperationError;

pub async fn get_messages_from_date(
    app: &AppContext,
    topic_key: TopicKeyRef<'_>,
    get_messages_from_date: DateTimeAsMicroseconds,
    max_amount: usize,
) -> Result<Vec<Arc<MessageProtobufModel>>, OperationError> {
    let (minute, year) = app
        .index_by_minute_utils
        .get_minute_within_the_year(get_messages_from_date);

    let topic_data = super::topics::get_topic(app, topic_key).await?;

    let now = DateTimeAsMicroseconds::now();

    let mut yearly_index = topic_data.yearly_index_by_minute.get(year, Some(now)).await;

    if yearly_index.is_none() {
        yearly_index = app.try_open_index_by_minute(topic_key, year).await;
    }

    if yearly_index.is_none() {
        return Ok(vec![]);
    }

    let yearly_index = yearly_index.unwrap();

    topic_data
        .yearly_index_by_minute
        .add(year, yearly_index.clone())
        .await;

    let result =
        read_from_yearly_index(app, topic_data.as_ref(), &yearly_index, minute, max_amount).await;

    return result;
}

async fn read_from_yearly_index(
    app: &AppContext,
    topic_data: &TopicData,
    yearly_index: &YearlyIndexByMinute,
    minute: MinuteWithinYear,
    max_amount: usize,
) -> Result<Vec<Arc<MessageProtobufModel>>, OperationError> {
    let message_id = yearly_index.get_message_id(minute).await;

    if message_id.is_none() {
        return Ok(vec![]);
    }

    let message_id = message_id.unwrap();

    let page_id = message_id.into();

    let page = crate::operations::get_page_to_read(app, topic_data, page_id).await;

    let result = page.get_from_message_id(message_id, max_amount).await;
    Ok(result)
}
