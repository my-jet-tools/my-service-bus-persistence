use super::contracts::*;
use super::{check_topic_id, parse_namespace};
use crate::app::AppContext;
use crate::topic_key::TopicKeyRef;
use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};
use rust_extensions::date_time::DateTimeAsMicroseconds;
use std::sync::Arc;

#[my_http_server::macros::http_route(method:"GET",
route:"/Read/ListFromDate",
controller:"Read",
description:"Reads message by Id",
summary:"Read message by Id",
input_data:"GetMessagesByIdInputContract",
result:[
    {status_code: 200, description: "Found messages"},
    {status_code: 404, description: "Topic not found"},
]
)]
pub struct ListFromDateAction {
    app: Arc<AppContext>,
}

impl ListFromDateAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &ListFromDateAction,
    input_data: GetMessagesByIdInputContract,
    _ctx: &mut HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let namespace = parse_namespace(input_data.namespace.as_str())?;
    check_topic_id(input_data.topic_id.as_str())?;

    let messages = crate::operations::get_messages_from_date(
        action.app.as_ref(),
        TopicKeyRef::new(namespace.as_str(), input_data.topic_id.as_str()),
        DateTimeAsMicroseconds::parse_iso_string(input_data.from_date.as_str()).unwrap(),
        input_data.max_amount,
    )
    .await?;

    let model = GetMessagesResponseModel::create(messages.iter());

    HttpOutput::as_json(model).into_ok_result(true).into()
}
