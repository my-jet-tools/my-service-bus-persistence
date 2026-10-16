use std::sync::Arc;

use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};
use my_service_bus::abstractions::AsMessageId;

use crate::app::AppContext;
use crate::topic_key::TopicKeyRef;

use super::contracts::*;
use super::{check_topic_id, parse_namespace};

#[my_http_server::macros::http_route(
    method:"GET",
route:"/Read/ById",
controller:"Read",
description:"Reads message by Id",
summary:"Read message by Id",
input_data:"GetMessageByIdInputContract",
result:[
    {status_code: 202, description: "Found message"},
    {status_code: 404, description: "Topic not found"},
]
)]

pub struct ByIdAction {
    app: Arc<AppContext>,
}

impl ByIdAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &ByIdAction,
    input_data: GetMessageByIdInputContract,
    _ctx: &mut HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let namespace = parse_namespace(input_data.namespace.as_str())?;
    check_topic_id(input_data.topic_id.as_str())?;

    let message = crate::operations::get_message_by_id(
        action.app.as_ref(),
        TopicKeyRef::new(namespace.as_str(), input_data.topic_id.as_str()),
        input_data.message_id.as_message_id(),
    )
    .await?;

    match message {
        Some(msg) => {
            let model = GetMessageResponseModel::create(msg.as_ref());
            return HttpOutput::as_json(model).into_ok_result(true).into();
        }
        None => {
            return Err(HttpFailResult::as_not_found(
                format!("Message {} not found", input_data.message_id),
                true,
            ));
        }
    }
}
