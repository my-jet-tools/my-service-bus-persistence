use std::sync::Arc;

use my_http_server::macros::{MyHttpInput, MyHttpObjectStructure};
use my_service_bus::shared::protobuf_models::MessageProtobufModel;
use rust_extensions::base64::IntoBase64;
use serde::{Deserialize, Serialize};

#[derive(MyHttpInput)]
pub struct GetMessageByIdInputContract {
    #[http_query(name = "topicId"; description="Id of topic")]
    pub topic_id: String,

    #[http_query(name = "messageId"; description="Id of message")]
    pub message_id: i64,

    #[http_query(name = "namespace"; description="Namespace of the topic. Empty means 'default'"; default: "")]
    pub namespace: String,
}

#[derive(MyHttpInput)]
pub struct GetMessagesByIdInputContract {
    #[http_query(name = "topicId"; description="Id of topic")]
    pub topic_id: String,

    #[http_query(name = "maxAmount"; description="Maximum amounts to read"; default: 1)]
    pub max_amount: usize,

    #[http_query(name = "fromDate"; description="From date")]
    pub from_date: String,

    #[http_query(name = "namespace"; description="Namespace of the topic. Empty means 'default'"; default: "")]
    pub namespace: String,
}

#[derive(Serialize, Deserialize, Debug, MyHttpObjectStructure)]
pub struct GetMessagesResponseModel {
    result: i32,
    data: Vec<MessageJsonModel>,
}

impl GetMessagesResponseModel {
    pub fn create<'s>(messages: impl Iterator<Item = &'s Arc<MessageProtobufModel>>) -> Self {
        let mut data = Vec::new();

        for msg in messages {
            data.push(MessageJsonModel::new(msg))
        }

        GetMessagesResponseModel { result: 0, data }
    }
}

#[derive(Serialize, Deserialize, Debug, MyHttpObjectStructure)]
pub struct GetMessageResponseModel {
    result: i32,
    data: MessageJsonModel,
}

impl GetMessageResponseModel {
    pub fn create(message: &MessageProtobufModel) -> GetMessageResponseModel {
        GetMessageResponseModel {
            result: 0,
            data: MessageJsonModel::new(message),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, MyHttpObjectStructure)]
pub struct MessageJsonModel {
    id: i64,
    created: String,
    content: String,
}

impl MessageJsonModel {
    pub fn new(src: &MessageProtobufModel) -> Self {
        let result = Self {
            id: src.get_message_id().into(),
            content: src.data.into_base64(),
            created: src.get_created().to_rfc3339(),
        };

        result
    }
}
