use my_service_bus::shared::{protobuf_models::MessageProtobufModel, sub_page::SubPageId};

use std::{collections::BTreeMap, time::Duration};

pub struct NewMessagesGrpcContract {
    pub topic_id: String,
    pub messages_by_sub_page: BTreeMap<i64, Vec<MessageProtobufModel>>,
}

pub async fn deserialize_uncompressed(
    req: &mut tonic::Streaming<crate::persistence_grpc::UnCompressedMessageChunkModel>,
    timeout: Duration,
) -> Result<NewMessagesGrpcContract, tonic::Status> {
    let mut payload: Vec<u8> = Vec::new();

    loop {
        let future = req.message();

        let next = tokio::time::timeout(timeout, future).await.unwrap()?;

        if next.is_none() {
            break;
        }

        let next = next.unwrap();
        payload.extend(next.chunk);
    }

    let contract = NewMessagesProtobufContract::parse(payload.as_slice());

    let mut messages_by_sub_page: BTreeMap<i64, Vec<MessageProtobufModel>> = BTreeMap::new();

    for msg in contract.messages {
        let sub_page_id: SubPageId = msg.get_message_id().into();

        if !messages_by_sub_page.contains_key(sub_page_id.as_ref()) {
            messages_by_sub_page.insert(sub_page_id.get_value(), Vec::new());
        }

        let messages = messages_by_sub_page.get_mut(sub_page_id.as_ref()).unwrap();

        messages.push(msg);
    }

    Ok(NewMessagesGrpcContract {
        messages_by_sub_page,
        topic_id: contract.topic_id,
    })
}
