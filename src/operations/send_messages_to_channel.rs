use std::sync::Arc;

use my_grpc_extensions::StreamedResponseProducer;
use my_logger::LogEventCtx;
use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::sub_page::SubPageId;

use crate::{app::AppContext, persistence_grpc::MessageContentGrpcModel, topic_key::TopicKey};

use super::{ReadSubPageError, SubPageMessagesReader};

/// Streams `from..=to` to the consumer, sub page by sub page, each message as soon as it is read -
/// an archived sub page is decoded straight off the disk or the cold tier, never held whole.
///
/// A storage failure ends the stream with an error rather than with a clean end: a consumer must
/// not be able to take "the cold tier did not answer" for "there are no such messages".
pub async fn send_messages_to_channel(
    app: Arc<AppContext>,
    topic_key: TopicKey,
    from_message_id: MessageId,
    to_message_id: MessageId,
    producer: StreamedResponseProducer<MessageContentGrpcModel>,
) {
    let from_sub_page: SubPageId = from_message_id.into();
    let to_sub_page: SubPageId = to_message_id.into();

    for sub_page_no in from_sub_page.get_value()..=to_sub_page.get_value() {
        let sub_page_id = SubPageId::new(sub_page_no);

        let result = send_sub_page(
            app.as_ref(),
            &topic_key,
            sub_page_id,
            from_message_id,
            to_message_id,
            &producer,
        )
        .await;

        match result {
            Ok(SendOutcome::Sent) => {}
            Ok(SendOutcome::ConsumerIsGone) => return,
            Err(err) => {
                my_logger::LOGGER.write_error(
                    "send_messages_to_channel",
                    format!("Can not read the sub page. Err: {:?}", err.err),
                    LogEventCtx::new()
                        .add("topicId", topic_key.to_string())
                        .add("subPageId", err.sub_page_id.get_value().to_string()),
                );

                let _ = producer
                    .send_error(tonic::Status::unavailable(format!(
                        "Can not read sub page {} of {}",
                        err.sub_page_id.get_value(),
                        topic_key
                    )))
                    .await;

                return;
            }
        }
    }
}

enum SendOutcome {
    Sent,
    ConsumerIsGone,
}

async fn send_sub_page(
    app: &AppContext,
    topic_key: &TopicKey,
    sub_page_id: SubPageId,
    from_message_id: MessageId,
    to_message_id: MessageId,
    producer: &StreamedResponseProducer<MessageContentGrpcModel>,
) -> Result<SendOutcome, ReadSubPageError> {
    let mut reader = SubPageMessagesReader::open(app, topic_key.to_ref(), sub_page_id).await?;

    // Read to the end even past `to_message_id`: the order is the stored one rather than a
    // promise, and a cold stream dropped half way costs its connection.
    while let Some(message) = reader.next().await? {
        let message_id = message.get_message_id().get_value();

        if message_id < from_message_id.get_value() || message_id > to_message_id.get_value() {
            continue;
        }

        // Closed - or not read within the send timeout, which aborts the stream on its own.
        if producer.send(message.as_ref().into()).await.is_err() {
            return Ok(SendOutcome::ConsumerIsGone);
        }
    }

    Ok(SendOutcome::Sent)
}
