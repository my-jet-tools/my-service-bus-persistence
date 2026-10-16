use std::sync::Arc;

use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::{page_compressor::CompressedPageBuilder, sub_page::SubPageId};

use crate::{app::AppContext, topic_key::TopicKeyRef};

use super::{ReadSubPageError, SubPageMessagesReader};

/// `from..=to` as one compressed page, split into pieces of at most `max_payload_size`. The
/// sub pages are read one message at a time; only the page being built is held.
pub async fn get_compressed_page(
    app: Arc<AppContext>,
    topic_key: TopicKeyRef<'_>,
    from_message_id: MessageId,
    to_message_id: MessageId,
    max_payload_size: usize,
) -> Result<Vec<Vec<u8>>, ReadSubPageError> {
    let mut compressed_writer = CompressedPageBuilder::new();

    let from_sub_page: SubPageId = from_message_id.into();
    let to_sub_page: SubPageId = to_message_id.into();

    for sub_page_no in from_sub_page.get_value()..=to_sub_page.get_value() {
        let mut reader =
            SubPageMessagesReader::open(app.as_ref(), topic_key, SubPageId::new(sub_page_no))
                .await?;

        while let Some(message) = reader.next().await? {
            let message_id = message.get_message_id().get_value();

            if message_id < from_message_id.get_value() || message_id > to_message_id.get_value() {
                continue;
            }

            compressed_writer.add_message(message.as_ref());
        }
    }

    let result = compressed_writer.get_payload().unwrap();

    Ok(split(result.as_slice(), max_payload_size))
}

fn split(src: &[u8], max_payload_size: usize) -> Vec<Vec<u8>> {
    let mut result: Vec<Vec<u8>> = Vec::new();

    let mut pos: usize = 0;

    while pos < src.len() {
        if src.len() - pos < max_payload_size {
            let payload = &src[pos..];
            result.push(payload.to_vec());
            break;
        }
        let payload = &src[pos..pos + max_payload_size];
        result.push(payload.to_vec());
        pos += max_payload_size;
    }

    result
}
