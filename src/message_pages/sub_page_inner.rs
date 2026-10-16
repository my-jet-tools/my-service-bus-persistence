use std::sync::Arc;

use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::{
    page_compressor::CompressedPageReaderError,
    protobuf_models::MessageProtobufModel,
    sub_page::{SizeAndAmount, SubPageId},
};
use rust_extensions::sorted_vec::SortedVecOfArc;

pub struct SubPageInner {
    pub sub_page_id: SubPageId,
    pub messages: SortedVecOfArc<i64, MessageProtobufModel>,

    size_and_amount: SizeAndAmount,
}

impl SubPageInner {
    pub fn new(sub_page_id: SubPageId) -> Self {
        Self {
            sub_page_id,
            messages: SortedVecOfArc::new(),
            size_and_amount: SizeAndAmount::new(),
        }
    }

    pub fn restore(
        sub_page_id: SubPageId,
        messages: SortedVecOfArc<i64, MessageProtobufModel>,
    ) -> Self {
        let mut size_and_amount = SizeAndAmount::new();

        for msg in messages.iter() {
            size_and_amount.added(msg.data.len());
        }

        Self {
            sub_page_id,
            messages,
            size_and_amount,
        }
    }

    pub fn add_message(
        &mut self,
        message: Arc<MessageProtobufModel>,
    ) -> Option<Arc<MessageProtobufModel>> {
        let message_id = message.get_message_id();

        if !self.sub_page_id.is_my_message_id(message_id) {
            println!(
                "Somehow we are uploading message_id {} to sub_page {}. Skipping message...",
                message_id.get_value(),
                self.sub_page_id.get_value()
            );
            return None;
        }

        self.size_and_amount.added(message.data.len());

        let (_, removed) = self.messages.insert_or_replace(message);

        if let Some(removed_item) = removed {
            self.size_and_amount.removed(removed_item.data.len());
            return Some(removed_item);
        }

        None
    }

    pub fn get_message(&self, msg_id: MessageId) -> Option<&Arc<MessageProtobufModel>> {
        self.messages.get(msg_id.as_ref())
    }

    pub fn get_all_messages(&self) -> SortedVecOfArc<i64, MessageProtobufModel> {
        self.messages.clone()
    }

    pub fn get_size_and_amount(&self) -> &SizeAndAmount {
        &self.size_and_amount
    }

    pub fn from_compressed_payload(
        sub_page_id: SubPageId,
        compressed_payload: &[u8],
    ) -> Result<SubPageInner, CompressedPageReaderError> {
        let mut compressed_payload =
            my_service_bus::shared::page_compressor::CompressedPageReader::new(compressed_payload)?;

        let mut messages = SortedVecOfArc::new();

        while let Some(msg) = compressed_payload.get_next_message() {
            messages.insert_or_replace(Arc::new(msg));
        }

        let sub_page_inner = SubPageInner::restore(sub_page_id, messages);

        Ok(sub_page_inner)
    }
}
