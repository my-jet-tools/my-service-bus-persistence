use std::sync::Arc;

use my_service_bus::shared::protobuf_models::MessageProtobufModel;
use rust_extensions::sorted_vec::SortedVecOfArc;

/// The messages of a sub page taken out from under its lock - `Arc`s, not copies.
pub struct SubPageReadCopy {
    messages: SortedVecOfArc<i64, MessageProtobufModel>,
}

impl SubPageReadCopy {
    pub fn new(messages: SortedVecOfArc<i64, MessageProtobufModel>) -> Self {
        Self { messages }
    }

    pub fn into_messages(self) -> Vec<Arc<MessageProtobufModel>> {
        self.messages.iter().cloned().collect()
    }
}
