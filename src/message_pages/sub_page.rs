use std::sync::Arc;

use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::{
    protobuf_models::MessageProtobufModel,
    sub_page::{SizeAndAmount, SubPageId},
};
use parking_lot::Mutex;
use rust_extensions::sorted_vec::{EntityWithKey, SortedVecOfArc};

use super::{SubPageInner, SubPageReadCopy};

pub enum SubPage {
    Active(SubPageId, Mutex<SubPageInner>),
    FromArchive(SubPageInner),
    Missing(SubPageId),
}

impl EntityWithKey<i64> for SubPage {
    fn get_key(&self) -> &i64 {
        match self {
            SubPage::Active(id, _) => id.as_ref(),
            SubPage::FromArchive(inner) => inner.sub_page_id.as_ref(),
            SubPage::Missing(id) => id.as_ref(),
        }
    }
}

impl SubPage {
    pub fn restore_from_archive(data: SubPageInner) -> Self {
        Self::FromArchive(data)
    }

    pub fn create_new(sub_page: SubPageInner) -> Self {
        Self::Active(sub_page.sub_page_id, Mutex::new(sub_page))
    }

    pub fn create_missing(sub_page_id: SubPageId) -> Self {
        Self::Missing(sub_page_id)
    }

    pub fn get_id(&self) -> SubPageId {
        match self {
            SubPage::Active(id, _) => *id,
            SubPage::FromArchive(inner) => inner.sub_page_id,
            SubPage::Missing(id) => *id,
        }
    }

    pub async fn new_messages(&self, messages: Vec<MessageProtobufModel>) {
        match self {
            SubPage::Active(_, inner) => {
                let mut data = inner.lock();
                for message in messages {
                    data.add_message(Arc::new(message));
                }
            }
            SubPage::FromArchive(_) => {}
            SubPage::Missing(_) => {}
        }
    }

    pub async fn get_message(&self, message_id: MessageId) -> Option<Arc<MessageProtobufModel>> {
        match self {
            SubPage::Active(_, inner) => {
                let data = inner.lock();
                data.get_message(message_id).cloned()
            }
            SubPage::FromArchive(data) => data.get_message(message_id).cloned(),
            SubPage::Missing(_) => None,
        }
    }

    pub async fn get_all_messages(&self) -> SubPageReadCopy {
        match self {
            SubPage::Active(_, inner) => {
                let data = inner.lock();
                let messages = data.get_all_messages();

                SubPageReadCopy::new(messages)
            }
            SubPage::FromArchive(data) => {
                let messages = data.get_all_messages();

                SubPageReadCopy::new(messages)
            }
            SubPage::Missing(_) => SubPageReadCopy::new(SortedVecOfArc::new()),
        }
    }

    pub async fn get_from_message_id(
        &self,
        from_message_id: MessageId,
        max_messages: usize,
    ) -> Vec<Arc<MessageProtobufModel>> {
        let mut result = Vec::with_capacity(max_messages);

        match self {
            SubPage::Active(_, inner) => {
                let access = inner.lock();
                for message_id in from_message_id.get_value()
                    ..access
                        .sub_page_id
                        .get_first_message_id_of_next_sub_page()
                        .get_value()
                {
                    if let Some(msg) = access.get_message(message_id.into()) {
                        result.push(msg.clone());
                    }
                }
            }
            SubPage::FromArchive(data) => {
                for message_id in from_message_id.get_value()
                    ..data
                        .sub_page_id
                        .get_first_message_id_of_next_sub_page()
                        .get_value()
                {
                    if let Some(msg) = data.get_message(message_id.into()) {
                        result.push(msg.clone());
                    }
                }
            }
            SubPage::Missing(_) => {}
        }

        result
    }

    pub async fn to_compressed_payload(&self) -> Option<Vec<u8>> {
        match self {
            SubPage::Active(_, sub_page_inner) => {
                let mut page_compressor =
                    my_service_bus::shared::page_compressor::CompressedPageBuilder::new();

                {
                    let data = sub_page_inner.lock();

                    for msg in data.messages.iter() {
                        page_compressor.add_message(msg);
                    }
                }

                let result = page_compressor.get_payload().unwrap();
                Some(result)
            }
            SubPage::FromArchive(_) => None,
            SubPage::Missing(_) => None,
        }
    }

    pub async fn get_size_and_amount(&self) -> SizeAndAmount {
        match self {
            SubPage::Active(_, data) => {
                let read_access = data.lock();
                read_access.get_size_and_amount().clone()
            }
            SubPage::FromArchive(archive) => archive.get_size_and_amount().clone(),
            SubPage::Missing(_) => SizeAndAmount { size: 0, amount: 0 },
        }
    }

    pub fn is_active(&self) -> bool {
        match self {
            SubPage::Active(_, _) => true,
            SubPage::FromArchive(_) => false,
            SubPage::Missing(_) => false,
        }
    }
}
