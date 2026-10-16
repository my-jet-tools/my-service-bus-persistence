use std::sync::Arc;

use my_service_bus::shared::sub_page::{SizeAndAmount, SubPageId};
use parking_lot::Mutex;
use rust_extensions::sorted_vec::SortedVecOfArc;

use super::{SubPage, SubPageInner};

pub struct PagesList {
    pub sub_pages: Mutex<SortedVecOfArc<i64, SubPage>>,
}

impl PagesList {
    pub fn new() -> Self {
        Self {
            sub_pages: Mutex::new(SortedVecOfArc::new()),
        }
    }

    pub async fn insert(&self, sub_page_inner: SubPageInner) {
        match self
            .sub_pages
            .lock()
            .insert_or_if_not_exists(sub_page_inner.sub_page_id.as_ref())
        {
            rust_extensions::sorted_vec::InsertIfNotExists::Insert(entry) => {
                let item = SubPage::create_new(sub_page_inner);
                entry.insert(Arc::new(item));
            }
            rust_extensions::sorted_vec::InsertIfNotExists::Exists(_) => {}
        }
    }

    pub async fn restore_from_archive(&self, sub_page: SubPage) {
        match self
            .sub_pages
            .lock()
            .insert_or_if_not_exists(sub_page.get_id().as_ref())
        {
            rust_extensions::sorted_vec::InsertIfNotExists::Insert(entry) => {
                entry.insert(Arc::new(sub_page));
            }
            rust_extensions::sorted_vec::InsertIfNotExists::Exists(_) => {}
        }
    }

    pub async fn get(&self, sub_page_id: SubPageId) -> Option<Arc<SubPage>> {
        let pages_access = self.sub_pages.lock();
        let result = pages_access.get(sub_page_id.as_ref())?;
        Some(result.clone())
    }

    pub async fn get_all(&self) -> Vec<Arc<SubPage>> {
        let read_access = self.sub_pages.lock();
        let mut result = Vec::with_capacity(read_access.len());
        for page in read_access.iter() {
            let itm = page.clone();
            result.push(itm);
        }

        result
    }

    pub async fn gc(&self) -> Option<Arc<SubPage>> {
        let mut pages_access = self.sub_pages.lock();

        if pages_access.len() <= 1 {
            return None;
        }

        let first_key = pages_access.first().unwrap().get_id().clone();
        pages_access.remove(first_key.as_ref())
    }

    /// The newest sub page - the open tail. `None` when the topic holds nothing, which happens
    /// for a topic that is known but has never received a message.
    pub async fn get_active_sub_page(&self) -> Option<Arc<SubPage>> {
        let read_access = self.sub_pages.lock();
        let result = read_access.last()?;
        Some(result.clone())
    }

    pub async fn get_messages_amount_to_save(&self) -> SizeAndAmount {
        let active_sub_pages: Vec<Arc<SubPage>> = {
            let read_access = self.sub_pages.lock();
            read_access
                .iter()
                .filter(|sp| sp.is_active())
                .cloned()
                .collect()
        };

        let mut result = SizeAndAmount::new();
        for sub_page in active_sub_pages {
            let size_and_amount = sub_page.get_size_and_amount().await;
            result.size += size_and_amount.size;
            result.amount += size_and_amount.amount;
        }

        result
    }
}
