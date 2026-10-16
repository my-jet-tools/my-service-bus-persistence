use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData};

use super::OperationError;

pub async fn gc_pages(app: &AppContext, topic_data: Arc<TopicData>) -> Result<(), OperationError> {
    while let Some(page_to_gc) = topic_data.pages_list.gc().await {
        crate::operations::archive_io::save_sub_page(app, &topic_data, &page_to_gc).await;
    }

    Ok(())
}
