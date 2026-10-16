use std::sync::Arc;

use my_logger::LogEventCtx;
use rust_extensions::StopWatch;

use crate::app::AppContext;

pub async fn init(app: Arc<AppContext>) {
    let sw = StopWatch::new();

    restore_pages(&app).await;

    my_logger::LOGGER.write_info(
        "Initialization".to_string(),
        format!("Application is initialized in {:?}", sw.duration()),
        LogEventCtx::new(),
    );

    app.app_states.set_initialized();
}

async fn restore_pages(app: &Arc<AppContext>) {
    my_logger::LOGGER.write_info(
        "Initialization".to_string(),
        "Loading messages since last shutdown".to_string(),
        LogEventCtx::new(),
    );
    let sub_pages = crate::operations::current_sub_pages_io::restore(&app).await;

    if !sub_pages.is_empty() {
        let sw = StopWatch::new();

        for restored in sub_pages {
            let topic_data = app.topics_list.init_topic_data(restored.topic_key.to_ref());

            my_logger::LOGGER.write_info(
                "Initialization".to_string(),
                format!(
                    "Loaded sub page {} for topic {} in {}",
                    restored.sub_page.sub_page_id.get_value(),
                    restored.topic_key,
                    sw.duration_as_string()
                ),
                LogEventCtx::new(),
            );
            topic_data.pages_list.insert(restored.sub_page).await;
        }
    } else {
        my_logger::LOGGER.write_info(
            "Initialization".to_string(),
            format!("No sub page data loaded"),
            LogEventCtx::new(),
        );
    }
}
