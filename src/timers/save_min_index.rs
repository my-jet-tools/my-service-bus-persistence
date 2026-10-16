use std::sync::Arc;

use rust_extensions::{MyTimerTick, RepeatTimerIteration};

use crate::app::AppContext;

pub struct SaveMinIndexTimer {
    app: Arc<AppContext>,
}

impl SaveMinIndexTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for SaveMinIndexTimer {
    async fn tick(&self) -> RepeatTimerIteration {
        let topics_snapshot = self.app.topics_list.get_all();
        for topic_data in topics_snapshot.iter() {
            for index in topic_data.yearly_index_by_minute.get_all().await {
                index.flush_to_storage().await;
            }
        }

        RepeatTimerIteration::WithInterval
    }
}
