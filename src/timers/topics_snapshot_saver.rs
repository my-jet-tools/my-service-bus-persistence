use std::sync::Arc;

use rust_extensions::{MyTimerTick, RepeatTimerIteration};

use crate::app::AppContext;

pub struct TopicsSnapshotSaverTimer {
    app: Arc<AppContext>,
}

impl TopicsSnapshotSaverTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for TopicsSnapshotSaverTimer {
    async fn tick(&self) -> RepeatTimerIteration {
        self.app
            .topics_snapshot
            .flush_topics_snapshot_to_blob()
            .await;

        RepeatTimerIteration::WithInterval
    }
}
