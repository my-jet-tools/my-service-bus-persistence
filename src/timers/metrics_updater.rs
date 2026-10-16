use std::sync::Arc;

use ahash::AHashMap;

use my_http_server::HttpConnectionsCounter;
use rust_extensions::{MyTimerTick, RepeatTimerIteration};

use crate::app::{AppContext, PrometheusMetricsToUpdate};

pub struct MetricsUpdater {
    app: Arc<AppContext>,
    http_connections_country: HttpConnectionsCounter,
}

impl MetricsUpdater {
    pub fn new(app: Arc<AppContext>, http_connections_country: HttpConnectionsCounter) -> Self {
        Self {
            app,
            http_connections_country,
        }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for MetricsUpdater {
    async fn tick(&self) -> RepeatTimerIteration {
        let topics_list = self.app.topics_snapshot.get_topics_list().await;

        let mut metrics = AHashMap::new();

        for topic_key in topics_list {
            match self.app.topics_list.get(topic_key.to_ref()) {
                Some(topic_data) => {
                    let queue_size = topic_data.pages_list.get_messages_amount_to_save().await;
                    metrics.insert(
                        topic_key,
                        PrometheusMetricsToUpdate {
                            not_persisted_size: queue_size.amount,
                            content_size: queue_size.size,
                        },
                    );
                }
                None => {
                    metrics.insert(
                        topic_key,
                        PrometheusMetricsToUpdate {
                            not_persisted_size: 0,
                            content_size: 0,
                        },
                    );
                }
            }
        }

        let http_connections = self.http_connections_country.get_connections_amount();

        self.app
            .metrics_keeper
            .update(metrics, http_connections)
            .await;

        RepeatTimerIteration::WithInterval
    }
}
