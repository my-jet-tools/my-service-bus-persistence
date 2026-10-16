use ahash::{AHashMap, AHashSet};
use parking_lot::Mutex;
use prometheus::{Encoder, IntGauge, Registry, TextEncoder};

use crate::topic_key::TopicKey;

use super::GaugeByTopic;

pub struct PrometheusMetricsToUpdate {
    pub not_persisted_size: usize,
    pub content_size: usize,
}

pub struct PrometheusMetrics {
    registry: Registry,
    topic_persist_queue_size: GaugeByTopic,
    cached_messages_size: GaugeByTopic,
    active_topics: Mutex<AHashSet<TopicKey>>,
    http_connections_amount: IntGauge,
}

impl PrometheusMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let topic_persist_queue_size = GaugeByTopic::new(
            &registry,
            "topic_persist_queue_size",
            "Topic persist queue size",
        );

        let cached_messages_size =
            GaugeByTopic::new(&registry, "cached_messages_size", "Cached messages size");

        let http_connections_amount = create_http_connections_amount();

        registry
            .register(Box::new(http_connections_amount.clone()))
            .unwrap();

        return Self {
            registry,
            topic_persist_queue_size,
            cached_messages_size,
            active_topics: Mutex::new(AHashSet::new()),
            http_connections_amount,
        };
    }
    pub async fn update(
        &self,
        mut update_data: AHashMap<TopicKey, PrometheusMetricsToUpdate>,
        http_connections_amount: i64,
    ) {
        self.http_connections_amount.set(http_connections_amount);
        let mut active_topics = self.active_topics.lock();

        let mut topics_to_remove = Vec::new();

        for active_topic_key in active_topics.iter() {
            match update_data.remove(active_topic_key) {
                Some(metrics) => {
                    self.topic_persist_queue_size
                        .update_value(active_topic_key.to_ref(), metrics.not_persisted_size as i64);

                    self.cached_messages_size
                        .update_value(active_topic_key.to_ref(), metrics.content_size as i64);
                }
                None => {
                    self.topic_persist_queue_size
                        .remove_topic(active_topic_key.to_ref());

                    self.cached_messages_size
                        .remove_topic(active_topic_key.to_ref());

                    topics_to_remove.push(active_topic_key.clone());
                }
            }
        }

        for topic_key in topics_to_remove {
            active_topics.remove(&topic_key);
        }

        for (topic_key, metrics) in update_data {
            self.topic_persist_queue_size
                .update_value(topic_key.to_ref(), metrics.not_persisted_size as i64);

            self.cached_messages_size
                .update_value(topic_key.to_ref(), metrics.content_size as i64);

            active_topics.insert(topic_key);
        }
    }

    pub fn build_prometheus_content(&self) -> String {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();

        return String::from_utf8(buffer).unwrap();
    }
}

fn create_http_connections_amount() -> IntGauge {
    IntGauge::new("http_connections_amount", "Amount of Http Connections").unwrap()
}
