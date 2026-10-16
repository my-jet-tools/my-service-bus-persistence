use prometheus::{IntGaugeVec, Registry};

use crate::topic_key::TopicKeyRef;

const NAMESPACE_LABEL: &str = "namespace";
const TOPIC_LABEL: &str = "topic";

pub struct GaugeByTopic(IntGaugeVec);

impl GaugeByTopic {
    pub fn new(registry: &Registry, name: &str, help: &str) -> Self {
        let gauge = IntGaugeVec::new(
            prometheus::Opts::new(name, help),
            &[NAMESPACE_LABEL, TOPIC_LABEL],
        )
        .unwrap();

        registry.register(Box::new(gauge.clone())).unwrap();
        Self(gauge)
    }

    pub fn update_value(&self, topic_key: TopicKeyRef<'_>, value: i64) {
        self.0
            .with_label_values(&[topic_key.namespace, topic_key.topic_id])
            .set(value);
    }

    pub fn remove_topic(&self, topic_key: TopicKeyRef<'_>) {
        let result = self
            .0
            .remove_label_values(&[topic_key.namespace, topic_key.topic_id]);

        if let Err(err) = result {
            println!(
                "Failed to remove topic {} from prometheus metrics: {}",
                topic_key, err
            );
        }
    }
}
