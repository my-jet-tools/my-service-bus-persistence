use super::{namespace_to_grpc, namespace_to_persist, Namespace};

/// Owned identity of a topic. Namespaces are fully isolated, so a topic is addressed by the
/// `(namespace, topic_id)` pair everywhere - `topic_id` alone is not a key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicKey {
    pub namespace: String,
    pub topic_id: String,
}

impl TopicKey {
    pub fn new(namespace: Namespace, topic_id: String) -> Self {
        Self {
            namespace: namespace.as_str().to_string(),
            topic_id,
        }
    }

    pub fn to_ref(&self) -> TopicKeyRef<'_> {
        TopicKeyRef {
            namespace: self.namespace.as_str(),
            topic_id: self.topic_id.as_str(),
        }
    }
}

impl std::fmt::Display for TopicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.topic_id)
    }
}

/// Borrowed identity of a topic - what every operation takes so no call site has to allocate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TopicKeyRef<'s> {
    pub namespace: &'s str,
    pub topic_id: &'s str,
}

impl<'s> TopicKeyRef<'s> {
    pub fn new(namespace: &'s str, topic_id: &'s str) -> Self {
        Self {
            namespace,
            topic_id,
        }
    }

    pub fn to_owned_key(self) -> TopicKey {
        TopicKey {
            namespace: self.namespace.to_string(),
            topic_id: self.topic_id.to_string(),
        }
    }

    pub fn namespace_to_persist(&self) -> String {
        namespace_to_persist(self.namespace)
    }

    pub fn namespace_to_grpc(&self) -> Option<String> {
        namespace_to_grpc(self.namespace)
    }
}

impl std::fmt::Display for TopicKeyRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.topic_id)
    }
}
