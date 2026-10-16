use my_http_server::HttpFailResult;

use crate::topic_key::{validate_topic_id, Namespace};

/// Resolves the optional `namespace` query parameter: empty means `default`.
pub fn parse_namespace(src: &str) -> Result<Namespace, HttpFailResult> {
    Namespace::parse(Some(src)).map_err(|err| {
        HttpFailResult::as_validation_error(format!("Invalid namespace. {}", err.as_string()))
    })
}

/// Same guard as on the gRPC side: the topic id becomes a path component.
pub fn check_topic_id(topic_id: &str) -> Result<(), HttpFailResult> {
    validate_topic_id(topic_id).map_err(|err| {
        HttpFailResult::as_validation_error(format!("Invalid topicId. {}", err.as_string()))
    })
}
