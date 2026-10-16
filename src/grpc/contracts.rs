use crate::app::AppContext;

/*
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NewMessagesProtobufContract {
    #[prost(string, tag = "1")]
    pub topic_id: ::prost::alloc::string::String,

    #[prost(repeated, message, tag = "2")]
    pub messages: Vec<MessageProtobufModel>,
}

impl NewMessagesProtobufContract {
    pub fn parse(protobuf: &[u8]) -> Self {
        prost::Message::decode(protobuf).unwrap()
    }
}
 */
use crate::topic_key::{validate_topic_id, Namespace};

/// The topic id becomes a path component and an S3 key segment, so it is checked before it can
/// reach storage - see `crate::topic_key::validate_topic_id`.
pub fn check_topic_id(topic_id: &str) -> Result<(), tonic::Status> {
    validate_topic_id(topic_id)
        .map_err(|err| tonic::Status::invalid_argument(format!("Invalid TopicId. {}", err)))
}

/// Resolves the `optional string Namespace` of a request. Absent or empty means `default` - that
/// is what a node which knows nothing about namespaces sends, so it keeps working unchanged.
pub fn get_namespace(src: Option<String>) -> Result<Namespace, tonic::Status> {
    Namespace::parse(src.as_deref()).map_err(|err| {
        tonic::Status::invalid_argument(format!("Invalid Namespace. {}", err.as_string()))
    })
}

/// Re-validates a namespace that already travelled through a mapper.
pub fn check_namespace_is_supported(namespace: &str) -> Result<(), tonic::Status> {
    get_namespace(Some(namespace.to_string()))?;
    Ok(())
}

pub fn check_flags(app: &AppContext) -> Result<(), tonic::Status> {
    if !app.app_states.is_initialized() {
        // `Unavailable`, not `Cancelled`: this is the standard "not ready, retry later" status,
        // and gRPC retry policies treat it as retryable while `Cancelled` means the caller gave
        // up. During a joint restart the bus node hits this for a few seconds - it must be able to
        // tell "come back shortly" from a real failure.
        return Err(tonic::Status::unavailable(
            "Application is not initialized yet",
        ));
    }

    if app.app_states.is_shutting_down() {
        return Err(tonic::Status::cancelled("Shutting down"));
    }

    Ok(())
}
