use my_http_server::HttpFailResult;

impl From<crate::operations::OperationError> for HttpFailResult {
    fn from(src: crate::operations::OperationError) -> Self {
        match &src {
            crate::operations::OperationError::TopicNotFound(msg) => {
                HttpFailResult::as_not_found(format!("Topic {} not found", msg), false)
            }
            _ => HttpFailResult::as_fatal_error(format!("{:?}", src)),
        }
    }
}
