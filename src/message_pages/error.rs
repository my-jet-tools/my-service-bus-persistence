#[derive(Debug)]
pub enum PageOperationError {
    NotInitialized,
    FileStorageError(String),
}

impl From<crate::file_storage::FileStorageError> for PageOperationError {
    fn from(src: crate::file_storage::FileStorageError) -> Self {
        Self::FileStorageError(format!("{}", src))
    }
}
