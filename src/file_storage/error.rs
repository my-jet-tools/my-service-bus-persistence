#[derive(Debug)]
pub enum FileStorageError {
    NotFound,
    IoError(std::io::Error),
}

impl From<std::io::Error> for FileStorageError {
    fn from(src: std::io::Error) -> Self {
        if let std::io::ErrorKind::NotFound = src.kind() {
            return Self::NotFound;
        }

        Self::IoError(src)
    }
}

impl std::fmt::Display for FileStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileStorageError::NotFound => write!(f, "File not found"),
            FileStorageError::IoError(err) => write!(f, "{}", err),
        }
    }
}
