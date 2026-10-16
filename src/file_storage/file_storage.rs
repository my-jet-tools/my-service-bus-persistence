use std::path::{Path, PathBuf};

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use super::FileStorageError;

/// Random-access file - what the page blob abstraction used to give us, without the 512-byte
/// page granularity, which a file does not have.
///
/// Reads past the end of the file come back zero-filled, matching how a pre-allocated page blob
/// behaved: the archive TOC and the year index are sized up front and addressed by offset, and
/// an untouched slot has to read as zeroes rather than as an error.
pub struct FileStorage {
    /// `tokio::sync::Mutex` on purpose: a seek and the read/write that follows it are one
    /// critical section, and both halves are `.await`s, so a `parking_lot` guard can not be held
    /// across them.
    file: Mutex<File>,
}

impl FileStorage {
    /// Opens the file, creating it - and every missing directory above it - if needed.
    pub async fn open_or_create(path: impl Into<PathBuf>) -> Result<Self, FileStorageError> {
        let path: PathBuf = path.into();

        if let Some(folder) = path.parent() {
            tokio::fs::create_dir_all(folder).await?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_path())
            .await?;

        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// `None` when the file does not exist - no directory is created.
    pub async fn open_if_exists(
        path: impl Into<PathBuf>,
    ) -> Result<Option<Self>, FileStorageError> {
        let path: PathBuf = path.into();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_path())
            .await;

        match file {
            Ok(file) => Ok(Some(Self {
                file: Mutex::new(file),
            })),
            Err(err) => {
                if let std::io::ErrorKind::NotFound = err.kind() {
                    return Ok(None);
                }

                Err(err.into())
            }
        }
    }

    pub async fn get_size(&self) -> Result<u64, FileStorageError> {
        let file = self.file.lock().await;
        let metadata = file.metadata().await?;
        Ok(metadata.len())
    }

    /// Grows the file to `size` if it is smaller. Never shrinks it.
    pub async fn ensure_size(&self, size: u64) -> Result<(), FileStorageError> {
        let file = self.file.lock().await;

        if file.metadata().await?.len() >= size {
            return Ok(());
        }

        file.set_len(size).await?;
        Ok(())
    }

    /// Reads `len` bytes at `offset`, zero-filling whatever falls past the end of the file.
    pub async fn read(&self, offset: usize, len: usize) -> Result<Vec<u8>, FileStorageError> {
        let mut result = vec![0u8; len];

        let mut file = self.file.lock().await;

        file.seek(std::io::SeekFrom::Start(offset as u64)).await?;

        let mut read_so_far = 0;

        while read_so_far < len {
            let read_now = file.read(&mut result[read_so_far..]).await?;

            if read_now == 0 {
                // End of file - the rest stays zero-filled.
                break;
            }

            read_so_far += read_now;
        }

        Ok(result)
    }

    pub async fn write(&self, offset: usize, payload: &[u8]) -> Result<(), FileStorageError> {
        let mut file = self.file.lock().await;

        file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
        file.write_all(payload).await?;
        file.flush().await?;

        Ok(())
    }

    /// Appends at the end and returns the offset the payload landed at.
    pub async fn append(&self, payload: &[u8]) -> Result<u64, FileStorageError> {
        let mut file = self.file.lock().await;

        let offset = file.seek(std::io::SeekFrom::End(0)).await?;
        file.write_all(payload).await?;
        file.flush().await?;

        Ok(offset)
    }

    pub async fn read_all(&self) -> Result<Vec<u8>, FileStorageError> {
        let mut result = Vec::new();

        let mut file = self.file.lock().await;

        file.seek(std::io::SeekFrom::Start(0)).await?;
        file.read_to_end(&mut result).await?;

        Ok(result)
    }

    /// Replaces the whole content - the file is truncated to exactly `payload`.
    pub async fn write_all(&self, payload: &[u8]) -> Result<(), FileStorageError> {
        let mut file = self.file.lock().await;

        file.set_len(0).await?;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        file.write_all(payload).await?;
        file.flush().await?;

        Ok(())
    }

    /// Pushes the content down to the device. Call it at commit points - not on every small
    /// write, or the year index would fsync once per minute of traffic.
    pub async fn sync(&self) -> Result<(), FileStorageError> {
        let file = self.file.lock().await;
        file.sync_data().await?;
        Ok(())
    }
}

pub async fn delete_file_if_exists(path: impl AsRef<Path>) -> Result<(), FileStorageError> {
    match tokio::fs::remove_file(path).await {
        Ok(_) => Ok(()),
        Err(err) => {
            if let std::io::ErrorKind::NotFound = err.kind() {
                return Ok(());
            }

            Err(err.into())
        }
    }
}

pub async fn delete_folder_if_exists(path: impl AsRef<Path>) -> Result<(), FileStorageError> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(_) => Ok(()),
        Err(err) => {
            if let std::io::ErrorKind::NotFound = err.kind() {
                return Ok(());
            }

            Err(err.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-test-{}", name));
        path
    }

    #[tokio::test]
    async fn write_and_read_at_offset() {
        let path = temp_path("write_and_read_at_offset");
        let _ = std::fs::remove_file(&path);

        let storage = FileStorage::open_or_create(&path).await.unwrap();

        storage.write(120, &[1u8, 2, 3, 4]).await.unwrap();

        let result = storage.read(120, 4).await.unwrap();
        assert_eq!(vec![1u8, 2, 3, 4], result);

        // Everything before the written offset is a hole and reads as zeroes
        let result = storage.read(0, 8).await.unwrap();
        assert_eq!(vec![0u8; 8], result);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn read_past_the_end_is_zero_filled() {
        let path = temp_path("read_past_the_end");
        let _ = std::fs::remove_file(&path);

        let storage = FileStorage::open_or_create(&path).await.unwrap();
        storage.write(0, &[7u8, 7]).await.unwrap();

        let result = storage.read(0, 8).await.unwrap();
        assert_eq!(vec![7u8, 7, 0, 0, 0, 0, 0, 0], result);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn append_returns_the_offset() {
        let path = temp_path("append_returns_the_offset");
        let _ = std::fs::remove_file(&path);

        let storage = FileStorage::open_or_create(&path).await.unwrap();

        assert_eq!(0, storage.append(&[1u8, 2, 3]).await.unwrap());
        assert_eq!(3, storage.append(&[4u8, 5]).await.unwrap());

        assert_eq!(vec![1u8, 2, 3, 4, 5], storage.read_all().await.unwrap());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn write_all_truncates() {
        let path = temp_path("write_all_truncates");
        let _ = std::fs::remove_file(&path);

        let storage = FileStorage::open_or_create(&path).await.unwrap();

        storage.write_all(&[1u8; 100]).await.unwrap();
        assert_eq!(100, storage.get_size().await.unwrap());

        storage.write_all(&[2u8; 10]).await.unwrap();
        assert_eq!(10, storage.get_size().await.unwrap());
        assert_eq!(vec![2u8; 10], storage.read_all().await.unwrap());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn open_if_exists_returns_none() {
        let path = temp_path("open_if_exists_returns_none");
        let _ = std::fs::remove_file(&path);

        assert!(FileStorage::open_if_exists(&path).await.unwrap().is_none());

        FileStorage::open_or_create(&path).await.unwrap();

        assert!(FileStorage::open_if_exists(&path).await.unwrap().is_some());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn ensure_size_never_shrinks() {
        let path = temp_path("ensure_size_never_shrinks");
        let _ = std::fs::remove_file(&path);

        let storage = FileStorage::open_or_create(&path).await.unwrap();

        storage.ensure_size(1024).await.unwrap();
        assert_eq!(1024, storage.get_size().await.unwrap());

        storage.ensure_size(512).await.unwrap();
        assert_eq!(1024, storage.get_size().await.unwrap());

        let _ = std::fs::remove_file(&path);
    }
}
