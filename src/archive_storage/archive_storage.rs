use std::{path::PathBuf, sync::Arc};

use my_service_bus::shared::sub_page::SubPageId;
use parking_lot::Mutex;

use crate::file_storage::{FileStorage, FileStorageError};

use super::{
    consts::{TOC_SIZE, TOC_SIZE_IN_BITES, TOC_STRUCTURE_SIZE},
    toc::SubPagePosition,
    ArchiveFileNo,
};
use crate::{
    cold_storage::{ColdStorage, ColdStorageStream},
    topic_key::TopicKey,
};

#[derive(Debug)]
// The payloads are read through `{:?}` in panics and logs, which dead-code analysis ignores.
#[allow(dead_code)]
pub enum ArchiveStorageError {
    FileStorageError(FileStorageError),
    ColdStorageError(String),
    /// A sealed file already uploaded to the cold tier never accepts writes again.
    Frozen,
}

impl From<FileStorageError> for ArchiveStorageError {
    fn from(src: FileStorageError) -> Self {
        Self::FileStorageError(src)
    }
}

/// An archive file: a fixed-size TOC at the head, then the compressed sub pages appended one
/// after another. A sub page is written exactly once, when it is already closed - see
/// [`ArchiveStorage::write_payload`].
///
/// The head of the file is reserved as `TOC_SIZE` (page-rounded) rather than the `TOC_SIZE_IN_BITES`
/// the entries actually occupy. That rounding comes from the page blob era, and it is kept so a
/// file copied out of the old storage keeps its data offsets valid.
pub struct ArchiveStorage {
    pub archive_file_no: ArchiveFileNo,
    source: ArchiveSource,
}

enum ArchiveSource {
    /// Local file - the only place a sub page can be written.
    Local(FileStorage),
    /// Uploaded and immutable. Read over ranged GETs; the TOC is fetched once and kept, since
    /// the object can no longer change.
    Cold(ColdArchive),
}

struct ColdArchive {
    cold_storage: Arc<ColdStorage>,
    /// Owned, because the handle outlives the request that opened it. `ColdStorage` turns the pair
    /// plus the file name into a bucket and a key, whichever layout is configured.
    topic_key: TopicKey,
    file_name: String,
    toc: Mutex<Option<Arc<Vec<u8>>>>,
}

impl ArchiveStorage {
    pub async fn open_or_create_local(
        archive_file_no: ArchiveFileNo,
        path: impl Into<PathBuf>,
    ) -> Result<Self, ArchiveStorageError> {
        let file = FileStorage::open_or_create(path).await?;

        file.ensure_size(TOC_SIZE as u64).await?;

        Ok(Self {
            archive_file_no,
            source: ArchiveSource::Local(file),
        })
    }

    pub async fn open_local_if_exists(
        archive_file_no: ArchiveFileNo,
        path: impl Into<PathBuf>,
    ) -> Result<Option<Self>, ArchiveStorageError> {
        let file = FileStorage::open_if_exists(path).await?;

        let Some(file) = file else {
            return Ok(None);
        };

        if file.get_size().await? < TOC_SIZE as u64 {
            return Ok(None);
        }

        Ok(Some(Self {
            archive_file_no,
            source: ArchiveSource::Local(file),
        }))
    }

    pub fn open_cold(
        archive_file_no: ArchiveFileNo,
        cold_storage: Arc<ColdStorage>,
        topic_key: TopicKey,
        file_name: String,
    ) -> Self {
        Self {
            archive_file_no,
            source: ArchiveSource::Cold(ColdArchive {
                cold_storage,
                topic_key,
                file_name,
                toc: Mutex::new(None),
            }),
        }
    }

    async fn get_sub_page_position(
        &self,
        sub_page_id: SubPageId,
    ) -> Result<SubPagePosition, ArchiveStorageError> {
        let toc_offset = self.archive_file_no.get_toc_offset(sub_page_id);

        match &self.source {
            ArchiveSource::Local(file) => {
                let payload = file.read(toc_offset, TOC_STRUCTURE_SIZE).await?;
                Ok(SubPagePosition::parse(payload.as_slice()))
            }
            ArchiveSource::Cold(cold) => {
                let toc = cold.get_toc().await?;
                Ok(SubPagePosition::parse(
                    &toc[toc_offset..toc_offset + TOC_STRUCTURE_SIZE],
                ))
            }
        }
    }

    pub async fn read_sub_page_payload(
        &self,
        sub_page_id: SubPageId,
    ) -> Result<Option<Vec<u8>>, ArchiveStorageError> {
        let pos = self.get_sub_page_position(sub_page_id).await?;

        if pos.is_empty() {
            return Ok(None);
        }

        match &self.source {
            ArchiveSource::Local(file) => {
                let payload = file.read(pos.offset as usize, pos.length as usize).await?;
                Ok(Some(payload))
            }
            ArchiveSource::Cold(cold) => {
                let payload = cold
                    .read_range(pos.offset, pos.offset + pos.length as u64 - 1)
                    .await?;
                Ok(Some(payload))
            }
        }
    }

    /// The compressed payload of a sub page as pieces to read one after another - `None` when the
    /// slot was never written.
    ///
    /// A local payload is read here, whole: it is a file on this disk, and reading it now means
    /// nothing is left to read once the caller lets go of the archive lock. A cold one is only
    /// opened - the TOC lookup and the response head - and its bytes stay on the socket until
    /// the caller asks for them.
    pub async fn open_sub_page_payload(
        &self,
        sub_page_id: SubPageId,
    ) -> Result<Option<SubPagePayloadStream>, ArchiveStorageError> {
        let pos = self.get_sub_page_position(sub_page_id).await?;

        if pos.is_empty() {
            return Ok(None);
        }

        match &self.source {
            ArchiveSource::Local(file) => {
                let payload = file.read(pos.offset as usize, pos.length as usize).await?;
                Ok(Some(SubPagePayloadStream::Local(Some(payload))))
            }
            ArchiveSource::Cold(cold) => {
                let stream = cold
                    .cold_storage
                    .open_range(
                        cold.topic_key.to_ref(),
                        cold.file_name.as_str(),
                        pos.offset,
                        pos.offset + pos.length as u64 - 1,
                    )
                    .await
                    .map_err(ArchiveStorageError::ColdStorageError)?;

                Ok(Some(SubPagePayloadStream::Cold(stream)))
            }
        }
    }

    /// Appends a closed sub page and points its TOC slot at it.
    ///
    /// A sub page is immutable once written: if the slot is already taken the call is a no-op.
    /// The data goes down first and the TOC entry second - the TOC write is the commit point, so
    /// a crash in between leaves an unreferenced tail rather than a dangling pointer.
    pub async fn write_payload(
        &self,
        sub_page_id: SubPageId,
        payload: &[u8],
    ) -> Result<(), ArchiveStorageError> {
        let ArchiveSource::Local(file) = &self.source else {
            return Err(ArchiveStorageError::Frozen);
        };

        let pos = self.get_sub_page_position(sub_page_id).await?;

        if !pos.is_empty() {
            println!(
                "Overwrite payload for sub_page_id: {}",
                sub_page_id.get_value()
            );
            return Ok(());
        }

        let offset = file.append(payload).await?;

        let pos = SubPagePosition {
            offset,
            length: payload.len() as u32,
        };

        file.write(
            self.archive_file_no.get_toc_offset(sub_page_id),
            pos.serialize().as_slice(),
        )
        .await?;

        file.sync().await?;

        Ok(())
    }
}

/// A sub page's compressed payload, handed over a piece at a time.
pub enum SubPagePayloadStream {
    /// Already read from the local file; handed over as the one piece.
    Local(Option<Vec<u8>>),
    /// A ranged GET still on the socket.
    Cold(ColdStorageStream),
}

impl SubPagePayloadStream {
    /// `Ok(None)` once the payload is delivered in full.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, ArchiveStorageError> {
        match self {
            Self::Local(payload) => Ok(payload.take()),
            Self::Cold(stream) => stream
                .next_chunk()
                .await
                .map_err(ArchiveStorageError::ColdStorageError),
        }
    }
}

impl ColdArchive {
    async fn get_toc(&self) -> Result<Arc<Vec<u8>>, ArchiveStorageError> {
        if let Some(toc) = self.toc.lock().as_ref() {
            return Ok(toc.clone());
        }

        // The object is sealed, so this is fetched once and kept for the lifetime of the process.
        let toc = self.read_range(0, TOC_SIZE_IN_BITES as u64 - 1).await?;
        let toc = Arc::new(toc);

        *self.toc.lock() = Some(toc.clone());

        Ok(toc)
    }

    async fn read_range(&self, from: u64, to: u64) -> Result<Vec<u8>, ArchiveStorageError> {
        self.cold_storage
            .download_range(self.topic_key.to_ref(), self.file_name.as_str(), from, to)
            .await
            .map_err(ArchiveStorageError::ColdStorageError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-archive-{}", name));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[tokio::test]
    async fn write_then_read_a_sub_page() {
        let path = temp_path("write_then_read");

        let storage = ArchiveStorage::open_or_create_local(ArchiveFileNo::new(0), &path)
            .await
            .unwrap();

        let sub_page_id = SubPageId::new(3);

        assert!(storage
            .read_sub_page_payload(sub_page_id)
            .await
            .unwrap()
            .is_none());

        storage
            .write_payload(sub_page_id, &[1u8, 2, 3, 4, 5])
            .await
            .unwrap();

        let result = storage.read_sub_page_payload(sub_page_id).await.unwrap();

        assert_eq!(vec![1u8, 2, 3, 4, 5], result.unwrap());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn payload_lands_after_the_reserved_toc() {
        let path = temp_path("payload_after_toc");

        let storage = ArchiveStorage::open_or_create_local(ArchiveFileNo::new(0), &path)
            .await
            .unwrap();

        storage
            .write_payload(SubPageId::new(0), &[9u8; 16])
            .await
            .unwrap();

        let pos = storage
            .get_sub_page_position(SubPageId::new(0))
            .await
            .unwrap();

        assert_eq!(TOC_SIZE as u64, pos.offset);
        assert_eq!(16, pos.length);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn several_sub_pages_keep_their_own_slots() {
        let path = temp_path("several_sub_pages");

        let storage = ArchiveStorage::open_or_create_local(ArchiveFileNo::new(0), &path)
            .await
            .unwrap();

        storage
            .write_payload(SubPageId::new(1), &[1u8; 8])
            .await
            .unwrap();
        storage
            .write_payload(SubPageId::new(7), &[7u8; 32])
            .await
            .unwrap();

        assert_eq!(
            vec![1u8; 8],
            storage
                .read_sub_page_payload(SubPageId::new(1))
                .await
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            vec![7u8; 32],
            storage
                .read_sub_page_payload(SubPageId::new(7))
                .await
                .unwrap()
                .unwrap()
        );
        assert!(storage
            .read_sub_page_payload(SubPageId::new(2))
            .await
            .unwrap()
            .is_none());

        let _ = std::fs::remove_file(&path);
    }

    /// A written sub page is immutable - a second write is dropped, not applied.
    #[tokio::test]
    async fn a_written_sub_page_is_not_overwritten() {
        let path = temp_path("not_overwritten");

        let storage = ArchiveStorage::open_or_create_local(ArchiveFileNo::new(0), &path)
            .await
            .unwrap();

        let sub_page_id = SubPageId::new(0);

        storage.write_payload(sub_page_id, &[1u8; 4]).await.unwrap();
        storage.write_payload(sub_page_id, &[2u8; 4]).await.unwrap();

        assert_eq!(
            vec![1u8; 4],
            storage
                .read_sub_page_payload(sub_page_id)
                .await
                .unwrap()
                .unwrap()
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn reopening_sees_what_was_written() {
        let path = temp_path("reopen");

        {
            let storage = ArchiveStorage::open_or_create_local(ArchiveFileNo::new(0), &path)
                .await
                .unwrap();
            storage
                .write_payload(SubPageId::new(5), &[3u8; 12])
                .await
                .unwrap();
        }

        let storage = ArchiveStorage::open_local_if_exists(ArchiveFileNo::new(0), &path)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            vec![3u8; 12],
            storage
                .read_sub_page_payload(SubPageId::new(5))
                .await
                .unwrap()
                .unwrap()
        );

        let _ = std::fs::remove_file(&path);
    }

    /// The cold read path end to end against a real socket: the TOC is fetched once and cached,
    /// then each sub page is a single ranged GET. This is the only place `ColdArchive` is
    /// exercised at all.
    #[tokio::test]
    async fn a_cold_archive_is_read_over_ranged_gets() {
        use crate::cold_storage::fake_s3::FakeS3;
        use crate::settings::S3ConnectionSettings;
        use crate::topic_key::TopicKeyRef;

        let path = temp_path("cold_read");

        // Build a real archive file locally...
        let local = ArchiveStorage::open_or_create_local(ArchiveFileNo::new(0), &path)
            .await
            .unwrap();
        local
            .write_payload(SubPageId::new(1), &[11u8; 40])
            .await
            .unwrap();
        local
            .write_payload(SubPageId::new(9), &[99u8; 24])
            .await
            .unwrap();
        drop(local);

        // ...upload it and read it back as if it had been sealed and dropped locally
        let fake = FakeS3::start().await;
        let cold_storage = Arc::new(ColdStorage::new(&S3ConnectionSettings {
            endpoint: fake.endpoint.clone(),
            region: "eu-central-1".to_string(),
            access_key: "AKIATEST".to_string(),
            secret_key: "secret".to_string(),
            bucket: "sb-data".to_string(),
            debug: false,
        }));

        let topic_key = TopicKeyRef::new("default", "orders");
        let file_name = "0000000000000000000.archive".to_string();

        cold_storage
            .upload_file(topic_key, file_name.as_str(), path.as_path())
            .await
            .unwrap();

        let cold = ArchiveStorage::open_cold(
            ArchiveFileNo::new(0),
            cold_storage,
            topic_key.to_owned_key(),
            file_name,
        );

        assert_eq!(
            vec![11u8; 40],
            cold.read_sub_page_payload(SubPageId::new(1))
                .await
                .unwrap()
                .unwrap()
        );
        assert_eq!(
            vec![99u8; 24],
            cold.read_sub_page_payload(SubPageId::new(9))
                .await
                .unwrap()
                .unwrap()
        );
        // An untouched slot reads as absent, not as an error
        assert!(cold
            .read_sub_page_payload(SubPageId::new(2))
            .await
            .unwrap()
            .is_none());

        // The TOC is immutable, so it is fetched once: 1 upload + 1 TOC + 2 payload reads.
        // Without the cache this would be one extra round trip per read.
        let gets = fake
            .requests()
            .iter()
            .filter(|itm| itm.starts_with("GET"))
            .count();
        assert_eq!(3, gets);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn open_local_if_exists_returns_none() {
        let path = temp_path("missing");

        assert!(
            ArchiveStorage::open_local_if_exists(ArchiveFileNo::new(0), &path)
                .await
                .unwrap()
                .is_none()
        );

        let _ = std::fs::remove_file(&path);
    }
}
