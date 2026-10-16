use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use my_s3::{S3Client, S3DownloadStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{app::storage_layout, settings::S3ConnectionSettings, topic_key::TopicKeyRef};

/// Read from the file and handed to the request one chunk at a time, so peak memory is a chunk
/// rather than the object. An archive is hundreds of megabytes; reading one whole was an OOM kill
/// in a 512 MB container - and an OOM arrives as SIGKILL, so it left no panic and no log line.
///
/// An upload happens only when an archive seals, so this is a rare burst rather than a hot path -
/// worth a comfortable chunk. Anything past a megabyte or so starts trading the point away again:
/// memory in flight is the chunk times the channel depth.
const UPLOAD_CHUNK_SIZE: usize = 512 * 1024;

/// How many chunks may sit between the reader and the socket. Four is ~2 MB in flight.
const UPLOAD_CHANNEL_SIZE: usize = 4;

/// Generous: it covers pushing the whole body out, not just waiting for the answer.
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(600);

/// A streamed upload is sent exactly once, so retrying is ours to do - and it is safe, because
/// `PutObject` replaces the object atomically.
const UPLOAD_RETRIES: usize = 3;

/// Appended to a file being downloaded until it is complete.
const DOWNLOADING_SUFFIX: &str = ".downloading";

/// The first segment of every key. A bucket is often shared with other services, and this is what
/// keeps our objects under one folder of their own.
const KEY_ROOT: &str = "my-sb-persistence";

/// The cold tier: sealed archives and closed year indexes, uploaded once and read back over ranged
/// GETs. Nothing here is ever modified in place - S3 objects can only be replaced whole, which is
/// exactly why only sealed files get here.
///
/// One bucket, out of the connection string, for every namespace. The key is the local layout under
/// a fixed root:
///
/// ```text
/// /{bucket}/my-sb-persistence/{namespace}/{topic}/{file}
/// ```
pub struct ColdStorage {
    client: S3Client,
    bucket: String,
    /// Set once the bucket has been confirmed, created, or written off for a reason a retry would
    /// not change. After that settling it is a flag check.
    ensured: AtomicBool,
}

impl ColdStorage {
    pub fn new(settings: &S3ConnectionSettings) -> Self {
        // The region argument is `impl Into<S3Region>`: `S3Region` knows the AWS and Hetzner
        // regions by name and keeps anything else as `Other`, so an unfamiliar endpoint still
        // signs correctly.
        let client = S3Client::new(
            settings.access_key.clone(),
            settings.secret_key.clone(),
            settings.region.clone(),
            settings.endpoint.clone(),
        );

        // `Debug=1` in the connection string. Every request is then traced to stdout - verb, url
        // and body size going out, and the whole answer body when it failed, which is the
        // `<Error><Code>` saying why. A successful answer is printed as a size, since it is the
        // archive that was just downloaded. The `Authorization` header is never printed.
        let client = if settings.debug {
            println!("S3 request tracing is ON (Debug in s3_conn_string)");
            client.debug_to_console()
        } else {
            client
        };

        Self {
            client,
            bucket: settings.bucket.clone(),
            ensured: AtomicBool::new(false),
        }
    }

    /// `my-sb-persistence/{namespace}/{topic}/{file}`
    fn get_key(topic_key: TopicKeyRef<'_>, file_name: &str) -> String {
        format!(
            "{}/{}",
            KEY_ROOT,
            storage_layout::get_relative_path(topic_key, file_name)
        )
    }

    /// Makes sure the bucket is there unless this process already did. **Best effort - it never
    /// fails the caller.**
    ///
    /// Every operation goes through it; after the first settling it is a flag check.
    ///
    /// Not being able to *create* a bucket says very little about being able to *use* it, which is
    /// why a failure here is reported and stepped over rather than raised. An access key scoped to
    /// one bucket is routinely denied `CreateBucket` while reading and writing inside that bucket
    /// perfectly well, and a bucket made by hand ahead of time is the normal case in a managed
    /// deployment. So the operation goes ahead: if the bucket really is unusable, the upload or the
    /// read says so on its own terms, about the file it was actually working on.
    pub async fn ensure_bucket(&self) {
        if self.ensured.load(Ordering::Relaxed) {
            return;
        }

        let bucket = self.bucket.as_str();

        if let Err(err) = validate_bucket_name(bucket) {
            // Nothing will work with this name, but that is the operator's to fix, and shouting
            // about it on every upload would bury it.
            self.report_bucket_problem("use", err.as_str(), false);
            return;
        }

        // The name is a fixed one out of the connection string: it is made once and then used
        // forever, so the question worth asking is whether it is *there*, not whether we can make
        // it. `HEAD /{bucket}` answers that in one round trip, and it is the question a scoped key
        // can actually answer - such a key is routinely allowed to use its one bucket while being
        // denied `CreateBucket`, which would otherwise log a permission error on every start of a
        // perfectly healthy deployment.
        match self.client.check_if_bucket_exists(bucket).await {
            Ok(true) => {
                println!("Cold storage bucket '{}' is there", bucket);
                self.ensured.store(true, Ordering::Relaxed);
                return;
            }

            // Not there at all - a first start against empty storage. Fall through and make it.
            Ok(false) => {}

            // A `HEAD` has no body, so there is no `<Error><Code>` to go on: a 403 here means
            // either the name is somebody else's or these credentials are wrong, and both are
            // worth saying out loud rather than papering over with a `CreateBucket` that would
            // fail differently.
            Err(err) => {
                self.report_bucket_problem(
                    "check",
                    format!("{:?}", err).as_str(),
                    err.is_retryable(),
                );
                return;
            }
        }

        // `create_bucket_if_not_exists` absorbs `BucketAlreadyOwnedByYou` - the answer when the
        // bucket appeared between the check and the create.
        let err = match self.client.create_bucket_if_not_exists(bucket).await {
            Ok(_) => {
                println!("Cold storage bucket '{}' is ready", bucket);
                self.ensured.store(true, Ordering::Relaxed);
                return;
            }
            Err(err) => err,
        };

        // `BucketAlreadyExists` wears similar words but means the opposite: the name is held by
        // *another account*. A bucket name is unique across every customer of the provider, so it
        // points at the name in `s3_conn_string` rather than at anything transient.
        let message = if err.is_bucket_already_exists() {
            format!(
                "the name belongs to another account - bucket names are unique across every customer of the provider, so check the one in s3_conn_string ({:?})",
                err
            )
        } else {
            format!("{:?}", err)
        };

        // A transient failure is worth another go on the next operation; a deterministic one -
        // denied permission, a name that is somebody else's - would only repeat itself, and
        // retrying it on every single upload turns one problem into a flood of requests.
        self.report_bucket_problem("create", message.as_str(), err.is_retryable());
    }

    fn report_bucket_problem(&self, what_failed: &str, message: &str, retry_later: bool) {
        let tail = if retry_later {
            "Going on without it - it will be tried again on the next operation."
        } else {
            "Going on without it - the reason is not one a retry would change, so it will not be tried again until a restart."
        };

        my_logger::LOGGER.write_error(
            "ColdStorage::ensure_bucket",
            format!(
                "Can not {} the cold storage bucket '{}': {}. {}",
                what_failed, self.bucket, message, tail
            ),
            my_logger::LogEventCtx::new().add("bucket", self.bucket.as_str()),
        );

        if !retry_later {
            self.ensured.store(true, Ordering::Relaxed);
        }
    }

    /// Streams a file up, one chunk at a time - the whole point being that memory does not depend
    /// on the size of the object.
    ///
    /// Each retry reopens the file from the beginning: a streamed body is consumed as it is sent,
    /// so a half-drained reader can not be reused. `PutObject` replaces the object atomically, so a
    /// failed attempt leaves either the previous object or nothing, never a partial one.
    pub async fn upload_file(
        &self,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
        path: &Path,
    ) -> Result<(), String> {
        self.ensure_bucket().await;

        let key = Self::get_key(topic_key, file_name);

        let content_length = tokio::fs::metadata(path)
            .await
            .map_err(|err| format!("Can not size {:?}: {}", path, err))?
            .len() as usize;

        let path = path.to_path_buf();

        self.client
            .upload_streamed_with_retries(
                self.bucket.as_str(),
                key.as_str(),
                content_length,
                UPLOAD_TIMEOUT,
                UPLOAD_RETRIES,
                || {
                    let (sender, receiver) = tokio::sync::mpsc::channel(UPLOAD_CHANNEL_SIZE);
                    let path = path.clone();

                    tokio::spawn(async move {
                        let Ok(mut file) = tokio::fs::File::open(path.as_path()).await else {
                            return;
                        };

                        let mut buffer = vec![0u8; UPLOAD_CHUNK_SIZE];

                        while let Ok(read) = file.read(&mut buffer).await {
                            if read == 0 {
                                break;
                            }

                            if sender.send(buffer[..read].to_vec()).await.is_err() {
                                break;
                            }
                        }
                    });

                    receiver
                },
            )
            .await
            .map_err(|err| format!("{:?}", err))
    }

    /// `from`/`to` are inclusive byte offsets, as in the HTTP `Range` header. Read whole - for
    /// what is small and needed whole, like an archive's TOC.
    pub async fn download_range(
        &self,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<u8>, String> {
        let mut stream = self.open_range(topic_key, file_name, from, to).await?;

        let mut result = Vec::with_capacity((to - from + 1) as usize);

        while let Some(chunk) = stream.next_chunk().await? {
            result.extend_from_slice(chunk.as_slice());
        }

        Ok(result)
    }

    /// The same range, handed over a chunk at a time as it comes off the socket - peak memory is
    /// a chunk, not the range. `from`/`to` are inclusive.
    pub async fn open_range(
        &self,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
        from: u64,
        to: u64,
    ) -> Result<ColdStorageStream, String> {
        self.ensure_bucket().await;

        let key = Self::get_key(topic_key, file_name);

        let stream = self
            .client
            .download_file_range(self.bucket.as_str(), key.as_str(), from, Some(to))
            .await
            .map_err(|err| format!("{:?}", err))?;

        Ok(ColdStorageStream { stream })
    }

    /// Streams a whole object into `path`. `Ok(false)` when there is no such object.
    ///
    /// Written next to `path` and renamed into place only once the whole object is down, so an
    /// interrupted download never leaves a short file under the real name - the uploader would
    /// take that for a sealed file and send it up over the complete one.
    pub async fn download_to_file(
        &self,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
        path: &Path,
    ) -> Result<bool, String> {
        self.ensure_bucket().await;

        let key = Self::get_key(topic_key, file_name);

        let mut stream = match self
            .client
            .download_file(self.bucket.as_str(), key.as_str())
            .await
        {
            Ok(stream) => stream,
            Err(err) => {
                if err.is_key_not_found() {
                    return Ok(false);
                }

                return Err(format!("{:?}", err));
            }
        };

        // Not a name the uploader recognises, so a leftover of a crash is never sent up.
        let mut partial = path.as_os_str().to_owned();
        partial.push(DOWNLOADING_SUFFIX);
        let partial = PathBuf::from(partial);

        if let Some(folder) = path.parent() {
            tokio::fs::create_dir_all(folder)
                .await
                .map_err(|err| format!("Can not create {:?}: {}", folder, err))?;
        }

        let mut file = tokio::fs::File::create(partial.as_path())
            .await
            .map_err(|err| format!("Can not create {:?}: {}", partial, err))?;

        while let Some(chunk) = stream
            .get_next_chunk()
            .await
            .map_err(|err| format!("{:?}", err))?
        {
            file.write_all(chunk.as_slice())
                .await
                .map_err(|err| format!("Can not write {:?}: {}", partial, err))?;
        }

        file.sync_all()
            .await
            .map_err(|err| format!("Can not flush {:?}: {}", partial, err))?;

        drop(file);

        tokio::fs::rename(partial.as_path(), path)
            .await
            .map_err(|err| format!("Can not rename {:?} to {:?}: {}", partial, path, err))?;

        Ok(true)
    }

    /// `HEAD` on the object - nothing is downloaded.
    pub async fn exists(
        &self,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
    ) -> Result<bool, String> {
        self.ensure_bucket().await;

        let key = Self::get_key(topic_key, file_name);

        match self
            .client
            .get_object_size(self.bucket.as_str(), key.as_str())
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.is_key_not_found() {
                    return Ok(false);
                }

                Err(format!("{:?}", err))
            }
        }
    }

    pub async fn delete(&self, topic_key: TopicKeyRef<'_>, file_name: &str) -> Result<(), String> {
        self.ensure_bucket().await;

        let key = Self::get_key(topic_key, file_name);

        match self
            .client
            .delete_file(self.bucket.as_str(), key.as_str())
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.is_key_not_found() {
                    return Ok(());
                }

                Err(format!("{:?}", err))
            }
        }
    }
}

/// A ranged download still on the socket. Read it to the end and the connection goes back to the
/// pool; dropped early, the connection is closed.
pub struct ColdStorageStream {
    stream: S3DownloadStream,
}

impl ColdStorageStream {
    /// The next piece as it came off the socket - its size is the socket's, never a record
    /// boundary. `Ok(None)` once the whole range is delivered; a range that ends short is an
    /// error, not the end.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, String> {
        self.stream
            .get_next_chunk()
            .await
            .map_err(|err| format!("{:?}", err))
    }
}

/// S3 bucket naming: 3-63 chars, lowercase letters, digits and hyphens, starting and ending on a
/// letter or a digit. The name is typed by hand into the connection string, so a typo is caught
/// here rather than as an unexplained failure of the first upload.
fn validate_bucket_name(bucket: &str) -> Result<(), String> {
    let invalid = |reason: &str| {
        Err(format!(
            "'{}' is not a valid bucket name: {}",
            bucket, reason
        ))
    };

    if bucket.len() < 3 || bucket.len() > 63 {
        return invalid("it must be 3 to 63 chars long");
    }

    for value in bucket.chars() {
        let is_valid = value.is_ascii_lowercase() || value.is_ascii_digit() || value == '-';

        if !is_valid {
            return invalid("only lowercase letters, digits and hyphens are allowed");
        }
    }

    let starts_ok = bucket
        .chars()
        .next()
        .map(|itm| itm.is_ascii_alphanumeric())
        .unwrap_or(false);
    let ends_ok = bucket
        .chars()
        .next_back()
        .map(|itm| itm.is_ascii_alphanumeric())
        .unwrap_or(false);

    if !starts_ok || !ends_ok {
        return invalid("it must start and end with a letter or a digit");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cold_storage::fake_s3::FakeS3;
    use crate::settings::S3ConnectionSettings;

    const BUCKET: &str = "sb-data";

    fn orders(namespace: &str) -> TopicKeyRef<'_> {
        TopicKeyRef::new(namespace, "orders")
    }

    fn cold_storage_over(fake: &FakeS3) -> ColdStorage {
        ColdStorage::new(&S3ConnectionSettings {
            endpoint: fake.endpoint.clone(),
            region: "eu-central-1".to_string(),
            access_key: "AKIATEST".to_string(),
            secret_key: "secret".to_string(),
            bucket: BUCKET.to_string(),
            debug: false,
        })
    }

    async fn connect() -> (FakeS3, ColdStorage) {
        let fake = FakeS3::start().await;
        let cold_storage = cold_storage_over(&fake);
        (fake, cold_storage)
    }

    fn count_requests(fake: &FakeS3, request: &str) -> usize {
        fake.requests()
            .iter()
            .filter(|itm| itm.as_str() == request)
            .count()
    }

    fn temp_file(name: &str, content: &[u8]) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-upload-{}", name));
        std::fs::write(&path, content).unwrap();
        path
    }

    /// Through `download_to_file`, the only whole-object read there is.
    async fn download(
        cold_storage: &ColdStorage,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
    ) -> Option<Vec<u8>> {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-download-{}", file_name));

        if !cold_storage
            .download_to_file(topic_key, file_name, path.as_path())
            .await
            .unwrap()
        {
            return None;
        }

        let result = std::fs::read(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        Some(result)
    }

    /// A download lands under its real name only once it is complete - nothing is left next to
    /// it, and a file already there is replaced whole.
    #[tokio::test]
    async fn a_download_to_file_replaces_the_file_whole() {
        let (_fake, cold_storage) = connect().await;

        let content: Vec<u8> = (0..UPLOAD_CHUNK_SIZE * 2 + 3)
            .map(|itm| (itm % 241) as u8)
            .collect();
        let source = temp_file("download_to_file_source", content.as_slice());

        cold_storage
            .upload_file(orders("default"), ".2025.yearindex", source.as_path())
            .await
            .unwrap();

        let mut dest = std::env::temp_dir();
        dest.push("my-sb-persistence-download-to-file");
        let _ = std::fs::remove_dir_all(&dest);
        std::fs::create_dir_all(&dest).unwrap();
        let path = dest.join(".2025.yearindex");
        std::fs::write(&path, [1u8; 10]).unwrap();

        assert!(cold_storage
            .download_to_file(orders("default"), ".2025.yearindex", path.as_path())
            .await
            .unwrap());

        assert_eq!(content, std::fs::read(&path).unwrap());
        assert_eq!(1, std::fs::read_dir(&dest).unwrap().count());

        assert!(!cold_storage
            .download_to_file(
                orders("default"),
                ".2024.yearindex",
                dest.join("x").as_path()
            )
            .await
            .unwrap());
        assert!(!dest.join("x").exists());

        let _ = std::fs::remove_dir_all(&dest);
        let _ = std::fs::remove_file(&source);
    }

    /// The whole round trip against a real socket: streamed upload, ranged read, whole read,
    /// exists, delete. This is the only place the `Range` header, 206, 204 and 404 are exercised.
    #[tokio::test]
    async fn upload_read_range_and_delete() {
        let (fake, cold_storage) = connect().await;

        let content: Vec<u8> = (0..=255u8).collect();
        let path = temp_file("round_trip", content.as_slice());
        let file_name = "0000000000000000000.archive";

        cold_storage
            .upload_file(orders("default"), file_name, path.as_path())
            .await
            .unwrap();

        assert_eq!(
            Some(content.clone()),
            download(&cold_storage, orders("default"), file_name).await
        );

        // Inclusive offsets, the way the archive TOC and a sub page are fetched
        let chunk = cold_storage
            .download_range(orders("default"), file_name, 10, 19)
            .await
            .unwrap();
        assert_eq!(content[10..=19].to_vec(), chunk);

        assert!(cold_storage
            .exists(orders("default"), file_name)
            .await
            .unwrap());
        assert!(!cold_storage
            .exists(orders("default"), "nope.archive")
            .await
            .unwrap());

        cold_storage
            .delete(orders("default"), file_name)
            .await
            .unwrap();
        assert_eq!(
            None,
            download(&cold_storage, orders("default"), file_name).await
        );
        // Deleting what is not there is fine
        cold_storage
            .delete(orders("default"), file_name)
            .await
            .unwrap();

        assert!(fake.object_paths().is_empty());

        let _ = std::fs::remove_file(&path);
    }

    /// The reason the streaming path exists: an object many chunks long has to arrive byte for
    /// byte, without the sender ever holding it whole.
    #[tokio::test]
    async fn a_multi_chunk_file_arrives_intact() {
        let (fake, cold_storage) = connect().await;

        // Several times UPLOAD_CHUNK_SIZE, with a pattern that would expose a lost or reordered
        // chunk rather than just a wrong length
        let content: Vec<u8> = (0..UPLOAD_CHUNK_SIZE * 3 + 7)
            .map(|itm| (itm % 251) as u8)
            .collect();

        let path = temp_file("multi_chunk", content.as_slice());
        let file_name = "0000000000000000001.archive";

        cold_storage
            .upload_file(orders("default"), file_name, path.as_path())
            .await
            .unwrap();

        assert_eq!(
            Some(content.clone()),
            download(&cold_storage, orders("default"), file_name).await
        );
        assert_eq!(
            content.len(),
            fake.get_object(
                "/sb-data/my-sb-persistence/default/orders/0000000000000000001.archive"
            )
            .unwrap()
            .len()
        );

        let _ = std::fs::remove_file(&path);
    }

    /// One bucket for everything: the key is the fixed root, then the namespace, then the topic.
    #[tokio::test]
    async fn every_namespace_goes_into_the_one_bucket_under_the_root() {
        let (fake, cold_storage) = connect().await;

        let path = temp_file("layout", &[1, 2, 3]);

        cold_storage
            .upload_file(orders("alpha"), ".2025.yearindex", path.as_path())
            .await
            .unwrap();
        cold_storage
            .upload_file(orders("default"), ".2025.yearindex", path.as_path())
            .await
            .unwrap();

        assert_eq!(
            vec![
                "/sb-data/my-sb-persistence/alpha/orders/.2025.yearindex".to_string(),
                "/sb-data/my-sb-persistence/default/orders/.2025.yearindex".to_string(),
            ],
            fake.object_paths()
        );

        // Two namespaces, still the one bucket settled once
        assert_eq!(1, count_requests(&fake, "PUT /sb-data"));

        let _ = std::fs::remove_file(&path);
    }

    /// The bucket already exists because a previous run created it - or because an operator did,
    /// by hand, before the first start. A fresh process finds it with the `HEAD` and never tries to
    /// create it again.
    ///
    /// A second `ColdStorage` over the same server is what makes this a restart rather than a
    /// repeat: within one instance `ensure_bucket` short-circuits on its own flag.
    #[tokio::test]
    async fn a_bucket_that_already_exists_is_accepted() {
        let (fake, first_run) = connect().await;

        first_run.ensure_bucket().await;

        let restarted = cold_storage_over(&fake);

        restarted.ensure_bucket().await;

        // ...and it is usable, not merely accepted
        let path = temp_file("after_restart", &[1, 2, 3]);
        restarted
            .upload_file(orders("default"), "active", path.as_path())
            .await
            .unwrap();

        assert_eq!(2, count_requests(&fake, "HEAD /sb-data"));
        assert_eq!(1, count_requests(&fake, "PUT /sb-data"));

        let _ = std::fs::remove_file(&path);
    }

    /// The name belongs to somebody else, because bucket names are unique across every customer of
    /// the provider. The `HEAD` answers 403.
    ///
    /// It is reported and stepped over, not raised - being unable to check a bucket says little
    /// about being able to use one. What must not happen is a retry: the answer is deterministic,
    /// so asking again on every upload would only multiply the requests.
    #[tokio::test]
    async fn a_bucket_owned_by_another_account_is_reported_and_stepped_over() {
        let (fake, cold_storage) = connect().await;

        fake.claim_bucket_for_another_account(BUCKET);

        cold_storage.ensure_bucket().await;
        cold_storage.ensure_bucket().await;

        assert_eq!(
            vec!["HEAD /sb-data".to_string()],
            fake.requests(),
            "a deterministic failure must not be retried"
        );

        // And the work goes on: the upload is attempted all the same
        let path = temp_file("foreign_bucket", &[1, 2, 3]);

        cold_storage
            .upload_file(orders("default"), "active", path.as_path())
            .await
            .unwrap();

        assert_eq!(
            vec!["/sb-data/my-sb-persistence/default/orders/active".to_string()],
            fake.object_paths()
        );

        let _ = std::fs::remove_file(&path);
    }

    /// The opposite half of the same rule: a 503 says nothing about the bucket, only about the
    /// moment, so the next operation tries again rather than writing the bucket off until restart.
    #[tokio::test]
    async fn a_transient_failure_is_tried_again() {
        let (fake, cold_storage) = connect().await;

        fake.fail_bucket_creation_times(BUCKET, 1);

        cold_storage.ensure_bucket().await;
        cold_storage.ensure_bucket().await;

        assert_eq!(2, count_requests(&fake, "PUT /sb-data"));

        // The second attempt got through, so a third one is the flag check again
        cold_storage.ensure_bucket().await;

        assert_eq!(2, count_requests(&fake, "PUT /sb-data"));
    }

    /// `HEAD /{bucket}` rather than an attempt to create what is already there. That is not a
    /// micro-optimisation: a key scoped to this one bucket is routinely allowed to use it and
    /// denied `CreateBucket`, so asking the other question would log a permission error on every
    /// start of a healthy deployment.
    #[tokio::test]
    async fn an_existing_bucket_is_checked_not_created() {
        let (fake, cold_storage) = connect().await;

        fake.create_bucket(BUCKET);

        cold_storage.ensure_bucket().await;

        assert_eq!(vec!["HEAD /sb-data".to_string()], fake.requests());

        // Confirmed once, then it is a flag check
        cold_storage.ensure_bucket().await;

        assert_eq!(1, fake.requests().len());
    }

    /// A first start against empty storage still has to make the bucket - the check is what comes
    /// first, not what replaces creating it.
    #[tokio::test]
    async fn a_missing_bucket_is_created() {
        let (fake, cold_storage) = connect().await;

        cold_storage.ensure_bucket().await;

        assert_eq!(
            vec!["HEAD /sb-data".to_string(), "PUT /sb-data".to_string()],
            fake.requests()
        );
    }

    #[test]
    fn an_unusable_bucket_name_is_refused_before_it_is_created() {
        assert!(validate_bucket_name("sb-alpha").is_ok());
        assert!(validate_bucket_name("sb-alpha-").is_err());
        assert!(validate_bucket_name("sb").is_err());
        assert!(validate_bucket_name("sb-Alpha").is_err());
        assert!(validate_bucket_name("-sb-alpha").is_err());
    }
}
