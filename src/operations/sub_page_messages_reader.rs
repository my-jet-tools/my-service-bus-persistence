use std::sync::Arc;

use my_logger::LogEventCtx;
use my_service_bus::shared::{protobuf_models::MessageProtobufModel, sub_page::SubPageId};

use crate::{
    app::AppContext,
    archive_storage::{ArchiveStorageError, SubPagePayloadStream},
    message_pages::{CompressedMessagesDecoder, DecodedItem},
    topic_key::TopicKeyRef,
};

/// The storage could not be read. Unlike a sub page that is simply not there, this says nothing
/// about the data - it is the disk or the cold tier answering badly right now - so it is handed
/// to the caller to report, never passed off as an empty sub page.
#[derive(Debug)]
pub struct ReadSubPageError {
    pub sub_page_id: SubPageId,
    pub err: ArchiveStorageError,
}

/// The messages of one sub page, one at a time, in the order they were stored.
///
/// A sub page in memory is read from there. An archived one is never materialized: its payload
/// comes off the disk or the cold tier in pieces, is inflated a step at a time, and every message
/// is handed out as soon as its record is complete - so a reader holds about one message rather
/// than the sub page, and the first message goes out before the last one has been downloaded.
pub enum SubPageMessagesReader {
    InMemory(std::vec::IntoIter<Arc<MessageProtobufModel>>),
    Archived(ArchivedSubPage),
    Missing,
}

pub struct ArchivedSubPage {
    sub_page_id: SubPageId,
    topic_key: String,
    payload: SubPagePayloadStream,
    decoder: CompressedMessagesDecoder,
}

impl SubPageMessagesReader {
    pub async fn open(
        app: &AppContext,
        topic_key: TopicKeyRef<'_>,
        sub_page_id: SubPageId,
    ) -> Result<Self, ReadSubPageError> {
        let Some(topic) = app.topics_list.get(topic_key) else {
            return Ok(Self::Missing);
        };

        if let Some(sub_page) = topic.pages_list.get(sub_page_id).await {
            let messages = sub_page.get_all_messages().await.into_messages();
            return Ok(Self::InMemory(messages.into_iter()));
        }

        let payload = {
            // Spans the open only: once the payload is opened nothing depends on the local file
            // any more - a local one has been read whole, a cold one is an object that can not
            // change. Holding it while the messages are streamed out would hold the archiver's
            // delete - and every save queued behind it - for as long as the slowest consumer
            // takes to read.
            let _guard = app.archive_locks.read(topic_key).await;

            let Some(archive) = app
                .archive_storage_list
                .try_get_or_open(sub_page_id.into(), topic_key, app)
                .await
            else {
                return Ok(Self::Missing);
            };

            archive
                .open_sub_page_payload(sub_page_id)
                .await
                .map_err(|err| ReadSubPageError { sub_page_id, err })?
        };

        let Some(payload) = payload else {
            return Ok(Self::Missing);
        };

        Ok(Self::Archived(ArchivedSubPage {
            sub_page_id,
            topic_key: topic_key.to_string(),
            payload,
            decoder: CompressedMessagesDecoder::new(),
        }))
    }

    /// `Ok(None)` once the sub page is read to the end.
    pub async fn next(&mut self) -> Result<Option<Arc<MessageProtobufModel>>, ReadSubPageError> {
        match self {
            Self::InMemory(messages) => Ok(messages.next()),
            Self::Archived(archived) => archived.next().await,
            Self::Missing => Ok(None),
        }
    }
}

impl ArchivedSubPage {
    async fn next(&mut self) -> Result<Option<Arc<MessageProtobufModel>>, ReadSubPageError> {
        loop {
            let item = match self.decoder.next() {
                Ok(item) => item,
                Err(err) => {
                    self.report_undecodable(format!("{:?}", err));
                    return Ok(None);
                }
            };

            match item {
                DecodedItem::Message(message) => return Ok(Some(Arc::new(message))),
                DecodedItem::Finished => return Ok(None),
                DecodedItem::NeedMoreInput => {
                    let chunk =
                        self.payload
                            .next_chunk()
                            .await
                            .map_err(|err| ReadSubPageError {
                                sub_page_id: self.sub_page_id,
                                err,
                            })?;

                    match chunk {
                        Some(chunk) => self.decoder.add_compressed(chunk.as_slice()),
                        None => {
                            // Every byte the TOC points at is in, and the zlib stream has not
                            // ended. A transfer that broke off is an error of `next_chunk`, so
                            // this is the stored payload itself being short.
                            self.report_undecodable(
                                "the payload ends before its zlib stream does".to_string(),
                            );
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }

    /// The bytes arrived, and they are not a sub page. Reading them again would give the same
    /// answer, so this is not reported to the caller as a failure: what was decoded up to here is
    /// delivered and the rest of the sub page reads as missing - which is what an undecodable sub
    /// page has always read as.
    fn report_undecodable(&self, reason: String) {
        my_logger::LOGGER.write_error(
            "SubPageMessagesReader",
            format!("Can not decode an archived sub page: {}", reason),
            LogEventCtx::new()
                .add("topicId", self.topic_key.to_string())
                .add("subPageId", self.sub_page_id.get_value().to_string()),
        );
    }
}

#[cfg(test)]
mod tests {
    use my_service_bus::abstractions::MessageId;
    use rust_extensions::{date_time::DateTimeAsMicroseconds, MyTimerTick};

    use super::*;
    use crate::{
        cold_storage::fake_s3::FakeS3, settings::SettingsModel,
        timers::cold_storage_uploader::ColdStorageUploaderTimer,
    };

    fn orders() -> TopicKeyRef<'static> {
        TopicKeyRef::new("default", "orders")
    }

    async fn app(name: &str, s3_endpoint: Option<&str>) -> Arc<AppContext> {
        let mut data_folder = std::env::temp_dir();
        data_folder.push(format!("my-sb-persistence-sub-page-reader-{}", name));
        let _ = std::fs::remove_dir_all(&data_folder);
        std::fs::create_dir_all(&data_folder).unwrap();

        let app = AppContext::new(SettingsModel {
            data: data_folder.to_str().unwrap().to_string(),
            max_response_records_amount: 100,
            delete_topic_secret_key: String::new(),
            listen_unix_socket: None,
            s3_conn_string: s3_endpoint.map(|endpoint| {
                format!(
                    "Endpoint={};Region=eu-central-1;AccessKey=AKIATEST;SecretKey=secret;Bucket=sb-data",
                    endpoint
                )
            }),
            legacy: None,
        })
        .await;

        Arc::new(app)
    }

    async fn publish(app: &AppContext, ids: impl Iterator<Item = i64>) {
        let messages = ids.map(|id| {
            MessageProtobufModel::new(
                MessageId::new(id),
                DateTimeAsMicroseconds::now(),
                vec![(id % 251) as u8; 300],
                vec![],
            )
        });

        crate::operations::new_messages(app, orders(), messages).await;
    }

    /// Sub page 5 gets archived into archive 0. Sub page 10_000 is archived too, so archive 1
    /// exists and archive 0 is sealed; 10_001 stays open as the tail.
    async fn archive_sub_page_5(app: &Arc<AppContext>) {
        publish(app, 5_000..5_400).await;
        publish(app, 10_000_000..10_000_001).await;
        publish(app, 10_001_000..10_001_001).await;

        let topic_data = app.topics_list.get(orders()).unwrap();
        crate::operations::gc_pages(app.as_ref(), topic_data)
            .await
            .unwrap();
    }

    async fn read_all(
        app: &AppContext,
        sub_page_id: SubPageId,
    ) -> Result<Vec<i64>, ReadSubPageError> {
        let mut reader = SubPageMessagesReader::open(app, orders(), sub_page_id).await?;
        let mut result = Vec::new();

        while let Some(message) = reader.next().await? {
            assert_eq!(
                vec![(message.get_message_id().get_value() % 251) as u8; 300],
                message.data
            );
            result.push(message.get_message_id().get_value());
        }

        Ok(result)
    }

    #[tokio::test]
    async fn an_archived_sub_page_is_read_message_by_message() {
        let app = app("local", None).await;
        archive_sub_page_5(&app).await;

        let topic_data = app.topics_list.get(orders()).unwrap();
        assert!(topic_data.pages_list.get(SubPageId::new(5)).await.is_none());

        assert_eq!(
            (5_000..5_400).collect::<Vec<_>>(),
            read_all(&app, SubPageId::new(5)).await.unwrap()
        );

        // Nothing stored there
        assert!(read_all(&app, SubPageId::new(6)).await.unwrap().is_empty());
    }

    /// From the cold tier the sub page is one ranged GET, decoded as it streams - the archive is
    /// never fetched whole.
    #[tokio::test]
    async fn a_cold_sub_page_is_streamed_from_one_ranged_get() {
        let fake = FakeS3::start().await;
        let app = app("cold", Some(fake.endpoint.as_str())).await;
        archive_sub_page_5(&app).await;

        ColdStorageUploaderTimer::new(app.clone()).tick().await;

        let key = "/sb-data/my-sb-persistence/default/orders/0000000000000000000.archive";
        assert!(fake.get_object(key).is_some());

        assert_eq!(
            (5_000..5_400).collect::<Vec<_>>(),
            read_all(&app, SubPageId::new(5)).await.unwrap()
        );

        let archive_gets = fake
            .requests()
            .into_iter()
            .filter(|itm| itm.as_str() == format!("GET {}", key))
            .count();

        // The TOC, then the sub page - both ranged; there is no whole-object read
        assert_eq!(2, archive_gets);
    }

    /// The cold tier failing is an error, never an empty sub page.
    #[tokio::test]
    async fn a_failing_cold_tier_is_an_error_not_an_empty_sub_page() {
        let fake = FakeS3::start().await;
        let app = app("cold-503", Some(fake.endpoint.as_str())).await;
        archive_sub_page_5(&app).await;

        ColdStorageUploaderTimer::new(app.clone()).tick().await;

        // Opened once, so the TOC is cached and the failure lands on the sub page itself
        read_all(&app, SubPageId::new(5)).await.unwrap();

        fake.fail_gets(true);

        let err = read_all(&app, SubPageId::new(5)).await.unwrap_err();
        assert_eq!(5, err.sub_page_id.get_value());

        fake.fail_gets(false);

        assert_eq!(400, read_all(&app, SubPageId::new(5)).await.unwrap().len());
    }
}
