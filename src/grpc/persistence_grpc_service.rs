use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcService;
use crate::persistence_grpc::*;
use crate::topic_key::{NamespaceError, TopicKey, TopicKeyRef};
use crate::topics_snapshot::TopicSnapshotProtobufModel;

use my_grpc_extensions::{server::*, StreamedRequestReader, StreamedResponseWriter};
use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::page_id::PageId;
use my_service_bus::shared::sub_page::SubPageId;

use super::contracts;

use super::server::MyServicePersistenceGrpc;

const MAX_PAYLOAD_SIZE: usize = 1024 * 1024 * 4;

#[tonic::async_trait]
impl MyServiceBusMessagesPersistenceGrpcService for MyServicePersistenceGrpc {
    generate_server_stream!(stream_name:"GetQueueSnapshotStream", item_name:"TopicAndQueuesSnapshotGrpcModel");
    async fn get_queue_snapshot(
        &self,
        _: tonic::Request<()>,
    ) -> Result<tonic::Response<Self::GetQueueSnapshotStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let result = self.app.topics_snapshot.get().await;

        my_grpc_extensions::grpc_server_streams::send_from_iterator(
            result.snapshot.data.into_iter(),
        )
        .await
    }

    async fn save_queue_snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<TopicAndQueuesSnapshotGrpcModel>>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let stream = request.into_inner();

        let values = StreamedRequestReader::new(stream);

        // One stream carries every namespace at once - that is how the node restores and saves
        // everything in a single call.
        let grpc_models: Vec<TopicAndQueuesSnapshotGrpcModel> = values.into_vec().await?;

        let mut snapshot: Vec<TopicSnapshotProtobufModel> = Vec::with_capacity(grpc_models.len());

        for grpc_model in grpc_models {
            let topic_snapshot: TopicSnapshotProtobufModel =
                grpc_model.try_into().map_err(|err: NamespaceError| {
                    tonic::Status::invalid_argument(format!("Invalid Namespace. {}", err))
                })?;

            contracts::check_namespace_is_supported(topic_snapshot.get_namespace())?;
            contracts::check_topic_id(topic_snapshot.topic_id.as_str())?;

            snapshot.push(topic_snapshot);
        }

        contracts::check_flags(self.app.as_ref())?;

        self.app.topics_snapshot.update(snapshot).await;

        Ok(tonic::Response::new(()))
    }

    async fn get_version(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<GetVersionGrpcResponse>, tonic::Status> {
        let result = GetVersionGrpcResponse {
            version: crate::app::APP_VERSION.to_string(),
        };

        return Ok(tonic::Response::new(result));
    }

    async fn get_message(
        &self,
        request: tonic::Request<GetMessageGrpcRequest>,
    ) -> Result<tonic::Response<MessageContentGrpcModel>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let namespace = contracts::get_namespace(req.namespace)?;
        contracts::check_topic_id(req.topic_id.as_str())?;

        let message = crate::operations::get_message_by_id(
            self.app.as_ref(),
            TopicKeyRef::new(namespace.as_str(), req.topic_id.as_str()),
            req.message_id.into(),
        )
        .await;

        let message = match message {
            Ok(message) => message,
            // An unknown topic is a business answer, not a failure: it reads the same as
            // "no such message". Panicking here would tear the stream down with RST_STREAM.
            Err(crate::operations::OperationError::TopicNotFound(_)) => None,
            Err(err) => {
                return Err(tonic::Status::internal(format!(
                    "get_message failed: {:?}",
                    err
                )))
            }
        };

        let result = match message {
            Some(msg) => msg.as_ref().into(),
            None => MessageContentGrpcModel {
                created: 0,
                data: Vec::new(),
                meta_data: Vec::new(),
                message_id: -1,
            },
        };

        return Ok(tonic::Response::new(result));
    }

    generate_server_stream!(stream_name:"GetPageCompressedStream", item_name:"CompressedMessageChunkModel");
    async fn get_page_compressed(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetPageCompressedGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetPageCompressedStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let namespace = contracts::get_namespace(req.namespace)?;
        contracts::check_topic_id(req.topic_id.as_str())?;

        let app = self.app.clone();

        let page_id = PageId::new(req.page_no);

        let mut from_message_id = page_id.get_first_message_id();
        let mut to_message_id = page_id.get_last_message_id();

        if req.from_message_id > 0 && req.to_message_id > 0 {
            from_message_id = MessageId::new(req.from_message_id);
            to_message_id = MessageId::new(req.to_message_id);
        }

        let compressed = crate::operations::compressed_page_compiler::get_compressed_page(
            app,
            TopicKeyRef::new(namespace.as_str(), req.topic_id.as_str()),
            from_message_id,
            to_message_id,
            MAX_PAYLOAD_SIZE,
        )
        .await
        .map_err(|err| {
            // Not an empty page: the storage did not answer, and the node has to know that.
            tonic::Status::unavailable(format!(
                "Can not read sub page {}. Err: {:?}",
                err.sub_page_id.get_value(),
                err.err
            ))
        })?;

        my_grpc_extensions::grpc_server_streams::send_from_iterator(compressed.into_iter()).await
    }

    generate_server_stream!(stream_name:"GetPageStream", item_name:"MessageContentGrpcModel");
    async fn get_page(
        &self,
        request: tonic::Request<crate::persistence_grpc::GetPageGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetPageStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let namespace = contracts::get_namespace(req.namespace)?;
        contracts::check_topic_id(req.topic_id.as_str())?;

        let app = self.app.clone();

        let page_id = PageId::new(req.page_no);

        let mut from_message_id = page_id.get_first_message_id();
        let mut to_message_id = page_id.get_last_message_id();

        if req.from_message_id <= req.to_message_id {
            from_message_id = MessageId::new(req.from_message_id);
            to_message_id = MessageId::new(req.to_message_id);
        }

        let topic_key = TopicKey::new(namespace, req.topic_id);

        let streamed_response = StreamedResponseWriter::new(1024);

        let producer = streamed_response.get_stream_producer();

        tokio::spawn(crate::operations::send_messages_to_channel(
            app,
            topic_key,
            from_message_id,
            to_message_id,
            producer,
        ));

        streamed_response.get_result()
    }

    generate_server_stream!(stream_name:"GetSubPageStream", item_name:"MessageContentGrpcModel");
    async fn get_sub_page(
        &self,
        request: tonic::Request<GetSubPageGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetSubPageStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let namespace = contracts::get_namespace(req.namespace)?;
        contracts::check_topic_id(req.topic_id.as_str())?;

        let sub_page_id = SubPageId::new(req.sub_page_no);

        let from_message_id = sub_page_id.get_first_message_id();
        let to_message_id = sub_page_id.get_last_message_id();

        let streamed_response = StreamedResponseWriter::new(1024);

        let producer = streamed_response.get_stream_producer();

        tokio::spawn(crate::operations::send_messages_to_channel(
            self.app.clone(),
            TopicKey::new(namespace, req.topic_id),
            from_message_id,
            to_message_id,
            producer,
        ));

        streamed_response.get_result()
    }

    async fn save_messages(
        &self,
        request: tonic::Request<tonic::Streaming<SaveMessagesGrpcRequest>>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let request = request.into_inner();

        let mut stream_reader = StreamedRequestReader::new(request);

        while let Some(message) = stream_reader.get_next().await {
            contracts::check_flags(self.app.as_ref())?;

            let message = message.unwrap();

            // Each message batch carries its own namespace - one stream may span several.
            let namespace = contracts::get_namespace(message.namespace)?;
            contracts::check_topic_id(message.topic_id.as_str())?;

            crate::operations::new_messages(
                &self.app,
                TopicKeyRef::new(namespace.as_str(), message.topic_id.as_str()),
                message.messages.into_iter().map(|itm| itm.into()),
            )
            .await;
        }

        Ok(tonic::Response::new(()))
    }

    async fn hard_delete_topic(
        &self,
        request: tonic::Request<HardDeleteTopicGrpcRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        let req = request.into_inner();

        let namespace = contracts::get_namespace(req.namespace)?;
        contracts::check_topic_id(req.topic_id.as_str())?;

        // Deletes the topic within this namespace only. Returns as soon as the topic stops being
        // served; wiping the data runs as a background job.
        crate::operations::hard_delete_topic(
            &self.app,
            TopicKeyRef::new(namespace.as_str(), req.topic_id.as_str()),
        );

        Ok(tonic::Response::new(()))
    }

    generate_server_stream!(stream_name:"GetHistoryByDateStream", item_name:"MessageContentGrpcModel");
    async fn get_history_by_date(
        &self,
        request: tonic::Request<GetHistoryByDateGrpcRequest>,
    ) -> Result<tonic::Response<Self::GetHistoryByDateStream>, tonic::Status> {
        contracts::check_flags(self.app.as_ref())?;

        // TODO: not implemented yet (see TODO.md). The namespace is still resolved so a caller
        // gets the same validation as everywhere else once this is filled in.
        let _namespace = contracts::get_namespace(request.into_inner().namespace)?;

        my_grpc_extensions::grpc_server_streams::send_from_iterator([].into_iter()).await
    }

    async fn ping(&self, _: tonic::Request<()>) -> Result<tonic::Response<()>, tonic::Status> {
        Ok(tonic::Response::new(()))
    }
}
