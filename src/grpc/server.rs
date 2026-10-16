use crate::app::AppContext;
use crate::persistence_grpc::my_service_bus_messages_persistence_grpc_service_server::MyServiceBusMessagesPersistenceGrpcServiceServer;
use anyhow::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

#[derive(Clone)]
pub struct MyServicePersistenceGrpc {
    pub app: Arc<AppContext>,
}

impl MyServicePersistenceGrpc {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

pub async fn start(app: Arc<AppContext>, port: u16) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let service = MyServicePersistenceGrpc::new(app);

    println!("Listening to {:?} as grpc endpoint", addr);
    Server::builder()
        .add_service(MyServiceBusMessagesPersistenceGrpcServiceServer::new(
            service,
        ))
        .serve(addr)
        .await
        .context("Server error")
}

pub async fn start_unix_socket(app: Arc<AppContext>, unix_socket_addr: String) -> Result<()> {
    let unix_socket_addr = rust_extensions::file_utils::format_path(unix_socket_addr);

    let _ = tokio::fs::remove_file(unix_socket_addr.as_str()).await;
    let uds = tokio::net::UnixListener::bind(unix_socket_addr.as_str()).unwrap();
    let uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

    let service = MyServicePersistenceGrpc::new(app);

    println!(
        "Listening to {:?} as grpc unix_socket endpoint",
        unix_socket_addr.as_str()
    );
    Server::builder()
        .add_service(MyServiceBusMessagesPersistenceGrpcServiceServer::new(
            service,
        ))
        .serve_with_incoming(uds_stream)
        .await
        .context("Server error")
}
