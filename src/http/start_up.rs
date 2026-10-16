use std::{net::SocketAddr, sync::Arc};

use my_http_server::{
    controllers::swagger::SwaggerMiddleware, HttpConnectionsCounter, MyHttpServer,
    StaticFilesMiddleware,
};

use crate::app::AppContext;

pub fn setup_server(app: &Arc<AppContext>, port: u16) -> HttpConnectionsCounter {
    let mut http_server = MyHttpServer::new(SocketAddr::from(([0, 0, 0, 0], port)));

    let controllers = Arc::new(super::builder::build(app));

    let swagger_middleware = SwaggerMiddleware::new(
        controllers.clone(),
        "MyServiceBusPersistence".to_string(),
        crate::app::APP_VERSION.to_string(),
    );

    let swagger_middleware = Arc::new(swagger_middleware);

    http_server.add_middleware(swagger_middleware);
    http_server.add_middleware(controllers);

    http_server.add_middleware(Arc::new(StaticFilesMiddleware::new()));
    http_server.start(app.app_states.clone(), my_logger::LOGGER.clone());
    http_server.get_http_connections_counter()
}
