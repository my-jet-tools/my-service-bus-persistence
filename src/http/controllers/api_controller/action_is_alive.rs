use std::{sync::Arc, time::SystemTime};

use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};
use serde::{Deserialize, Serialize};

use crate::app::AppContext;

#[my_http_server::macros::http_route(method: "GET", route: "/api/is_alive")]
pub struct IsAliveAction {
    app: Arc<AppContext>,
}

impl IsAliveAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}
pub async fn handle_request(
    action: &IsAliveAction,
    _ctx: &HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let version = env!("CARGO_PKG_VERSION");

    let time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let model = ApiModel {
        name: "MyServiceBus.Persistence".to_string(),
        time: time,
        version: version.to_string(),
        env_info: action.app.get_env_info(),
    };

    HttpOutput::as_json(model).into_ok_result(true)
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiModel {
    name: String,
    time: u64,
    version: String,
    env_info: String,
}
