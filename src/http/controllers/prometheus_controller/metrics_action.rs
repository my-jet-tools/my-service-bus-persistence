use std::sync::Arc;

use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};

use crate::app::AppContext;

#[my_http_server::macros::http_route(method: "GET",route: "/metrics",)]
pub struct MetricsAction {
    app: Arc<AppContext>,
}

impl MetricsAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &MetricsAction,
    _ctx: &mut HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let result = action.app.metrics_keeper.build_prometheus_content();

    HttpOutput::from_builder()
        .set_content(result.into_bytes())
        .into_ok_result(false)
}
