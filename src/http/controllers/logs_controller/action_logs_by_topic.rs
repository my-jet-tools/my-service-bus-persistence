use std::sync::Arc;

use my_http_server::{HttpContext, HttpFailResult, HttpOkResult};
use rust_extensions::{StopWatch, StringBuilder};

use crate::app::AppContext;

use super::contracts::*;

#[my_http_server::macros::http_route(
    method: "GET", 
    route: "/logs/topic/{topicId}",
    input_data: "GetLogsByTopicHttpInput",
)]
pub struct ActionLogsByTopic {
    app: Arc<AppContext>,
}

impl ActionLogsByTopic {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &ActionLogsByTopic,
    http_input: GetLogsByTopicHttpInput,
    _ctx: &HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let mut sw = StopWatch::new();
    sw.start();
    let logs = action
        .app
        .logs
        .get_by_topic(http_input.topic_id.as_str())
        .await;

    if logs.is_none() {
        return Ok("---".to_string().into());
    }

    let logs = logs.unwrap();

    let mut sb = StringBuilder::new();

    for log_item in logs {
        let line = format!("{} {}", log_item.date.to_rfc3339(), log_item.level.as_str());
        sb.append_line(&line);

        let line = format!("Process: {}", log_item.process);
        sb.append_line(line.as_str());

        sb.append_line(log_item.message.as_str());

        if let Some(ctx) = &log_item.ctx {
            let line = format!("Ctx: {:?}", ctx);
            sb.append_line(line.as_str());
        }

        sb.append_line("-----------------------------");
    }

    sw.pause();

    let line = format!("Rendered in {:?}", sw.duration());
    sb.append_line(line.as_str());

    Ok(sb.to_string_utf8().into())
}
