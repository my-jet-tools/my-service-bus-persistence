// TODO: HTTP GET /api/Topic — list topics that are soft-deleted and pending GC.
// See `TODO.md`.
//
// use std::sync::Arc;
//
// use my_http_server::macros::MyHttpObjectStructure;
// use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};
// use rust_extensions::date_time::DateTimeAsMicroseconds;
// use serde::*;
//
// use crate::app::AppContext;
//
// #[my_http_server::macros::http_route(
//     method: "GET",
//     route: "/api/Topic",
//     description: "Get deleted topics",
//     summary: "Get deleted topics",
//     controller: "Topic",
//     result:[
//         {status_code: 200, description: "Deleted topics", model:"Vec<DeletedTopic>"},
//         {status_code: 404, description: "Topic not found"},
//     ]
// )]
// pub struct GetDeletedTopicsAction {
//     app: Arc<AppContext>,
// }
//
// impl GetDeletedTopicsAction {
//     pub fn new(app: Arc<AppContext>) -> Self {
//         Self { app }
//     }
// }
//
// async fn handle_request(
//     action: &GetDeletedTopicsAction,
//
//     _ctx: &HttpContext,
// ) -> Result<HttpOkResult, HttpFailResult> {
//     let mut result = Vec::new();
//
//     {
//         let topics_snapshot = action.app.topics_snapshot.get().await;
//         for deleted in &topics_snapshot.snapshot.deleted_topics {
//             result.push(DeletedTopic {
//                 topic_id: deleted.topic_id.to_string(),
//                 gc_after: DateTimeAsMicroseconds::new(deleted.gc_after).to_rfc3339(),
//             })
//         }
//     }
//
//     return HttpOutput::as_json(result).into_ok_result(true).into();
// }
//
// #[derive(Debug, MyHttpObjectStructure, Serialize)]
// pub struct DeletedTopic {
//     pub topic_id: String,
//     pub gc_after: String,
// }
