// TODO: HTTP DELETE /api/Topic. See `TODO.md`.
// Should: validate the secret key, parse `delete_after`, and call the
// soft-delete operation that schedules the topic for GC.
//
// use std::sync::Arc;
//
// use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};
//
// use crate::app::AppContext;
//
// use super::contracts::*;
//
// #[my_http_server::macros::http_route(
//     method: "DELETE",
//     route: "/api/Topic",
//     input_data: "DeleteTopicHttpContract",
//     description: "Deletes Topic",
//     summary: "Delete Topic",
//     controller: "Topic",
//     result:[
//         {status_code: 202, description: "Topic is deleted"},
//         {status_code: 404, description: "Topic not found"},
//     ]
// )]
// pub struct DeleteTopicAction {
//     app: Arc<AppContext>,
// }
//
// impl DeleteTopicAction {
//     pub fn new(app: Arc<AppContext>) -> Self {
//         Self { app }
//     }
// }
//
// async fn handle_request(
//     action: &DeleteTopicAction,
//     input_data: DeleteTopicHttpContract,
//     _ctx: &HttpContext,
// ) -> Result<HttpOkResult, HttpFailResult> {
//     if action.app.settings.delete_topic_secret_key.as_str() != input_data.api_key {
//         return HttpOutput::as_unauthorized(Some("Invalid Secret Key")).into_err(false, false);
//     }
//
//     // Parse delete_after (RFC3339). Default: now + 24h to give grace period.
//     let delete_after = {
//         let parsed: Result<rust_extensions::date_time::DateTimeAsMicroseconds, HttpFailResult> =
//             match input_data.delete_after {
//                 None => {
//                     let mut v = rust_extensions::date_time::DateTimeAsMicroseconds::now();
//                     v.add_days(1);
//                     Ok(v)
//                 }
//                 Some(ref delete_after) if delete_after.is_empty() => {
//                     let mut v = rust_extensions::date_time::DateTimeAsMicroseconds::now();
//                     v.add_days(1);
//                     Ok(v)
//                 }
//                 Some(delete_after) => {
//                     if let Some(parsed) =
//                         rust_extensions::date_time::DateTimeAsMicroseconds::parse_iso_string(
//                             delete_after.as_str(),
//                         )
//                     {
//                         Ok(parsed)
//                     } else {
//                         Err(HttpFailResult::as_validation_error(format!(
//                             "Invalid deleteAfter: expected RFC3339, got '{}'",
//                             delete_after
//                         )))
//                     }
//                 }
//             };
//
//         match parsed {
//             Ok(val) => val,
//             Err(err) => return Err(err),
//         }
//     };
//
//     match crate::operations::delete_topic(
//         action.app.as_ref(),
//         input_data.topic_id.as_str(),
//         delete_after,
//     )
//     .await
//     {
//         Ok(_) => HttpOutput::Empty.into_ok_result(true).into(),
//         Err(crate::operations::OperationError::TopicNotFound(topic)) => Err(
//             HttpFailResult::as_not_found(format!("Topic {} not found", topic), true),
//         ),
//         Err(err) => Err(HttpFailResult::as_fatal_error(format!("{:?}", err))),
//     }
// }
