// TODO: timer that drives `gc_expired_deleted_topics`. See `TODO.md`.
//
// use std::sync::Arc;
//
// use rust_extensions::MyTimerTick;
//
// use crate::app::AppContext;
//
// pub struct DeletedTopicsGcTimer {
//     app: Arc<AppContext>,
// }
//
// impl DeletedTopicsGcTimer {
//     pub fn new(app: Arc<AppContext>) -> Self {
//         Self { app }
//     }
// }
//
// #[async_trait::async_trait]
// impl MyTimerTick for DeletedTopicsGcTimer {
//     async fn tick(&self) {
//         crate::operations::gc_expired_deleted_topics(self.app.as_ref()).await;
//     }
// }
