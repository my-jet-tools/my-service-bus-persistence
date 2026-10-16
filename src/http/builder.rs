use std::sync::Arc;

use my_http_server::controllers::ControllersMiddleware;

use crate::app::AppContext;

pub fn build(app: &Arc<AppContext>) -> ControllersMiddleware {
    let mut result = ControllersMiddleware::new(None, None);

    result.register_get_action(Arc::new(
        super::controllers::api_controller::IsAliveAction::new(app.clone()),
    ));
    result.register_get_action(Arc::new(
        super::controllers::api_controller::GetStatusAction::new(app.clone()),
    ));

    /*
       result.register_get_action(Arc::new(
           super::controllers::logs_controller::ActionLogs::new(app.clone()),
       ));

       result.register_get_action(Arc::new(
           super::controllers::logs_controller::ActionLogsByTopic::new(app.clone()),
       ));
    */
    //Controller Topic
    // TODO: re-enable with soft-delete + GC (see TODO.md)
    // result.register_delete_action(Arc::new(
    //     super::controllers::topic_controller::DeleteTopicAction::new(app.clone()),
    // ));
    //
    // result.register_get_action(Arc::new(
    //     super::controllers::topic_controller::GetDeletedTopicsAction::new(app.clone()),
    // ));

    result.register_get_action(Arc::new(
        super::controllers::prometheus_controller::MetricsAction::new(app.clone()),
    ));

    result.register_get_action(Arc::new(
        super::controllers::home_controller::IndexAction::new(),
    ));

    result.register_get_action(Arc::new(
        super::controllers::read_controller::ByIdAction::new(app.clone()),
    ));

    result.register_get_action(Arc::new(
        super::controllers::read_controller::ListFromDateAction::new(app.clone()),
    ));

    result
}
