use my_http_server::{HttpFailResult, HttpOkResult, HttpOutput};
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[my_http_server::macros::http_route(method: "GET",route: "/",)]
pub struct IndexAction {
    rnd: i64,
}

impl IndexAction {
    pub fn new() -> Self {
        IndexAction {
            rnd: DateTimeAsMicroseconds::now().unix_microseconds,
        }
    }
}

async fn handle_request(
    action: &IndexAction,
    _ctx: &my_http_server::HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let content = format!(
        r###"<html><head><title>{ver} MyServiceBusPersistence</title>
        <link href="/css/bootstrap.css" type="text/css" rel="stylesheet" /><link href="/css/site.css?ver={rnd}" type="text/css" rel="stylesheet" /> <script src="/lib/jquery.js"></script><script src="/js/app.js?ver={rnd}"></script>
        </head><body></body></html>"###,
        ver = crate::app::APP_VERSION,
        rnd = action.rnd
    );

    HttpOutput::as_html(content).into_ok_result(false)
}
