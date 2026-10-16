use rand::Rng;

pub fn get_index_page_content() -> Result<HttpOkResult, HttpFailResult> {
    let mut rng = rand::thread_rng();

    let rnd: u64 = rng.gen();

    let head = format!(
        r###"<link href="/css/bootstrap.css" type="text/css" rel="stylesheet" /><link href="/css/site.css?ver={rnd}" type="text/css" rel="stylesheet" /> <script src="/lib/jquery.js"></script><script src="/js/app.js?ver={rnd}"></script>"###,
        rnd = rnd
    );

    let result = HttpOkResult::Html {
        head,
        body: "".to_string(),
    };

    Ok(result)
}
