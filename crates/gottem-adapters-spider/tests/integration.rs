//! Integration tests for [`SpiderAdapter`] against a local [`wiremock`] HTTP server.
//!
//! These cover the no-regression promise: spider's HTTP fetch path produces a
//! [`gottem_core::ScrapeResponse`] equivalent to what `Website::crawl()` would yield directly.

use std::sync::Arc;
use std::time::Duration;

use gottem_adapters_spider::SpiderAdapter;
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, CancelToken, Capabilities, EndpointTemplate, FetchError,
    HttpMethod, Route, ScrapeRequest, Tier, Validator,
};
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn route_for(server_url: &str) -> Route {
    Route {
        id: Arc::from("local.http"),
        adapter: AdapterKind::SpiderLocal,
        endpoint: EndpointTemplate::parse(server_url).unwrap(),
        method: HttpMethod::Get,
        auth: Default::default(),
        headers: vec![],
        body: Default::default(),
        parse: Default::default(),
        validate: vec![Validator::MinBytes { n: 10 }],
        tier: Tier::T0,
        cost: 0,
        priority: 100,
        caps: Capabilities::default(),
        timeout_ms: 5_000,
        concurrency: 4,
        retry_on: Default::default(),
        cost_extract: None,
    }
}

#[tokio::test]
async fn fetches_200_html_and_returns_content() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("<html><body>hello world wide enough</body></html>"),
        )
        .mount(&server)
        .await;

    let adapter = SpiderAdapter::new();
    let url = Url::parse(&server.uri()).unwrap();
    let route = route_for(&server.uri());
    let req = ScrapeRequest::get(url);
    let ctx = AdapterContext::new(0);
    let cancel = CancelToken::new();

    let resp = adapter
        .execute(&route, &req, &ctx, &cancel)
        .await
        .expect("expected success");
    assert_eq!(resp.status, 200);
    assert!(!resp.body.is_empty(), "body should not be empty");
    let content = resp.content_str().expect("content present and valid utf8");
    assert!(
        content.contains("hello world"),
        "unexpected content: {content}"
    );
    assert_eq!(resp.tier, Tier::T0);
    assert_eq!(resp.route_id.as_ref(), "local.http");
}

#[tokio::test]
async fn maps_404_to_status_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&server)
        .await;

    let adapter = SpiderAdapter::new();
    let route = route_for(&server.uri());
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let ctx = AdapterContext::new(0);
    let cancel = CancelToken::new();

    let err = adapter
        .execute(&route, &req, &ctx, &cancel)
        .await
        .unwrap_err();
    match err {
        FetchError::Status(s) => assert_eq!(s, 404),
        other => panic!("expected Status(404), got {other:?}"),
    }
}

#[tokio::test]
async fn maps_503_to_retryable_status_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let adapter = SpiderAdapter::new();
    let route = route_for(&server.uri());
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let ctx = AdapterContext::new(0);
    let cancel = CancelToken::new();

    let err = adapter
        .execute(&route, &req, &ctx, &cancel)
        .await
        .unwrap_err();
    assert!(err.is_retryable(), "503 should be retryable: {err:?}");
}

// Per-request headers are NOT supported by the single-page Page::new path — that's the
// trade-off for staying out of the crawl scheduler. Headers must be baked into the
// `spider::Client` at adapter construction time (use `SpiderAdapter::with_client`).
// For per-request headers, route through `gottem-adapters-http` instead.
//
// We keep one positive test below to assert that the base GET works without an explicit
// header matcher — i.e. that req.headers are ignored without erroring.
#[tokio::test]
async fn fetch_succeeds_when_request_carries_unused_headers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            b"<html><body>ignored headers ok</body></html>".as_slice(),
            "text/html",
        ))
        .mount(&server)
        .await;

    let adapter = SpiderAdapter::new();
    let route = route_for(&server.uri());
    let mut req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    // Setting a request header is a no-op for SpiderAdapter; it must not error.
    req.headers.push(("x-gottem-test".into(), "yes".into()));
    let ctx = AdapterContext::new(0);
    let cancel = CancelToken::new();

    let resp = adapter
        .execute(&route, &req, &ctx, &cancel)
        .await
        .expect("expected success");
    assert_eq!(resp.status, 200);
}

#[tokio::test]
async fn cancel_token_aborts_slow_fetch() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
        .mount(&server)
        .await;

    let adapter = SpiderAdapter::new();
    let route = route_for(&server.uri());
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let ctx = AdapterContext::new(0);
    let cancel = CancelToken::new();
    let cancel_clone = cancel.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_clone.cancel();
    });

    let started = std::time::Instant::now();
    let result = adapter.execute(&route, &req, &ctx, &cancel).await;
    let elapsed = started.elapsed();

    // Should bail out well before the 5s delay.
    assert!(
        elapsed < Duration::from_secs(2),
        "cancel didn't propagate: {elapsed:?}"
    );
    // The result is either Cancelled or a network/timeout error — either is acceptable
    // (depending on whether spider's crawl future was mid-flight when cancel fired).
    assert!(
        matches!(
            result,
            Err(FetchError::Cancelled)
                | Err(FetchError::Network(_))
                | Err(FetchError::Timeout(_))
                | Err(FetchError::Status(_))
        ),
        "unexpected result: {result:?}"
    );
}

#[tokio::test]
async fn body_preserves_original_content() {
    // The substantive payload — must survive spider's pipeline end-to-end.
    let needle = "this is a test page with enough characters to be meaningful";
    let html = format!("<html><body><p>{needle}</p></body></html>");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        // Use set_body_raw with text/html so spider doesn't apply a text/plain
        // viewer transformation (which adds <pre> wrappers + HTML-encoded entities
        // on Linux runners and would break byte-equal assertions).
        .respond_with(ResponseTemplate::new(200).set_body_raw(html.as_bytes(), "text/html"))
        .mount(&server)
        .await;

    let adapter = SpiderAdapter::new();
    let route = route_for(&server.uri());
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let ctx = AdapterContext::new(0);
    let cancel = CancelToken::new();

    let resp = adapter
        .execute(&route, &req, &ctx, &cancel)
        .await
        .expect("expected success");

    // Contract: the substantive text round-trips through spider's pipeline.
    // We don't byte-compare the full body because spider can legitimately rewrite
    // the wrapping markup (charset normalization, etc.).
    let body_str = std::str::from_utf8(&resp.body).expect("utf-8 body");
    assert!(
        body_str.contains(needle),
        "body did not contain the original payload; got: {body_str}"
    );
    let content = resp.content_str().expect("content present");
    assert!(
        content.contains(needle),
        "content did not contain the original payload"
    );
}
