//! Integration tests for [`SpiderAdapter`] against a local [`wiremock`] HTTP server.
//!
//! These cover the no-regression promise: spider's HTTP fetch path produces a
//! [`gottem_core::ScrapeResponse`] equivalent to what `Website::crawl()` would yield directly.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, CancelToken, Capabilities, EndpointTemplate, FetchError,
    HttpMethod, Route, ScrapeRequest, Tier, Validator,
};
use gottem_adapters_spider::SpiderAdapter;
use url::Url;
use wiremock::matchers::{header, method, path};
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
        caps: Capabilities::default(),
        timeout_ms: 5_000,
        concurrency: 4,
        retry_on: Default::default(),
    }
}

#[tokio::test]
async fn fetches_200_html_and_returns_content() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html><body>hello world wide enough</body></html>"))
        .mount(&server)
        .await;

    let adapter = SpiderAdapter::new();
    let url = Url::parse(&server.uri()).unwrap();
    let route = route_for(&server.uri());
    let req = ScrapeRequest::get(url);
    let ctx = AdapterContext::new(0);
    let cancel = CancelToken::new();

    let resp = adapter.execute(&route, &req, &ctx, &cancel).await.expect("expected success");
    assert_eq!(resp.status, 200);
    assert!(resp.body.len() > 0, "body should not be empty");
    let content = resp.content.expect("content present");
    assert!(content.contains("hello world"), "unexpected content: {content}");
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

    let err = adapter.execute(&route, &req, &ctx, &cancel).await.unwrap_err();
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

    let err = adapter.execute(&route, &req, &ctx, &cancel).await.unwrap_err();
    assert!(err.is_retryable(), "503 should be retryable: {err:?}");
}

#[tokio::test]
async fn passes_custom_request_headers_through() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .and(header("x-gottem-test", "yes"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "<html><body>headers received correctly with enough bytes</body></html>",
        ))
        .mount(&server)
        .await;

    let adapter = SpiderAdapter::new();
    let route = route_for(&server.uri());
    let mut req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    req.headers.push(("x-gottem-test".into(), "yes".into()));
    let ctx = AdapterContext::new(0);
    let cancel = CancelToken::new();

    let resp = adapter.execute(&route, &req, &ctx, &cancel).await.expect("expected success");
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
    assert!(elapsed < Duration::from_secs(2), "cancel didn't propagate: {elapsed:?}");
    // The result is either Cancelled or a network/timeout error — either is acceptable
    // (depending on whether spider's crawl future was mid-flight when cancel fired).
    assert!(
        matches!(result, Err(FetchError::Cancelled) | Err(FetchError::Network(_)) | Err(FetchError::Timeout(_)) | Err(FetchError::Status(_))),
        "unexpected result: {result:?}"
    );
}

#[tokio::test]
async fn body_matches_html_bytes() {
    let html = "<html><body><p>this is a test page with enough characters to be meaningful</p></body></html>";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string(html))
        .mount(&server)
        .await;

    let adapter = SpiderAdapter::new();
    let route = route_for(&server.uri());
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let ctx = AdapterContext::new(0);
    let cancel = CancelToken::new();

    let resp = adapter.execute(&route, &req, &ctx, &cancel).await.expect("expected success");
    assert_eq!(resp.body, Bytes::copy_from_slice(html.as_bytes()));
}
