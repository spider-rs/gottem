//! End-to-end tests for [`DirectHttpAdapter`], [`HttpJsonAdapter`], and
//! [`HttpJsonlStreamAdapter`] against a local [`wiremock`] HTTP server.

use std::sync::Arc;
use std::time::Duration;

use gottem_adapters_http::{
    build_default_client, DirectHttpAdapter, HttpJsonAdapter, HttpJsonlStreamAdapter,
};
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, AuthSpec, BodyTemplate, CancelToken, Capabilities,
    EndpointTemplate, FetchError, HttpMethod, ResponseParse, Route, ScrapeRequest, Tier, Validator,
};
use url::Url;
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn base_route(server: &MockServer, adapter: AdapterKind, parse: ResponseParse) -> Route {
    Route {
        id: Arc::from("route.test"),
        adapter,
        endpoint: EndpointTemplate::parse(&server.uri()).unwrap(),
        method: HttpMethod::Get,
        auth: AuthSpec::None,
        headers: vec![],
        body: BodyTemplate::Empty,
        parse,
        validate: vec![Validator::MinBytes { n: 1 }],
        tier: Tier::T4,
        cost: 10,
        priority: 100,
        caps: Capabilities::default(),
        timeout_ms: 5_000,
        concurrency: 4,
        retry_on: Default::default(),
    }
}

// ---- DirectHttp -------------------------------------------------------------

#[tokio::test]
async fn direct_http_get_returns_raw_text() {
    let server = MockServer::start().await;
    Mock::given(method("GET")).and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>hello</html>"))
        .mount(&server).await;

    let adapter = DirectHttpAdapter::new(build_default_client());
    let route = base_route(&server, AdapterKind::DirectHttp, ResponseParse::RawText);
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let resp = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap();
    assert_eq!(resp.status, 200);
    assert_eq!(resp.content.as_deref(), Some("<html>hello</html>"));
}

#[tokio::test]
async fn direct_http_maps_404_to_status_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET")).and(path("/"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server).await;

    let adapter = DirectHttpAdapter::new(build_default_client());
    let route = base_route(&server, AdapterKind::DirectHttp, ResponseParse::RawText);
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let err = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap_err();
    assert!(matches!(err, FetchError::Status(404)), "got {err:?}");
}

#[tokio::test]
async fn direct_http_passes_request_body_through() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/"))
        .and(body_string("raw-payload"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&server).await;

    let adapter = DirectHttpAdapter::new(build_default_client());
    let mut route = base_route(&server, AdapterKind::DirectHttp, ResponseParse::RawText);
    route.method = HttpMethod::Post;
    let mut req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    req.method = HttpMethod::Post;
    req.body = Some(bytes::Bytes::from_static(b"raw-payload"));
    let resp = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap();
    assert_eq!(resp.status, 200);
}

// ---- HttpJson ---------------------------------------------------------------

#[tokio::test]
async fn http_json_substitutes_url_and_extracts_jsonpath() {
    let server = MockServer::start().await;
    let target = "https://example.com/page";
    let expected_body = format!(r#"{{"url":"{target}","formats":["markdown"]}}"#);

    Mock::given(method("POST")).and(path("/"))
        .and(body_string(expected_body))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"data":{"markdown":"# Title"}}))
        )
        .mount(&server).await;

    let adapter = HttpJsonAdapter::new(build_default_client());
    let mut route = base_route(&server, AdapterKind::HttpJson, ResponseParse::JsonPath { path: "$.data.markdown".into() });
    route.method = HttpMethod::Post;
    route.body = BodyTemplate::Json {
        template: r#"{"url":"{{url}}","formats":["markdown"]}"#.into(),
    };
    let req = ScrapeRequest::get(Url::parse(target).unwrap());
    let resp = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap();
    assert_eq!(resp.status, 200);
    assert_eq!(resp.content.as_deref(), Some("# Title"));
}

#[tokio::test]
async fn http_json_bearer_auth_sets_authorization_header() {
    let server = MockServer::start().await;
    std::env::set_var("GOTTEM_TEST_BEARER", "sk-test-token");
    Mock::given(method("POST")).and(path("/"))
        .and(header("authorization", "Bearer sk-test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"data":"ok"}"#))
        .mount(&server).await;

    let adapter = HttpJsonAdapter::new(build_default_client());
    let mut route = base_route(&server, AdapterKind::HttpJson, ResponseParse::JsonPath { path: "$.data".into() });
    route.method = HttpMethod::Post;
    route.auth = AuthSpec::Bearer { env: "GOTTEM_TEST_BEARER".into() };
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let resp = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap();
    assert_eq!(resp.content.as_deref(), Some("ok"));
}

#[tokio::test]
async fn http_json_api_key_auth_with_prefix() {
    let server = MockServer::start().await;
    std::env::set_var("GOTTEM_TEST_APIKEY", "abc123");
    Mock::given(method("POST")).and(path("/"))
        .and(header("x-api-key", "Key abc123"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"x":"y"}"#))
        .mount(&server).await;

    let adapter = HttpJsonAdapter::new(build_default_client());
    let mut route = base_route(&server, AdapterKind::HttpJson, ResponseParse::JsonPath { path: "$.x".into() });
    route.method = HttpMethod::Post;
    route.auth = AuthSpec::ApiKey {
        header: "x-api-key".into(),
        prefix: Some("Key ".into()),
        env: "GOTTEM_TEST_APIKEY".into(),
    };
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let resp = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap();
    assert_eq!(resp.content.as_deref(), Some("y"));
}

#[tokio::test]
async fn http_json_missing_env_returns_auth_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server).await;

    let adapter = HttpJsonAdapter::new(build_default_client());
    let mut route = base_route(&server, AdapterKind::HttpJson, ResponseParse::RawText);
    route.method = HttpMethod::Post;
    route.auth = AuthSpec::Bearer { env: "GOTTEM_DEFINITELY_NOT_SET_XYZ".into() };
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let err = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap_err();
    assert!(matches!(err, FetchError::Auth(_)), "got {err:?}");
}

// ---- HttpJsonlStream --------------------------------------------------------

#[tokio::test]
async fn jsonl_stream_takes_first_record() {
    let server = MockServer::start().await;
    // Spider Cloud-style chunked JSONL: multiple newline-delimited records.
    let body = "{\"content\":\"first record body that we care about\"}\n\
                {\"content\":\"subsequent record ignored\"}\n";
    Mock::given(method("POST")).and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server).await;

    let adapter = HttpJsonlStreamAdapter::new(build_default_client());
    let mut route = base_route(&server, AdapterKind::HttpJsonlStream, ResponseParse::JsonlFirst { path: "$.content".into() });
    route.method = HttpMethod::Post;
    route.body = BodyTemplate::Json {
        template: r#"{"url":"{{url}}","request":"smart"}"#.into(),
    };
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let resp = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap();
    assert_eq!(resp.content.as_deref(), Some("first record body that we care about"));
}

#[tokio::test]
async fn jsonl_stream_unwraps_single_element_array() {
    let server = MockServer::start().await;
    // Spider Cloud sometimes wraps the first emission as [{...}].
    let body = r#"[{"content":"wrapped record"}]"#;
    Mock::given(method("POST")).and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server).await;

    let adapter = HttpJsonlStreamAdapter::new(build_default_client());
    let mut route = base_route(&server, AdapterKind::HttpJsonlStream, ResponseParse::JsonlFirst { path: "$.content".into() });
    route.method = HttpMethod::Post;
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let resp = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap();
    assert_eq!(resp.content.as_deref(), Some("wrapped record"));
}

// ---- Cancellation -----------------------------------------------------------

#[tokio::test]
async fn cancel_aborts_in_flight_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET")).and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
        .mount(&server).await;

    let adapter = DirectHttpAdapter::new(build_default_client());
    let route = base_route(&server, AdapterKind::DirectHttp, ResponseParse::RawText);
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let cancel = CancelToken::new();
    let cancel_clone = cancel.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_clone.cancel();
    });

    let started = std::time::Instant::now();
    let err = adapter.execute(&route, &req, &AdapterContext::new(0), &cancel).await.unwrap_err();
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(2), "cancel propagation took too long: {elapsed:?}");
    assert!(matches!(err, FetchError::Cancelled), "got {err:?}");
}
