//! End-to-end tests for [`DirectHttpAdapter`], [`HttpJsonAdapter`], and
//! [`HttpJsonlStreamAdapter`] against a local [`wiremock`] HTTP server.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use gottem_adapters_http::{
    build_default_client, DirectHttpAdapter, HttpJsonAdapter, HttpJsonlStreamAdapter,
    HttpJsonlStreamManyAdapter,
};
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, AuthSpec, BodyTemplate, CancelToken, Capabilities,
    CrawlAdapter, CrawlRequest, EndpointTemplate, FetchError, HttpMethod, ResponseParse, Route,
    ScrapeRequest, Tier, Validator,
};
use url::Url;
use wiremock::matchers::{body_string, header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

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
        cost_extract: None,
    }
}

// ---- DirectHttp -------------------------------------------------------------

#[tokio::test]
async fn direct_http_get_returns_raw_text() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>hello</html>"))
        .mount(&server)
        .await;

    let adapter = DirectHttpAdapter::new(build_default_client());
    let route = base_route(&server, AdapterKind::DirectHttp, ResponseParse::RawText);
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let resp = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(resp.status, 200);
    assert_eq!(resp.content_str(), Some("<html>hello</html>"));
}

#[tokio::test]
async fn direct_http_maps_404_to_status_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let adapter = DirectHttpAdapter::new(build_default_client());
    let route = base_route(&server, AdapterKind::DirectHttp, ResponseParse::RawText);
    let req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    let err = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Status(404)), "got {err:?}");
}

#[tokio::test]
async fn direct_http_passes_request_body_through() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .and(body_string("raw-payload"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&server)
        .await;

    let adapter = DirectHttpAdapter::new(build_default_client());
    let mut route = base_route(&server, AdapterKind::DirectHttp, ResponseParse::RawText);
    route.method = HttpMethod::Post;
    let mut req = ScrapeRequest::get(Url::parse(&server.uri()).unwrap());
    req.method = HttpMethod::Post;
    req.body = Some(bytes::Bytes::from_static(b"raw-payload"));
    let resp = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(resp.status, 200);
}

// ---- HttpJson ---------------------------------------------------------------

#[tokio::test]
async fn http_json_substitutes_url_and_extracts_jsonpath() {
    let server = MockServer::start().await;
    let target = "https://example.com/page";
    let expected_body = format!(r#"{{"url":"{target}","formats":["markdown"]}}"#);

    Mock::given(method("POST"))
        .and(path("/"))
        .and(body_string(expected_body))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"data":{"markdown":"# Title"}})),
        )
        .mount(&server)
        .await;

    let adapter = HttpJsonAdapter::new(build_default_client());
    let mut route = base_route(
        &server,
        AdapterKind::HttpJson,
        ResponseParse::JsonPath {
            path: "$.data.markdown".into(),
        },
    );
    route.method = HttpMethod::Post;
    route.body = BodyTemplate::Json {
        template: r#"{"url":"{{url}}","formats":["markdown"]}"#.into(),
    };
    let req = ScrapeRequest::get(Url::parse(target).unwrap());
    let resp = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(resp.status, 200);
    assert_eq!(resp.content_str(), Some("# Title"));
}

#[tokio::test]
async fn http_json_bearer_auth_sets_authorization_header() {
    let server = MockServer::start().await;
    std::env::set_var("GOTTEM_TEST_BEARER", "sk-test-token");
    Mock::given(method("POST"))
        .and(path("/"))
        .and(header("authorization", "Bearer sk-test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"data":"ok"}"#))
        .mount(&server)
        .await;

    let adapter = HttpJsonAdapter::new(build_default_client());
    let mut route = base_route(
        &server,
        AdapterKind::HttpJson,
        ResponseParse::JsonPath {
            path: "$.data".into(),
        },
    );
    route.method = HttpMethod::Post;
    route.auth = AuthSpec::Bearer {
        env: "GOTTEM_TEST_BEARER".into(),
    };
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let resp = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(resp.content_str(), Some("ok"));
}

#[tokio::test]
async fn http_json_api_key_auth_with_prefix() {
    let server = MockServer::start().await;
    std::env::set_var("GOTTEM_TEST_APIKEY", "abc123");
    Mock::given(method("POST"))
        .and(path("/"))
        .and(header("x-api-key", "Key abc123"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"x":"y"}"#))
        .mount(&server)
        .await;

    let adapter = HttpJsonAdapter::new(build_default_client());
    let mut route = base_route(
        &server,
        AdapterKind::HttpJson,
        ResponseParse::JsonPath { path: "$.x".into() },
    );
    route.method = HttpMethod::Post;
    route.auth = AuthSpec::ApiKey {
        header: "x-api-key".into(),
        prefix: Some("Key ".into()),
        env: "GOTTEM_TEST_APIKEY".into(),
    };
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let resp = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(resp.content_str(), Some("y"));
}

#[tokio::test]
async fn http_json_missing_env_returns_auth_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let adapter = HttpJsonAdapter::new(build_default_client());
    let mut route = base_route(&server, AdapterKind::HttpJson, ResponseParse::RawText);
    route.method = HttpMethod::Post;
    route.auth = AuthSpec::Bearer {
        env: "GOTTEM_DEFINITELY_NOT_SET_XYZ".into(),
    };
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let err = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Auth(_)), "got {err:?}");
}

// ---- HttpJsonlStream --------------------------------------------------------

#[tokio::test]
async fn jsonl_stream_takes_first_record() {
    let server = MockServer::start().await;
    // Spider-style chunked JSONL: multiple newline-delimited records.
    let body = "{\"content\":\"first record body that we care about\"}\n\
                {\"content\":\"subsequent record ignored\"}\n";
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;

    let adapter = HttpJsonlStreamAdapter::new(build_default_client());
    let mut route = base_route(
        &server,
        AdapterKind::HttpJsonlStream,
        ResponseParse::JsonlFirst {
            path: "$.content".into(),
        },
    );
    route.method = HttpMethod::Post;
    route.body = BodyTemplate::Json {
        template: r#"{"url":"{{url}}","request":"smart"}"#.into(),
    };
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let resp = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(
        resp.content_str(),
        Some("first record body that we care about")
    );
}

#[tokio::test]
async fn jsonl_stream_unwraps_single_element_array() {
    let server = MockServer::start().await;
    // Spider sometimes wraps the first emission as [{...}].
    let body = r#"[{"content":"wrapped record"}]"#;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;

    let adapter = HttpJsonlStreamAdapter::new(build_default_client());
    let mut route = base_route(
        &server,
        AdapterKind::HttpJsonlStream,
        ResponseParse::JsonlFirst {
            path: "$.content".into(),
        },
    );
    route.method = HttpMethod::Post;
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let resp = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(resp.content_str(), Some("wrapped record"));
}

// ---- Cancellation -----------------------------------------------------------

#[tokio::test]
async fn cancel_aborts_in_flight_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
        .mount(&server)
        .await;

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
    let err = adapter
        .execute(&route, &req, &AdapterContext::new(0), &cancel)
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "cancel propagation took too long: {elapsed:?}"
    );
    assert!(matches!(err, FetchError::Cancelled), "got {err:?}");
}

// ============================================================================
// HttpJsonlStreamManyAdapter — streaming JSONL crawl
// ============================================================================

fn crawl_route(server: &MockServer) -> Route {
    Route {
        id: Arc::from("spider.crawl"),
        adapter: AdapterKind::HttpJsonlStreamMany,
        endpoint: EndpointTemplate::parse(&format!("{}/crawl", server.uri())).unwrap(),
        method: HttpMethod::Post,
        auth: AuthSpec::Bearer {
            env: "TEST_SPIDER_CLOUD_KEY".into(),
        },
        headers: vec![],
        body: BodyTemplate::Json {
            template: r#"{"url":"{{url}}","limit":{{param:limit|10}},"depth":{{param:depth|2}},"subdomains":{{param:subdomains|false}},"tld":{{param:tld|false}},"blacklist":{{param:deny|[]}},"whitelist":{{param:allow|[]}},"respect_robots_txt":{{param:respect_robots|false}},"return_format":"markdown"}"#.into(),
        },
        parse: ResponseParse::JsonlEach { path: "$.content".into() },
        validate: vec![],
        tier: Tier::T4,
        cost: 10,
        priority: 0,
        caps: Capabilities::default(),
        timeout_ms: 5_000,
        concurrency: 4,
        retry_on: Default::default(),
        cost_extract: None,
    }
}

/// End-to-end: POST a CrawlRequest, verify the wire body carries every generic
/// param mapped to Spider's field names, and the JSONL response streams
/// into PageEntry yields one record at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonl_many_maps_all_crawl_params_and_streams_records() {
    std::env::set_var("TEST_SPIDER_CLOUD_KEY", "sk-test");

    let server = MockServer::start().await;
    let expected_body = r#"{"url":"https://example.com/","limit":50,"depth":3,"subdomains":true,"tld":false,"blacklist":["/admin","\\.pdf$"],"whitelist":["/blog"],"respect_robots_txt":true,"return_format":"markdown"}"#;

    Mock::given(method("POST"))
        .and(path("/crawl"))
        .and(header("authorization", "Bearer sk-test"))
        .and(body_string(expected_body))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/jsonl")
                .set_body_string(concat!(
                    "{\"url\":\"https://example.com/\",\"status\":200,\"depth\":0,\"content\":\"# root\",\"costs\":{\"total\":1.0}}\n",
                    "{\"url\":\"https://example.com/a\",\"status\":200,\"depth\":1,\"content\":\"# a\",\"links\":[\"https://example.com/b\"]}\n",
                    "{\"url\":\"https://example.com/b\",\"status\":200,\"depth\":2,\"content\":\"# b\"}\n",
                )),
        )
        .mount(&server)
        .await;

    let adapter = HttpJsonlStreamManyAdapter::new(build_default_client());
    let route = crawl_route(&server);
    let req = CrawlRequest::new(Url::parse("https://example.com/").unwrap())
        .with_limit(50)
        .with_depth(3)
        .with_subdomains(true)
        .with_allow(vec!["/blog".into()])
        .with_deny(vec!["/admin".into(), "\\.pdf$".into()])
        .with_respect_robots(true);

    let mut stream = adapter
        .execute(&route, &req, &CancelToken::new())
        .await
        .expect("adapter execute");

    let mut entries = Vec::new();
    while let Some(item) = stream.next().await {
        entries.push(item.expect("page entry should be Ok"));
    }
    assert_eq!(entries.len(), 3, "three JSONL records → three PageEntries");
    assert_eq!(entries[0].url.as_str(), "https://example.com/");
    assert_eq!(entries[0].status, 200);
    assert_eq!(entries[0].depth, 0);
    assert_eq!(
        entries[0]
            .content
            .as_deref()
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("# root")
    );
    assert_eq!(entries[1].url.as_str(), "https://example.com/a");
    assert_eq!(entries[1].depth, 1);
    assert_eq!(
        entries[1].links.as_ref().unwrap()[0].as_str(),
        "https://example.com/b"
    );
    assert_eq!(entries[2].depth, 2);
    assert_eq!(entries[0].route_id.as_ref(), "spider.crawl");
    assert_eq!(entries[0].tier, Tier::T4);
    assert_eq!(entries[0].cost_milli, 10);
}

/// Stream survives the server closing without a trailing newline on the last
/// record — typical for some streaming APIs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonl_many_flushes_trailing_record_without_newline() {
    std::env::set_var("TEST_SPIDER_CLOUD_KEY", "sk-test");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "{\"url\":\"https://x.test/\",\"content\":\"first\"}\n\
             {\"url\":\"https://x.test/2\",\"content\":\"second\"}", // no trailing \n
        ))
        .mount(&server)
        .await;

    let adapter = HttpJsonlStreamManyAdapter::new(build_default_client());
    let route = crawl_route(&server);
    let req = CrawlRequest::new(Url::parse("https://x.test/").unwrap());
    let mut stream = adapter
        .execute(&route, &req, &CancelToken::new())
        .await
        .unwrap();
    let mut count = 0;
    while let Some(item) = stream.next().await {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 2, "trailing record without newline must still flush");
}

/// HTTP-level failure surfaces as `FetchError::Status` from execute itself —
/// the stream is never created.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonl_many_http_error_surfaces_immediately() {
    std::env::set_var("TEST_SPIDER_CLOUD_KEY", "sk-test");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;
    let adapter = HttpJsonlStreamManyAdapter::new(build_default_client());
    let route = crawl_route(&server);
    let req = CrawlRequest::new(Url::parse("https://x.test/").unwrap());
    let err = match adapter.execute(&route, &req, &CancelToken::new()).await {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert!(matches!(err, FetchError::Status(429)), "got {err:?}");
}

/// Dropping the consumer cancels the underlying task — verified by observing
/// that the spawned driver doesn't keep sending after the receiver disconnects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonl_many_consumer_drop_stops_driver() {
    std::env::set_var("TEST_SPIDER_CLOUD_KEY", "sk-test");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "{\"url\":\"https://x.test/1\",\"content\":\"a\"}\n\
             {\"url\":\"https://x.test/2\",\"content\":\"b\"}\n\
             {\"url\":\"https://x.test/3\",\"content\":\"c\"}\n",
        ))
        .mount(&server)
        .await;
    let adapter = HttpJsonlStreamManyAdapter::new(build_default_client());
    let route = crawl_route(&server);
    let req = CrawlRequest::new(Url::parse("https://x.test/").unwrap());
    let mut stream = adapter
        .execute(&route, &req, &CancelToken::new())
        .await
        .unwrap();
    // pull just one, then drop
    let _first = stream.next().await.unwrap().unwrap();
    drop(stream);
    // give the spawned task a chance to notice — if it leaks, this test won't
    // catch it visually, but the test still passes if no panic / no hang.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// Silence unused-imports if a future refactor removes Respond/Request.
#[allow(dead_code)]
fn _hint(_r: Request) -> ResponseTemplate {
    ResponseTemplate::new(200)
}
#[allow(dead_code)]
fn _resp_hint<R: Respond>(_r: R) {}
