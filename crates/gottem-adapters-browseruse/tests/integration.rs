//! End-to-end tests for [`BrowserUseAdapter`] against a local wiremock server.
//! Covers submit + poll + status transitions + error paths.

use std::sync::Arc;
use std::time::Duration;

use gottem_adapters_browseruse::{BrowserUseAdapter, ADAPTER_KIND_NAME};
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, AuthSpec, BodyTemplate, CancelToken, Capabilities,
    EndpointTemplate, FetchError, HttpMethod, Route, ScrapeRequest, Tier,
};
use url::Url;
use wiremock::matchers::{header, method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn browseruse_route() -> Route {
    Route {
        id: Arc::from("browseruse.cloud"),
        adapter: AdapterKind::Custom(Arc::from(ADAPTER_KIND_NAME)),
        endpoint: EndpointTemplate::parse("https://api.browser-use.com/").unwrap(),
        method: HttpMethod::Post,
        auth: AuthSpec::Bearer {
            env: "GOTTEM_TEST_BROWSERUSE_KEY".into(),
        },
        headers: vec![],
        body: BodyTemplate::Json {
            template: r#"{"task":"visit {{url}}","use_proxy":true}"#.into(),
        },
        parse: Default::default(),
        validate: vec![],
        tier: Tier::T9,
        cost: 1000,
        priority: 100,
        caps: Capabilities::default(),
        timeout_ms: 600_000,
        concurrency: 2,
        retry_on: Default::default(),
    }
}

fn fast_adapter(server: &MockServer) -> BrowserUseAdapter {
    BrowserUseAdapter::new()
        .with_base_url(server.uri())
        // Fast timing for tests; production defaults are 8s + 6s polls.
        .with_initial_delay(Duration::from_millis(10))
        .with_poll_interval(Duration::from_millis(20))
        .with_max_polls(8)
}

#[tokio::test]
async fn submit_then_poll_returns_output() {
    std::env::set_var("GOTTEM_TEST_BROWSERUSE_KEY", "sk-test-1");
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v1/run-task"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"id":"task-42"}"#))
        .mount(&server)
        .await;

    // First poll: running. Second poll: finished with output.
    Mock::given(method("GET"))
        .and(path("/api/v1/task/task-42"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"id":"task-42","status":"running","output":null}"#),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/task/task-42"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r##"{"id":"task-42","status":"finished","output":"# example.com\n\nThe page content."}"##,
        ))
        .mount(&server)
        .await;

    let adapter = fast_adapter(&server);
    let route = browseruse_route();
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let resp = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .expect("task resolves");
    assert_eq!(
        resp.content.as_deref(),
        Some("# example.com\n\nThe page content.")
    );
    // Output is the AI's final answer; we don't constrain its byte-for-byte shape, only
    // that the substantive text round-trips.
    assert_eq!(resp.tier, Tier::T9);
    assert_eq!(resp.route_id.as_ref(), "browseruse.cloud");
}

#[tokio::test]
async fn task_failed_status_returns_network_error() {
    std::env::set_var("GOTTEM_TEST_BROWSERUSE_KEY", "sk-test");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/run-task"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"id":"task-fail"}"#))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v1/task/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(
                r#"{"id":"task-fail","status":"failed","error":"agent timed out"}"#,
            ),
        )
        .mount(&server)
        .await;

    let adapter = fast_adapter(&server);
    let route = browseruse_route();
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let err = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap_err();
    match err {
        FetchError::Network(msg) => {
            assert!(msg.contains("failed"), "unexpected msg: {msg}");
            assert!(msg.contains("agent timed out"), "unexpected msg: {msg}");
        }
        other => panic!("expected Network for failed status, got {other:?}"),
    }
}

#[tokio::test]
async fn submit_401_returns_auth_error() {
    std::env::set_var("GOTTEM_TEST_BROWSERUSE_KEY", "sk-bad");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/run-task"))
        .respond_with(ResponseTemplate::new(401).set_body_string(r#"{"detail":"Invalid API key"}"#))
        .mount(&server)
        .await;

    let adapter = fast_adapter(&server);
    let route = browseruse_route();
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let err = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Auth(_)), "got {err:?}");
}

#[tokio::test]
async fn poll_exhausted_returns_timeout() {
    std::env::set_var("GOTTEM_TEST_BROWSERUSE_KEY", "sk-test");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/run-task"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"id":"task-stuck"}"#))
        .mount(&server)
        .await;
    // Every poll returns "running" — we never finish.
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v1/task/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"id":"task-stuck","status":"running","output":null}"#),
        )
        .mount(&server)
        .await;

    let adapter = fast_adapter(&server);
    let route = browseruse_route();
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let err = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Timeout(_)), "got {err:?}");
}

#[tokio::test]
async fn submit_response_missing_id_is_parse_error() {
    std::env::set_var("GOTTEM_TEST_BROWSERUSE_KEY", "sk-test");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/run-task"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"unexpected":"shape"}"#))
        .mount(&server)
        .await;

    let adapter = fast_adapter(&server);
    let route = browseruse_route();
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let err = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Parse(_)), "got {err:?}");
}

#[tokio::test]
async fn missing_env_var_is_auth_error_no_network() {
    // Make sure the env is NOT set so resolve_api_key fails before we hit the network.
    std::env::remove_var("GOTTEM_BROWSERUSE_UNSET_FOR_SURE");
    let server = MockServer::start().await;
    let adapter = fast_adapter(&server);
    let mut route = browseruse_route();
    route.auth = AuthSpec::Bearer {
        env: "GOTTEM_BROWSERUSE_UNSET_FOR_SURE".into(),
    };
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let err = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Auth(_)), "got {err:?}");
}

#[tokio::test]
async fn empty_body_template_is_config_error() {
    std::env::set_var("GOTTEM_TEST_BROWSERUSE_KEY", "sk-test");
    let server = MockServer::start().await;
    let adapter = fast_adapter(&server);
    let mut route = browseruse_route();
    route.body = BodyTemplate::Empty;
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let err = adapter
        .execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await
        .unwrap_err();
    assert!(matches!(err, FetchError::Config(_)), "got {err:?}");
}

#[tokio::test]
async fn cancel_aborts_during_polling() {
    std::env::set_var("GOTTEM_TEST_BROWSERUSE_KEY", "sk-test");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/run-task"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"id":"task-slow"}"#))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/api/v1/task/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"id":"task-slow","status":"running","output":null}"#),
        )
        .mount(&server)
        .await;

    let adapter = BrowserUseAdapter::new()
        .with_base_url(server.uri())
        .with_initial_delay(Duration::from_millis(5))
        .with_poll_interval(Duration::from_secs(2)) // long poll, we want cancel to fire mid-sleep
        .with_max_polls(10);
    let route = browseruse_route();
    let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    let cancel = CancelToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
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
        "cancel didn't propagate quickly: {elapsed:?}"
    );
    assert!(matches!(err, FetchError::Cancelled), "got {err:?}");
}
