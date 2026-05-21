//! End-to-end tests for [`Captcha2CaptchaAdapter`] using a local wiremock server in
//! place of 2Captcha. Covers the full submit + poll state machine plus all error paths.

use std::sync::Arc;
use std::time::Duration;

use gottem_adapters_captcha::{Captcha2CaptchaAdapter, ADAPTER_KIND_NAME};
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, AuthSpec, CancelToken, Capabilities, EndpointTemplate,
    FetchError, HttpMethod, Route, ScrapeRequest, Tier,
};
use serde_json::json;
use url::Url;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn captcha_route() -> Route {
    Route {
        id: Arc::from("captcha.2captcha"),
        adapter: AdapterKind::Custom(Arc::from(ADAPTER_KIND_NAME)),
        endpoint: EndpointTemplate::parse("https://2captcha.com/").unwrap(),
        method: HttpMethod::Post,
        auth: AuthSpec::Bearer { env: "GOTTEM_TEST_2CAPTCHA_KEY".into() },
        headers: vec![],
        body: Default::default(),
        parse: Default::default(),
        validate: vec![],
        tier: Tier::T9,
        cost: 200,
        caps: Capabilities::default(),
        timeout_ms: 120_000,
        concurrency: 4,
        retry_on: Default::default(),
    }
}

fn captcha_req(captcha_type: &str, site_key: &str, page_url: &str) -> ScrapeRequest {
    let mut req = ScrapeRequest::get(Url::parse(page_url).unwrap());
    req.extra.insert("captchaType".into(), json!(captcha_type));
    req.extra.insert("siteKey".into(), json!(site_key));
    req.extra.insert("pageUrl".into(), json!(page_url));
    req
}

fn adapter_against(server: &MockServer) -> Captcha2CaptchaAdapter {
    Captcha2CaptchaAdapter::new()
        .with_endpoints(
            format!("{}/in.php", server.uri()),
            format!("{}/res.php", server.uri()),
        )
        // Fast timing for tests — production defaults are 10s initial + 5s polls.
        .with_initial_delay(Duration::from_millis(10))
        .with_poll_interval(Duration::from_millis(20))
        .with_max_polls(5)
}

#[tokio::test]
async fn submit_then_poll_resolves_token() {
    std::env::set_var("GOTTEM_TEST_2CAPTCHA_KEY", "test-key-abc");
    let server = MockServer::start().await;

    // Submit returns task id 12345.
    Mock::given(method("POST")).and(path("/in.php"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"status":1,"request":"12345"}"#),
        )
        .mount(&server).await;

    // First poll: NOT_READY. Second poll: OK with token.
    Mock::given(method("GET")).and(path("/res.php")).and(query_param("id", "12345"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"status":0,"request":"CAPCHA_NOT_READY"}"#),
        )
        .up_to_n_times(1)
        .mount(&server).await;
    Mock::given(method("GET")).and(path("/res.php")).and(query_param("id", "12345"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"status":1,"request":"SOLVED-TOKEN-PAYLOAD-very-long-string-of-base64-or-similar-data"}"#),
        )
        .mount(&server).await;

    let adapter = adapter_against(&server);
    let route = captcha_route();
    let req = captcha_req("recaptcha_v2", "6Lc-test-key", "https://example.com/login");
    let resp = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.expect("captcha solved");

    assert_eq!(resp.content.as_deref(), Some("SOLVED-TOKEN-PAYLOAD-very-long-string-of-base64-or-similar-data"));
    assert_eq!(resp.tier, Tier::T9);
    assert_eq!(resp.route_id.as_ref(), "captcha.2captcha");
}

#[tokio::test]
async fn submit_wrong_key_returns_auth_error() {
    std::env::set_var("GOTTEM_TEST_2CAPTCHA_KEY", "bad-key");
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/in.php"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"status":0,"request":"ERROR_WRONG_USER_KEY"}"#),
        )
        .mount(&server).await;

    let adapter = adapter_against(&server);
    let route = captcha_route();
    let req = captcha_req("recaptcha_v2", "6Lc-test", "https://example.com/");
    let err = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap_err();
    assert!(matches!(err, FetchError::Auth(_)), "got {err:?}");
}

#[tokio::test]
async fn poll_exhausted_returns_timeout() {
    std::env::set_var("GOTTEM_TEST_2CAPTCHA_KEY", "test-key");
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/in.php"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"status":1,"request":"7777"}"#),
        )
        .mount(&server).await;
    // Every poll returns NOT_READY — we should give up after max_polls.
    Mock::given(method("GET")).and(path("/res.php"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"status":0,"request":"CAPCHA_NOT_READY"}"#),
        )
        .mount(&server).await;

    let adapter = adapter_against(&server);
    let route = captcha_route();
    let req = captcha_req("recaptcha_v2", "6Lc-test", "https://example.com/");
    let err = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap_err();
    assert!(matches!(err, FetchError::Timeout(_)), "got {err:?}");
}

#[tokio::test]
async fn missing_capt_type_extra_is_config_error() {
    std::env::set_var("GOTTEM_TEST_2CAPTCHA_KEY", "test-key");
    let server = MockServer::start().await;
    let adapter = adapter_against(&server);
    let route = captcha_route();
    let mut req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    req.extra.insert("siteKey".into(), json!("6Lc-test"));
    // intentionally NO captchaType
    let err = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap_err();
    assert!(matches!(err, FetchError::Config(_)), "got {err:?}");
}

#[tokio::test]
async fn missing_site_key_extra_is_config_error() {
    std::env::set_var("GOTTEM_TEST_2CAPTCHA_KEY", "test-key");
    let server = MockServer::start().await;
    let adapter = adapter_against(&server);
    let route = captcha_route();
    let mut req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
    req.extra.insert("captchaType".into(), json!("recaptcha_v2"));
    // intentionally NO siteKey
    let err = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap_err();
    assert!(matches!(err, FetchError::Config(_)), "got {err:?}");
}

#[tokio::test]
async fn unknown_captcha_type_is_config_error() {
    std::env::set_var("GOTTEM_TEST_2CAPTCHA_KEY", "test-key");
    let server = MockServer::start().await;
    let adapter = adapter_against(&server);
    let route = captcha_route();
    let req = captcha_req("solvepuzzle_v1", "6Lc-test", "https://example.com/");
    let err = adapter.execute(&route, &req, &AdapterContext::new(0), &CancelToken::new())
        .await.unwrap_err();
    assert!(matches!(err, FetchError::Config(_)), "got {err:?}");
}

#[tokio::test]
async fn cancel_aborts_during_initial_delay() {
    std::env::set_var("GOTTEM_TEST_2CAPTCHA_KEY", "test-key");
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/in.php"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"status":1,"request":"99999"}"#),
        )
        .mount(&server).await;

    let adapter = Captcha2CaptchaAdapter::new()
        .with_endpoints(
            format!("{}/in.php", server.uri()),
            format!("{}/res.php", server.uri()),
        )
        // Long initial delay we expect to be cut short by cancel.
        .with_initial_delay(Duration::from_secs(5))
        .with_poll_interval(Duration::from_millis(20))
        .with_max_polls(5);

    let route = captcha_route();
    let req = captcha_req("recaptcha_v2", "6Lc-test", "https://example.com/");

    let cancel = CancelToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_clone.cancel();
    });

    let started = std::time::Instant::now();
    let err = adapter.execute(&route, &req, &AdapterContext::new(0), &cancel)
        .await.unwrap_err();
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(2), "cancel propagation too slow: {elapsed:?}");
    assert!(matches!(err, FetchError::Cancelled), "got {err:?}");
}
