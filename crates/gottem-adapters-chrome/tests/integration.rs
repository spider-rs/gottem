//! Lifecycle tests for the CDP adapters — the browser-session paths where
//! leaks live: does a failed create leak a session, does a failed drive still
//! release the (per-second-billed) Kernel session, does the release retry,
//! and does a dead CDP endpoint fail fast instead of hanging.
//!
//! wiremock stands in for Kernel's REST API; a closed local port stands in
//! for the CDP WebSocket (drive must fail fast, which is exactly what these
//! tests want — no real browser is involved).

use std::sync::Arc;
use std::time::Duration;

use gottem_adapters_chrome::{ChromeCdpAdapter, KernelCdpAdapter, KernelConfig};
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, AuthSpec, CancelToken, EndpointTemplate, Route,
    ScrapeRequest, Tier,
};
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn kernel_route(create_url: &str) -> Route {
    Route {
        id: Arc::from("kernel.cdp"),
        adapter: AdapterKind::Custom(Arc::from("kernel_cdp")),
        endpoint: EndpointTemplate::parse(create_url).unwrap(),
        method: gottem_core::HttpMethod::Post,
        auth: AuthSpec::Bearer {
            env: "TEST_KERNEL_KEY".into(),
        },
        headers: vec![],
        body: Default::default(),
        parse: Default::default(),
        validate: vec![],
        tier: Tier::T8,
        cost: 100,
        priority: 100,
        caps: Default::default(),
        timeout_ms: 10_000,
        concurrency: 4,
        retry_on: Default::default(),
        cost_extract: None,
        geo_map: None,
    }
}

fn req() -> ScrapeRequest {
    let mut r = ScrapeRequest::get(Url::parse("https://target.example/").unwrap());
    r.credentials
        .insert("TEST_KERNEL_KEY".into(), "test-key".into());
    r
}

/// Raw-chromey driver so the drive step is a fast connect-refused failure —
/// no spider machinery, no real browser.
fn chromey_config() -> KernelConfig {
    KernelConfig {
        default_use_spider: false,
        ..KernelConfig::default()
    }
}

/// Create returns 500 → execute errors and NO delete is fired (there is no
/// session to release).
#[tokio::test]
async fn kernel_create_failure_fires_no_delete() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/browsers"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    // Any DELETE would be a bug — expect exactly zero.
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let adapter = KernelCdpAdapter::with_config(reqwest::Client::new(), chromey_config());
    let route = kernel_route(&format!("{}/browsers", server.uri()));
    let err = adapter
        .execute(&route, &req(), &AdapterContext::new(0), &CancelToken::new())
        .await
        .expect_err("create 500 must fail the scrape");
    assert!(
        err.to_string().contains("500"),
        "error should carry the create status: {err}"
    );
    // Mock expectations (0 DELETEs) verified on server drop.
}

/// Create succeeds, drive fails fast (dead WS port) → the session release
/// still fires. This is the money-leak path: Kernel bills per second until
/// the DELETE lands.
#[tokio::test]
async fn kernel_drive_failure_still_releases_session() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/browsers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            // Port 1 is never listening — connect refused, fast.
            "cdp_ws_url": "ws://127.0.0.1:1/",
            "session_id": "sess-test-1",
            "headless": true,
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/browsers/sess-test-1"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = KernelCdpAdapter::with_config(reqwest::Client::new(), chromey_config())
        .with_connect_timeout(Duration::from_secs(2));
    let route = kernel_route(&format!("{}/browsers", server.uri()));
    let res = adapter
        .execute(&route, &req(), &AdapterContext::new(0), &CancelToken::new())
        .await;
    assert!(res.is_err(), "dead CDP endpoint must fail the scrape");

    // The release runs on a spawned task off the response path — poll until
    // the DELETE lands (the mock's expect(1) also verifies on drop). Don't
    // assert the process-global inflight gauge here: sibling tests share it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let hits = server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| r.method.as_str() == "DELETE")
            .count();
        if hits >= 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "session release DELETE never fired"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// DELETE fails twice (500) → the release retries once (2 hits total) and the
/// failure counter moves. Slow test (~5s retry backoff) but it pins the exact
/// behavior that bounds the billing leak.
#[tokio::test]
async fn kernel_release_retries_once_then_counts_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/browsers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "cdp_ws_url": "ws://127.0.0.1:1/",
            "session_id": "sess-test-2",
            "headless": true,
        })))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/browsers/sess-test-2"))
        .respond_with(ResponseTemplate::new(500))
        .expect(2) // first attempt + one retry, then give up
        .mount(&server)
        .await;

    let failures_before = gottem_adapters_chrome::kernel_release_failures();
    let adapter = KernelCdpAdapter::with_config(reqwest::Client::new(), chromey_config())
        .with_connect_timeout(Duration::from_secs(2));
    let route = kernel_route(&format!("{}/browsers", server.uri()));
    let _ = adapter
        .execute(&route, &req(), &AdapterContext::new(0), &CancelToken::new())
        .await;

    // attempt(≤10s bound, real ~instant) + 5s backoff + retry — poll rather
    // than a fixed sleep so the test finishes as soon as the counter moves.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if gottem_adapters_chrome::kernel_release_failures() > failures_before {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "release failure counter never moved"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A dead static CDP endpoint fails fast — no hang, no deadlock. The whole
/// execute is wrapped in an outer timeout that would trip if the adapter's
/// internal connect bound was broken.
#[tokio::test]
async fn chrome_cdp_connect_refused_fails_fast() {
    let adapter = ChromeCdpAdapter::new().with_connect_timeout(Duration::from_secs(2));
    let mut route = kernel_route("ws://127.0.0.1:1/");
    route.adapter = AdapterKind::ChromeCdp;
    route.auth = AuthSpec::None;

    let out = tokio::time::timeout(
        Duration::from_secs(10),
        adapter.execute(&route, &req(), &AdapterContext::new(0), &CancelToken::new()),
    )
    .await
    .expect("must not hang past the connect bound");
    assert!(out.is_err(), "connect refused must surface as an error");
}
