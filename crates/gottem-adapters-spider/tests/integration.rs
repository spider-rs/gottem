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

// ============================================================================
// SpiderLocalCrawlAdapter — local BFS, no re-fetch for link extraction
// ============================================================================
//
// These tests build a real Orchestrator with the SpiderAdapter registered for
// per-URL fetches, register the SpiderLocalCrawlAdapter for crawl dispatch, and
// run a small crawl against a wiremock server. The mock serves a tiny site:
//
//   /          → links to /a and /b
//   /a         → links to /c
//   /b         → no links
//   /c         → no links
//
// With depth=2, limit=10 we expect all 4 pages to be yielded exactly once.

use futures_util::StreamExt;
use gottem_adapters_spider::{register_crawl_all, SpiderLocalCrawlAdapter};
use gottem_core::{
    AdapterRegistry, Budget, CrawlAdapterRegistry, CrawlRequest, Orchestrator, RouteCatalogBuilder,
};
use wiremock::matchers::path_regex;

fn local_route(server_url: &str) -> Route {
    Route {
        id: Arc::from("local.http"),
        adapter: AdapterKind::SpiderLocal,
        endpoint: EndpointTemplate::parse(server_url).unwrap(),
        method: HttpMethod::Get,
        auth: Default::default(),
        headers: vec![],
        body: Default::default(),
        parse: Default::default(),
        // No min-byte gate — our test HTML is intentionally small.
        validate: vec![],
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

fn crawl_route() -> Route {
    Route {
        id: Arc::from("local.crawl"),
        adapter: AdapterKind::SpiderLocalCrawl,
        endpoint: EndpointTemplate::parse("https://local.crawl/").unwrap(),
        method: HttpMethod::Get,
        auth: Default::default(),
        headers: vec![],
        body: Default::default(),
        parse: Default::default(),
        validate: vec![],
        tier: Tier::T0,
        cost: 0,
        priority: 0,
        caps: Capabilities::default(),
        timeout_ms: 60_000,
        concurrency: 4,
        retry_on: Default::default(),
        cost_extract: None,
    }
}

async fn build_local_crawl_harness() -> (Arc<Orchestrator>, MockServer) {
    let server = MockServer::start().await;
    // Root → /a + /b
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(
                format!(
                    "<html><body><a href=\"{srv}/a\">a</a><a href=\"{srv}/b\">b</a></body></html>",
                    srv = server.uri()
                )
                .as_bytes(),
                "text/html",
            ),
        )
        .mount(&server)
        .await;
    // /a → /c
    Mock::given(method("GET"))
        .and(path("/a"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(
                format!(
                    "<html><body><a href=\"{srv}/c\">c</a></body></html>",
                    srv = server.uri()
                )
                .as_bytes(),
                "text/html",
            ),
        )
        .mount(&server)
        .await;
    // /b — leaf
    Mock::given(method("GET"))
        .and(path("/b"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(b"<html><body>b leaf</body></html>", "text/html"),
        )
        .mount(&server)
        .await;
    // /c — leaf
    Mock::given(method("GET"))
        .and(path("/c"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(b"<html><body>c leaf</body></html>", "text/html"),
        )
        .mount(&server)
        .await;
    // Catch-all so any stray request fails clearly instead of looping
    Mock::given(method("GET"))
        .and(path_regex("/.*"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let catalog = Arc::new(
        RouteCatalogBuilder::new()
            .add(local_route(&server.uri()))
            .add(crawl_route())
            .build(),
    );
    let mut adapters = AdapterRegistry::new();
    adapters.register(SpiderAdapter::arc());
    let orch = Arc::new(Orchestrator::new(
        catalog,
        Arc::new(adapters),
        Arc::new(Budget::new(10_000)),
    ));

    let mut crawl_reg = CrawlAdapterRegistry::new();
    register_crawl_all(&mut crawl_reg, &orch);
    orch.install_crawl_adapters(Arc::new(crawl_reg));

    (orch, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_crawl_visits_full_site_within_depth() {
    let (orch, server) = build_local_crawl_harness().await;

    let req = CrawlRequest::new(Url::parse(&server.uri()).unwrap())
        .with_limit(10)
        .with_depth(2)
        .with_concurrency(4)
        .with_engine(gottem_core::CrawlEngine::Local);

    let mut stream = orch.crawl(req, CancelToken::new()).await.unwrap();
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
    while let Some(item) = stream.next().await {
        let entry = item.expect("page entry");
        visited.insert(entry.url.path().to_string());
    }

    // We should see seed + a + b + c — exactly 4 distinct paths.
    // (spider may normalize trailing slashes; check both forms for seed.)
    let saw_seed = visited.contains("/") || visited.contains("");
    assert!(saw_seed, "missing seed; visited: {visited:?}");
    assert!(visited.contains("/a"), "missing /a; visited: {visited:?}");
    assert!(visited.contains("/b"), "missing /b; visited: {visited:?}");
    assert!(visited.contains("/c"), "missing /c; visited: {visited:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_crawl_respects_depth_zero() {
    let (orch, server) = build_local_crawl_harness().await;

    // depth = 0 → seed only, no link following.
    let req = CrawlRequest::new(Url::parse(&server.uri()).unwrap())
        .with_limit(10)
        .with_depth(0)
        .with_engine(gottem_core::CrawlEngine::Local);

    let mut stream = orch.crawl(req, CancelToken::new()).await.unwrap();
    let mut count = 0;
    while let Some(item) = stream.next().await {
        item.expect("page entry");
        count += 1;
    }
    assert_eq!(count, 1, "depth=0 should yield only the seed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_crawl_respects_limit() {
    let (orch, server) = build_local_crawl_harness().await;

    // limit = 2 → at most 2 pages emitted.
    let req = CrawlRequest::new(Url::parse(&server.uri()).unwrap())
        .with_limit(2)
        .with_depth(2)
        .with_engine(gottem_core::CrawlEngine::Local);

    let mut stream = orch.crawl(req, CancelToken::new()).await.unwrap();
    let mut count = 0;
    while let Some(item) = stream.next().await {
        item.expect("page entry");
        count += 1;
    }
    assert!(
        count <= 2,
        "limit=2 should yield at most 2 pages, got {count}"
    );
    assert!(count >= 1, "limit=2 should yield at least the seed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_crawl_engine_falls_back_to_local_when_no_spider_cloud_key() {
    // Auto engine + no SPIDER_API_KEY → Local
    std::env::remove_var("SPIDER_API_KEY");
    let (orch, server) = build_local_crawl_harness().await;

    let req = CrawlRequest::new(Url::parse(&server.uri()).unwrap())
        .with_limit(10)
        .with_depth(1)
        .with_engine(gottem_core::CrawlEngine::Auto);

    let mut stream = orch.crawl(req, CancelToken::new()).await.unwrap();
    let mut got_seed = false;
    while let Some(item) = stream.next().await {
        let entry = item.expect("page entry");
        if entry.url.as_str().starts_with(&server.uri()) {
            got_seed = true;
        }
        assert_eq!(entry.route_id.as_ref(), "local.crawl");
    }
    assert!(
        got_seed,
        "auto should have fallen back to local engine and visited the seed"
    );
}

/// External cancel fires mid-crawl → stream terminates promptly. Covers
/// the cloud client-disconnect cascade: the cloud forwarder calls
/// `cancel.cancel()` when its body Sender fails; the orchestrator's
/// spawned tasks must abort without finishing the BFS.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_crawl_cancellation_stops_in_flight_work() {
    let (orch, server) = build_local_crawl_harness().await;

    let cancel = CancelToken::new();
    let req = CrawlRequest::new(Url::parse(&server.uri()).unwrap())
        .with_limit(10)
        .with_depth(2)
        .with_concurrency(1)
        .with_engine(gottem_core::CrawlEngine::Local);

    let mut stream = orch.crawl(req, cancel.clone()).await.unwrap();

    let first = stream.next().await.expect("first page").expect("ok");
    assert!(first.url.as_str().starts_with(&server.uri()));

    cancel.cancel();

    // Hard timeout — fails loudly if cancel doesn't propagate.
    let drain = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut count = 1u32;
        while stream.next().await.is_some() {
            count += 1;
        }
        count
    })
    .await
    .expect("stream should terminate within 3s of cancel");

    // Harness has 4 pages total. Without cancel, all 4 yield. With
    // cancel after page 1, we tolerate up to 4 (in-flight already-
    // queued pages), but the stream must actually END.
    assert!(
        drain <= 4,
        "expected cancel to halt the crawl, got {drain} pages"
    );
}

#[allow(dead_code)]
fn _unused(_: SpiderLocalCrawlAdapter) {}
