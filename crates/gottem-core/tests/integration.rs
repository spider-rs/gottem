//! End-to-end orchestrator tests using an in-memory MockAdapter.
//!
//! Covers the no-deadlock / no-regression promises:
//! - Ladder escalation across tiers (mirror of spider-cli probe_tiers.py)
//! - Validator-driven escalation (`min_bytes=500` short-content gate)
//! - Race winner cancels losers (tail-latency improvement)
//! - Budget ceiling halts escalation (cost cap)
//! - Outer CancelToken aborts in-flight fetches promptly

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, AdapterRegistry, Budget, CancelToken, Capabilities,
    EndpointTemplate, FetchError, HedgeConfig, HttpMethod, LadderStrategy, Orchestrator, Route,
    RouteCatalog, RouteCatalogBuilder, ScrapeRequest, ScrapeResponse, Tier, Validator,
};
use url::Url;

/// In-memory adapter for tests. Per-route status + body overrides; default success.
#[derive(Default)]
struct MockAdapter {
    behavior: Mutex<HashMap<String, (u16, Vec<u8>)>>,
    delay_per_route: Mutex<HashMap<String, u64>>,
    calls: AtomicUsize,
    delay_ms: AtomicUsize,
}

impl MockAdapter {
    fn new() -> Arc<Self> { Arc::new(Self::default()) }

    fn set(&self, id: &str, status: u16, body: &[u8]) {
        self.behavior.lock().unwrap().insert(id.into(), (status, body.to_vec()));
    }

    fn behavior_for(&self, id: &str) -> (u16, Vec<u8>) {
        self.behavior.lock().unwrap()
            .get(id).cloned()
            .unwrap_or_else(|| (200, b"this is a long enough response to clear the min_bytes=500 validator. ".repeat(20)))
    }

    fn set_delay(&self, id: &str, ms: u64) {
        self.delay_per_route.lock().unwrap().insert(id.into(), ms);
    }

    fn delay_for(&self, id: &str) -> u64 {
        self.delay_per_route.lock().unwrap().get(id).copied()
            .unwrap_or_else(|| self.delay_ms.load(Ordering::Relaxed) as u64)
    }
}

#[async_trait]
impl Adapter for MockAdapter {
    fn kind(&self) -> AdapterKind { AdapterKind::Custom(Arc::from("mock")) }

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        _ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let delay = self.delay_for(&route.id);
        if delay > 0 {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(FetchError::Cancelled),
                _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {}
            }
        }
        let (status, body) = self.behavior_for(&route.id);
        if status >= 400 {
            return Err(FetchError::Status(status));
        }
        let body_bytes = Bytes::from(body.clone());
        Ok(ScrapeResponse {
            url: req.url.clone(),
            status,
            headers: vec![],
            body: body_bytes,
            content: Some(String::from_utf8_lossy(&body).into_owned()),
            route_id: route.id.clone(),
            tier: route.tier,
            cost_milli: route.cost,
            elapsed: std::time::Duration::ZERO,
            attempt: 0,
            metadata: Default::default(),
        })
    }
}

fn route(id: &str, tier: Tier, cost: u64) -> Route {
    Route {
        id: Arc::from(id),
        adapter: AdapterKind::Custom(Arc::from("mock")),
        endpoint: EndpointTemplate::parse("https://example.test/").unwrap(),
        method: HttpMethod::Get,
        auth: Default::default(),
        headers: vec![],
        body: Default::default(),
        parse: Default::default(),
        validate: vec![Validator::MinBytes { n: 500 }],
        tier,
        cost,
        priority: 100,
        caps: Capabilities::default(),
        timeout_ms: 5_000,
        concurrency: 8,
        retry_on: Default::default(),
    }
}

struct Harness {
    orch: Arc<Orchestrator>,
    catalog: Arc<RouteCatalog>,
    mock: Arc<MockAdapter>,
}

fn build(budget: u64) -> Harness {
    let catalog = Arc::new(
        RouteCatalogBuilder::new()
            .add(route("local.http", Tier::T0, 0))
            .add(route("cloud.cheap", Tier::T4, 10))
            .add(route("cloud.smart", Tier::T7, 100))
            .build(),
    );
    let mock = MockAdapter::new();
    let mut reg = AdapterRegistry::new();
    reg.register(mock.clone() as Arc<dyn Adapter>);
    let orch = Arc::new(Orchestrator::new(
        catalog.clone(),
        Arc::new(reg),
        Arc::new(Budget::new(budget)),
    ));
    Harness { orch, catalog, mock }
}

fn ladder(catalog: Arc<RouteCatalog>, max_retries: u32) -> Arc<LadderStrategy> {
    Arc::new(LadderStrategy::new(
        catalog,
        Tier::T0,
        Tier::T9,
        Capabilities::default(),
        max_retries,
    ))
}

#[tokio::test]
async fn ladder_succeeds_at_lowest_tier() {
    let h = build(10_000);
    let strategy = ladder(h.catalog.clone(), 5);

    let req = ScrapeRequest::get(Url::parse("https://example.test/").unwrap());
    let resp = h.orch
        .fetch_cheap(req, strategy, CancelToken::new())
        .await
        .expect("expected success");
    assert_eq!(resp.tier, Tier::T0);
    assert_eq!(h.mock.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn ladder_escalates_on_validation_failure() {
    let h = build(10_000);
    // T0 returns too few bytes -> MinBytes validator fails -> escalate.
    h.mock.set("local.http", 200, b"tiny");
    let strategy = ladder(h.catalog.clone(), 5);
    let req = ScrapeRequest::get(Url::parse("https://example.test/").unwrap());
    let resp = h.orch.fetch_cheap(req, strategy, CancelToken::new()).await.unwrap();
    assert_eq!(resp.tier, Tier::T4);
    assert!(h.mock.calls.load(Ordering::Relaxed) >= 2);
}

#[tokio::test]
async fn ladder_escalates_on_5xx_status() {
    let h = build(10_000);
    h.mock.set("local.http", 503, b"upstream down");
    h.mock.set("cloud.cheap", 503, b"upstream down");
    let strategy = ladder(h.catalog.clone(), 5);
    let req = ScrapeRequest::get(Url::parse("https://example.test/").unwrap());
    let resp = h.orch.fetch_cheap(req, strategy, CancelToken::new()).await.unwrap();
    assert_eq!(resp.tier, Tier::T7);
}

#[tokio::test]
async fn budget_ceiling_blocks_escalation() {
    let h = build(50); // Allows T0 ($0) and T4 ($0.001 = 10 mc) but not T7 ($0.01 = 100 mc).
    h.mock.set("local.http", 200, b"tiny");
    h.mock.set("cloud.cheap", 200, b"tiny");
    let strategy = ladder(h.catalog.clone(), 5);
    let req = ScrapeRequest::get(Url::parse("https://example.test/").unwrap());
    let err = h.orch.fetch_cheap(req, strategy, CancelToken::new()).await.unwrap_err();
    assert!(matches!(err, FetchError::BudgetExceeded { .. }), "got {err:?}");
}

#[tokio::test]
async fn race_winner_cancels_losers() {
    let h = build(10_000);
    h.mock.delay_ms.store(50, Ordering::Relaxed);
    let req = ScrapeRequest::get(Url::parse("https://example.test/").unwrap());
    let started = std::time::Instant::now();
    let resp = h.orch
        .fetch_race(req, &["local.http", "cloud.cheap", "cloud.smart"], CancelToken::new())
        .await
        .unwrap();
    let elapsed = started.elapsed();
    // All three start in parallel; first finishes in ~50ms.
    assert!(elapsed.as_millis() < 200, "race took too long: {elapsed:?}");
    assert!(matches!(resp.route_id.as_ref(), "local.http" | "cloud.cheap" | "cloud.smart"));
}

#[tokio::test]
async fn hedge_primary_wins_when_fast() {
    let h = build(10_000);
    // No delay on primary T0, slow delay on hedge T4. Primary should return first.
    h.mock.set_delay("cloud.cheap", 200);
    let strategy = ladder(h.catalog.clone(), 5);
    let req = ScrapeRequest::get(Url::parse("https://example.test/").unwrap());
    let hedge_cfg = HedgeConfig {
        delay: std::time::Duration::from_millis(30),
        max_hedges: 1,
        enabled: true,
    };
    let started = std::time::Instant::now();
    let resp = h.orch
        .fetch_hedge(req, strategy, hedge_cfg, CancelToken::new())
        .await
        .expect("hedge fetch ok");
    let elapsed = started.elapsed();
    assert_eq!(resp.tier, Tier::T0, "primary T0 should win");
    assert!(elapsed.as_millis() < 100, "primary should win fast: {elapsed:?}");
}

#[tokio::test]
async fn hedge_backup_wins_when_primary_slow() {
    let h = build(10_000);
    // Primary T0 hangs for 500ms; hedge T4 is fast. Hedge should beat primary.
    h.mock.set_delay("local.http", 500);
    let strategy = ladder(h.catalog.clone(), 5);
    let req = ScrapeRequest::get(Url::parse("https://example.test/").unwrap());
    let hedge_cfg = HedgeConfig {
        delay: std::time::Duration::from_millis(30),
        max_hedges: 1,
        enabled: true,
    };
    let started = std::time::Instant::now();
    let resp = h.orch
        .fetch_hedge(req, strategy, hedge_cfg, CancelToken::new())
        .await
        .expect("hedge fetch ok");
    let elapsed = started.elapsed();
    assert_eq!(resp.tier, Tier::T4, "hedge T4 should win when primary is slow");
    // Should NOT wait the full 500ms — hedge fires at ~30ms and wins quickly.
    assert!(elapsed.as_millis() < 400, "hedge didn't accelerate the win: {elapsed:?}");
}

#[tokio::test]
async fn hedge_disabled_falls_back_to_cheap() {
    let h = build(10_000);
    let strategy = ladder(h.catalog.clone(), 5);
    let req = ScrapeRequest::get(Url::parse("https://example.test/").unwrap());
    let hedge_cfg = HedgeConfig {
        delay: std::time::Duration::from_millis(10),
        max_hedges: 1,
        enabled: false, // disabled — should run fetch_cheap path
    };
    let resp = h.orch
        .fetch_hedge(req, strategy, hedge_cfg, CancelToken::new())
        .await
        .expect("hedge disabled still fetches");
    assert_eq!(resp.tier, Tier::T0);
    assert_eq!(h.mock.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn outer_cancel_token_aborts_fetch() {
    let h = build(10_000);
    h.mock.delay_ms.store(500, Ordering::Relaxed);
    let cancel = CancelToken::new();
    let cancel_clone = cancel.clone();
    let orch = h.orch.clone();
    let handle = tokio::spawn(async move {
        let req = ScrapeRequest::get(Url::parse("https://example.test/").unwrap());
        orch.fetch_race(req, &["local.http"], cancel_clone).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    cancel.cancel();
    let result = handle.await.unwrap();
    assert!(matches!(result, Err(FetchError::Cancelled) | Err(FetchError::Exhausted)), "got {result:?}");
}

#[tokio::test]
async fn toml_round_trip() {
    let toml_str = r#"
[[route]]
id = "firecrawl.scrape"
adapter = "http_json"
endpoint = "https://api.firecrawl.dev/v1/scrape"
method = "POST"
tier = 4
cost = 10
timeout_ms = 30000
concurrency = 20

[route.auth]
kind = "bearer"
env = "FIRECRAWL_API_KEY"

[route.body]
kind = "json"
template = '{"url":"{{url}}","formats":["markdown"]}'

[route.parse]
kind = "json_path"
path = "$.data.markdown"

[[route.validate]]
kind = "min_bytes"
n = 500
"#;
    let catalog = RouteCatalogBuilder::new()
        .add_toml(toml_str)
        .unwrap()
        .build();
    assert_eq!(catalog.len(), 1);
    let r = catalog.get("firecrawl.scrape").unwrap();
    assert_eq!(r.tier, Tier::T4);
    assert_eq!(r.cost, 10);
    assert_eq!(r.timeout_ms, 30_000);
    assert_eq!(r.concurrency, 20);
}
