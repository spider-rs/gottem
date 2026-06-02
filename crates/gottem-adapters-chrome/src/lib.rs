//! gottem CDP adapter — connects to a remote (or local) Chrome instance via WebSocket
//! and drives it with the DevTools Protocol.
//!
//! Targets tier **T8** of gottem's ladder. Vendors covered:
//!
//! | Vendor                       | Endpoint shape                                      | Auth                  |
//! |------------------------------|-----------------------------------------------------|------------------------|
//! | Brightdata Scraping Browser  | `wss://brd.superproxy.io:9222`                      | `WsUserinfo` env       |
//! | Browserless                  | `wss://chrome.browserless.io?token={{env:TOKEN}}`   | URL template (no auth) |
//! | Spider Browser Cloud         | `wss://browser.spider.cloud/v1/browser?api_key=...` | URL template (no auth) |
//! | Local Chrome / chrome-remote | `ws://localhost:9222/devtools/browser/<id>`         | None                   |
//!
//! [`KernelCdpAdapter`] covers Kernel ([onkernel.com](https://kernel.sh)), which
//! has no *static* endpoint: it `POST`s `/browsers` to mint a per-session
//! `cdp_ws_url`, drives it (raw chromey [`drive_cdp`] by default, or
//! `spider::Website` when `provider_options.kernel.spider = true`), then
//! `DELETE`s the session (Kernel bills per running second). It reports
//! [`AdapterKind::Custom`]`("kernel_cdp")`.
//!
//! ## CDP via chromey
//!
//! We depend on the **`chromey` package** directly — the chromiumoxide fork
//! the spider stack standardized on. The package's lib name is still
//! `chromiumoxide`, so code reads as `chromey::Browser` etc., matching
//! the original surface 1:1. Going through `chromey` rather than
//! `spider::chromiumoxide` keeps this adapter decoupled from spider's
//! feature flags and pins the fork explicitly in Cargo.toml.
//!
//! ## Cancellation + no-deadlock
//!
//! Three async checkpoints, each guarded by `tokio::select!` against [`CancelToken`]:
//! 1. **Connect** — bounded by a 15-second connect timeout AND the outer cancel token.
//! 2. **Navigate + content** — bounded by `route.timeout()` AND cancel.
//! 3. **Handler task** — required to drive CDP event processing; on
//!    cleanup the browser is dropped first, which closes the WebSocket; the
//!    handler stream then yields None and the loop exits naturally. The handle
//!    is `await`-ed with a 2-second timeout so the task is deterministically
//!    joined (no detached zombies in a hot fetch loop), and on the rare timeout
//!    the JoinHandle is dropped — task ends shortly after with no live resources.
//!
//! Dropping the browser handle closes the WebSocket connection and (for
//! Brightdata-style per-session billing) ends the remote browser session.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
// Alias the chromey package's lib (it ships as `chromiumoxide` for source
// compatibility with the original crate) under its package name so code in
// this module reads as `chromey::Browser` etc. — the name we standardized
// on across the spider stack. One-line shim; no upstream changes needed.
use chromiumoxide as chromey;
use futures_util::StreamExt;
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, AuthSpec, CancelToken, FetchError, Route, ScrapeRequest,
    ScrapeResponse,
};

/// Adapter for [`AdapterKind::ChromeCdp`]. One instance is reusable across many requests —
/// each call to [`execute`](Adapter::execute) opens a fresh CDP connection so that vendor
/// sessions (e.g. Brightdata Scraping Browser) are properly isolated.
#[derive(Debug, Default, Clone)]
pub struct ChromeCdpAdapter {
    connect_timeout: Duration,
}

impl ChromeCdpAdapter {
    pub fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(15),
        }
    }

    pub fn with_connect_timeout(mut self, t: Duration) -> Self {
        self.connect_timeout = t;
        self
    }

    pub fn arc() -> Arc<dyn Adapter> {
        Arc::new(Self::new())
    }
}

#[async_trait]
impl Adapter for ChromeCdpAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::ChromeCdp
    }

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        let ws_url = build_ws_url(route, req)?;
        let (status, html) = drive_cdp(
            &ws_url,
            req.url.as_str(),
            self.connect_timeout,
            route.timeout(),
            cancel,
        )
        .await?;

        if status >= 400 {
            return Err(FetchError::Status(status));
        }
        Ok(cdp_response(route, req, ctx, status, html))
    }
}

/// Connect to a CDP WebSocket, navigate to `target_url`, return `(status, html)`.
///
/// This is the shared chromey-driving core: both [`ChromeCdpAdapter`] (static
/// vendor endpoint) and [`KernelCdpAdapter`] (per-session endpoint minted via
/// REST) funnel through here so the connect / handler-task / cancellation /
/// no-deadlock machinery lives in exactly one place.
///
/// Three async checkpoints, each guarded by `tokio::select!` against `cancel`:
/// 1. **Connect** — bounded by `connect_timeout` AND the cancel token.
/// 2. **Navigate + content** — bounded by `route_timeout` AND cancel.
/// 3. **Handler task** — required to drive CDP events or every op hangs; the
///    browser is dropped first (closes the WebSocket), the handler stream then
///    yields `None` and the loop exits, and the handle is joined with a 2s
///    safety-net timeout so there are no detached zombies in a hot fetch loop.
async fn drive_cdp(
    ws_url: &str,
    target_url: &str,
    connect_timeout: Duration,
    route_timeout: Duration,
    cancel: &CancelToken,
) -> Result<(u16, String), FetchError> {
    // ---- Connect (cancel-aware + connect_timeout-bounded) -----------------
    let connect_fut = tokio::time::timeout(connect_timeout, chromey::Browser::connect(ws_url));
    let (browser, mut handler) = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(FetchError::Cancelled),
        r = connect_fut => match r {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => return Err(FetchError::Network(format!("chrome connect: {e}"))),
            Err(_) => return Err(FetchError::Timeout(connect_timeout)),
        },
    };

    // chromey requires the handler to be polled or *all* operations hang.
    let handler_task = tokio::spawn(async move { while handler.next().await.is_some() {} });

    // ---- Navigate + content (cancel + per-route timeout) ------------------
    let work = navigate_and_extract(&browser, target_url);
    let work_outcome = tokio::time::timeout(route_timeout, async {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(FetchError::Cancelled),
            r = work => r,
        }
    })
    .await;

    // ---- Cleanup: close browser, let the handler exit naturally -----------
    // Drop the browser first so the WebSocket closes — chromey's handler stream
    // yields None on disconnect and the spawned loop exits on its own. Awaiting
    // the handle joins it deterministically (no detached zombie in a hot fetch
    // loop). The 2s timeout is the safety net for the edge case where the
    // WS-close signal doesn't promptly reach the handler (vendor proxy holding
    // the connection open, etc.) — on timeout the JoinHandle is dropped, which
    // detaches; the task ends shortly after with no resources held except a dead
    // browser reference. We deliberately do NOT `abort()` — aborting mid-stream
    // would interrupt the handler's own teardown and surface as a JoinError.
    drop(browser);
    let _ = tokio::time::timeout(Duration::from_secs(2), handler_task).await;

    match work_outcome {
        Ok(Ok(out)) => Ok(out),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(FetchError::Timeout(route_timeout)),
    }
}

/// Build the `ScrapeResponse` shared by both CDP adapters. The page HTML is
/// moved into a single `Bytes` and shared as both `body` and `content` — a
/// refcount bump on clone, not a second copy of the (potentially large) HTML.
fn cdp_response(
    route: &Route,
    req: &ScrapeRequest,
    ctx: &AdapterContext,
    status: u16,
    html: String,
) -> ScrapeResponse {
    let body = Bytes::from(html.into_bytes());
    ScrapeResponse {
        url: req.url.clone(),
        status,
        headers: vec![],
        body: body.clone(),
        content: Some(body),
        route_id: route.id.clone(),
        tier: route.tier,
        cost_milli: route.cost,
        cost_actual_units: None,
        cost_actual_unit: None,
        elapsed: ctx.elapsed(),
        attempt: ctx.attempt,
        metadata: Default::default(),
    }
}

/// Open a fresh tab, navigate to `url`, wait for `load`, return `(status, html)`.
async fn navigate_and_extract(
    browser: &chromey::Browser,
    url: &str,
) -> Result<(u16, String), FetchError> {
    let page = browser
        .new_page(url)
        .await
        .map_err(|e| FetchError::Network(format!("new_page: {e}")))?;

    // Wait for the main frame to finish navigating. We don't fail on wait_for_navigation
    // errors because some single-page apps never emit a final load — we still want to
    // capture whatever DOM is present.
    let _ = page.wait_for_navigation().await;

    let html = page
        .content()
        .await
        .map_err(|e| FetchError::Parse(format!("content: {e}")))?;

    // chromey doesn't always surface the main-frame HTTP status code in a
    // first-class way; we default to 200 on successful HTML extraction.
    Ok((200, html))
}

/// Resolve the route's CDP endpoint into a final WebSocket URL string.
///
/// Steps:
/// 1. Render endpoint template (`{{url}}`, `{{env:NAME}}` substitution).
/// 2. If `AuthSpec::WsUserinfo` is set, inject the env-var-derived `user:pass` into the URL.
pub(crate) fn build_ws_url(route: &Route, req: &ScrapeRequest) -> Result<String, FetchError> {
    let mut url = route.endpoint.render(req)?;

    if let AuthSpec::WsUserinfo { env } = &route.auth {
        // Per-request resolver — BYOK keys shadow the process env for CDP
        // websocket userinfo just like for HTTP bearer auth.
        let userinfo = req
            .resolve_env(env)
            .ok_or_else(|| FetchError::Auth(format!("missing env var: {env}")))?;
        let (user, pass) = match userinfo.split_once(':') {
            Some((u, p)) => (u.to_string(), Some(p.to_string())),
            None => (userinfo, None),
        };
        url.set_username(&user).map_err(|_| {
            FetchError::Config("WebSocket URL scheme doesn't support username".into())
        })?;
        if let Some(p) = pass {
            url.set_password(Some(&p)).map_err(|_| {
                FetchError::Config("WebSocket URL scheme doesn't support password".into())
            })?;
        }
    }

    Ok(url.to_string())
}

// ===========================================================================
// Kernel (onkernel.com) — cloud Chromium driven over CDP
// ===========================================================================

/// Adapter id Kernel routes set as `adapter = "kernel_cdp"`. Resolved by the
/// catalog deserializer to [`AdapterKind::Custom`] (the documented escape hatch
/// for user-registered adapters), so adding Kernel needs no `gottem-core` change.
pub const KERNEL_ADAPTER_ID: &str = "kernel_cdp";

/// Base for the create/delete REST round-trip when the route's endpoint is
/// itself a WebSocket (it isn't, for Kernel — but keep the constant near the
/// adapter that owns the default).
const KERNEL_DEFAULT_CREATE_URL: &str = "https://api.onkernel.com/browsers";

/// Adapter for Kernel ([onkernel.com](https://kernel.sh)). Kernel has no static
/// CDP endpoint: each scrape first `POST`s `/browsers` to mint a per-session
/// `cdp_ws_url`, drives it, then `DELETE`s the session — Kernel bills per
/// *running* second, so releasing promptly is a cost guarantee, not just hygiene.
///
/// Two drivers, switchable per request:
/// - **default** — raw chromey ([`drive_cdp`]): direct CDP, lean and fast.
/// - **`provider_options.kernel.spider = true`** — [`spider::Website`] via
///   [`scrape_via_spider`]: crawl() + subscription (streams pages to the user),
///   stealth/fingerprint off. Requires spider's `chrome_intercept` feature for
///   page-completion detection over the remote browser.
///
/// Browser config is tunable per request via `provider_options.kernel` (e.g.
/// `{ "headless": false, "gpu": true, "proxy_id": "...", "viewport": {...} }`),
/// layered over the adapter's cheap defaults (`headless` + `stealth`).
///
/// Billing is **actual, not estimated**: on success the adapter reads Kernel's
/// own `usage.uptime_ms` meter (`GET /browsers/{id}`) before releasing, prices
/// it at the session's per-second rate, and reports it as `cost_actual_unit =
/// "dollars"`. If the meter read fails the response carries no actual cost and
/// the cloud falls back to the route's static `cost_milli` estimate.
///
/// The route reports `adapter = "kernel_cdp"`; this adapter's [`kind`] returns
/// the matching [`AdapterKind::Custom`] so the registry dispatches to it.
///
/// [`kind`]: Adapter::kind
#[derive(Debug, Clone)]
pub struct KernelCdpAdapter {
    /// Shared client for the create/uptime/delete REST calls (connection pooling).
    http: reqwest::Client,
    /// Bound on the chromey WebSocket connect after the session is minted.
    connect_timeout: Duration,
}

impl Default for KernelCdpAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl KernelCdpAdapter {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
            // Kernel cold-starts a sandboxed browser; the WS is live by the time
            // create returns, but give the CDP handshake generous headroom.
            connect_timeout: Duration::from_secs(20),
        }
    }

    /// Reuse a pre-configured client (e.g. the app's shared pooled client).
    pub fn with_client(http: reqwest::Client) -> Self {
        Self {
            http,
            connect_timeout: Duration::from_secs(20),
        }
    }

    pub fn with_connect_timeout(mut self, t: Duration) -> Self {
        self.connect_timeout = t;
        self
    }

    pub fn arc() -> Arc<dyn Adapter> {
        Arc::new(Self::new())
    }

    /// Build an `Arc<dyn Adapter>` reusing the caller's pooled HTTP client —
    /// matches the `arc_with_client` convention of the other REST adapters.
    pub fn arc_with_client(http: reqwest::Client) -> Arc<dyn Adapter> {
        Arc::new(Self::with_client(http))
    }
}

#[async_trait]
impl Adapter for KernelCdpAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::Custom(Arc::from(KERNEL_ADAPTER_ID))
    }

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        // ---- Resolve API key (BYOK-aware, same as HTTP bearer auth) --------
        let api_key = match &route.auth {
            AuthSpec::Bearer { env } => req
                .resolve_env(env)
                .ok_or_else(|| FetchError::Auth(format!("missing env var: {env}")))?,
            _ => {
                return Err(FetchError::Config(
                    "kernel_cdp route requires `auth.kind = \"bearer\"`".into(),
                ))
            }
        };

        // Create endpoint comes from the route (templated), falling back to the
        // public default so a bare route still works.
        let create_url = match route.endpoint.render(req) {
            Ok(u) => u.to_string(),
            Err(_) => KERNEL_DEFAULT_CREATE_URL.to_string(),
        };
        let create_body = build_kernel_create_body(req);

        // ---- 1. Mint a browser session (cancel + timeout bounded) ----------
        let session = create_kernel_browser(
            &self.http,
            &create_url,
            &api_key,
            &create_body,
            route.timeout(),
            cancel,
        )
        .await?;

        // ---- 2. Drive the remote browser. Default is raw chromey ([`drive_cdp`])
        // — direct CDP, lean and fast. Opt into the spider::Website path
        // (crawl() + subscription, quality steps) per request with
        // `provider_options.kernel.spider = true`.
        let driven = if kernel_use_spider(req) {
            scrape_via_spider(&session.cdp_ws_url, req.url.as_str(), route.timeout(), cancel).await
        } else {
            drive_cdp(
                &session.cdp_ws_url,
                req.url.as_str(),
                self.connect_timeout,
                route.timeout(),
                cancel,
            )
            .await
        };

        // ---- 3. On success, read Kernel's own usage meter for actual-cost
        // billing *before* releasing (uptime is unreadable after the session is
        // deleted). On failure we skip it — the error path bills the min-charge
        // floor, not vendor cost. Best-effort: a meter miss falls back to the
        // route's static `cost_milli` estimate.
        let cost_dollars = match &driven {
            Ok((status, _)) if *status < 400 => {
                fetch_kernel_cost_dollars(&self.http, &create_url, &session, &api_key).await
            }
            _ => None,
        };

        // ---- 4. Always release the session — Kernel bills per running second.
        // Best-effort: a failed delete must not mask a successful scrape, and a
        // session left dangling is reaped by Kernel's own `timeout_seconds`.
        delete_kernel_browser(&self.http, &create_url, &session.session_id, &api_key).await;

        let (status, html) = driven?;
        if status >= 400 {
            return Err(FetchError::Status(status));
        }
        let mut resp = cdp_response(route, req, ctx, status, html);
        // Reported in dollars; the cloud's billing maps "dollars" → credits
        // (×10,000). Absent (meter miss) → it keeps the static estimate.
        if let Some(dollars) = cost_dollars {
            resp.cost_actual_units = Some(dollars);
            resp.cost_actual_unit = Some("dollars".to_string());
        }
        Ok(resp)
    }
}

/// A minted Kernel browser session. Carries the provisioned mode (`headless` /
/// `gpu`) so the per-second billing rate is known without re-deriving it from
/// the request body — Kernel echoes what it actually applied.
struct KernelSession {
    cdp_ws_url: String,
    session_id: String,
    headless: bool,
    gpu: bool,
}

// Kernel per-second rates (https://www.kernel.sh/docs/info/pricing). Time-based
// billing: cost = uptime_seconds × rate. GPU is headful-only and dominates.
const KERNEL_RATE_HEADLESS_PER_SEC: f64 = 0.000_016_666_7;
const KERNEL_RATE_HEADFUL_PER_SEC: f64 = 0.000_133_333_6;
const KERNEL_RATE_GPU_PER_SEC: f64 = 0.000_800_001_6;

/// Dollar cost of a Kernel session from its metered `uptime_ms` and mode. Pure
/// so the rate selection is unit-testable. GPU (headful-only) wins, then
/// headless vs headful.
fn kernel_cost_dollars(uptime_ms: u64, headless: bool, gpu: bool) -> f64 {
    let rate_per_sec = if gpu {
        KERNEL_RATE_GPU_PER_SEC
    } else if headless {
        KERNEL_RATE_HEADLESS_PER_SEC
    } else {
        KERNEL_RATE_HEADFUL_PER_SEC
    };
    (uptime_ms as f64 / 1000.0) * rate_per_sec
}

/// Keys the adapter consumes from `provider_options.kernel` as its own control
/// flags — they must NOT be forwarded to Kernel's create-browser API.
const KERNEL_CONTROL_KEYS: &[&str] = &["spider"];

/// Default create body (cheap + stealthy), with `provider_options.kernel`
/// layered over the top so callers can flip `headless`, request `gpu`, pin a
/// `proxy_id`, set a `viewport`, raise `timeout_seconds`, etc. Caller keys win.
/// Adapter-control flags ([`KERNEL_CONTROL_KEYS`]) are stripped so they never
/// reach Kernel's API.
fn build_kernel_create_body(req: &ScrapeRequest) -> serde_json::Value {
    let mut body = serde_json::json!({ "headless": true, "stealth": true });
    if let (Some(serde_json::Value::Object(opts)), serde_json::Value::Object(base)) =
        (req.provider_options.get("kernel"), &mut body)
    {
        for (k, v) in opts {
            if KERNEL_CONTROL_KEYS.contains(&k.as_str()) {
                continue;
            }
            base.insert(k.clone(), v.clone());
        }
    }
    body
}

/// Whether this request opted into the spider::Website driver
/// (`provider_options.kernel.spider = true`). Default is raw chromey.
fn kernel_use_spider(req: &ScrapeRequest) -> bool {
    req.provider_options
        .get("kernel")
        .and_then(|o| o.get("spider"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// `POST {create_url}` with `Authorization: Bearer <api_key>` → parse the
/// session's `cdp_ws_url` + `session_id`. Cancel- and timeout-bounded.
async fn create_kernel_browser(
    http: &reqwest::Client,
    create_url: &str,
    api_key: &str,
    body: &serde_json::Value,
    timeout: Duration,
    cancel: &CancelToken,
) -> Result<KernelSession, FetchError> {
    let send = http.post(create_url).bearer_auth(api_key).json(body).send();
    let resp = tokio::time::timeout(timeout, async {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(FetchError::Cancelled),
            r = send => r.map_err(|e| FetchError::Network(format!("kernel create: {e}"))),
        }
    })
    .await
    .map_err(|_| FetchError::Timeout(timeout))??;

    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| FetchError::Network(format!("kernel create body: {e}")))?;
    if !status.is_success() {
        return Err(FetchError::Network(format!(
            "kernel create returned {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(&bytes)
        )));
    }

    let v: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| FetchError::Parse(format!("kernel create json: {e}")))?;
    let cdp_ws_url = v
        .get("cdp_ws_url")
        .and_then(|x| x.as_str())
        .ok_or_else(|| FetchError::Parse("kernel create: missing cdp_ws_url".into()))?
        .to_string();
    let session_id = v
        .get("session_id")
        .and_then(|x| x.as_str())
        .ok_or_else(|| FetchError::Parse("kernel create: missing session_id".into()))?
        .to_string();
    // Mode Kernel actually provisioned (echoed back). Default to our request
    // defaults if absent: headless on, gpu off.
    let headless = v.get("headless").and_then(|x| x.as_bool()).unwrap_or(true);
    let gpu = v.get("gpu").and_then(|x| x.as_bool()).unwrap_or(false);
    Ok(KernelSession {
        cdp_ws_url,
        session_id,
        headless,
        gpu,
    })
}

/// Best-effort read of Kernel's own usage meter for actual-cost billing.
/// `GET /browsers/{id}` → `usage.uptime_ms`, priced at the session's mode rate.
/// Returns `None` on any failure (network, parse, missing field) so the caller
/// gracefully falls back to the route's static `cost_milli` estimate.
async fn fetch_kernel_cost_dollars(
    http: &reqwest::Client,
    create_url: &str,
    session: &KernelSession,
    api_key: &str,
) -> Option<f64> {
    let url = format!(
        "{}/{}",
        create_url.trim_end_matches('/'),
        session.session_id
    );
    let resp = tokio::time::timeout(
        Duration::from_secs(10),
        http.get(&url).bearer_auth(api_key).send(),
    )
    .await
    .ok()?
    .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    // `usage.uptime_ms` is the documented shape; tolerate a flattened `uptime_ms`.
    let uptime_ms = v
        .get("usage")
        .and_then(|u| u.get("uptime_ms"))
        .and_then(|x| x.as_u64())
        .or_else(|| v.get("uptime_ms").and_then(|x| x.as_u64()))?;
    // Prefer the mode Kernel reports now; fall back to the create-time mode.
    let headless = v
        .get("headless")
        .and_then(|x| x.as_bool())
        .unwrap_or(session.headless);
    let gpu = v
        .get("gpu")
        .and_then(|x| x.as_bool())
        .unwrap_or(session.gpu);
    Some(kernel_cost_dollars(uptime_ms, headless, gpu))
}

/// `DELETE {create_url}/{session_id}` — best-effort, short-bounded. Errors are
/// swallowed: a failed release can't fail a scrape that already succeeded, and
/// Kernel reaps the session on its own `timeout_seconds` anyway.
async fn delete_kernel_browser(
    http: &reqwest::Client,
    create_url: &str,
    session_id: &str,
    api_key: &str,
) {
    let url = format!("{}/{}", create_url.trim_end_matches('/'), session_id);
    let fut = http.delete(&url).bearer_auth(api_key).send();
    let _ = tokio::time::timeout(Duration::from_secs(10), fut).await;
}

/// Default Kernel driver: scrape `target_url` through `spider::Website`,
/// connected to Kernel's remote browser via its CDP ws url. The same engine the
/// rest of gottem rides on, just pointed at a remote endpoint.
///
/// Driven with `crawl()` (renders via the remote browser — the "good data"),
/// which streams pages over a broadcast **subscription** rather than retaining
/// them — the same channel mechanism gottem uses to send pages to the user — so
/// we subscribe before crawling and collect after.
///
/// `with_stealth(false)` + `with_fingerprint(false)` are deliberate: Kernel
/// provisions a managed anti-detect browser server-side, so spider must NOT
/// inject its own stealth/fingerprint patches — double-spoofing produces
/// internally inconsistent signals that are *easier* to detect. `with_limit(1)`
/// is the guardrail that keeps a chrome crawl from walking the whole site.
///
/// Bounded by the route timeout AND the cancel token.
async fn scrape_via_spider(
    cdp_ws_url: &str,
    target_url: &str,
    route_timeout: Duration,
    cancel: &CancelToken,
) -> Result<(u16, String), FetchError> {
    use spider::website::Website;

    let mut website = Website::new(target_url)
        .with_limit(1)
        .with_stealth(false)
        .with_fingerprint(false)
        .with_chrome_connection(Some(cdp_ws_url.to_string()))
        .build()
        .map_err(|_| FetchError::Config("spider website build failed (invalid url?)".into()))?;

    // Subscribe before crawling: crawl() emits pages on this broadcast channel
    // rather than retaining them.
    let mut rx = website.subscribe(16);

    tokio::time::timeout(route_timeout, async {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(FetchError::Cancelled),
            _ = website.crawl() => Ok(()),
        }
    })
    .await
    .map_err(|_| FetchError::Timeout(route_timeout))??;

    // Take the last page the crawl emitted (≤1 under with_limit(1)).
    let mut page = None;
    loop {
        match rx.try_recv() {
            Ok(p) => page = Some(p),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
    let page = page.ok_or_else(|| FetchError::Parse("spider crawl returned no page".into()))?;
    let status = page.status_code.as_u16();
    let html = page.get_html();
    if html.is_empty() {
        return Err(FetchError::Parse("spider crawl returned empty html".into()));
    }
    Ok((status, html))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gottem_core::{
        AdapterKind, Capabilities, EndpointTemplate, HttpMethod, Route, ScrapeRequest, Tier,
    };
    use std::sync::Arc;
    use url::Url;

    fn mk_route(endpoint: &str, auth: AuthSpec) -> Route {
        Route {
            id: Arc::from("chrome.test"),
            adapter: AdapterKind::ChromeCdp,
            endpoint: EndpointTemplate::parse(endpoint).unwrap(),
            method: HttpMethod::Get,
            auth,
            headers: vec![],
            body: Default::default(),
            parse: Default::default(),
            validate: vec![],
            tier: Tier::T8,
            cost: 150,
            priority: 100,
            caps: Capabilities::default(),
            timeout_ms: 30_000,
            concurrency: 4,
            retry_on: Default::default(),
            cost_extract: None,
        }
    }

    #[test]
    fn kind_returns_chrome_cdp() {
        assert_eq!(ChromeCdpAdapter::new().kind(), AdapterKind::ChromeCdp);
    }

    #[test]
    fn build_ws_url_injects_userinfo_from_env() {
        std::env::set_var("GOTTEM_CHROME_TEST_AUTH", "user-x:pass-y");
        let route = mk_route(
            "wss://brd.superproxy.io:9222",
            AuthSpec::WsUserinfo {
                env: "GOTTEM_CHROME_TEST_AUTH".into(),
            },
        );
        let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        let resolved = build_ws_url(&route, &req).unwrap();
        assert!(
            resolved.contains("user-x:pass-y@brd.superproxy.io:9222"),
            "got {resolved}"
        );
    }

    #[test]
    fn build_ws_url_renders_template_env_substitution() {
        std::env::set_var("GOTTEM_CHROME_TPL_TOKEN", "tok-abc");
        let route = mk_route(
            "wss://chrome.browserless.io?token={{env:GOTTEM_CHROME_TPL_TOKEN}}",
            AuthSpec::None,
        );
        let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        let resolved = build_ws_url(&route, &req).unwrap();
        assert!(resolved.contains("token=tok-abc"), "got {resolved}");
    }

    #[test]
    fn build_ws_url_missing_env_is_auth_error() {
        let route = mk_route(
            "wss://brd.superproxy.io:9222",
            AuthSpec::WsUserinfo {
                env: "GOTTEM_CHROME_DEFINITELY_UNSET".into(),
            },
        );
        let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        let err = build_ws_url(&route, &req).unwrap_err();
        assert!(matches!(err, FetchError::Auth(_)), "got {err:?}");
    }

    #[test]
    fn kernel_adapter_kind_is_custom_kernel_cdp() {
        assert_eq!(
            KernelCdpAdapter::new().kind(),
            AdapterKind::Custom(Arc::from(KERNEL_ADAPTER_ID))
        );
    }

    #[test]
    fn kernel_create_body_defaults_headless_stealth() {
        let req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        let body = build_kernel_create_body(&req);
        assert_eq!(body["headless"], serde_json::json!(true));
        assert_eq!(body["stealth"], serde_json::json!(true));
    }

    #[test]
    fn kernel_create_body_provider_options_override_defaults() {
        let mut req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        req.provider_options.insert(
            "kernel".to_string(),
            serde_json::json!({ "headless": false, "gpu": true }),
        );
        let body = build_kernel_create_body(&req);
        assert_eq!(body["headless"], serde_json::json!(false)); // caller wins
        assert_eq!(body["gpu"], serde_json::json!(true));
        assert_eq!(body["stealth"], serde_json::json!(true)); // untouched default kept
    }

    #[test]
    fn kernel_control_flag_not_forwarded_to_create_body() {
        let mut req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        req.provider_options.insert(
            "kernel".to_string(),
            serde_json::json!({ "spider": true, "headless": false }),
        );
        let body = build_kernel_create_body(&req);
        assert_eq!(body["headless"], serde_json::json!(false)); // real Kernel field passes
        assert!(body.get("spider").is_none()); // control flag stripped
    }

    #[test]
    fn kernel_use_spider_reads_flag() {
        let base = || ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        assert!(!kernel_use_spider(&base())); // absent → chromey (default)
        let mut on = base();
        on.provider_options
            .insert("kernel".into(), serde_json::json!({ "spider": true }));
        assert!(kernel_use_spider(&on));
        let mut off = base();
        off.provider_options
            .insert("kernel".into(), serde_json::json!({ "spider": false }));
        assert!(!kernel_use_spider(&off));
    }

    #[test]
    fn kernel_cost_dollars_picks_rate_by_mode() {
        // 60s headless → 60 × $0.0000166667.
        assert!((kernel_cost_dollars(60_000, true, false) - 0.001_000_002).abs() < 1e-9);
        // 60s headful → 60 × $0.0001333336.
        assert!((kernel_cost_dollars(60_000, false, false) - 0.008_000_016).abs() < 1e-9);
        // GPU is headful-only and dominates even if `headless` is somehow set.
        assert!((kernel_cost_dollars(10_000, true, true) - 0.008_000_016).abs() < 1e-9);
        // No uptime → no cost (caller then keeps the static estimate).
        assert_eq!(kernel_cost_dollars(0, true, false), 0.0);
    }
}
