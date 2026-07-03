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
//! `cdp_ws_url`, drives it (`spider::Website` by default, or raw chromey
//! [`drive_cdp`] when `provider_options.kernel.spider = false`), then
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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    if tokio::time::timeout(Duration::from_secs(2), handler_task)
        .await
        .is_err()
    {
        // Detached — the task ends on its own shortly after, but count it so
        // a soak run can prove detaches aren't accumulating (a vendor proxy
        // holding connections open would show up here first).
        DETACHED_HANDLER_TASKS.fetch_add(1, Ordering::Relaxed);
    }

    match work_outcome {
        Ok(Ok(out)) => Ok(out),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(FetchError::Timeout(route_timeout)),
    }
}

/// CDP handler tasks that outlived the 2-second post-drop join window and
/// were detached (see [`drive_cdp`] cleanup). Monotonic; steady growth means
/// remote endpoints are holding WebSockets open past browser drop.
static DETACHED_HANDLER_TASKS: AtomicU64 = AtomicU64::new(0);

/// Total detached CDP handler tasks — host observability hook, the
/// chrome-adapter counterpart of [`kernel_release_failures`].
pub fn detached_handler_tasks() -> u64 {
    DETACHED_HANDLER_TASKS.load(Ordering::Relaxed)
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
///
/// The tab is explicitly closed (best-effort, 2s-bounded) before returning —
/// on vendor-shared remote browsers (Browserless, Brightdata Scraping
/// Browser) the browser process outlives our WebSocket, so a tab that's only
/// dropped client-side accumulates server-side.
async fn navigate_and_extract(
    browser: &chromey::Browser,
    url: &str,
) -> Result<(u16, String), FetchError> {
    let page = browser
        .new_page(url)
        .await
        .map_err(|e| FetchError::Network(format!("new_page: {e}")))?;

    let result = async {
        // Wait for the main frame to finish navigating. We don't fail on
        // wait_for_navigation errors because some single-page apps never emit
        // a final load — we still want to capture whatever DOM is present.
        let _ = page.wait_for_navigation().await;

        let html = page
            .content()
            .await
            .map_err(|e| FetchError::Parse(format!("content: {e}")))?;

        // chromey doesn't always surface the main-frame HTTP status code in a
        // first-class way; we default to 200 on successful HTML extraction.
        Ok((200, html))
    }
    .await;

    // Close the tab regardless of outcome. Errors swallowed — the fetch
    // result matters more than the close ack, and drive_cdp's browser drop
    // is the local backstop either way.
    let _ = tokio::time::timeout(Duration::from_secs(2), page.close()).await;

    result
}

/// Resolve the route's CDP endpoint into a final WebSocket URL string.
///
/// Steps:
/// 1. Render endpoint template (`{{url}}`, `{{env:NAME}}` substitution).
/// 2. If `AuthSpec::WsUserinfo` is set, inject the env-var-derived `user:pass` into the URL.
pub(crate) fn build_ws_url(route: &Route, req: &ScrapeRequest) -> Result<String, FetchError> {
    let mut url = route.endpoint.render(req)?;

    // Query-kind geo mapping: CDP vendors that accept a country pin do it on
    // the WS URL's query string (e.g. Browserless `proxyCountry`).
    if let (Some(spec), Some(geo)) = (&route.geo_map, &req.geo) {
        if spec.kind == gottem_core::GeoParamKind::Query {
            url.query_pairs_mut()
                .append_pair(&spec.param, &spec.format_value(geo));
        }
    }

    // Per-vendor provider_options ride the WS URL query string for CDP
    // vendors (Browserless `blockAds`, Browserbase session flags, …) — the
    // query-param twin of the HTTP adapters' JSON-body merge. Scalars only;
    // caller values replace same-named pairs so callers can override the
    // route template's baked-in flags. Applied BEFORE userinfo injection so
    // a malformed option errors before credentials are attached.
    merge_ws_query_provider_options(&mut url, route, req)?;

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

/// Merge the caller's per-vendor `provider_options` bucket onto the WS URL's
/// query string. Vendor key = route-id first dot segment, matching the HTTP
/// adapters' body merge, so only the bucket for the route that actually runs
/// is applied. Scalar values only (string/number/bool); nested values are a
/// config error — same strictness as the JSON-body merge.
fn merge_ws_query_provider_options(
    url: &mut url::Url,
    route: &Route,
    req: &ScrapeRequest,
) -> Result<(), FetchError> {
    if req.provider_options.is_empty() {
        return Ok(());
    }
    let vendor = route.id.split('.').next().unwrap_or(route.id.as_ref());
    let Some(opts) = req.provider_options.get(vendor) else {
        return Ok(());
    };
    let serde_json::Value::Object(opts) = opts else {
        return Err(FetchError::Config(format!(
            "provider_options.{vendor} must be a JSON object"
        )));
    };
    if opts.is_empty() {
        return Ok(());
    }

    let mut pairs: Vec<(String, String)> = Vec::with_capacity(opts.len());
    for (k, v) in opts {
        let value = match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            _ => {
                return Err(FetchError::Config(format!(
                    "provider_options.{vendor}.{k} must be a scalar for CDP WS-URL vendors"
                )))
            }
        };
        pairs.push((k.clone(), value));
    }

    // Rebuild the query so caller keys REPLACE same-named pairs.
    let existing: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| !pairs.iter().any(|(nk, _)| nk == k.as_ref()))
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    {
        let mut qp = url.query_pairs_mut();
        qp.clear();
        for (k, v) in existing.iter().chain(pairs.iter()) {
            qp.append_pair(k, v);
        }
    }
    Ok(())
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

/// Per-second USD pricing for Kernel's session modes. Used to turn the metered
/// `uptime_ms` into an actual-cost figure. Supplied by the host — this crate
/// ships no rates of its own.
#[derive(Debug, Clone, Copy)]
pub struct KernelRates {
    pub headless_per_sec: f64,
    pub headful_per_sec: f64,
    pub gpu_per_sec: f64,
}

/// Host-supplied policy for the Kernel adapter. The tuned defaults, driver
/// choice, and pricing live with the host (e.g. the gottem-cloud backend), so
/// they stay configurable and out of this public crate. The [`Default`] here is
/// deliberately neutral — lean chromey, a minimal create body, spider's own
/// stealth/fingerprint behavior, and NO pricing.
#[derive(Debug, Clone)]
pub struct KernelConfig {
    /// Driver used when the request doesn't set `provider_options.kernel.spider`.
    /// `true` = spider::Website, `false` = raw chromey.
    pub default_use_spider: bool,
    /// Base create-browser body (a JSON object) before per-request
    /// `provider_options.kernel` overrides are layered on.
    pub create_defaults: serde_json::Value,
    /// spider::Website stealth / fingerprint toggles for the spider driver.
    pub spider_stealth: bool,
    pub spider_fingerprint: bool,
    /// Enable spider request interception (blocks visuals/CSS/ads/analytics,
    /// keeps JS) — faster + cheaper Kernel time, HTML DOM intact. Off in OSS.
    pub spider_intercept: bool,
    /// Per-second pricing. `None` skips the usage meter entirely — no actual
    /// cost is reported and the host falls back to the route's static estimate.
    pub rates: Option<KernelRates>,
}

impl Default for KernelConfig {
    fn default() -> Self {
        Self {
            default_use_spider: false,
            create_defaults: serde_json::json!({ "headless": true }),
            spider_stealth: true,
            spider_fingerprint: true,
            spider_intercept: false,
            rates: None,
        }
    }
}

/// Adapter for Kernel ([onkernel.com](https://kernel.sh)). Kernel has no static
/// CDP endpoint: each scrape first `POST`s `/browsers` to mint a per-session
/// `cdp_ws_url`, drives it, then `DELETE`s the session — Kernel bills per
/// *running* second, so releasing promptly is a cost guarantee, not just hygiene.
///
/// Two drivers, switchable per request:
/// - **default** — [`spider::Website`] via [`scrape_via_spider`]: crawl() +
///   subscription (streams pages to the user), stealth/fingerprint off. Faster
///   on a ≥2 vCPU task; requires spider's `chrome_intercept` feature for
///   page-completion detection over the remote browser.
/// - **`provider_options.kernel.spider = false`** — raw chromey
///   ([`drive_cdp`]): direct CDP, lean; survives a 1 vCPU task.
///
/// Policy — driver default, create-browser defaults, spider stealth/fingerprint,
/// and pricing — is supplied by the host via [`KernelConfig`] (see
/// [`with_config`]). The OSS default is neutral; the gottem-cloud backend passes
/// its tuned config so those values stay configurable and out of this crate.
/// Per request, `provider_options.kernel` overrides the create body and the
/// `spider` driver flag.
///
/// Billing is **actual, not estimated** *when the host configures rates*: on
/// success the adapter reads Kernel's own `usage.uptime_ms` meter
/// (`GET /browsers/{id}`) before releasing, prices it at the session's
/// per-second rate, and reports it as `cost_actual_unit = "dollars"`. No rates
/// (or a meter miss) → no actual cost, and the host falls back to the route's
/// static `cost_milli` estimate.
///
/// [`with_config`]: KernelCdpAdapter::with_config
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
    /// Host-supplied policy (driver default, create defaults, pricing).
    config: KernelConfig,
}

impl Default for KernelCdpAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl KernelCdpAdapter {
    pub fn new() -> Self {
        Self::with_config(reqwest::Client::new(), KernelConfig::default())
    }

    /// Reuse a pre-configured client (e.g. the app's shared pooled client).
    pub fn with_client(http: reqwest::Client) -> Self {
        Self::with_config(http, KernelConfig::default())
    }

    /// Build with host-supplied policy. The gottem-cloud backend passes its
    /// tuned defaults + pricing here so they stay out of this public crate.
    pub fn with_config(http: reqwest::Client, config: KernelConfig) -> Self {
        Self {
            http,
            // Kernel cold-starts a sandboxed browser; the WS is live by the time
            // create returns, but give the CDP handshake generous headroom.
            connect_timeout: Duration::from_secs(20),
            config,
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

    /// `arc_with_client` + host policy. This is what gottem-cloud uses.
    pub fn arc_with_config(http: reqwest::Client, config: KernelConfig) -> Arc<dyn Adapter> {
        Arc::new(Self::with_config(http, config))
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
        let create_body = build_kernel_create_body(req, &self.config.create_defaults);

        // Bound the whole create→drive op by a SINGLE deadline (= route timeout),
        // handing each step only the time left. Otherwise a slow create plus a
        // slow drive could each consume the full timeout (~2× total) and 504 at
        // the ALB. Keeps the response inside the ALB idle window.
        let deadline = Instant::now() + route.timeout();
        let remaining = || deadline.saturating_duration_since(Instant::now());

        // ---- 1. Mint a browser session (cancel + remaining-bounded) --------
        let session = create_kernel_browser(
            &self.http,
            &create_url,
            &api_key,
            &create_body,
            remaining(),
            cancel,
        )
        .await?;

        // ---- 2. Drive the remote browser. Driver default + spider tuning come
        // from host config; the request can override via
        // `provider_options.kernel.spider`.
        let driven = if kernel_use_spider(req, self.config.default_use_spider) {
            scrape_via_spider(
                &session.cdp_ws_url,
                req.url.as_str(),
                self.config.spider_stealth,
                self.config.spider_fingerprint,
                self.config.spider_intercept,
                remaining(),
                cancel,
            )
            .await
        } else {
            drive_cdp(
                &session.cdp_ws_url,
                req.url.as_str(),
                self.connect_timeout.min(remaining()),
                remaining(),
                cancel,
            )
            .await
        };

        // ---- 3. On success — and only if the host configured pricing — read
        // Kernel's own usage meter for actual-cost billing *before* releasing
        // (uptime is unreadable after the session is deleted). No rates → no
        // meter read; the host falls back to the route's static estimate.
        let cost_dollars = match (&driven, self.config.rates) {
            (Ok((status, _)), Some(rates)) if *status < 400 => {
                fetch_kernel_cost_dollars(&self.http, &create_url, &session, &api_key, &rates).await
            }
            _ => None,
        };

        // ---- 4. Release the session OFF the response critical path. Kernel
        // bills per running second, so we still fire the DELETE — but spawned,
        // not awaited, so a slow release can't push the response past the ALB
        // timeout. The spawned task retries once and logs on final failure
        // (see `release_kernel_browser`); if even the task is dropped,
        // Kernel's own `timeout_seconds` reaps the session.
        tokio::spawn(release_kernel_browser(
            self.http.clone(),
            create_url.clone(),
            session.session_id.clone(),
            api_key.clone(),
        ));

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

/// Dollar cost of a Kernel session from its metered `uptime_ms`, mode, and the
/// host-supplied [`KernelRates`]. Pure so the rate selection is unit-testable.
/// GPU (headful-only) wins, then headless vs headful.
fn kernel_cost_dollars(uptime_ms: u64, headless: bool, gpu: bool, rates: &KernelRates) -> f64 {
    let rate_per_sec = if gpu {
        rates.gpu_per_sec
    } else if headless {
        rates.headless_per_sec
    } else {
        rates.headful_per_sec
    };
    (uptime_ms as f64 / 1000.0) * rate_per_sec
}

/// Keys the adapter consumes from `provider_options.kernel` as its own control
/// flags — they must NOT be forwarded to Kernel's create-browser API.
const KERNEL_CONTROL_KEYS: &[&str] = &["spider"];

/// Create body = the host's `defaults` with `provider_options.kernel` layered
/// over the top (caller keys win), so callers can flip `headless`, request
/// `gpu`, pin a `proxy_id`, set a `viewport`, raise `timeout_seconds`, etc.
/// Adapter-control flags ([`KERNEL_CONTROL_KEYS`]) are stripped so they never
/// reach Kernel's API.
fn build_kernel_create_body(
    req: &ScrapeRequest,
    defaults: &serde_json::Value,
) -> serde_json::Value {
    let mut body = defaults.clone();
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

/// Whether to use the spider::Website driver. Falls back to the host's
/// `default` when the request doesn't set `provider_options.kernel.spider`.
fn kernel_use_spider(req: &ScrapeRequest, default: bool) -> bool {
    req.provider_options
        .get("kernel")
        .and_then(|o| o.get("spider"))
        .and_then(|v| v.as_bool())
        .unwrap_or(default)
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
    rates: &KernelRates,
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
    Some(kernel_cost_dollars(uptime_ms, headless, gpu, rates))
}

/// In-flight Kernel session releases — incremented when a release task starts,
/// decremented when it finishes (either outcome). A host can poll
/// [`kernel_releases_inflight`] to prove releases aren't piling up: Kernel
/// bills per running second, so a stuck release is a money leak, not just a
/// task leak.
static KERNEL_RELEASES_INFLIGHT: AtomicU64 = AtomicU64::new(0);

/// Kernel session releases that failed even after the retry — each one leaves
/// a session billing until Kernel's own `timeout_seconds` reaper fires. This
/// crate carries no logging dependency (errors surface through return values),
/// so the counter is the host's alerting hook.
static KERNEL_RELEASE_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Current number of in-flight Kernel session-release tasks (see
/// [`KERNEL_RELEASES_INFLIGHT`]). Surfaced for host observability endpoints.
pub fn kernel_releases_inflight() -> u64 {
    KERNEL_RELEASES_INFLIGHT.load(Ordering::Relaxed)
}

/// Total Kernel session releases that failed after retry (see
/// [`KERNEL_RELEASE_FAILURES`]). Monotonic; a moving value means money is
/// leaking to the Kernel reaper window and warrants a look at connectivity.
pub fn kernel_release_failures() -> u64 {
    KERNEL_RELEASE_FAILURES.load(Ordering::Relaxed)
}

/// `DELETE {create_url}/{session_id}` — short-bounded; returns whether Kernel
/// acknowledged the release. A failed release can't fail a scrape that already
/// succeeded, and Kernel reaps the session on its own `timeout_seconds` anyway
/// — but callers retry once because every second until the reaper fires bills.
async fn delete_kernel_browser(
    http: &reqwest::Client,
    create_url: &str,
    session_id: &str,
    api_key: &str,
) -> bool {
    let url = format!("{}/{}", create_url.trim_end_matches('/'), session_id);
    let fut = http.delete(&url).bearer_auth(api_key).send();
    match tokio::time::timeout(Duration::from_secs(10), fut).await {
        Ok(Ok(resp)) => resp.status().is_success() || resp.status().as_u16() == 404,
        _ => false,
    }
}

/// Release a Kernel session with one retry. Runs inside the spawned release
/// task (off the response critical path); the gauge brackets the whole
/// attempt so hosts can observe pile-ups.
async fn release_kernel_browser(
    http: reqwest::Client,
    create_url: String,
    session_id: String,
    api_key: String,
) {
    KERNEL_RELEASES_INFLIGHT.fetch_add(1, Ordering::Relaxed);
    let ok = delete_kernel_browser(&http, &create_url, &session_id, &api_key).await || {
        tokio::time::sleep(Duration::from_secs(5)).await;
        delete_kernel_browser(&http, &create_url, &session_id, &api_key).await
    };
    if !ok {
        // No logging dep in this crate — the counter is the observability
        // surface. The session keeps billing until Kernel's timeout_seconds
        // reaper fires; hosts should alert on this moving.
        KERNEL_RELEASE_FAILURES.fetch_add(1, Ordering::Relaxed);
    }
    KERNEL_RELEASES_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
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
/// `stealth` / `fingerprint` come from host config. Hosts driving a managed
/// anti-detect browser (Kernel) pass `false` for both: that browser already
/// handles stealth server-side, so spider injecting its own patches would
/// double-spoof and produce internally inconsistent, *easier*-to-detect signals.
/// `with_limit(1)` is the guardrail that keeps a chrome crawl from walking the
/// whole site.
///
/// Bounded by the route timeout AND the cancel token.
async fn scrape_via_spider(
    cdp_ws_url: &str,
    target_url: &str,
    stealth: bool,
    fingerprint: bool,
    intercept: bool,
    route_timeout: Duration,
    cancel: &CancelToken,
) -> Result<(u16, String), FetchError> {
    use spider::features::chrome_common::RequestInterceptConfiguration;
    use spider::website::Website;

    let mut builder = Website::new(target_url);
    builder
        .with_limit(1)
        .with_stealth(stealth)
        .with_fingerprint(fingerprint)
        .with_chrome_connection(Some(cdp_ws_url.to_string()));
    if intercept {
        // new(true) keeps JS but blocks visuals/CSS/ads/analytics — faster +
        // cheaper Kernel time, HTML DOM intact.
        builder.with_chrome_intercept(RequestInterceptConfiguration::new(true));
    }
    let mut website = builder
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
            geo_map: None,
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
    fn kernel_create_body_layers_overrides_on_host_defaults() {
        let defaults = serde_json::json!({ "headless": true, "stealth": true });
        let mut req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        req.provider_options.insert(
            "kernel".to_string(),
            serde_json::json!({ "headless": false, "gpu": true }),
        );
        let body = build_kernel_create_body(&req, &defaults);
        assert_eq!(body["headless"], serde_json::json!(false)); // caller wins
        assert_eq!(body["gpu"], serde_json::json!(true)); // added
        assert_eq!(body["stealth"], serde_json::json!(true)); // untouched host default kept
    }

    #[test]
    fn kernel_control_flag_not_forwarded_to_create_body() {
        let defaults = serde_json::json!({ "headless": true });
        let mut req = ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        req.provider_options.insert(
            "kernel".to_string(),
            serde_json::json!({ "spider": true, "headless": false }),
        );
        let body = build_kernel_create_body(&req, &defaults);
        assert_eq!(body["headless"], serde_json::json!(false)); // real Kernel field passes
        assert!(body.get("spider").is_none()); // control flag stripped
    }

    #[test]
    fn kernel_use_spider_honors_host_default_and_override() {
        let base = || ScrapeRequest::get(Url::parse("https://example.com/").unwrap());
        // Absent → host default (either way).
        assert!(kernel_use_spider(&base(), true));
        assert!(!kernel_use_spider(&base(), false));
        // Request override wins over the host default.
        let mut off = base();
        off.provider_options
            .insert("kernel".into(), serde_json::json!({ "spider": false }));
        assert!(!kernel_use_spider(&off, true));
        let mut on = base();
        on.provider_options
            .insert("kernel".into(), serde_json::json!({ "spider": true }));
        assert!(kernel_use_spider(&on, false));
    }

    #[test]
    fn kernel_cost_dollars_picks_rate_by_mode() {
        let rates = KernelRates {
            headless_per_sec: 0.000_016_666_7,
            headful_per_sec: 0.000_133_333_6,
            gpu_per_sec: 0.000_800_001_6,
        };
        // 60s headless → 60 × headless rate.
        assert!((kernel_cost_dollars(60_000, true, false, &rates) - 0.001_000_002).abs() < 1e-9);
        // 60s headful → 60 × headful rate.
        assert!((kernel_cost_dollars(60_000, false, false, &rates) - 0.008_000_016).abs() < 1e-9);
        // GPU is headful-only and dominates even if `headless` is somehow set.
        assert!((kernel_cost_dollars(10_000, true, true, &rates) - 0.008_000_016).abs() < 1e-9);
        // No uptime → no cost.
        assert_eq!(kernel_cost_dollars(0, true, false, &rates), 0.0);
    }

    #[test]
    fn kernel_config_default_is_neutral() {
        // OSS ships no pricing and a lean, non-tuned policy.
        let c = KernelConfig::default();
        assert!(!c.default_use_spider);
        assert!(c.rates.is_none());
    }
}
