//! Crawl primitives: request, response entries, stream/subscriber API.
//!
//! Crawl in gottem **never** accumulates pages in memory. The orchestrator
//! returns a `Stream<Item = Result<PageEntry, FetchError>>`; pages flow
//! through the consumer one at a time and are dropped after yield. Backed
//! by spider's [`Website`](spider::website::Website) for link tracking
//! (visited set, depth, allow/deny, robots, budget) — gottem never
//! reimplements that bookkeeping.
//!
//! Two engines:
//!
//! - [`CrawlEngine::SpiderCloud`] — Spider's native `/crawl` endpoint,
//!   which streams JSONL. Each line becomes one [`PageEntry`].
//! - [`CrawlEngine::Local`] — local BFS using the existing scrape ladder
//!   for *each* page (so escalation works per URL), with spider's
//!   [`Page::links`](spider::page::Page::links) extracting outlinks from
//!   the bytes we already fetched (no re-fetch for discovery).
//!
//! [`CrawlEngine::Auto`] picks SpiderCloud when `SPIDER_API_KEY` is
//! resolvable for the request, else Local.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    cancel::CancelToken, catalog::RouteCatalog, error::FetchError, orchestrator::Orchestrator,
    request::ScrapeRequest, route::RouteId, tier::Tier,
};

/// One page result emitted by a crawl. Carries enough metadata to make
/// each yield self-describing — consumers don't need to maintain state
/// to correlate a page back to its crawl.
#[derive(Debug, Clone)]
pub struct PageEntry {
    /// Final URL after vendor-side redirects.
    pub url: Url,
    /// Depth from the seed (`0` = seed itself).
    pub depth: u32,
    /// HTTP status. `0` when the engine doesn't surface a status (vendor
    /// crawl that hands back only content).
    pub status: u16,
    /// Raw response body bytes.
    pub body: Bytes,
    /// Parsed content per the route's parse spec (markdown, text, JSON
    /// field). Same shape as [`ScrapeResponse::content`](crate::ScrapeResponse::content).
    pub content: Option<Bytes>,
    /// Outlinks extracted from this page. Populated for `Local` (always)
    /// and `SpiderCloud` when the vendor returns links — `None` otherwise.
    pub links: Option<Vec<Url>>,
    /// Which route fetched this page (e.g. `spider.crawl`,
    /// `firecrawl.scrape`, `local.crawl`).
    pub route_id: RouteId,
    pub tier: Tier,
    /// Static cost from the route (milli-cents).
    pub cost_milli: u64,
    /// Time from "URL pulled off frontier" to "PageEntry ready to yield".
    pub elapsed: Duration,
}

/// Aggregate stats emitted at the end of a crawl run via the subscriber
/// API. The streaming API doesn't surface stats — the consumer computes
/// whatever it wants from the page sequence.
#[derive(Debug, Clone, Default)]
pub struct CrawlStats {
    pub pages: u32,
    pub errors: u32,
    /// Sum of `PageEntry::cost_milli` across yielded pages. The hosted
    /// vendor's per-request cost (when surfaced) isn't double-counted
    /// here — this is the **route's** static cost.
    pub cost_milli_total: u64,
    pub elapsed: Duration,
}

/// Which engine to use for a crawl. The catalog is consulted at dispatch
/// time — `SpiderCloud` requires the `spider.crawl` route to be
/// loaded and authenticatable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrawlEngine {
    /// Pick `SpiderCloud` if the Spider key resolves for this
    /// request, else fall back to `Local`.
    #[default]
    Auto,
    /// Use the `spider.crawl` route — JSONL streaming over Spider
    /// Cloud's native crawl endpoint.
    SpiderCloud,
    /// Local BFS using the gottem scrape ladder + spider's link
    /// extraction primitives.
    Local,
}

impl CrawlEngine {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::SpiderCloud => "spider_cloud",
            Self::Local => "local",
        }
    }
}

/// A crawl request. Embeds a full [`ScrapeRequest`] — every per-page knob
/// (render_js, formats, return_links, credentials, extra, required_caps)
/// carries straight through to each fetched URL. Crawl-only knobs
/// (limit/depth/allow/deny/...) sit alongside.
#[derive(Debug, Clone)]
pub struct CrawlRequest {
    /// Per-page request shape. `scrape.url` is the seed URL.
    pub scrape: ScrapeRequest,
    /// Hard cap on total pages emitted. `0` = unlimited.
    pub limit: u32,
    /// Max link depth from the seed. `0` = seed only (no link following).
    pub depth: u32,
    /// Follow links into subdomains of the seed's host.
    pub subdomains: bool,
    /// Follow links across same TLD (e.g. `.com` siblings of the seed host).
    pub tld: bool,
    /// Whitelist (regex/glob, depending on spider features) — a non-empty
    /// list restricts crawling to URLs matching any entry.
    pub allow: Vec<String>,
    /// Blacklist — URLs matching any entry are skipped.
    pub deny: Vec<String>,
    /// Honor `robots.txt`. When `true`, the local engine fetches and
    /// parses `/robots.txt` via spider's robots parser before crawling.
    pub respect_robots: bool,
    /// Which engine to use. See [`CrawlEngine`].
    pub engine: CrawlEngine,
    /// Worker concurrency for the local engine (URLs fetched in parallel).
    /// Ignored by `SpiderCloud` (the vendor controls parallelism).
    pub concurrency: u32,
    /// Optional externally-owned worker-concurrency gate. When set, the
    /// local engine prefers this `Arc<tokio::sync::Semaphore>` over a
    /// freshly-built `Semaphore::new(concurrency)`, letting an admission
    /// controller resize the crawl's worker fan-out mid-flight (typically
    /// by holding the other end of an
    /// [`spider::utils::adaptive_concurrency::AdaptiveSemaphore`] and
    /// calling `set_target`). `None` is the default and preserves the
    /// existing static-`concurrency` behavior byte-for-byte. Ignored by
    /// `SpiderCloud` (the vendor controls parallelism there too).
    pub adaptive_concurrency: Option<Arc<tokio::sync::Semaphore>>,
    /// Optional override for the catalog the **local** engine's per-URL scrape
    /// ladder draws from. `None` (the default) leaves the crawl adapter's baked
    /// strategy untouched — typically a ladder over the orchestrator's full
    /// catalog. When set, the local engine builds a fresh ladder over this
    /// catalog for every per-URL fetch, so a hosted layer can restrict crawling
    /// to the vendors it can authenticate (pooled key or per-request BYOK)
    /// without OSS gottem ever knowing about keys. Ignored by the `SpiderCloud`
    /// engine — there the vendor runs the whole crawl remotely, so there's no
    /// per-URL ladder to constrain.
    pub ladder_catalog: Option<Arc<RouteCatalog>>,
    /// Free-form crawl-level hints — surfaced to adapters that want them
    /// (e.g. Spider-specific knobs not modelled here).
    pub extra: HashMap<String, serde_json::Value>,
}

impl CrawlRequest {
    /// Construct a crawl with sensible defaults: depth 2, limit 10,
    /// auto engine, single worker, no allow/deny, no robots.
    pub fn new(seed: Url) -> Self {
        Self {
            scrape: ScrapeRequest::get(seed),
            limit: 10,
            depth: 2,
            subdomains: false,
            tld: false,
            allow: Vec::new(),
            deny: Vec::new(),
            respect_robots: false,
            engine: CrawlEngine::default(),
            concurrency: 4,
            adaptive_concurrency: None,
            ladder_catalog: None,
            extra: HashMap::new(),
        }
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }
    pub fn with_depth(mut self, depth: u32) -> Self {
        self.depth = depth;
        self
    }
    pub fn with_subdomains(mut self, on: bool) -> Self {
        self.subdomains = on;
        self
    }
    pub fn with_tld(mut self, on: bool) -> Self {
        self.tld = on;
        self
    }
    pub fn with_allow(mut self, allow: Vec<String>) -> Self {
        self.allow = allow;
        self
    }
    pub fn with_deny(mut self, deny: Vec<String>) -> Self {
        self.deny = deny;
        self
    }
    pub fn with_respect_robots(mut self, on: bool) -> Self {
        self.respect_robots = on;
        self
    }
    pub fn with_engine(mut self, e: CrawlEngine) -> Self {
        self.engine = e;
        self
    }
    pub fn with_concurrency(mut self, n: u32) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Hand the crawl an external worker-concurrency gate. The local
    /// engine will acquire from this semaphore instead of one it builds
    /// itself from `concurrency`, so the controller holding the matching
    /// [`spider::utils::adaptive_concurrency::AdaptiveSemaphore`] (or any
    /// `Arc<tokio::sync::Semaphore>` clone) can resize the worker pool
    /// in-flight via `set_target` / `add_permits` / `forget_permits`.
    ///
    /// Pass `None` to clear an existing handle and fall back to the
    /// static-`concurrency` path — strictly opt-in; existing callers
    /// see no behavior change.
    pub fn with_adaptive_concurrency(mut self, sem: Option<Arc<tokio::sync::Semaphore>>) -> Self {
        self.adaptive_concurrency = sem;
        self
    }

    /// Constrain the local engine's per-URL scrape ladder to a specific
    /// catalog. See [`CrawlRequest::ladder_catalog`]. Pass `None` to clear an
    /// existing override and fall back to the crawl adapter's default strategy.
    /// Strictly opt-in: existing callers see no behavior change.
    pub fn with_ladder_catalog(mut self, catalog: Option<Arc<RouteCatalog>>) -> Self {
        self.ladder_catalog = catalog;
        self
    }

    /// Seed URL — convenience accessor pointing at `scrape.url`.
    pub fn seed(&self) -> &Url {
        &self.scrape.url
    }
}

// ============================================================================
// Subscriber sugar over the underlying Stream
// ============================================================================

/// What a subscriber returns from its [`on_page`](CrawlBuilder::on_page)
/// handler — either keep reading the stream or stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFlow {
    Continue,
    Stop,
}

type PageHandler = Arc<
    dyn Fn(PageEntry) -> Pin<Box<dyn Future<Output = ControlFlow> + Send>> + Send + Sync + 'static,
>;
type ErrHandler =
    Arc<dyn Fn(FetchError) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;
type DoneHandler =
    Arc<dyn Fn(CrawlStats) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

/// Builder for the subscriber-style crawl API. Construct via
/// [`Orchestrator::crawl_builder`]; install handlers; then `.run(cancel)`.
///
/// Under the hood this drives the same stream returned by
/// [`Orchestrator::crawl`] — the builder just hides the `while let
/// Some(...)` loop. The two APIs are interchangeable; pick whichever
/// reads better at the call site.
#[derive(Clone)]
pub struct CrawlBuilder {
    orch: Arc<Orchestrator>,
    req: CrawlRequest,
    on_page: Option<PageHandler>,
    on_error: Option<ErrHandler>,
    on_complete: Option<DoneHandler>,
}

impl CrawlBuilder {
    pub fn new(orch: Arc<Orchestrator>, req: CrawlRequest) -> Self {
        Self {
            orch,
            req,
            on_page: None,
            on_error: None,
            on_complete: None,
        }
    }

    /// Handler invoked for every successful page. Returning
    /// `ControlFlow::Stop` ends the crawl early; pending in-flight pages
    /// are cancelled.
    pub fn on_page<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(PageEntry) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ControlFlow> + Send + 'static,
    {
        self.on_page = Some(Arc::new(move |p| Box::pin(f(p))));
        self
    }

    /// Handler invoked when an individual page errors. The crawl
    /// continues — handler is purely observational.
    pub fn on_error<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(FetchError) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_error = Some(Arc::new(move |e| Box::pin(f(e))));
        self
    }

    /// Handler invoked once after the stream terminates (success,
    /// cancel, or `Stop`). Receives aggregate stats.
    pub fn on_complete<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(CrawlStats) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_complete = Some(Arc::new(move |s| Box::pin(f(s))));
        self
    }

    /// Drive the crawl to completion (or early stop). Returns aggregate
    /// stats; the same stats object is passed to `on_complete` if
    /// installed.
    pub async fn run(self, cancel: CancelToken) -> Result<CrawlStats, FetchError> {
        let started = Instant::now();
        let mut stream = self.orch.crawl(self.req, cancel.clone()).await?;

        let mut stats = CrawlStats::default();
        while let Some(item) = stream.next().await {
            match item {
                Ok(page) => {
                    stats.pages = stats.pages.saturating_add(1);
                    stats.cost_milli_total = stats.cost_milli_total.saturating_add(page.cost_milli);
                    if let Some(h) = &self.on_page {
                        if h(page).await == ControlFlow::Stop {
                            cancel.cancel();
                            break;
                        }
                    }
                }
                Err(e) => {
                    stats.errors = stats.errors.saturating_add(1);
                    if let Some(h) = &self.on_error {
                        h(e).await;
                    }
                }
            }
        }

        stats.elapsed = started.elapsed();
        if let Some(h) = &self.on_complete {
            h(stats.clone()).await;
        }
        Ok(stats)
    }
}
