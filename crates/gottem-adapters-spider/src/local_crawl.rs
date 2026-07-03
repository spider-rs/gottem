//! `SpiderLocalCrawlAdapter` — local BFS crawl that **never re-fetches** for
//! link discovery. The orchestrator's normal scrape ladder handles per-URL
//! fetching (so escalation across cloud/local routes works on every page);
//! the bytes returned are re-used to build a synthetic
//! [`spider::page::Page`] that we hand to
//! [`Page::links`](spider::page::Page::links) for outlink extraction. No
//! second network round-trip per URL.
//!
//! ## Concurrency model — actor pattern, **no mutexes**
//!
//! State ownership is single-task by construction:
//!
//! - **Dispatcher task** — owns the [`spider::website::Website`] and the
//!   pending counter. Pulls URLs off the frontier, calls
//!   [`Website::is_allowed`](spider::website::Website::is_allowed) to gate
//!   each one (visited / depth / allow / deny / robots), and spawns a
//!   worker per accepted URL. Receives [`WorkerReport`]s back, emits
//!   [`PageEntry`] to the output channel, pushes discovered links back
//!   into the frontier. Single owner = no mutex.
//! - **Worker tasks** — stateless. Receive a `(url, depth, …)` tuple, run
//!   `Orchestrator::fetch`, build a synthetic [`Page`](spider::page::Page)
//!   from the returned bytes, run [`Page::links`](spider::page::Page::links)
//!   to extract outlinks, and ship the [`WorkerReport`] back to the
//!   dispatcher. Each worker owns its own [`Page`] — no shared state.
//! - **Frontier** — single-consumer `tokio::sync::mpsc::unbounded_channel`.
//!   The dispatcher is the only consumer; no `Mutex<Receiver>` needed.
//! - **Visited / depth / allow / deny / robots** — all delegated to
//!   `Website::is_allowed` which the dispatcher calls under exclusive
//!   ownership. We don't reimplement any of spider's filtering.
//!
//! ## Leveraging spider's remote-client hook
//!
//! `spider::Website` natively supports
//! [`SpiderCloudConfig`](spider::configuration::SpiderCloudConfig) (modes:
//! `Proxy` / `Api` / `Unblocker` / `Fallback` / `Smart`) — when configured,
//! spider routes its internal fetcher through Spider's remote API
//! transparently. Combined with [`Website::subscribe`](spider::website::Website::subscribe)'s
//! broadcast channel, the entire crawl loop (visited, depth, allow/deny,
//! robots, fetch, link extraction) runs inside spider's core engine with
//! gottem acting purely as the consumer.
//!
//! The current adapter intentionally **does not** use the spider fetcher
//! — it uses gottem's orchestrator for each per-URL fetch so the scrape
//! ladder's escalation (T0 → T7 vendor switching) works mid-crawl. A
//! future `SpiderNativeCrawlAdapter` (gated behind the `spider_cloud`
//! feature, which forwards to `spider/spider_cloud`) will offer the
//! fully-delegated path for callers who prefer spider's fetcher with
//! Spider remote routing over gottem's ladder.
//!
//! ## Tracking parity with `Website::crawl_concurrent_raw`
//!
//! | Spider primitive (in [`crawl_concurrent_raw`])                   | Local-crawl analogue       |
//! |------------------------------------------------------------------|----------------------------|
//! | [`setup_selectors`](spider::website::Website::setup_selectors)   | dispatcher computes once, clones into each worker |
//! | [`is_allowed`](spider::website::Website::is_allowed)             | dispatcher per-URL gate    |
//! | [`insert_link`](spider::website::Website::insert_link)           | dispatcher post-gate       |
//! | `JoinSet<HashSet<CaseInsensitiveString>>`                         | per-worker `tokio::spawn` (no JoinSet needed — reports come back via `result_tx`) |
//! | `Semaphore` for concurrency limit                                | `Semaphore` for concurrency limit |
//!
//! [`crawl_concurrent_raw`]: spider::website::Website
//!
//! ## Termination
//!
//! The dispatcher tracks pending work via a local `u64` counter. Seed
//! contributes `+1`; each filter-block decrements; each worker dispatch
//! is balanced by the eventual `WorkerReport` arrival. When pending
//! reaches zero the dispatcher exits, dropping `frontier_tx` and
//! `output_tx`; the consumer's stream completes cleanly.

use std::sync::{Arc, Weak};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use gottem_core::{
    adapter::PageEntryStream, AdapterKind, CancelToken, CrawlAdapter, CrawlRequest, FetchError,
    LadderStrategy, Orchestrator, PageEntry, RetryStrategy, Route, ScrapeRequest, ScrapeResponse,
    Tier,
};
use spider::{
    compact_str::CompactString,
    page::{build, get_page_selectors},
    utils::PageResponse,
    website::ProcessLinkStatus,
    CaseInsensitiveString, RelativeSelectors,
};
use tokio::sync::{mpsc, Semaphore};
use url::Url;

/// Hard page ceiling applied when a crawl requests `limit == 0` ("unlimited").
/// The frontier channel and spider's visited set grow with dispatched URLs, so
/// a truly uncapped crawl of a huge site would grow process memory without
/// bound. 100k pages ≈ tens of MB of frontier/visited state — far above any
/// realistic single crawl, low enough to bound the pathological case. Callers
/// needing more set an explicit `limit`.
pub const UNLIMITED_PAGE_CEILING: u32 = 100_000;

/// Local BFS crawl adapter. Holds a `Weak<Orchestrator>` to avoid creating
/// a reference cycle — the orchestrator owns the
/// [`CrawlAdapterRegistry`](gottem_core::CrawlAdapterRegistry) that owns
/// this adapter, and this adapter would otherwise own an
/// `Arc<Orchestrator>` back.
#[derive(Clone)]
pub struct SpiderLocalCrawlAdapter {
    orch: Weak<Orchestrator>,
    strategy: Arc<dyn RetryStrategy>,
    /// `spider::Client` used **only** for
    /// [`Website::configure_robots_parser`](spider::website::Website::configure_robots_parser)
    /// — fetching `/robots.txt`. The per-URL crawl fetches go through the
    /// gottem orchestrator, not this client. Built lazily; failure to
    /// build surfaces from `execute` as a [`FetchError::Config`] rather
    /// than a panic.
    spider_client: Option<spider::Client>,
}

impl SpiderLocalCrawlAdapter {
    /// Build with an explicit retry strategy. Pass the same one you use
    /// for `fetch` on the orchestrator — each child URL inside the
    /// crawl runs through `fetch` with this strategy.
    ///
    /// Returns an error if the internal `spider::Client` (used only for
    /// the robots.txt fetch when `respect_robots=true`) fails to build.
    /// Most callers should use [`Self::with_defaults`] instead.
    pub fn try_new(
        orch: &Arc<Orchestrator>,
        strategy: Arc<dyn RetryStrategy>,
    ) -> Result<Self, FetchError> {
        let spider_client = spider::ClientBuilder::default()
            .build()
            .map_err(|e| FetchError::Config(format!("spider::Client build: {e}")))?;
        Ok(Self {
            orch: Arc::downgrade(orch),
            strategy,
            spider_client: Some(spider_client),
        })
    }

    /// Build with the orchestrator's catalog as the strategy source and a
    /// permissive default ladder (`T0..T9`, 5 retries). Suitable for the
    /// majority of callers that just want crawling to work. Returns an
    /// error if the internal `spider::Client` fails to build.
    pub fn try_with_defaults(orch: &Arc<Orchestrator>) -> Result<Self, FetchError> {
        let strategy: Arc<dyn RetryStrategy> = Arc::new(LadderStrategy::new(
            orch.catalog_arc(),
            Tier::T0,
            Tier::T9,
            Default::default(),
            5,
        ));
        Self::try_new(orch, strategy)
    }

    /// Convenience that delegates to [`Self::try_with_defaults`]. If the
    /// internal client fails to build, returns an adapter with no robots
    /// client — `respect_robots=true` requests then surface
    /// [`FetchError::Config`] from `execute`. This keeps the construction
    /// API panic-free while still being one-liner ergonomic.
    pub fn with_defaults(orch: &Arc<Orchestrator>) -> Self {
        match Self::try_with_defaults(orch) {
            Ok(a) => a,
            Err(_) => Self {
                orch: Arc::downgrade(orch),
                strategy: Arc::new(LadderStrategy::new(
                    orch.catalog_arc(),
                    Tier::T0,
                    Tier::T9,
                    Default::default(),
                    5,
                )),
                spider_client: None,
            },
        }
    }

    /// Convenience for plugging into a [`CrawlAdapterRegistry`].
    pub fn arc(orch: &Arc<Orchestrator>) -> Arc<dyn CrawlAdapter> {
        Arc::new(Self::with_defaults(orch))
    }

    /// Plug in a custom `spider::Client` (proxies, custom UA, etc.) for
    /// the robots fetch.
    ///
    /// **Proxy caveat:** per-URL *page* fetches go through the orchestrator's
    /// scrape ladder, so they exit via whatever proxy class the winning vendor
    /// route provides. The **robots.txt** fetch is the exception — it uses
    /// this client, and the default (`ClientBuilder::default()`) carries no
    /// proxy, so robots requests leave from the host's own egress IP. Pass a
    /// proxied client here when that bypass matters for your deployment.
    pub fn with_spider_client(mut self, client: spider::Client) -> Self {
        self.spider_client = Some(client);
        self
    }
}

/// Message sent from a worker back to the dispatcher after a per-URL fetch.
struct WorkerReport {
    /// Depth of the URL this worker fetched. Used when pushing the
    /// worker's outlinks into the frontier (`depth + 1`).
    depth: u32,
    /// The fetch result. `Ok` carries the scrape + already-extracted links;
    /// `Err` is forwarded to the consumer and never increments link counts.
    result: Result<WorkerSuccess, FetchError>,
    /// When the dispatcher dispatched this worker — used to populate
    /// [`PageEntry::elapsed`] without each worker carrying around an
    /// `Instant` clone.
    dispatch_started: Instant,
}

struct WorkerSuccess {
    scrape: ScrapeResponse,
    /// Outlinks already extracted from the scrape's HTML via
    /// `Page::links`. Done in the worker so HTML parsing parallelises
    /// across workers — the dispatcher then only needs to push these
    /// into the frontier.
    new_links: Vec<Url>,
}

#[async_trait]
impl CrawlAdapter for SpiderLocalCrawlAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::SpiderLocalCrawl
    }

    async fn execute(
        &self,
        route: &Route,
        req: &CrawlRequest,
        cancel: &CancelToken,
    ) -> Result<PageEntryStream, FetchError> {
        // Upgrade the weak reference — Orchestrator must still be alive.
        let orch = self
            .orch
            .upgrade()
            .ok_or_else(|| FetchError::Config("orchestrator dropped before crawl()".into()))?;

        // ── Build spider::Website with the full CrawlRequest config ─────
        let mut website = spider::website::Website::new(req.scrape.url.as_str());
        website.with_depth(req.depth as usize);
        website.with_subdomains(req.subdomains);
        website.with_tld(req.tld);
        if !req.deny.is_empty() {
            let deny: Vec<CompactString> = req
                .deny
                .iter()
                .map(|s| CompactString::from(s.as_str()))
                .collect();
            website.with_blacklist_url(Some(deny));
        }
        if !req.allow.is_empty() {
            let allow: Vec<CompactString> = req
                .allow
                .iter()
                .map(|s| CompactString::from(s.as_str()))
                .collect();
            website.with_whitelist_url(Some(allow));
        }
        website.with_respect_robots_txt(req.respect_robots);
        // Limit enforcement delegated to spider's built-in wildcard
        // budget. The budget decrements on every `is_allowed` call, so
        // dispatch is naturally capped at `limit` URLs — workers in
        // flight when the budget runs out have already been dispatched
        // and will yield their `PageEntry`, but no further URLs reach
        // the worker pool. Off-by-one note: spider treats `budget == 1`
        // as already exceeded, so `limit + 1` allows exactly `limit`
        // URLs through. `limit == 0` means "unlimited" — but still gets
        // [`UNLIMITED_PAGE_CEILING`] as a hard budget: the frontier and
        // visited set grow with dispatched URLs, so a genuinely uncapped
        // crawl of a huge site would grow process memory without bound.
        {
            let effective = if req.limit > 0 {
                req.limit
            } else {
                UNLIMITED_PAGE_CEILING
            };
            let mut budget: spider::hashbrown::HashMap<&str, u32> = Default::default();
            budget.insert("*", effective.saturating_add(1));
            website.with_budget(Some(budget));
        }
        // `determine_limits()` must run after `with_budget` to flip the
        // `wild_card_budgeting` flag on; without it the budget map is
        // ignored. This is the same call spider makes inside
        // `crawl_concurrent_raw`.
        website.determine_limits();
        if req.respect_robots {
            match &self.spider_client {
                Some(client) => website.configure_robots_parser(client).await,
                None => {
                    return Err(FetchError::Config(
                        "respect_robots=true but spider::Client failed to build".into(),
                    ));
                }
            }
        }

        let selectors: RelativeSelectors = website.setup_selectors();
        let base: Option<Box<Url>> = Some(Box::new(req.scrape.url.clone()));

        let (frontier_tx, mut frontier_rx) = mpsc::unbounded_channel::<(Url, u32)>();
        let (result_tx, mut result_rx) = mpsc::unbounded_channel::<WorkerReport>();
        let (output_tx, output_rx) = mpsc::channel::<Result<PageEntry, FetchError>>(64);

        // Seed the frontier with the start URL. Errors here can only be
        // "receiver dropped" — but we just constructed it, so it's safe.
        if frontier_tx.send((req.scrape.url.clone(), 0)).is_err() {
            return Err(FetchError::Config("frontier closed before seed".into()));
        }

        // Worker concurrency gate. Resolution order:
        //   1. Caller-provided `adaptive_concurrency` handle — lets an
        //      admission controller resize the crawl's worker fan-out
        //      mid-flight (e.g. a gottem-cloud LoadTracker handing its
        //      live `AdaptiveSemaphore` here).
        //   2. Fresh `Semaphore::new(concurrency)` — byte-for-byte the
        //      pre-adaptive behavior for callers that never touched the
        //      new builder.
        let workers_n = req.concurrency.max(1) as usize;
        let semaphore = match req.adaptive_concurrency.clone() {
            Some(sem) => sem,
            None => Arc::new(Semaphore::new(workers_n)),
        };
        let cancel = cancel.clone();
        let scrape_template = req.scrape.clone();
        // Per-request ladder-catalog override (hosted-layer auth gate): when the
        // caller pins a catalog, build a fresh ladder over it so per-URL fetches
        // only climb into vendors the caller can authenticate — never surfacing
        // a late `missing env var` auth error mid-crawl. We keep the adapter's
        // configured retry budget and use this page's required caps, mirroring
        // the default ladder's shape. `None` = the adapter's baked strategy,
        // unchanged byte-for-byte.
        let strategy: Arc<dyn RetryStrategy> = match &req.ladder_catalog {
            Some(catalog) => Arc::new(LadderStrategy::new(
                catalog.clone(),
                Tier::T0,
                Tier::T9,
                req.scrape.required_caps,
                self.strategy.max_retries(),
            )),
            None => self.strategy.clone(),
        };
        let route_id = route.id.clone();
        let tier = route.tier;
        let cost_milli = route.cost;
        let max_depth = req.depth;

        // ── Dispatcher task ─────────────────────────────────────────────
        //
        // Owns Website (mutable, never shared). Tracks pending work.
        // Polls two streams via `tokio::select!`: incoming URLs from the
        // frontier (gate via Website, spawn worker), and incoming worker
        // reports (emit entry, enqueue new links). Exits when pending
        // hits 0 or cancel fires; drops `frontier_tx` + `output_tx` →
        // workers' channel sends start failing, consumer's stream ends.
        tokio::spawn(async move {
            let mut pending: u64 = 1; // seed
                                      // Holds onto worker spawn permits implicitly via the workers
                                      // themselves dropping them; semaphore is shared across workers.
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,

                    Some((url, depth)) = frontier_rx.recv() => {
                        // Filter via Website (mut access — single-task safe).
                        // Depth + visited + allow/deny + robots all collapse
                        // into this one call.
                        let allowed = {
                            let ci = CaseInsensitiveString::from(url.as_str());
                            let st = website.is_allowed(&ci);
                            if matches!(st, ProcessLinkStatus::Allowed) {
                                website.insert_link(&ci).await;
                                true
                            } else {
                                false
                            }
                        };
                        if !allowed {
                            pending = pending.saturating_sub(1);
                            if pending == 0 { break; }
                            continue;
                        }
                        // Reserve a worker slot. If the semaphore is closed
                        // (shouldn't happen unless we explicitly close it),
                        // bail out gracefully.
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => break,
                        };
                        let orch = orch.clone();
                        let strategy = strategy.clone();
                        let scrape_template = scrape_template.clone();
                        let result_tx = result_tx.clone();
                        let cancel = cancel.clone();
                        let selectors = selectors.clone();
                        let base = base.clone();
                        let dispatch_started = Instant::now();
                        // Worker: pure stateless — no shared mutable state.
                        // Owns its Page entirely (built from response bytes,
                        // dropped after links extracted). Cancel-respecting
                        // via the orchestrator's CancelToken wiring.
                        tokio::spawn(async move {
                            let _permit = permit;
                            // Fast-path bail: if cancel already fired before
                            // we even started, skip the fetch entirely. The
                            // dispatcher tracks pending — even when the
                            // crawl is winding down, a WorkerReport must
                            // arrive so pending decrements; we send a
                            // synthetic Cancelled report.
                            if cancel.is_cancelled() {
                                let _ = result_tx.send(WorkerReport {
                                    depth,
                                    result: Err(FetchError::Cancelled),
                                    dispatch_started,
                                });
                                return;
                            }
                            let mut child = scrape_template;
                            child.url = url.clone();
                            let fetch_result =
                                orch.fetch(child, strategy, cancel.clone()).await;
                            // Skip the CPU-bound link extraction when
                            // cancel fired during the fetch — extracting
                            // outlinks on a page the consumer will never
                            // see is wasted work.
                            let result = match fetch_result {
                                Ok(_) if cancel.is_cancelled() => Err(FetchError::Cancelled),
                                Ok(scrape) => {
                                    let new_links = extract_links_locally(
                                        &url,
                                        &scrape,
                                        &selectors,
                                        &base,
                                        depth,
                                        max_depth,
                                    )
                                    .await;
                                    Ok(WorkerSuccess { scrape, new_links })
                                }
                                Err(e) => Err(e),
                            };
                            let _ = result_tx.send(WorkerReport {
                                depth,
                                result,
                                dispatch_started,
                            });
                        });
                    }

                    Some(report) = result_rx.recv() => {
                        pending = pending.saturating_sub(1);
                        match report.result {
                            Ok(success) => {
                                // Dispatch gating already enforced limit via
                                // spider's wildcard budget — every report
                                // that reaches us corresponds to an
                                // `is_allowed → Allowed` decision spider
                                // made. So we emit unconditionally and let
                                // the next `is_allowed` round naturally
                                // turn `BudgetExceeded` for over-quota
                                // candidates.
                                let entry = PageEntry {
                                    url: success.scrape.url.clone(),
                                    depth: report.depth,
                                    status: success.scrape.status,
                                    body: success.scrape.body.clone(),
                                    content: success.scrape.content.clone(),
                                    links: Some(success.new_links.clone()),
                                    route_id: route_id.clone(),
                                    tier,
                                    cost_milli,
                                    elapsed: report.dispatch_started.elapsed(),
                                };
                                if output_tx.send(Ok(entry)).await.is_err() {
                                    // Consumer disconnected. Fire cancel so
                                    // in-flight worker fetches abort
                                    // immediately via the CancelToken
                                    // arm in `orch.fetch` — otherwise
                                    // they'd run to completion before
                                    // noticing the dropped frontier_tx.
                                    cancel.cancel();
                                    break;
                                }
                                for link in success.new_links {
                                    if frontier_tx.send((link, report.depth + 1)).is_ok() {
                                        pending = pending.saturating_add(1);
                                    }
                                }
                            }
                            Err(e) => {
                                if output_tx.send(Err(e)).await.is_err() {
                                    cancel.cancel();
                                    break;
                                }
                            }
                        }
                        if pending == 0 { break; }
                    }

                    // Both channels are open as long as the dispatcher
                    // holds its senders. `else` only fires when both
                    // recv calls return None simultaneously, which only
                    // happens after we've dropped our own sender — i.e.,
                    // never inside the loop.
                    else => break,
                }
            }
            // Explicit drops are a no-op (sends end-of-scope drops them),
            // but documenting the lifecycle: frontier_tx and output_tx
            // drop here → workers' result_tx -> output_tx sends start
            // failing → workers exit → all clones drop → caller's
            // stream completes.
            drop(frontier_tx);
            drop(output_tx);
        });

        Ok(Box::pin(ReceiverStream::new(output_rx)))
    }
}

/// Build a synthetic `spider::page::Page` from the bytes already fetched
/// and call `page.links` to extract outlinks. **Zero network calls** —
/// `Page::build` is pure and `Page::links` only reads `self.html`.
async fn extract_links_locally(
    url: &Url,
    scrape: &ScrapeResponse,
    selectors: &RelativeSelectors,
    base: &Option<Box<Url>>,
    depth: u32,
    max_depth: u32,
) -> Vec<Url> {
    if depth >= max_depth {
        return Vec::new();
    }
    // Out-of-range status (impossible from reqwest::Response, but possible
    // from custom adapters): default to a benign 200 so Page::build doesn't
    // reclassify the body.
    let status_code =
        reqwest::StatusCode::from_u16(scrape.status).unwrap_or(reqwest::StatusCode::OK);
    let page_resp = PageResponse {
        content: Some(scrape.body.to_vec()),
        final_url: Some(scrape.url.to_string()),
        status_code,
        ..Default::default()
    };
    let mut page = build(url.as_str(), page_resp);
    let extracted = page.links(selectors, base).await;
    extracted
        .iter()
        .filter_map(|l| Url::parse(l.inner()).ok())
        .collect()
}

// ---- Minimal Stream wrapper over an mpsc::Receiver ------------------------

struct ReceiverStream<T> {
    rx: mpsc::Receiver<T>,
}

impl<T> ReceiverStream<T> {
    fn new(rx: mpsc::Receiver<T>) -> Self {
        Self { rx }
    }
}

impl<T: Send + 'static> futures_core::Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

// Unused-import suppressors. `get_page_selectors`, `Bytes`, and
// `ScrapeRequest` are referenced in the module's doc comments by name;
// keep them in scope so the doc links resolve.
#[allow(dead_code)]
fn _selectors_marker(u: &str, sub: bool, tld: bool) -> RelativeSelectors {
    get_page_selectors(u, sub, tld)
}
#[allow(dead_code)]
fn _body_marker(b: &Bytes) -> usize {
    b.len()
}
#[allow(dead_code)]
fn _req_marker(_r: &ScrapeRequest) {}
