use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;

use crate::{
    adapter::{AdapterContext, AdapterRegistry, CrawlAdapterRegistry, PageEntryStream},
    budget::Budget,
    cancel::CancelToken,
    catalog::RouteCatalog,
    circuit::CircuitBreaker,
    crawl::{CrawlBuilder, CrawlEngine, CrawlRequest},
    error::FetchError,
    request::ScrapeRequest,
    response::ScrapeResponse,
    retry::{AttemptOutcome, RetryStrategy},
    route::{Route, RouteId},
    waterfall::{WaterfallConfig, WaterfallStats},
};

pub use spider::utils::hedge::{HedgeConfig, HedgeTracker};

/// Orchestration mode passed to [`Orchestrator::fetch`].
#[derive(Debug, Clone, Default)]
pub enum Mode {
    /// Single route + adaptive hedge after `HedgeTracker::adaptive_delay`.
    #[default]
    Hedge,
    /// Fire `max_parallel` routes at the same tier in parallel; first valid wins.
    Race { max_parallel: usize },
    /// Ladder: sequential mode, but launch a hedge at tier-N+1 after `hedge_delay`.
    Ladder { hedge_delay: Duration },
    /// Sequential mode capped by an inline budget ceiling.
    Budget { ceiling_milli: u64 },
}

pub struct Orchestrator {
    catalog: Arc<RouteCatalog>,
    adapters: Arc<AdapterRegistry>,
    /// Set once via [`Orchestrator::install_crawl_adapters`]. Unset orchestrators
    /// can still serve scrape requests — only `crawl()` requires it.
    crawl_adapters: OnceLock<Arc<CrawlAdapterRegistry>>,
    semaphores: HashMap<RouteId, Arc<Semaphore>>,
    breakers: HashMap<RouteId, Arc<CircuitBreaker>>,
    hedge_tracker: Arc<HedgeTracker>,
    budget: Arc<Budget>,
    stats: Arc<WaterfallStats>,
}

impl Orchestrator {
    pub fn new(
        catalog: Arc<RouteCatalog>,
        adapters: Arc<AdapterRegistry>,
        budget: Arc<Budget>,
    ) -> Self {
        let mut semaphores = HashMap::with_capacity(catalog.len());
        let mut breakers = HashMap::with_capacity(catalog.len());
        for r in catalog.all() {
            semaphores.insert(
                r.id.clone(),
                Arc::new(Semaphore::new(r.concurrency.max(1) as usize)),
            );
            breakers.insert(
                r.id.clone(),
                Arc::new(CircuitBreaker::new(5, Duration::from_secs(30))),
            );
        }
        Self {
            catalog,
            adapters,
            crawl_adapters: OnceLock::new(),
            semaphores,
            breakers,
            hedge_tracker: Arc::new(HedgeTracker::new(2.0, 8)),
            budget,
            stats: Arc::new(WaterfallStats::default()),
        }
    }

    /// Install the crawl-adapter registry. Idempotent — only the first call
    /// takes effect (subsequent calls are silently ignored). Call this once,
    /// after constructing the orchestrator, before `crawl()` is invoked.
    /// Scrape-only callers can skip this entirely.
    pub fn install_crawl_adapters(&self, reg: Arc<CrawlAdapterRegistry>) {
        let _ = self.crawl_adapters.set(reg);
    }

    pub fn crawl_adapters(&self) -> Option<&Arc<CrawlAdapterRegistry>> {
        self.crawl_adapters.get()
    }

    pub fn catalog(&self) -> &RouteCatalog {
        &self.catalog
    }

    /// Access the shared catalog Arc directly — useful for handing the same
    /// catalog to a [`LadderStrategy`] without re-wrapping.
    pub fn catalog_arc(&self) -> Arc<RouteCatalog> {
        self.catalog.clone()
    }
    pub fn adapters(&self) -> &AdapterRegistry {
        &self.adapters
    }
    pub fn budget(&self) -> &Budget {
        &self.budget
    }
    pub fn hedge_tracker(&self) -> &HedgeTracker {
        &self.hedge_tracker
    }
    pub fn waterfall_stats(&self) -> &Arc<WaterfallStats> {
        &self.stats
    }

    /// Replace the default [`WaterfallStats`] (uses [`WaterfallConfig::default`]) with a
    /// custom-configured one. Useful for tuning the promotion threshold or memory cap.
    pub fn with_waterfall_config(mut self, cfg: WaterfallConfig) -> Self {
        self.stats = Arc::new(WaterfallStats::new(cfg));
        self
    }

    /// Plug in an externally-owned stats instance — handy for sharing one stats store
    /// across multiple Orchestrators (e.g. per-tenant orchestrators in a server).
    pub fn with_waterfall_stats(mut self, stats: Arc<WaterfallStats>) -> Self {
        self.stats = stats;
        self
    }

    /// Single-attempt dispatch through a specific route. Enforces budget, circuit breaker,
    /// per-route concurrency limit, and propagates cancellation via `tokio::select!`.
    pub async fn execute_once(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        attempt: u32,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        let breaker = self
            .breakers
            .get(&route.id)
            .ok_or_else(|| FetchError::Config(format!("no breaker for route {}", route.id)))?;
        if !breaker.allow() {
            return Err(FetchError::CircuitOpen(route.id.clone()));
        }

        self.budget.try_spend(route.cost)?;

        let sem = self
            .semaphores
            .get(&route.id)
            .ok_or_else(|| FetchError::Config(format!("no semaphore for route {}", route.id)))?
            .clone();
        let _permit = sem
            .acquire_owned()
            .await
            .map_err(|_| FetchError::Network("semaphore closed".into()))?;

        let adapter = self
            .adapters
            .get(&route.adapter)
            .ok_or_else(|| FetchError::UnknownAdapter(route.adapter.as_str().into()))?;

        let started = Instant::now();
        let ctx = AdapterContext { attempt, started };

        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(FetchError::Cancelled),
            r = adapter.execute(route, req, &ctx, cancel) => r,
        };

        let elapsed = started.elapsed();
        self.hedge_tracker.record(elapsed);

        match &result {
            Ok(_) => {
                breaker.record_success();
                self.hedge_tracker.record_success();
                self.stats
                    .record_success(&route.id, &req.url, elapsed.as_millis() as u64);
            }
            Err(e) if e.is_retryable() => {
                breaker.record_failure();
                self.hedge_tracker.record_error();
                self.stats.record_failure(&route.id, &req.url);
            }
            Err(_) => {}
        }

        result.map(|mut resp| {
            resp.route_id = route.id.clone();
            resp.tier = route.tier;
            resp.cost_milli = route.cost;
            resp.attempt = attempt;
            resp.elapsed = elapsed;
            resp
        })
    }

    /// Sequential mode: sequential ladder. The strategy's `initial()` provides the first route;
    /// subsequent retries escalate via `on_retry()` until success, exhaustion, or non-retryable error.
    ///
    /// **Promotion shortcut:** before consulting the strategy, the orchestrator checks
    /// its [`WaterfallStats`] — if the request's URL has a domain with a route that's
    /// met the promotion thresholds (default: 100 successes at ≥80% rate), the ladder
    /// is skipped and that proven route runs first. The strategy still handles retries
    /// on failure, so promotion is an *acceleration*, never a constraint.
    #[allow(unused_assignments)]
    pub async fn fetch(
        &self,
        req: ScrapeRequest,
        strategy: Arc<dyn RetryStrategy>,
        cancel: CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        let max_retries = strategy.max_retries();

        let mut current_route = match self.stats.promoted_route(&req.url, &self.catalog) {
            Some(promoted) => promoted,
            None => match strategy.initial() {
                Some(r) => r,
                None => return Err(FetchError::Exhausted),
            },
        };

        let mut attempt: u32 = 0;
        let mut last_err: Option<FetchError> = None;
        let mut last_status: Option<u16> = None;
        let mut last_html_length: usize = 0;

        loop {
            let route_for_attempt = current_route.clone();

            match self
                .execute_once(&route_for_attempt, &req, attempt, &cancel)
                .await
            {
                Ok(resp) => {
                    if let Some(reason) =
                        validate(&route_for_attempt, &resp.body, resp.content_str())
                    {
                        last_status = Some(resp.status);
                        last_html_length = resp.body.len();
                        last_err = Some(FetchError::Validation(reason));
                    } else {
                        return Ok(resp);
                    }
                }
                Err(e) if !e.is_retryable() => return Err(e),
                Err(e) => {
                    if let FetchError::Status(s) = &e {
                        last_status = Some(*s);
                    }
                    last_err = Some(e);
                }
            }

            attempt += 1;
            if attempt > max_retries {
                return Err(last_err.unwrap_or(FetchError::Exhausted));
            }

            let outcome = AttemptOutcome {
                attempt,
                last_route: Some(route_for_attempt.as_ref()),
                last_status,
                last_html_length,
                last_error: last_err.as_ref(),
                url: &req.url,
                anti_bot: None,
                waf_check: matches!(last_err, Some(FetchError::Validation(_))),
            };

            let directive = strategy.on_retry(&outcome);
            if !directive.should_retry() {
                return Err(last_err.unwrap_or(FetchError::Exhausted));
            }

            current_route = directive.route.unwrap_or(route_for_attempt);

            if let Some(backoff) = directive.inner.backoff {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(FetchError::Cancelled),
                    _ = tokio::time::sleep(backoff) => {}
                }
            }
        }
    }

    /// Hedge mode: walk the strategy's ladder once to enumerate up to
    /// `hedge_config.max_hedges + 1` routes (primary + N hedges), then fire them with
    /// staircase delays.
    ///
    /// - Route 0 fires at t=0.
    /// - Route i fires at t = i * `HedgeTracker::adaptive_delay(hedge_config.delay)`.
    ///
    /// The adaptive delay shrinks when the EMA shows tail-latency is bad (or the hedge
    /// win rate is high), so hedging gets more aggressive automatically. First validated
    /// response wins; in-flight losers are cancelled by [`CancelToken`] propagation.
    ///
    /// ## No deadlock / no panic
    ///
    /// - Per-task delay math uses `saturating_mul` — even pathological config can't overflow.
    /// - Every adapter call sits inside `tokio::select!` against both the outer and the
    ///   per-race [`CancelToken`], so a winner promptly aborts all losers.
    /// - On error-only branches the loop drains [`FuturesUnordered`] and returns the
    ///   last failure or `Exhausted` — never blocks forever.
    pub async fn fetch_hedge(
        &self,
        req: ScrapeRequest,
        strategy: Arc<dyn RetryStrategy>,
        hedge_config: HedgeConfig,
        cancel: CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        if !hedge_config.enabled || hedge_config.max_hedges == 0 {
            return self.fetch(req, strategy, cancel).await;
        }

        // Build the ladder of routes by walking the strategy. Cap by max_hedges + 1 (primary + N).
        let first = strategy.initial().ok_or(FetchError::Exhausted)?;
        let mut routes: Vec<Arc<Route>> = vec![first.clone()];
        let mut current = first;
        for n in 1..=hedge_config.max_hedges {
            let outcome = AttemptOutcome {
                attempt: n as u32,
                last_route: Some(current.as_ref()),
                last_status: None,
                last_html_length: 0,
                last_error: None,
                url: &req.url,
                anti_bot: None,
                waf_check: false,
            };
            let directive = strategy.on_retry(&outcome);
            if !directive.should_retry() {
                break;
            }
            match directive.route {
                Some(next) => {
                    routes.push(next.clone());
                    current = next;
                }
                None => break,
            }
        }

        // Degenerate case: strategy produced only one route — fall back to a single fetch.
        if routes.len() == 1 {
            let only = routes.pop().expect("non-empty");
            let resp = self.execute_once(&only, &req, 0, &cancel).await?;
            if let Some(reason) = validate(&only, &resp.body, resp.content_str()) {
                return Err(FetchError::Validation(reason));
            }
            return Ok(resp);
        }

        let base_delay = self.hedge_tracker.adaptive_delay(hedge_config.delay);

        let race_cancel = CancelToken::new();
        let mut tasks = FuturesUnordered::new();

        for (i, route) in routes.iter().enumerate() {
            let route = route.clone();
            let outer = cancel.clone();
            let inner = race_cancel.clone();
            let req_ref = &req;
            // saturating_mul ensures pathological config (huge base + huge i) can never overflow.
            let delay = base_delay
                .checked_mul(i as u32)
                .unwrap_or(Duration::from_secs(u64::MAX / 2));
            let hedge_tracker = self.hedge_tracker.clone();
            let attempt = i as u32;
            tasks.push(async move {
                if !delay.is_zero() {
                    tokio::select! {
                        biased;
                        _ = outer.cancelled() => return Err(FetchError::Cancelled),
                        _ = inner.cancelled() => return Err(FetchError::Cancelled),
                        _ = tokio::time::sleep(delay) => {}
                    }
                    hedge_tracker.record_fired();
                }
                tokio::select! {
                    biased;
                    _ = outer.cancelled() => Err(FetchError::Cancelled),
                    _ = inner.cancelled() => Err(FetchError::Cancelled),
                    r = self.execute_once(&route, req_ref, attempt, &inner) => r,
                }
            });
        }

        let mut last_err: Option<FetchError> = None;
        while let Some(result) = tasks.next().await {
            match result {
                Ok(resp) => {
                    // Validate against the route's declared validators before declaring victory.
                    if let Some(route) = self.catalog.get(&resp.route_id) {
                        if let Some(reason) = validate(&route, &resp.body, resp.content_str()) {
                            last_err = Some(FetchError::Validation(reason));
                            continue;
                        }
                    }
                    // Stats: hedge_won = the winner wasn't the primary (attempt > 0).
                    let hedge_won = resp.attempt > 0;
                    self.hedge_tracker.record_outcome(hedge_won);
                    race_cancel.cancel();
                    return Ok(resp);
                }
                Err(FetchError::Cancelled) => continue,
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(FetchError::Exhausted))
    }

    /// Race mode: fire all `route_ids` in parallel; the first valid response wins and
    /// the rest are cancelled via the inner [`CancelToken`].
    pub async fn fetch_race(
        &self,
        req: ScrapeRequest,
        route_ids: &[&str],
        cancel: CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        if route_ids.is_empty() {
            return Err(FetchError::Exhausted);
        }
        let mut routes: Vec<Arc<Route>> = Vec::with_capacity(route_ids.len());
        for rid in route_ids {
            if let Some(r) = self.catalog.get(rid) {
                routes.push(r);
            }
        }
        if routes.is_empty() {
            return Err(FetchError::Exhausted);
        }

        let race_cancel = CancelToken::new();
        let mut tasks = FuturesUnordered::new();

        for route in routes {
            let outer = cancel.clone();
            let inner = race_cancel.clone();
            let req_ref = &req;
            let route_arc = route;
            tasks.push(async move {
                tokio::select! {
                    biased;
                    _ = outer.cancelled() => Err(FetchError::Cancelled),
                    _ = inner.cancelled() => Err(FetchError::Cancelled),
                    r = self.execute_once(&route_arc, req_ref, 0, &inner) => r,
                }
            });
        }

        let mut last_err: Option<FetchError> = None;
        while let Some(r) = tasks.next().await {
            match r {
                Ok(mut resp) => {
                    race_cancel.cancel();
                    // Validate before declaring winner.
                    if let Some(route) = self.catalog.get(&resp.route_id) {
                        if let Some(reason) = validate(&route, &resp.body, resp.content_str()) {
                            last_err = Some(FetchError::Validation(reason));
                            // Reset cancel so still-pending tasks get a chance (rare edge case).
                            continue;
                        }
                    }
                    resp.attempt = 0;
                    return Ok(resp);
                }
                Err(FetchError::Cancelled) => continue,
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(FetchError::Exhausted))
    }

    // ============================================================================
    // Crawl dispatch
    // ============================================================================

    /// Begin a crawl. Returns a stream of [`PageEntry`](crate::PageEntry) results
    /// — one yield per page discovered/fetched. Streaming-only: no in-memory
    /// accumulation, no race/hedge across engines (single engine per crawl,
    /// per-page escalation happens *inside* the local engine via the scrape
    /// ladder).
    ///
    /// Dispatch:
    ///
    /// - [`CrawlEngine::SpiderCloud`] — looks up the `spider.crawl` route
    ///   and dispatches via the registered crawl adapter for `HttpJsonlStreamMany`.
    /// - [`CrawlEngine::Local`] — looks up the `local.crawl` route and dispatches
    ///   via the `SpiderLocalCrawl` adapter, which BFSes using *this* orchestrator's
    ///   `fetch` per URL.
    /// - [`CrawlEngine::Auto`] — picks `SpiderCloud` when `SPIDER_API_KEY`
    ///   resolves on the embedded scrape request, otherwise falls back to `Local`.
    ///
    /// Returns [`FetchError::Config`] if the chosen route or adapter isn't
    /// registered. Per-page errors flow through the stream as `Err` items
    /// — they don't terminate the crawl.
    pub async fn crawl(
        &self,
        req: CrawlRequest,
        cancel: CancelToken,
    ) -> Result<PageEntryStream, FetchError> {
        let engine = self.resolve_crawl_engine(&req);
        let route_id: &str = match engine {
            CrawlEngine::SpiderCloud => "spider.crawl",
            CrawlEngine::Local => "local.crawl",
            CrawlEngine::Auto => unreachable!("resolve_crawl_engine never returns Auto"),
        };
        let route = self
            .catalog
            .get(route_id)
            .ok_or_else(|| FetchError::Config(format!("crawl route not in catalog: {route_id}")))?;

        let registry = self.crawl_adapters.get().ok_or_else(|| {
            FetchError::Config(
                "crawl adapters not installed — call Orchestrator::install_crawl_adapters before crawl()".into(),
            )
        })?;
        let adapter = registry
            .get(&route.adapter)
            .ok_or_else(|| FetchError::UnknownAdapter(route.adapter.as_str().into()))?;

        adapter.execute(&route, &req, &cancel).await
    }

    /// Builder-style entrypoint for the subscriber API.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use gottem_core::{Orchestrator, CrawlRequest, CancelToken, ControlFlow};
    /// # use url::Url;
    /// # async fn ex(orch: Arc<Orchestrator>) -> Result<(), Box<dyn std::error::Error>> {
    /// let req = CrawlRequest::new(Url::parse("https://example.com")?)
    ///     .with_limit(50)
    ///     .with_depth(2);
    /// orch.crawl_builder(req)
    ///     .on_page(|page| async move {
    ///         println!("{}", page.url);
    ///         ControlFlow::Continue
    ///     })
    ///     .run(CancelToken::new())
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn crawl_builder(self: &Arc<Self>, req: CrawlRequest) -> CrawlBuilder {
        CrawlBuilder::new(self.clone(), req)
    }

    /// Pick a concrete engine for a crawl request. `Auto` resolves via the
    /// embedded scrape request's credential map first, then process env —
    /// `SpiderCloud` if `SPIDER_API_KEY` is set, else `Local`.
    fn resolve_crawl_engine(&self, req: &CrawlRequest) -> CrawlEngine {
        match req.engine {
            CrawlEngine::SpiderCloud => CrawlEngine::SpiderCloud,
            CrawlEngine::Local => CrawlEngine::Local,
            CrawlEngine::Auto => {
                if req.scrape.resolve_env("SPIDER_API_KEY").is_some() {
                    CrawlEngine::SpiderCloud
                } else {
                    CrawlEngine::Local
                }
            }
        }
    }
}

fn validate(route: &Route, body: &[u8], content: Option<&str>) -> Option<String> {
    for v in &route.validate {
        if let Err(reason) = v.check(body, content) {
            return Some(reason);
        }
    }
    None
}
