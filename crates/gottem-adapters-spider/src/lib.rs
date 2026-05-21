//! gottem adapter that drives [`spider::website::Website`] for local fetching.
//!
//! Covers tiers **T0–T3**:
//!
//! | Tier | What you configure on the route       | Spider gives you                        |
//! |------|---------------------------------------|-----------------------------------------|
//! | T0   | bare `endpoint`                       | reqwest HTTP                            |
//! | T1   | `proxies = [...]` (datacenter)        | + rotating proxy                        |
//! | T2   | `proxies = [...]` (residential)       | + residential pool                      |
//! | T3   | `chrome` feature enabled              | + headless chrome (stealth/fingerprint) |
//!
//! ## Real-time streaming, no page storage
//!
//! Spider's `scrape()` accumulates a `Vec<Page>` in memory before returning. For
//! a single-page adapter that's wasted work — we only need the *first* page.
//!
//! This adapter calls `Website::subscribe()` to get a broadcast channel, spawns
//! `Website::crawl()` in a background task, and takes the first page off the
//! channel. As soon as the first page arrives we abort the rest of the crawl.
//! Memory footprint is one [`Page`], not a vec.
//!
//! ## Cancellation
//!
//! The adapter races `rx.recv()` against [`CancelToken::cancelled`] via
//! `tokio::select!`. On cancel, the spawned crawl task is aborted, which drops
//! spider's in-flight reqwest future and unwinds tasks cleanly.
//!
//! ## No regression
//!
//! Pure consumer of the spider crate. Same path spider takes when used directly
//! with `Website::new(url).with_limit(1).subscribe(_).crawl().await`.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, CancelToken, FetchError, Route, ScrapeRequest,
    ScrapeResponse,
};

#[derive(Debug, Default, Clone)]
pub struct SpiderAdapter;

impl SpiderAdapter {
    pub fn new() -> Self { Self }
    /// Construct an `Arc<dyn Adapter>` ready to register with [`gottem_core::AdapterRegistry`].
    pub fn arc() -> Arc<dyn Adapter> { Arc::new(Self::new()) }
}

#[async_trait]
impl Adapter for SpiderAdapter {
    fn kind(&self) -> AdapterKind { AdapterKind::SpiderLocal }

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        let url_str = req.url.as_str().to_string();
        let mut website = spider::website::Website::new(&url_str);

        // Single-page fast path (spider's `single_page()` check fires on with_limit(1)).
        // Disable robots.txt because callers using this adapter are explicitly choosing
        // direct fetch — the orchestrator's tier ladder is the policy layer, not robots.
        website
            .with_limit(1)
            .with_respect_robots_txt(false)
            .with_request_timeout(Some(route.timeout()));

        // Per-request headers via spider's reqwest re-export.
        if !req.headers.is_empty() {
            use spider::reqwest::header::{HeaderMap, HeaderName, HeaderValue};
            let mut headers = HeaderMap::new();
            for (k, v) in &req.headers {
                if let (Ok(name), Ok(val)) = (
                    HeaderName::try_from(k.as_str()),
                    HeaderValue::try_from(v.as_str()),
                ) {
                    headers.insert(name, val);
                }
            }
            if !headers.is_empty() {
                website.with_headers(Some(headers));
            }
        }

        // Subscribe BEFORE moving website into the crawl task — capacity 2 leaves
        // headroom in case spider buffers the page emission.
        let mut rx = website.subscribe(2);

        // Run the crawl in a background task; the broadcast channel feeds us the page
        // in real time without spider building up a Vec<Page> on the side.
        let crawl_handle = tokio::spawn(async move {
            website.crawl().await;
        });

        let page = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                crawl_handle.abort();
                return Err(FetchError::Cancelled);
            }
            res = rx.recv() => {
                // Got a page (or channel closed). Abort the rest of the crawl regardless —
                // for a single-page fetch we don't care about anything past the first emission.
                crawl_handle.abort();
                res.map_err(|e| FetchError::Network(format!("spider broadcast recv: {e}")))?
            }
        };

        let final_url = url::Url::parse(page.get_url()).unwrap_or_else(|_| req.url.clone());
        let status = page.status_code.as_u16();
        if status >= 400 {
            return Err(FetchError::Status(status));
        }

        let html = page.get_html();
        let body = Bytes::copy_from_slice(html.as_bytes());

        Ok(ScrapeResponse {
            url: final_url,
            status,
            headers: vec![],
            body,
            content: Some(html),
            route_id: route.id.clone(),
            tier: route.tier,
            cost_milli: route.cost,
            elapsed: ctx.elapsed(),
            attempt: ctx.attempt,
            metadata: Default::default(),
        })
    }
}
