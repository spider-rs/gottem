//! gottem adapter for **single-page** fetching via spider — uses `Page::new(url, &client)`,
//! the most direct primitive in spider's API. No crawl scheduler, no broadcast channels,
//! no link discovery. One URL in, one Page out.
//!
//! ## What you get
//!
//! - Spider's hardened HTTP client (cookies, UA generator, encoding handling, TLS).
//! - Predictable status code propagation — upstream 5xx surfaces as the real status,
//!   not filtered by the crawler's retry logic.
//! - Cross-platform consistency: Linux and macOS behave identically because there's no
//!   crawl scheduler in between.
//!
//! ## What this adapter *doesn't* do
//!
//! - **No crawling.** This is single-URL fetch only. Recursive crawling lives elsewhere
//!   in your stack — gottem stays in "fetch one resource" lane.
//! - **No per-request headers.** Headers are baked into the shared `spider::Client` at
//!   adapter construction time. If you need per-request headers, route through
//!   `gottem-adapters-http` instead.
//! - **No `provider_options` / `geo`.** There is no vendor API to forward options to —
//!   this adapter drives a local client. Everything request-shaping (proxy, headers,
//!   UA) is baked into the `spider::Client` at construction; per-request knobs that
//!   only make sense for cloud vendors are deliberately ignored here.
//!
//! ## Tier coverage
//!
//! `AdapterKind::SpiderLocal` is the bridge for T0–T3:
//!
//! | Tier | What you configure              | What spider provides                   |
//! |------|---------------------------------|----------------------------------------|
//! | T0   | bare `endpoint`                 | reqwest HTTP (via spider's Client)     |
//! | T1   | proxy list on the Client builder| + rotating datacenter proxy            |
//! | T2   | residential proxy on Client     | + residential pool                     |
//! | T3   | `chrome` feature flag           | (use the chrome adapter for that tier) |
//!
//! ## Cancellation
//!
//! `Page::new` is wrapped in `tokio::select!` against the orchestrator's
//! [`CancelToken`]. On cancel the future is dropped, which closes the underlying
//! reqwest connection cleanly.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, CancelToken, FetchError, Route, ScrapeRequest,
    ScrapeResponse,
};

mod local_crawl;
pub use local_crawl::SpiderLocalCrawlAdapter;

/// Convenience: register the local-crawl adapter into a [`CrawlAdapterRegistry`]
/// with the orchestrator's default ladder strategy. Call this after building
/// the orchestrator so the adapter can take a weak back-reference to it.
///
/// [`CrawlAdapterRegistry`]: gottem_core::CrawlAdapterRegistry
pub fn register_crawl_all(
    reg: &mut gottem_core::CrawlAdapterRegistry,
    orch: &Arc<gottem_core::Orchestrator>,
) {
    reg.register(SpiderLocalCrawlAdapter::arc(orch));
}

/// Single-page adapter. One [`spider::Client`] is built per adapter instance and reused
/// across every call — connection pooling, DNS caching, and cookie state all persist.
#[derive(Debug, Clone)]
pub struct SpiderAdapter {
    client: spider::Client,
}

impl Default for SpiderAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SpiderAdapter {
    pub fn new() -> Self {
        // ClientBuilder::default() returns a fully-configured spider Client with sensible
        // defaults (UA generator, cookies, encoding, TLS). For T1/T2 (proxy tiers),
        // construct via `with_client` passing a Client that has proxies wired in.
        let client = spider::ClientBuilder::default()
            .build()
            .expect("spider default client");
        Self { client }
    }

    /// Plug in a pre-configured `spider::Client` — e.g. one with proxies, custom UA, or
    /// headers baked in. Use this to make a single SpiderAdapter cover T0–T2.
    pub fn with_client(client: spider::Client) -> Self {
        Self { client }
    }

    pub fn arc() -> Arc<dyn Adapter> {
        Arc::new(Self::new())
    }

    pub fn arc_with_client(client: spider::Client) -> Arc<dyn Adapter> {
        Arc::new(Self::with_client(client))
    }
}

#[async_trait]
impl Adapter for SpiderAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::SpiderLocal
    }

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        let url_str = req.url.as_str();

        // Single-page fetch via spider's raw HTTP path. `new_page` keeps the same
        // signature whether or not the `chrome` feature is enabled on spider (Page::new
        // gets overloaded to take chrome-specific args when chrome is on, which would
        // break feature unification across the workspace). new_page is the stable
        // HTTP-only entry point. Returns a Page even on upstream errors; we read
        // status_code below and map >=400 to FetchError::Status.
        let page = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FetchError::Cancelled),
            p = spider::page::Page::new_page(url_str, &self.client) => p,
        };

        let final_url = url::Url::parse(page.get_url()).unwrap_or_else(|_| req.url.clone());
        let status = page.status_code.as_u16();
        if status >= 400 {
            return Err(FetchError::Status(status));
        }

        // Move the page HTML into a `Bytes` (the String's allocation becomes the Bytes'
        // buffer — no copy) and share it as both `body` and `content`. The clone is a
        // refcount bump, not a memcpy — one allocation instead of two for large pages.
        let body = Bytes::from(page.get_html().into_bytes());

        Ok(ScrapeResponse {
            url: final_url,
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
        })
    }
}
