//! gottem HTTP adapters: dispatch routes against vendor REST APIs.
//!
//! Three adapter types, all sharing one `reqwest::Client` for connection pooling:
//!
//! - [`DirectHttpAdapter`] — plain GET/POST. Body passed through as-is. Useful for routes
//!   that just want to fetch a URL through a particular endpoint without JSON wrapping.
//! - [`HttpJsonAdapter`] — POST a JSON body (rendered from the route's
//!   [`BodyTemplate`](gottem_core::BodyTemplate) with `{{url}}` substitution), parse the
//!   JSON response with the route's [`ResponseParse`](gottem_core::ResponseParse) spec
//!   (`JsonPath`, `JsonlFirst`, `RawText`, etc.). Covers Firecrawl, ZenRows, ScrapingBee,
//!   Zyte, Brightdata Web Unlocker.
//! - [`HttpJsonlStreamAdapter`] — same as [`HttpJsonAdapter`] but the response is treated as
//!   newline-delimited JSONL. Spider's `/scrape` endpoint *requires* this — calling
//!   `.json()` on its response hangs because it streams chunked JSONL.
//!
//! ## Cancellation
//!
//! Each adapter wraps the reqwest `send()` and `bytes()` futures in
//! `tokio::select!` against [`CancelToken::cancelled`](gottem_core::CancelToken). On cancel
//! the in-flight request is dropped, which closes the TCP connection.
//!
//! ## Registration
//!
//! ```no_run
//! use std::sync::Arc;
//! use gottem_core::AdapterRegistry;
//! use gottem_adapters_http::register_all;
//!
//! let mut reg = AdapterRegistry::new();
//! register_all(&mut reg, None);
//! let reg = Arc::new(reg);
//! ```

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use std::sync::Arc;

use async_trait::async_trait;
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, AdapterRegistry, CancelToken, CrawlAdapterRegistry,
    FetchError, Route, ScrapeRequest, ScrapeResponse,
};
use reqwest::Client;

mod jsonl_many;
mod shared;
pub use jsonl_many::HttpJsonlStreamManyAdapter;
pub use shared::build_default_client;

use shared::{
    apply_auth, extract_content_and_cost, render_body, send_with_cancel, to_reqwest_method,
    HttpOutcome,
};

/// Register all three single-page HTTP adapters into a [`AdapterRegistry`], sharing one
/// `reqwest::Client` across them for connection pooling. Pass `None` to use the default
/// client builder, or `Some(client)` to plug in a pre-configured one.
pub fn register_all(reg: &mut AdapterRegistry, client: Option<Client>) {
    let client = client.unwrap_or_else(build_default_client);
    reg.register(Arc::new(DirectHttpAdapter::new(client.clone())));
    reg.register(Arc::new(HttpJsonAdapter::new(client.clone())));
    reg.register(Arc::new(HttpJsonlStreamAdapter::new(client)));
}

/// Register all crawl-capable HTTP adapters. Currently just
/// [`HttpJsonlStreamManyAdapter`] for Spider `/crawl`.
pub fn register_crawl_all(reg: &mut CrawlAdapterRegistry, client: Option<Client>) {
    let client = client.unwrap_or_else(build_default_client);
    reg.register(Arc::new(HttpJsonlStreamManyAdapter::new(client)));
}

/// Plain GET/POST adapter. Sends `req.body` directly (if any), returns the response body
/// as content per the route's parse spec.
#[derive(Debug, Clone)]
pub struct DirectHttpAdapter {
    client: Client,
}

impl DirectHttpAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub fn shared() -> Arc<dyn Adapter> {
        Arc::new(Self::new(build_default_client()))
    }
}

#[async_trait]
impl Adapter for DirectHttpAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::DirectHttp
    }

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        let resolved = route.endpoint.render(req)?;
        let mut builder = self
            .client
            .request(to_reqwest_method(route.method), resolved)
            .timeout(route.timeout());

        for (k, v) in &route.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        for (k, v) in &req.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        builder = apply_auth(builder, &route.auth, req)?;

        if let Some(body) = req.body.clone() {
            builder = builder.body(body);
        }

        let outcome = send_with_cancel(builder, cancel).await?;
        if outcome.status >= 400 {
            return Err(FetchError::Status(outcome.status));
        }
        let (content, cost) = extract_content_and_cost(
            &route.parse,
            route.cost_extract.as_ref(),
            &outcome.headers,
            &outcome.body,
        )?;
        Ok(response_from(route, req, ctx, outcome, content, cost))
    }
}

/// JSON-API adapter. Renders the route's body template with `{{url}}` substitution,
/// POSTs (or whatever `route.method` is) it as `application/json`, parses the JSON
/// response per `route.parse`.
#[derive(Debug, Clone)]
pub struct HttpJsonAdapter {
    client: Client,
}

impl HttpJsonAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
    pub fn shared() -> Arc<dyn Adapter> {
        Arc::new(Self::new(build_default_client()))
    }
}

#[async_trait]
impl Adapter for HttpJsonAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::HttpJson
    }

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        let resolved = route.endpoint.render(req)?;
        let mut builder = self
            .client
            .request(to_reqwest_method(route.method), resolved)
            .timeout(route.timeout())
            .header("content-type", "application/json")
            .header("accept", "application/json");

        for (k, v) in &route.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        for (k, v) in &req.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        builder = apply_auth(builder, &route.auth, req)?;

        if let Some(body) = render_body(&route.body, req)? {
            builder = builder.body(body);
        }

        let outcome = send_with_cancel(builder, cancel).await?;
        if outcome.status >= 400 {
            return Err(FetchError::Status(outcome.status));
        }
        let (content, cost) = extract_content_and_cost(
            &route.parse,
            route.cost_extract.as_ref(),
            &outcome.headers,
            &outcome.body,
        )?;
        Ok(response_from(route, req, ctx, outcome, content, cost))
    }
}

/// Streaming JSONL adapter. Identical to [`HttpJsonAdapter`] on the request side, but
/// the response is parsed as newline-delimited JSONL — take the first complete record.
/// Spider's `/scrape` endpoint requires this; calling `.json()` on its response hangs.
#[derive(Debug, Clone)]
pub struct HttpJsonlStreamAdapter {
    client: Client,
}

impl HttpJsonlStreamAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
    pub fn shared() -> Arc<dyn Adapter> {
        Arc::new(Self::new(build_default_client()))
    }
}

#[async_trait]
impl Adapter for HttpJsonlStreamAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::HttpJsonlStream
    }

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        let resolved = route.endpoint.render(req)?;
        let mut builder = self
            .client
            .request(to_reqwest_method(route.method), resolved)
            .timeout(route.timeout())
            .header("content-type", "application/json")
            .header(
                "accept",
                "application/jsonl, application/x-ndjson, application/json",
            );

        for (k, v) in &route.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        for (k, v) in &req.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        builder = apply_auth(builder, &route.auth, req)?;

        if let Some(body) = render_body(&route.body, req)? {
            builder = builder.body(body);
        }

        let outcome = send_with_cancel(builder, cancel).await?;
        if outcome.status >= 400 {
            return Err(FetchError::Status(outcome.status));
        }
        // The canonical parse for this adapter is JsonlFirst, but we honor whatever the
        // route declared — extract_content_and_cost dispatches on route.parse directly.
        let (content, cost) = extract_content_and_cost(
            &route.parse,
            route.cost_extract.as_ref(),
            &outcome.headers,
            &outcome.body,
        )?;
        Ok(response_from(route, req, ctx, outcome, content, cost))
    }
}

fn response_from(
    route: &Route,
    req: &ScrapeRequest,
    ctx: &AdapterContext,
    outcome: HttpOutcome,
    content: Option<bytes::Bytes>,
    cost: Option<(f64, String)>,
) -> ScrapeResponse {
    // Per-request cost was extracted alongside content in one JSON parse. Missing data
    // (header absent, JSON path miss, malformed) is normal — it arrives here as None.
    let (actual_units, actual_unit) = match cost {
        Some((n, u)) => (Some(n), Some(u)),
        None => (None, None),
    };
    ScrapeResponse {
        url: req.url.clone(),
        status: outcome.status,
        headers: outcome.headers,
        body: outcome.body,
        content,
        route_id: route.id.clone(),
        tier: route.tier,
        cost_milli: route.cost,
        cost_actual_units: actual_units,
        cost_actual_unit: actual_unit,
        elapsed: ctx.elapsed(),
        attempt: ctx.attempt,
        metadata: Default::default(),
    }
}
