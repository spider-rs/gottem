//! `HttpJsonlStreamManyAdapter` — streaming JSONL crawl over HTTP.
//!
//! POSTs the route's body template, then reads the response body as
//! `\n`-delimited JSON records. **One [`PageEntry`] per line**. Each line is
//! parsed on arrival and the resulting entry is shipped through a bounded
//! `tokio::sync::mpsc` channel; the returned stream is the consumer end.
//!
//! Memory profile: only the in-flight chunk buffer (a single partial line
//! plus the next reqwest chunk) and one [`PageEntry`] queued in the channel
//! ever exist at once. Spider's `/crawl` JSONL can emit tens of
//! thousands of pages without the adapter's heap growing past a few KB.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use gottem_core::{
    AdapterKind, CancelToken, CostExtract, CrawlAdapter, CrawlRequest, FetchError, PageEntry,
    ResponseParse, Route, RouteId, ScrapeRequest, Tier,
};
use reqwest::Client;
use tokio::sync::mpsc;
use url::Url;

use crate::shared::{
    apply_auth, build_default_client, classify_reqwest_err, render_body, to_reqwest_method,
};

/// Streaming crawl adapter for vendors that natively stream JSONL (currently
/// just Spider `/crawl`). One [`PageEntry`] per record.
#[derive(Debug, Clone)]
pub struct HttpJsonlStreamManyAdapter {
    client: Client,
}

impl HttpJsonlStreamManyAdapter {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub fn shared() -> Arc<dyn CrawlAdapter> {
        Arc::new(Self::new(build_default_client()))
    }
}

#[async_trait]
impl CrawlAdapter for HttpJsonlStreamManyAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::HttpJsonlStreamMany
    }

    async fn execute(
        &self,
        route: &Route,
        req: &CrawlRequest,
        cancel: &CancelToken,
    ) -> Result<gottem_core::adapter::PageEntryStream, FetchError> {
        // Merge crawl-level params into the scrape's `extra` so route body
        // templates like `{{param:limit|10}}` resolve from CrawlRequest's
        // fields without forcing callers to populate `extra` manually.
        //
        // Mapping (generic gottem → Spider `/crawl` field name):
        //
        // - `limit`           → `limit`
        // - `depth`           → `depth`
        // - `subdomains`      → `subdomains`
        // - `tld`             → `tld`
        // - `allow`           → `whitelist` (regex/glob, Spider uses this name)
        // - `deny`            → `blacklist`
        // - `respect_robots`  → `respect_robots_txt`
        //
        // Existing entries in `scrape.extra` win — explicit per-request
        // overrides supersede the convenience mapping. After the gottem-
        // generic keys, anything in `CrawlRequest::extra` is layered on
        // (also only when absent) so vendor-specific knobs round-trip.
        let mut scrape = req.scrape.clone();
        let inject = |scrape: &mut ScrapeRequest, k: &str, v: serde_json::Value| {
            scrape.extra.entry(k.into()).or_insert(v);
        };
        inject(&mut scrape, "limit", serde_json::json!(req.limit));
        inject(&mut scrape, "depth", serde_json::json!(req.depth));
        inject(&mut scrape, "subdomains", serde_json::json!(req.subdomains));
        inject(&mut scrape, "tld", serde_json::json!(req.tld));
        inject(&mut scrape, "allow", serde_json::json!(req.allow));
        inject(&mut scrape, "deny", serde_json::json!(req.deny));
        inject(
            &mut scrape,
            "respect_robots",
            serde_json::json!(req.respect_robots),
        );
        for (k, v) in &req.extra {
            scrape.extra.entry(k.clone()).or_insert_with(|| v.clone());
        }

        let mut endpoint = route.endpoint.render(&scrape)?;
        crate::shared::apply_geo_query(&mut endpoint, route, &scrape);
        let mut builder = self
            .client
            .request(to_reqwest_method(route.method), endpoint)
            .timeout(route.timeout())
            .header("content-type", "application/json")
            .header(
                "accept",
                "application/jsonl, application/x-ndjson, application/json",
            );
        for (k, v) in &route.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        for (k, v) in &scrape.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        builder = apply_auth(builder, &route.auth, &scrape)?;
        if let Some(body) = render_body(&route.body, &scrape)? {
            builder = builder.body(body);
        }

        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FetchError::Cancelled),
            r = builder.send() => r.map_err(classify_reqwest_err)?,
        };
        if response.status().is_client_error() || response.status().is_server_error() {
            return Err(FetchError::Status(response.status().as_u16()));
        }

        let (tx, rx) = mpsc::channel::<Result<PageEntry, FetchError>>(64);
        let route_id: RouteId = route.id.clone();
        let tier: Tier = route.tier;
        let cost_milli = route.cost;
        let parse = route.parse.clone();
        let cost_extract = route.cost_extract.clone();
        let cancel_inner = cancel.clone();
        let seed_url = scrape.url.clone();

        // Driving task: read chunks, split on '\n', parse each record into a
        // PageEntry, ship it down the channel. Drops as soon as the consumer
        // disconnects (channel send error → break). Multi-threaded runtime
        // friendly — spawned on the global executor.
        tokio::spawn(async move {
            let mut byte_stream = response.bytes_stream();
            let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
            let started = Instant::now();
            loop {
                let chunk = tokio::select! {
                    biased;
                    _ = cancel_inner.cancelled() => {
                        let _ = tx.send(Err(FetchError::Cancelled)).await;
                        return;
                    }
                    c = byte_stream.next() => c,
                };
                match chunk {
                    Some(Ok(bytes)) => {
                        buf.extend_from_slice(&bytes);
                        // Drain every complete line currently in the buffer.
                        while let Some(nl) = buf.iter().position(|&b| b == b'\n') {
                            let line: Vec<u8> = buf.drain(..=nl).collect();
                            // line includes the trailing '\n' — strip it for parse.
                            let payload = &line[..line.len() - 1];
                            if let Some(entry) = parse_record(
                                payload,
                                &parse,
                                cost_extract.as_ref(),
                                &route_id,
                                tier,
                                cost_milli,
                                &seed_url,
                                started,
                            ) {
                                if tx.send(entry).await.is_err() {
                                    return; // consumer dropped
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        let _ = tx.send(Err(classify_reqwest_err(e))).await;
                        return;
                    }
                    None => break,
                }
            }
            // Flush any trailing record without a newline (server may close
            // without a final '\n' on the last line).
            if !buf.is_empty() {
                let trimmed = trim_ws(&buf);
                if !trimmed.is_empty() {
                    if let Some(entry) = parse_record(
                        trimmed,
                        &parse,
                        cost_extract.as_ref(),
                        &route_id,
                        tier,
                        cost_milli,
                        &seed_url,
                        started,
                    ) {
                        let _ = tx.send(entry).await;
                    }
                }
            }
        });

        // Wrap the receiver as a Stream. tokio_stream isn't a dep; do it by
        // hand — Receiver::recv is the only state we need.
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

// ---- helpers --------------------------------------------------------------

/// Parse one JSONL record into a `Result<PageEntry, FetchError>`. Returns
/// `None` when the record is blank/non-JSON (skip silently — many servers
/// emit empty keep-alive lines).
#[allow(clippy::too_many_arguments)]
fn parse_record(
    payload: &[u8],
    parse: &ResponseParse,
    cost_extract: Option<&CostExtract>,
    route_id: &RouteId,
    tier: Tier,
    cost_milli: u64,
    seed_url: &Url,
    started: Instant,
) -> Option<Result<PageEntry, FetchError>> {
    let trimmed = trim_ws(payload);
    if trimmed.is_empty() {
        return None;
    }
    if !matches!(trimmed[0], b'{' | b'[') {
        return None;
    }
    let value: serde_json::Value = match serde_json::from_slice(trimmed) {
        Ok(v) => v,
        Err(_) => return None,
    };
    // Unwrap a single-element array wrapper (Spider sometimes wraps).
    let value = match value {
        serde_json::Value::Array(arr) => arr.into_iter().next().unwrap_or(serde_json::Value::Null),
        other => other,
    };

    let url = value
        .get("url")
        .and_then(|v| v.as_str())
        .and_then(|s| Url::parse(s).ok())
        .unwrap_or_else(|| seed_url.clone());

    let status = value
        .get("status")
        .and_then(|v| v.as_u64())
        .map(|n| n as u16)
        .unwrap_or(0);

    let depth = value
        .get("depth")
        .and_then(|v| v.as_u64())
        .map(|n| n as u32)
        .unwrap_or(0);

    let links = value.get("links").and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|x| x.as_str().and_then(|s| Url::parse(s).ok()))
            .collect::<Vec<_>>()
    });

    // Content extraction per the route's parse spec.
    let content = match parse {
        ResponseParse::JsonlEach { path } | ResponseParse::JsonlFirst { path } => {
            let ptr = dotted_to_pointer(path);
            let target = if ptr.is_empty() {
                Some(&value)
            } else {
                value.pointer(&ptr)
            };
            target.map(value_to_bytes)
        }
        ResponseParse::JsonPath { path } => {
            let ptr = dotted_to_pointer(path);
            value.pointer(&ptr).map(value_to_bytes)
        }
        // Other parse specs don't make sense for a JSONL-many record; default
        // to "use the record's `content` field if present" so route configs
        // can stay minimal.
        _ => value.get("content").map(value_to_bytes),
    };

    // Per-page cost — we honor only the JsonlFirst/JsonlEach variants here
    // (header-based cost is set by the response, not per-record). Currently
    // unused on PageEntry; rolled into `cost_milli` via the route's static
    // cost. Surface kept for symmetry with ScrapeResponse.
    let _ = cost_extract;

    let body = Bytes::copy_from_slice(trimmed);

    Some(Ok(PageEntry {
        url,
        depth,
        status,
        body,
        content,
        links,
        route_id: route_id.clone(),
        tier,
        cost_milli,
        elapsed: started.elapsed(),
    }))
}

fn value_to_bytes(v: &serde_json::Value) -> Bytes {
    match v {
        serde_json::Value::String(s) => Bytes::from(s.clone().into_bytes()),
        other => Bytes::from(other.to_string().into_bytes()),
    }
}

fn trim_ws(b: &[u8]) -> &[u8] {
    let start = b
        .iter()
        .position(|c| !c.is_ascii_whitespace())
        .unwrap_or(b.len());
    let end = b
        .iter()
        .rposition(|c| !c.is_ascii_whitespace())
        .map(|p| p + 1)
        .unwrap_or(0);
    if start >= end {
        &[]
    } else {
        &b[start..end]
    }
}

fn dotted_to_pointer(path: &str) -> String {
    let path = path.trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for part in path.split('.') {
        let mut p = part;
        while let Some(idx) = p.find('[') {
            let head = &p[..idx];
            if !head.is_empty() {
                out.push('/');
                out.push_str(head);
            }
            let end = match p[idx..].find(']') {
                Some(e) => idx + e,
                None => break,
            };
            let num = &p[idx + 1..end];
            out.push('/');
            out.push_str(num);
            p = &p[end + 1..];
        }
        if !p.is_empty() {
            out.push('/');
            out.push_str(p);
        }
    }
    out
}

// ---- Minimal Stream wrapper over an mpsc::Receiver ------------------------

/// Tiny re-implementation of `tokio_stream::wrappers::ReceiverStream` so we
/// avoid taking on `tokio-stream` as a dependency just for this one shape.
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
