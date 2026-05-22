//! gottem adapter for Browser Use Cloud — AI browser agent exposed as an Adapter.
//!
//! Browser Use Cloud's API is async-only:
//!
//! 1. `POST /api/v1/run-task` — submit a natural-language task, receive `{"id": "..."}`.
//! 2. `GET  /api/v1/task/{id}` — poll until `status == "finished"`, then read `output`.
//!
//! This adapter handles the full state machine inside one [`Adapter::execute`] call,
//! mirroring the 2Captcha adapter's submit-then-poll pattern but with longer timing
//! defaults (AI agent runs take 1–5 minutes).
//!
//! ## Body templating
//!
//! The submission body comes from the route's [`BodyTemplate`](gottem_core::BodyTemplate),
//! rendered via [`gottem_core::templating::render_body`]. That means `{{url}}`,
//! `{{method}}`, and `{{env:NAME}}` placeholders all work the way they do for HttpJson.
//! Typical Browser Use templates look like:
//!
//! ```toml
//! [route.body]
//! kind     = "json"
//! template = '''{"task":"Visit {{url}} and return the main content as markdown.","use_proxy":true}'''
//! ```
//!
//! ## Cancellation + no-deadlock
//!
//! - Every `.await` (submit, sleep, each poll, body read) is wrapped in `tokio::select!`
//!   against [`CancelToken`] so a winning race or Ctrl-C aborts mid-flight.
//! - The poll loop is bounded by `max_polls × poll_interval` — never infinite.
//! - Missing env / missing body / upstream failures map to typed [`FetchError`]s.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use gottem_core::{
    templating, Adapter, AdapterContext, AdapterKind, AuthSpec, BodyTemplate, CancelToken,
    FetchError, Route, ScrapeRequest, ScrapeResponse,
};
use reqwest::Client;
use serde_json::Value;

const DEFAULT_BASE_URL: &str = "https://api.browser-use.com";

/// Stable identifier used in the catalog TOML's `adapter` field.
pub const ADAPTER_KIND_NAME: &str = "browser_use";

#[derive(Debug, Clone)]
pub struct BrowserUseAdapter {
    client: Client,
    base_url: String,
    poll_interval: Duration,
    initial_delay: Duration,
    max_polls: u32,
}

impl Default for BrowserUseAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl BrowserUseAdapter {
    pub fn new() -> Self {
        Self::with_client(
            Client::builder()
                .pool_idle_timeout(Duration::from_secs(60))
                .timeout(Duration::from_secs(60))
                .gzip(true)
                .brotli(true)
                .user_agent(concat!("gottem-browseruse/", env!("CARGO_PKG_VERSION")))
                .build()
                .expect("default reqwest client"),
        )
    }

    pub fn with_client(client: Client) -> Self {
        Self {
            client,
            base_url: DEFAULT_BASE_URL.into(),
            // AI tasks take 1–5 min typically. Give the agent a head start before polling.
            initial_delay: Duration::from_secs(8),
            poll_interval: Duration::from_secs(6),
            // 8s + 120 × 6s = ~12 min ceiling.
            max_polls: 120,
        }
    }

    /// Override the Browser Use API base URL — typically used by tests with wiremock.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    pub fn with_initial_delay(mut self, d: Duration) -> Self {
        self.initial_delay = d;
        self
    }

    pub fn with_poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    pub fn with_max_polls(mut self, n: u32) -> Self {
        self.max_polls = n;
        self
    }

    pub fn arc() -> Arc<dyn Adapter> {
        Arc::new(Self::new())
    }

    /// Build an [`Arc<dyn Adapter>`] sharing an externally-owned `reqwest::Client`.
    /// Use this when registering alongside other HTTP-based adapters so a single
    /// connection pool serves the whole stack (http + captcha + browseruse).
    pub fn arc_with_client(client: Client) -> Arc<dyn Adapter> {
        Arc::new(Self::with_client(client))
    }
}

#[async_trait]
impl Adapter for BrowserUseAdapter {
    fn kind(&self) -> AdapterKind {
        AdapterKind::Custom(Arc::from(ADAPTER_KIND_NAME))
    }

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError> {
        let api_key = resolve_api_key(&route.auth, req)?;
        let body = render_route_body(&route.body, req)?;

        let task_id = submit_task(&self.client, &self.base_url, &api_key, &body, cancel).await?;

        // Wait a beat before the first poll — the agent always needs at least a few seconds
        // to spin up its browser. Polling sooner just burns requests on "running" status.
        wait_with_cancel(self.initial_delay, cancel).await?;

        let output = poll_until_done(
            &self.client,
            &self.base_url,
            &api_key,
            &task_id,
            self.poll_interval,
            self.max_polls,
            cancel,
        )
        .await?;

        // Move the agent's output into a `Bytes` and share body+content — refcount bump
        // on clone, no second allocation for the (sometimes large) markdown payload.
        let body = Bytes::from(output.into_bytes());
        Ok(ScrapeResponse {
            url: req.url.clone(),
            status: 200,
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

// ============================================================================
// internals
// ============================================================================

/// Per-request resolver: BYOK keys live on `req.credentials` and shadow the
/// process env. See [`ScrapeRequest::resolve_env`].
fn resolve_api_key(auth: &AuthSpec, req: &ScrapeRequest) -> Result<String, FetchError> {
    match auth {
        AuthSpec::Bearer { env } | AuthSpec::ApiKey { env, .. } => req
            .resolve_env(env)
            .ok_or_else(|| FetchError::Auth(format!("missing env var: {env}"))),
        other => Err(FetchError::Config(format!(
            "browser_use adapter expects AuthSpec::Bearer or ApiKey, got {other:?}"
        ))),
    }
}

fn render_route_body(body: &BodyTemplate, req: &ScrapeRequest) -> Result<Bytes, FetchError> {
    match body {
        BodyTemplate::Empty => Err(FetchError::Config(
            "browser_use route must define a JSON body with a 'task' field".into(),
        )),
        BodyTemplate::Json { template } => Ok(Bytes::from(
            templating::render_body(template, req)?.into_bytes(),
        )),
        other => Err(FetchError::Config(format!(
            "browser_use route body must be 'json', got {other:?}"
        ))),
    }
}

async fn submit_task(
    client: &Client,
    base_url: &str,
    api_key: &str,
    body: &Bytes,
    cancel: &CancelToken,
) -> Result<String, FetchError> {
    let url = format!("{base_url}/api/v1/run-task");
    let send_fut = client
        .post(&url)
        .bearer_auth(api_key)
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .body(body.clone())
        .send();
    let resp = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(FetchError::Cancelled),
        r = send_fut => r.map_err(|e| FetchError::Network(format!("browser_use submit: {e}")))?,
    };
    let status = resp.status().as_u16();
    let body_text_fut = resp.text();
    let body_text = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(FetchError::Cancelled),
        r = body_text_fut => r
            .map_err(|e| FetchError::Network(format!("browser_use submit body: {e}")))?,
    };
    if status >= 400 {
        return Err(map_submit_error(status, &body_text));
    }
    let v: Value = serde_json::from_str(&body_text).map_err(|e| {
        FetchError::Parse(format!("browser_use submit JSON: {e} (body: {body_text})"))
    })?;
    v.get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            FetchError::Parse(format!(
                "browser_use submit: response has no 'id' field (body: {body_text})"
            ))
        })
}

async fn poll_until_done(
    client: &Client,
    base_url: &str,
    api_key: &str,
    task_id: &str,
    poll_interval: Duration,
    max_polls: u32,
    cancel: &CancelToken,
) -> Result<String, FetchError> {
    let url = format!("{base_url}/api/v1/task/{task_id}");
    for _attempt in 0..max_polls {
        let send_fut = client
            .get(&url)
            .bearer_auth(api_key)
            .header("accept", "application/json")
            .send();
        let resp = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FetchError::Cancelled),
            r = send_fut => r.map_err(|e| FetchError::Network(format!("browser_use poll: {e}")))?,
        };
        let status = resp.status().as_u16();
        let body_text_fut = resp.text();
        let body_text = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FetchError::Cancelled),
            r = body_text_fut => r
                .map_err(|e| FetchError::Network(format!("browser_use poll body: {e}")))?,
        };
        if status >= 400 {
            return Err(FetchError::Status(status));
        }
        let v: Value = serde_json::from_str(&body_text).map_err(|e| {
            FetchError::Parse(format!("browser_use poll JSON: {e} (body: {body_text})"))
        })?;
        let task_status = v.get("status").and_then(Value::as_str).unwrap_or("");
        match task_status {
            "finished" | "completed" => {
                let output = v.get("output").and_then(Value::as_str).ok_or_else(|| {
                    FetchError::Parse(format!(
                        "browser_use: finished task has no 'output' field (body: {body_text})"
                    ))
                })?;
                return Ok(output.to_string());
            }
            "failed" | "stopped" => {
                let err_detail = v
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or(task_status);
                return Err(FetchError::Network(format!(
                    "browser_use task {task_status}: {err_detail}"
                )));
            }
            _ => {
                // "created", "running", "paused", anything else → keep polling.
                wait_with_cancel(poll_interval, cancel).await?;
            }
        }
    }
    Err(FetchError::Timeout(poll_interval.saturating_mul(max_polls)))
}

async fn wait_with_cancel(d: Duration, cancel: &CancelToken) -> Result<(), FetchError> {
    if d.is_zero() {
        return Ok(());
    }
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(FetchError::Cancelled),
        _ = tokio::time::sleep(d) => Ok(()),
    }
}

fn map_submit_error(status: u16, body: &str) -> FetchError {
    match status {
        401 | 403 => FetchError::Auth(format!("browser_use submit: HTTP {status} {body}")),
        429 | 500..=599 => FetchError::Status(status),
        _ => FetchError::Network(format!("browser_use submit failed: HTTP {status} — {body}")),
    }
}
