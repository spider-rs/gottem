//! gottem CAPTCHA solver adapter — 2Captcha service exposed as an Adapter.
//!
//! Solving a CAPTCHA is a two-step protocol (submit → poll), so it can't fit the
//! single-request adapters in `gottem-adapters-http`. This crate handles the full
//! state machine inside one [`Adapter::execute`] call:
//!
//! 1. `POST https://2captcha.com/in.php` — submit the challenge, receive a task id.
//! 2. `GET  https://2captcha.com/res.php?action=get&id=<task>` — poll every
//!    `poll_interval` until the solver returns `OK|<token>` or fails.
//! 3. Return the token as `content` on the `ScrapeResponse`. The caller then embeds
//!    it in a follow-up request (cookie / form field / header — vendor-specific).
//!
//! ## Composing into a "chain"
//!
//! gottem doesn't have a built-in multi-step route type — chains live in your
//! [`RetryStrategy`] or in straight-line glue code. The pattern is:
//!
//! ```ignore
//! // 1. Try the cheap route.
//! let first = orch.execute_once(&route, &req, 0, &cancel).await;
//!
//! // 2. If it returned a CAPTCHA challenge, solve it.
//! let token = orch.execute_once(&captcha_route, &solver_req, 0, &cancel).await?
//!     .content.unwrap();
//!
//! // 3. Replay the original request with the token embedded.
//! let mut req2 = req.clone();
//! req2.headers.push(("X-Recaptcha-Token".into(), token));
//! orch.execute_once(&route, &req2, 1, &cancel).await
//! ```
//!
//! ## CAPTCHA types
//!
//! The adapter reads `req.extra` to know what to submit:
//!
//! | extra field   | meaning                                                              |
//! |---------------|----------------------------------------------------------------------|
//! | `captchaType` | one of `recaptcha_v2`, `hcaptcha`, `turnstile`                       |
//! | `siteKey`     | the captcha site key extracted from the protected page               |
//! | `pageUrl`     | the URL of the protected page (defaults to `req.url` if not set)     |
//! | `action`      | turnstile/recaptcha_v3 action (optional)                             |
//! | `invisible`   | `"1"` for invisible reCAPTCHA v2 (optional)                          |
//!
//! ## No-deadlock / no-panic
//!
//! - The poll loop is bounded by `max_polls × poll_interval`; never infinite.
//! - Every `.await` is wrapped in `tokio::select!` against the orchestrator's
//!   [`CancelToken`] so a winning race or Ctrl-C aborts mid-poll.
//! - Missing env / missing extras / upstream errors map to typed [`FetchError`]s,
//!   never panics.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use gottem_core::{
    Adapter, AdapterContext, AdapterKind, AuthSpec, CancelToken, FetchError, Route, ScrapeRequest,
    ScrapeResponse,
};
use reqwest::Client;
use serde_json::Value;

const SUBMIT_URL: &str = "https://2captcha.com/in.php";
const RESULT_URL: &str = "https://2captcha.com/res.php";

/// Stable name used in the catalog TOML's `adapter` field.
pub const ADAPTER_KIND_NAME: &str = "captcha_2captcha";

#[derive(Debug, Clone)]
pub struct Captcha2CaptchaAdapter {
    client: Client,
    submit_url: String,
    result_url: String,
    poll_interval: Duration,
    max_polls: u32,
    initial_delay: Duration,
}

impl Default for Captcha2CaptchaAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Captcha2CaptchaAdapter {
    pub fn new() -> Self {
        Self::with_client(
            Client::builder()
                .pool_idle_timeout(Duration::from_secs(60))
                .timeout(Duration::from_secs(30))
                .gzip(true)
                .brotli(true)
                .user_agent(concat!("gottem-captcha/", env!("CARGO_PKG_VERSION")))
                .build()
                .expect("default reqwest client"),
        )
    }

    pub fn with_client(client: Client) -> Self {
        Self {
            client,
            submit_url: SUBMIT_URL.into(),
            result_url: RESULT_URL.into(),
            poll_interval: Duration::from_secs(5),
            max_polls: 24,
            initial_delay: Duration::from_secs(10),
        }
    }

    /// Override the 2Captcha base URLs (primarily for tests with a local wiremock server).
    pub fn with_endpoints(mut self, submit: impl Into<String>, result: impl Into<String>) -> Self {
        self.submit_url = submit.into();
        self.result_url = result.into();
        self
    }

    pub fn with_poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    pub fn with_initial_delay(mut self, d: Duration) -> Self {
        self.initial_delay = d;
        self
    }

    pub fn with_max_polls(mut self, n: u32) -> Self {
        self.max_polls = n;
        self
    }

    pub fn arc() -> Arc<dyn Adapter> {
        Arc::new(Self::new())
    }
}

#[async_trait]
impl Adapter for Captcha2CaptchaAdapter {
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
        let api_key = resolve_api_key(&route.auth)?;
        let challenge = Challenge::from_request(req)?;

        let task_id =
            submit_task(&self.client, &self.submit_url, &api_key, &challenge, cancel).await?;

        // Most CAPTCHAs need 10-30s; do an initial delay before the first poll to avoid
        // hammering res.php with NOT_READY responses.
        wait_with_cancel(self.initial_delay, cancel).await?;

        let token = poll_solution(
            &self.client,
            &self.result_url,
            &api_key,
            &task_id,
            self.poll_interval,
            self.max_polls,
            cancel,
        )
        .await?;

        let body = Bytes::copy_from_slice(token.as_bytes());
        Ok(ScrapeResponse {
            url: req.url.clone(),
            status: 200,
            headers: vec![],
            body,
            content: Some(token),
            route_id: route.id.clone(),
            tier: route.tier,
            cost_milli: route.cost,
            elapsed: ctx.elapsed(),
            attempt: ctx.attempt,
            metadata: Default::default(),
        })
    }
}

// ============================================================================
// internals
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
enum CaptchaType {
    RecaptchaV2,
    Hcaptcha,
    Turnstile,
}

impl CaptchaType {
    fn parse(s: &str) -> Result<Self, FetchError> {
        match s {
            "recaptcha_v2" => Ok(Self::RecaptchaV2),
            "hcaptcha" => Ok(Self::Hcaptcha),
            "turnstile" => Ok(Self::Turnstile),
            other => Err(FetchError::Config(format!(
                "captcha: unknown captchaType {other:?} (expected recaptcha_v2 / hcaptcha / turnstile)"
            ))),
        }
    }

    fn method_str(&self) -> &'static str {
        match self {
            Self::RecaptchaV2 => "userrecaptcha",
            Self::Hcaptcha => "hcaptcha",
            Self::Turnstile => "turnstile",
        }
    }

    fn site_key_param(&self) -> &'static str {
        match self {
            Self::RecaptchaV2 => "googlekey",
            Self::Hcaptcha | Self::Turnstile => "sitekey",
        }
    }
}

#[derive(Debug)]
struct Challenge {
    kind: CaptchaType,
    site_key: String,
    page_url: String,
    action: Option<String>,
    invisible: bool,
}

impl Challenge {
    fn from_request(req: &ScrapeRequest) -> Result<Self, FetchError> {
        let captcha_type = req
            .extra
            .get("captchaType")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                FetchError::Config("captcha: req.extra['captchaType'] is required".into())
            })?;
        let kind = CaptchaType::parse(captcha_type)?;

        let site_key = req
            .extra
            .get("siteKey")
            .and_then(Value::as_str)
            .ok_or_else(|| FetchError::Config("captcha: req.extra['siteKey'] is required".into()))?
            .to_string();

        let page_url = req
            .extra
            .get("pageUrl")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| req.url.as_str().to_string());

        let action = req
            .extra
            .get("action")
            .and_then(Value::as_str)
            .map(str::to_string);

        let invisible = matches!(
            req.extra.get("invisible").and_then(Value::as_str),
            Some("1" | "true")
        );

        Ok(Self {
            kind,
            site_key,
            page_url,
            action,
            invisible,
        })
    }
}

fn resolve_api_key(auth: &AuthSpec) -> Result<String, FetchError> {
    match auth {
        AuthSpec::Bearer { env } | AuthSpec::ApiKey { env, .. } => {
            std::env::var(env).map_err(|_| FetchError::Auth(format!("missing env var: {env}")))
        }
        other => Err(FetchError::Config(format!(
            "captcha solver expects AuthSpec::Bearer or ApiKey, got {other:?}"
        ))),
    }
}

async fn submit_task(
    client: &Client,
    submit_url: &str,
    api_key: &str,
    challenge: &Challenge,
    cancel: &CancelToken,
) -> Result<String, FetchError> {
    let mut params: Vec<(&str, &str)> = vec![
        ("key", api_key),
        ("method", challenge.kind.method_str()),
        (challenge.kind.site_key_param(), &challenge.site_key),
        ("pageurl", &challenge.page_url),
        ("json", "1"),
    ];
    if challenge.invisible {
        params.push(("invisible", "1"));
    }
    if let Some(action) = &challenge.action {
        params.push(("action", action));
    }

    let send_fut = client.post(submit_url).form(&params).send();
    let resp = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(FetchError::Cancelled),
        r = send_fut => r.map_err(|e| FetchError::Network(format!("2captcha submit: {e}")))?,
    };

    let status = resp.status().as_u16();
    let body_fut = resp.text();
    let body = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(FetchError::Cancelled),
        r = body_fut => r.map_err(|e| FetchError::Network(format!("2captcha submit body: {e}")))?,
    };

    if status >= 400 {
        return Err(FetchError::Status(status));
    }

    let v: Value = serde_json::from_str(&body)
        .map_err(|e| FetchError::Parse(format!("2captcha submit JSON: {e} (body: {body})")))?;
    let status_field = v.get("status").and_then(Value::as_i64).unwrap_or(0);
    let request_field = v.get("request").and_then(Value::as_str).unwrap_or("");
    if status_field != 1 {
        return Err(map_2captcha_error(request_field));
    }
    Ok(request_field.to_string())
}

async fn poll_solution(
    client: &Client,
    result_url: &str,
    api_key: &str,
    task_id: &str,
    poll_interval: Duration,
    max_polls: u32,
    cancel: &CancelToken,
) -> Result<String, FetchError> {
    for _attempt in 0..max_polls {
        let params = [
            ("key", api_key),
            ("action", "get"),
            ("id", task_id),
            ("json", "1"),
        ];

        let send_fut = client.get(result_url).query(&params).send();
        let resp = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FetchError::Cancelled),
            r = send_fut => r.map_err(|e| FetchError::Network(format!("2captcha poll: {e}")))?,
        };
        let status = resp.status().as_u16();
        let body_fut = resp.text();
        let body = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FetchError::Cancelled),
            r = body_fut => r.map_err(|e| FetchError::Network(format!("2captcha poll body: {e}")))?,
        };
        if status >= 400 {
            return Err(FetchError::Status(status));
        }
        let v: Value = serde_json::from_str(&body)
            .map_err(|e| FetchError::Parse(format!("2captcha poll JSON: {e} (body: {body})")))?;
        let status_field = v.get("status").and_then(Value::as_i64).unwrap_or(0);
        let request_field = v.get("request").and_then(Value::as_str).unwrap_or("");
        if status_field == 1 {
            return Ok(request_field.to_string());
        }
        if request_field != "CAPCHA_NOT_READY" {
            return Err(map_2captcha_error(request_field));
        }
        wait_with_cancel(poll_interval, cancel).await?;
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

fn map_2captcha_error(code: &str) -> FetchError {
    match code {
        "" => FetchError::Network("2captcha returned empty error code".into()),
        "ERROR_WRONG_USER_KEY" | "ERROR_KEY_DOES_NOT_EXIST" => {
            FetchError::Auth(format!("2captcha: {code}"))
        }
        "ERROR_NO_SLOT_AVAILABLE" | "ERROR_ZERO_BALANCE" => {
            FetchError::Network(format!("2captcha: {code}"))
        }
        "ERROR_CAPTCHA_UNSOLVABLE" | "ERROR_WRONG_CAPTCHA_ID" => {
            FetchError::Parse(format!("2captcha: {code}"))
        }
        other => FetchError::Network(format!("2captcha error: {other}")),
    }
}
