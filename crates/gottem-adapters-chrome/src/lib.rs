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
//! ## chromiumoxide via spider
//!
//! We use `spider::chromiumoxide` (spider re-exports it when the `chrome` feature is on).
//! This guarantees one chromiumoxide build across the workspace — no duplicate chromium
//! protocol crates, no compile-time blowup on top of what spider already brings.
//!
//! ## Cancellation + no-deadlock
//!
//! Three async checkpoints, each guarded by `tokio::select!` against [`CancelToken`]:
//! 1. **Connect** — bounded by a 15-second connect timeout AND the outer cancel token.
//! 2. **Navigate + content** — bounded by `route.timeout()` AND cancel.
//! 3. **Handler task** — required to drive chromiumoxide event processing; aborted via
//!    `JoinHandle::abort()` after the fetch resolves (cancelled or otherwise).
//!
//! The browser handle is dropped at function exit, which closes the WebSocket connection
//! and (for Brightdata-style per-session billing) ends the remote browser session.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
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

        // ---- Connect (cancel-aware + connect_timeout-bounded) -------------
        let connect_fut = tokio::time::timeout(
            self.connect_timeout,
            spider::chromiumoxide::Browser::connect(&ws_url),
        );
        let (browser, mut handler) = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(FetchError::Cancelled),
            r = connect_fut => match r {
                Ok(Ok(b)) => b,
                Ok(Err(e)) => return Err(FetchError::Network(format!("chrome connect: {e}"))),
                Err(_) => return Err(FetchError::Timeout(self.connect_timeout)),
            },
        };

        // ---- Drive CDP events on a background task ------------------------
        // chromiumoxide requires the handler to be polled or *all* operations hang.
        // Aborting at the end closes the connection cleanly.
        let handler_task = tokio::spawn(async move { while handler.next().await.is_some() {} });

        // ---- Navigate + content (cancel + per-route timeout) --------------
        let work = navigate_and_extract(&browser, req.url.as_str());
        let work_outcome = tokio::time::timeout(route.timeout(), async {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(FetchError::Cancelled),
                r = work => r,
            }
        })
        .await;

        // ---- Cleanup: always abort handler; browser drops at scope exit ---
        handler_task.abort();
        // best-effort browser close — ignore errors, drop will close anyway
        drop(browser);

        let (status, html) = match work_outcome {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(FetchError::Timeout(route.timeout())),
        };

        if status >= 400 {
            return Err(FetchError::Status(status));
        }

        let body = Bytes::copy_from_slice(html.as_bytes());
        Ok(ScrapeResponse {
            url: req.url.clone(),
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

/// Open a fresh tab, navigate to `url`, wait for `load`, return `(status, html)`.
async fn navigate_and_extract(
    browser: &spider::chromiumoxide::Browser,
    url: &str,
) -> Result<(u16, String), FetchError> {
    let page = browser
        .new_page(url)
        .await
        .map_err(|e| FetchError::Network(format!("new_page: {e}")))?;

    // Wait for the main frame to finish navigating. We don't fail on wait_for_navigation
    // errors because some single-page apps never emit a final load — we still want to
    // capture whatever DOM is present.
    let _ = page.wait_for_navigation().await;

    let html = page
        .content()
        .await
        .map_err(|e| FetchError::Parse(format!("content: {e}")))?;

    // chromiumoxide doesn't always surface the main-frame HTTP status code in a
    // first-class way; we default to 200 on successful HTML extraction.
    Ok((200, html))
}

/// Resolve the route's CDP endpoint into a final WebSocket URL string.
///
/// Steps:
/// 1. Render endpoint template (`{{url}}`, `{{env:NAME}}` substitution).
/// 2. If `AuthSpec::WsUserinfo` is set, inject the env-var-derived `user:pass` into the URL.
pub(crate) fn build_ws_url(route: &Route, req: &ScrapeRequest) -> Result<String, FetchError> {
    let mut url = route.endpoint.render(req)?;

    if let AuthSpec::WsUserinfo { env } = &route.auth {
        let userinfo =
            std::env::var(env).map_err(|_| FetchError::Auth(format!("missing env var: {env}")))?;
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
}
