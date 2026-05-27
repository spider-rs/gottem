use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::capabilities::Capabilities;

/// Output format the caller wants for a scraped page. Multi-format requests
/// supply a set; gottem-cloud's transform pipeline produces one byte payload
/// per requested format. Each variant maps 1:1 onto a
/// `spider_transformations::transformation::content::ReturnFormat` — the
/// same enum spider_service uses — so behavior matches the upstream
/// transform pipeline exactly.
///
/// Note: link extraction is **not** a format here, mirroring spider_service.
/// Use [`ScrapeRequest::return_links`] for that — it's a separate flag and
/// produces a separate `links` field on the response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    /// GFM-flavoured markdown — `spider_transformations::ReturnFormat::Markdown`.
    Markdown,
    /// HTML source passthrough — `spider_transformations::ReturnFormat::Raw`.
    Html,
    /// Plain text from HTML — `spider_transformations::ReturnFormat::Html2Text`.
    Text,
    /// Base64-encoded PNG rendered from the HTML by a headless browser —
    /// `spider_transformations::ReturnFormat::Screenshot`. Source must be
    /// HTML; text-already payloads have nothing to render. Decode client-
    /// side with `atob` or `Buffer.from(b64, "base64")`.
    Screenshot,
}

#[derive(Debug, Clone)]
pub struct ScrapeRequest {
    pub url: Url,
    pub method: HttpMethod,
    pub headers: Vec<(String, String)>,
    pub body: Option<Bytes>,
    pub timeout: Option<Duration>,
    pub render_js: bool,
    pub render_wait_ms: Option<u32>,
    pub geo: Option<String>,
    /// Caps the orchestrator must guarantee. Routes whose own caps don't satisfy
    /// this set are skipped during ladder/race selection.
    pub required_caps: Capabilities,
    /// Free-form per-request hints passed through to adapters (e.g. chrome args).
    pub extra: HashMap<String, serde_json::Value>,
    /// Per-request credential overrides keyed by env-var name (e.g.
    /// `"SPIDER_API_KEY" → "sk-..."`). When an adapter resolves an
    /// [`crate::AuthSpec`] env var or a `{{env:NAME}}` template, it consults
    /// this map first and only falls back to [`std::env::var`] if the name
    /// isn't present. This is how BYOK injects a user-supplied vendor key
    /// without mutating the process environment (which would leak across
    /// concurrent requests). Empty by default; populated by callers that
    /// thread per-request credentials in (e.g. gottem-cloud's BYOK path).
    pub credentials: HashMap<String, String>,
    /// Output formats the caller wants. Empty = "whatever the route returns
    /// natively" (legacy single-format behavior). Non-empty = gottem-cloud's
    /// transform pipeline produces one payload per format from the vendor
    /// response. See [`Format`].
    pub formats: HashSet<Format>,
    /// Populate the response's `links` field with absolute URLs scraped
    /// from the page's `<a href>` anchors. Mirrors spider_service's
    /// `return_page_links` flag — links live alongside the content
    /// payloads, not inside them.
    pub return_links: bool,
}

impl ScrapeRequest {
    pub fn get(url: Url) -> Self {
        Self {
            url,
            method: HttpMethod::Get,
            headers: Vec::new(),
            body: None,
            timeout: None,
            render_js: false,
            render_wait_ms: None,
            geo: None,
            required_caps: Capabilities::default(),
            extra: HashMap::new(),
            credentials: HashMap::new(),
            formats: HashSet::new(),
            return_links: false,
        }
    }

    /// Builder-style: attach a set of requested output formats. The orchestrator
    /// passes this through unchanged; gottem-cloud's transform pipeline
    /// consumes it after the orchestrator returns.
    pub fn with_formats(mut self, formats: HashSet<Format>) -> Self {
        self.formats = formats;
        self
    }

    /// Builder-style: enable link extraction on the response. Spider_service-
    /// style: links sit beside the content payloads, not inside the format map.
    pub fn with_return_links(mut self, on: bool) -> Self {
        self.return_links = on;
        self
    }

    /// Look up an env-var name, preferring this request's [`credentials`]
    /// override over the process environment. Used by every adapter's auth
    /// resolver so BYOK keys scope to a single request.
    ///
    /// [`credentials`]: ScrapeRequest::credentials
    pub fn resolve_env(&self, name: &str) -> Option<String> {
        self.credentials
            .get(name)
            .cloned()
            .or_else(|| std::env::var(name).ok())
    }

    /// Builder-style: attach a credential override map. Consumes and returns
    /// `self` so it composes with the other `with_*` builders.
    pub fn with_credentials(mut self, creds: HashMap<String, String>) -> Self {
        self.credentials = creds;
        self
    }

    pub fn with_required_caps(mut self, caps: Capabilities) -> Self {
        self.required_caps = caps;
        self
    }

    pub fn with_render_js(mut self, render: bool) -> Self {
        self.render_js = render;
        if render {
            self.required_caps.js = true;
        }
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
}

impl HttpMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Patch => "PATCH",
            Self::Head => "HEAD",
        }
    }
}
