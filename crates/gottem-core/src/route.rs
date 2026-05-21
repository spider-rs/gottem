use std::sync::Arc;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    capabilities::Capabilities,
    error::FetchError,
    request::{HttpMethod, ScrapeRequest},
    templating,
    tier::Tier,
    validator::Validator,
};

pub type RouteId = Arc<str>;

/// A vendor endpoint described as data. The orchestrator dispatches a request to a route
/// via the [`Adapter`](crate::adapter::Adapter) registered for the route's [`AdapterKind`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Route {
    pub id: RouteId,
    pub adapter: AdapterKind,
    /// Endpoint URL or template (`{{url}}`, `{{env:NAME}}`, `{{method}}` placeholders).
    /// See [`templating`](crate::templating) for the substitution rules.
    pub endpoint: EndpointTemplate,

    #[serde(default = "default_method")]
    pub method: HttpMethod,

    #[serde(default)]
    pub auth: AuthSpec,

    #[serde(default)]
    pub headers: Vec<(String, String)>,

    #[serde(default)]
    pub body: BodyTemplate,

    #[serde(default)]
    pub parse: ResponseParse,

    #[serde(default)]
    pub validate: Vec<Validator>,

    pub tier: Tier,

    /// Cost in milli-cents. 10 = $0.001, 100 = $0.01.
    #[serde(default)]
    pub cost: u64,

    /// Tiebreaker within a tier when cost is equal. Lower = preferred.
    /// Spider Cloud routes set this to 0; everything else defaults to 100.
    #[serde(default = "default_priority")]
    pub priority: u32,

    #[serde(default)]
    pub caps: Capabilities,

    /// Request timeout in milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    #[serde(default = "default_concurrency")]
    pub concurrency: u32,

    #[serde(default)]
    pub retry_on: RetryClassifier,
}

fn default_method() -> HttpMethod {
    HttpMethod::Get
}
fn default_timeout_ms() -> u64 {
    30_000
}
fn default_concurrency() -> u32 {
    16
}
fn default_priority() -> u32 {
    100
}

impl Route {
    pub fn timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.timeout_ms)
    }
}

/// Endpoint specification — either a literal URL (parsed and cached at load time) or
/// a template string with `{{url}}` / `{{env:NAME}}` placeholders rendered per request.
///
/// Serializes/deserializes as a plain string. The TOML format is unchanged whether or
/// not a vendor needs templating.
#[derive(Debug, Clone)]
pub struct EndpointTemplate {
    template: String,
    /// `Some(parsed)` when `template` has no placeholders. Fast path: clone the parsed Url
    /// instead of re-parsing on every fetch.
    cached: Option<Url>,
}

impl EndpointTemplate {
    /// Parse a raw template string. A literal URL is validated up-front; a template is
    /// stored as-is and validated when [`render`](Self::render) is called.
    pub fn parse(s: &str) -> Result<Self, FetchError> {
        if templating::has_placeholder(s) {
            Ok(Self {
                template: s.to_string(),
                cached: None,
            })
        } else {
            let url =
                Url::parse(s).map_err(|e| FetchError::Config(format!("endpoint URL: {e}")))?;
            Ok(Self {
                template: s.to_string(),
                cached: Some(url),
            })
        }
    }

    /// Render the template against a request, returning a fully-resolved [`Url`].
    /// Returns [`FetchError::Auth`] if `{{env:NAME}}` references a missing env var.
    pub fn render(&self, req: &ScrapeRequest) -> Result<Url, FetchError> {
        if let Some(u) = &self.cached {
            return Ok(u.clone());
        }
        let rendered = templating::render_endpoint(&self.template, req)?;
        Url::parse(&rendered)
            .map_err(|e| FetchError::Config(format!("rendered endpoint not a valid URL: {e}")))
    }

    /// Raw template string (with placeholders unresolved). Useful for logging.
    pub fn as_str(&self) -> &str {
        &self.template
    }

    /// `true` if this endpoint requires per-request substitution.
    pub fn is_template(&self) -> bool {
        self.cached.is_none()
    }
}

impl std::fmt::Display for EndpointTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.template)
    }
}

impl Serialize for EndpointTemplate {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.template)
    }
}

impl<'de> Deserialize<'de> for EndpointTemplate {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// The protocol family used to talk to an upstream. Small and finite by design — most
/// vendors fit into one of these. New protocols (gRPC, WebSocket, etc.) need a new variant.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AdapterKind {
    /// Plain reqwest GET/POST against the endpoint. No JSON wrapping.
    DirectHttp,
    /// POST JSON body, parse JSON response. Most cloud APIs (Firecrawl, ScrapingBee, Zyte, Brightdata Unblocker).
    HttpJson,
    /// POST JSON body, parse chunked JSONL stream. Spider Cloud requires this — `.json()` will hang.
    HttpJsonlStream,
    /// Connect to a Chrome WebSocket endpoint (local or Browserless/Brightdata Scraping Browser).
    ChromeCdp,
    /// Dispatch through `spider::Website` for full local power (stealth, fingerprint, intercept, anti-bot).
    SpiderLocal,
    /// Escape hatch for user-registered adapters.
    Custom(Arc<str>),
}

impl AdapterKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::DirectHttp => "direct_http",
            Self::HttpJson => "http_json",
            Self::HttpJsonlStream => "http_jsonl_stream",
            Self::ChromeCdp => "chrome_cdp",
            Self::SpiderLocal => "spider_local",
            Self::Custom(s) => s,
        }
    }
}

impl Serialize for AdapterKind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AdapterKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(match s.as_str() {
            "direct_http" => Self::DirectHttp,
            "http_json" => Self::HttpJson,
            "http_jsonl_stream" => Self::HttpJsonlStream,
            "chrome_cdp" => Self::ChromeCdp,
            "spider_local" => Self::SpiderLocal,
            _ => Self::Custom(Arc::from(s)),
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuthSpec {
    #[default]
    None,
    /// `Authorization: Bearer <env value>`
    Bearer { env: String },
    /// Custom header with optional prefix, e.g. `X-API-Key: sk-...`
    ApiKey {
        header: String,
        #[serde(default)]
        prefix: Option<String>,
        env: String,
    },
    /// HTTP basic auth. Pass `pass_env = None` for token-as-user style (Zyte).
    Basic {
        user_env: String,
        #[serde(default)]
        pass_env: Option<String>,
    },
    /// Embed credentials in a websocket URL's userinfo. Used by Brightdata Scraping Browser.
    WsUserinfo { env: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BodyTemplate {
    #[default]
    Empty,
    /// Raw template string with `{{url}}` placeholder. Substituted at fetch time.
    Json { template: String },
    /// Form-encoded fields. Each value supports `{{url}}` substitution.
    Form { fields: Vec<(String, String)> },
    /// Raw base64 bytes — pre-encoded payload.
    Raw { bytes_base64: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResponseParse {
    /// Use the raw response body as the content (utf-8 lossy decode).
    #[default]
    RawText,
    /// Don't decode — body bytes only.
    RawBytes,
    /// JSON Pointer-ish dotted path like `$.data.markdown` or `$.results[0].html`.
    JsonPath { path: String },
    /// First non-empty JSONL line, then dotted path into that record. Spider Cloud format.
    JsonlFirst { path: String },
    /// Body is already markdown — pass through.
    Markdown,
    /// Body is HTML — pass through.
    Html,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryClassifier {
    pub retry_on_status: Vec<u16>,
    pub retry_on_empty: bool,
    pub retry_on_waf: bool,
}

impl Default for RetryClassifier {
    fn default() -> Self {
        Self {
            retry_on_status: vec![408, 425, 429, 500, 502, 503, 504],
            retry_on_empty: true,
            retry_on_waf: true,
        }
    }
}
