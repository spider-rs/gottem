//! Shared helpers for HTTP adapters: client builder, auth, body templating, response parsing.

use std::time::Duration;

use bytes::Bytes;
use gottem_core::{
    templating, AuthSpec, BodyTemplate, CancelToken, CostExtract, FetchError, HttpMethod,
    ResponseParse, Route, ScrapeRequest,
};
use reqwest::{Client, Method, RequestBuilder};

/// Full outcome of an HTTP send: status, raw headers (lowercased keys), body.
/// Replaces the older `(u16, Bytes)` tuple so cost-extraction adapters can read headers.
pub struct HttpOutcome {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

/// Default shared `reqwest::Client`. Connection pooling makes one client across all three
/// HTTP adapters far more efficient than spinning a new one per request.
pub fn build_default_client() -> Client {
    Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .timeout(Duration::from_secs(120))
        .connect_timeout(Duration::from_secs(15))
        .gzip(true)
        .brotli(true)
        .user_agent(concat!("gottem/", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("default reqwest client")
}

/// Map gottem `HttpMethod` to a reqwest `Method`.
pub fn to_reqwest_method(m: HttpMethod) -> Method {
    match m {
        HttpMethod::Get => Method::GET,
        HttpMethod::Post => Method::POST,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Delete => Method::DELETE,
        HttpMethod::Patch => Method::PATCH,
        HttpMethod::Head => Method::HEAD,
    }
}

/// Apply [`AuthSpec`] to a `RequestBuilder`. Resolves env-var names through
/// [`ScrapeRequest::resolve_env`] — the request's `credentials` override is
/// preferred over [`std::env::var`], which is what makes BYOK keys scope to
/// a single request instead of leaking through the process environment.
/// Missing values surface as [`FetchError::Auth`].
pub fn apply_auth(
    builder: RequestBuilder,
    auth: &AuthSpec,
    req: &ScrapeRequest,
) -> Result<RequestBuilder, FetchError> {
    let resolve = |env: &str| -> Result<String, FetchError> {
        req.resolve_env(env)
            .ok_or_else(|| FetchError::Auth(format!("missing env var: {env}")))
    };
    match auth {
        AuthSpec::None => Ok(builder),
        AuthSpec::Bearer { env } => Ok(builder.bearer_auth(resolve(env)?)),
        AuthSpec::ApiKey {
            header,
            prefix,
            env,
        } => {
            let raw = resolve(env)?;
            let value = match prefix.as_deref() {
                Some(p) => format!("{p}{raw}"),
                None => raw,
            };
            Ok(builder.header(header.as_str(), value))
        }
        AuthSpec::Basic { user_env, pass_env } => {
            let user = resolve(user_env)?;
            let pass = match pass_env.as_deref() {
                Some(p) => Some(resolve(p)?),
                None => None,
            };
            Ok(builder.basic_auth(user, pass))
        }
        AuthSpec::WsUserinfo { .. } => Err(FetchError::Config(
            "ws_userinfo auth not valid for HTTP adapter".into(),
        )),
    }
}

/// Materialize a route's `BodyTemplate` into a byte payload, rendering `{{url}}`,
/// `{{method}}`, and `{{env:NAME}}` via [`gottem_core::templating::render_body`].
pub fn render_body(body: &BodyTemplate, req: &ScrapeRequest) -> Result<Option<Bytes>, FetchError> {
    match body {
        BodyTemplate::Empty => Ok(None),
        BodyTemplate::Json { template } => Ok(Some(Bytes::from(
            templating::render_body(template, req)?.into_bytes(),
        ))),
        BodyTemplate::Form { .. } => Err(FetchError::Config(
            "Form body not yet implemented in gottem-adapters-http".into(),
        )),
        BodyTemplate::Raw { .. } => Err(FetchError::Config(
            "Raw body not yet implemented in gottem-adapters-http".into(),
        )),
    }
}

/// Render a route's JSON body, then layer the caller's per-vendor
/// [`provider_options`](ScrapeRequest::provider_options) over it. The vendor
/// key is the route-id prefix (`"firecrawl.scrape"` → `"firecrawl"`), so only
/// the bucket for the route that actually runs is applied — a vendor-specific
/// option never reaches a different vendor when the ladder fails over. The
/// caller's keys win over the route template's defaults. Empty/non-object
/// bodies pass through unchanged.
pub fn render_json_body(route: &Route, req: &ScrapeRequest) -> Result<Option<Bytes>, FetchError> {
    let Some(bytes) = render_body(&route.body, req)? else {
        return Ok(None);
    };
    if req.provider_options.is_empty() {
        return Ok(Some(bytes));
    }
    let vendor = route.id.split('.').next().unwrap_or(route.id.as_ref());
    let Some(opts) = req.provider_options.get(vendor) else {
        return Ok(Some(bytes));
    };
    let serde_json::Value::Object(opts) = opts else {
        return Err(FetchError::Config(format!(
            "provider_options.{vendor} must be a JSON object"
        )));
    };
    let mut doc: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| FetchError::Config(format!("route body is not JSON: {e}")))?;
    let serde_json::Value::Object(doc_map) = &mut doc else {
        // Body isn't a JSON object — nothing to merge into; send it as-is.
        return Ok(Some(bytes));
    };
    for (k, v) in opts {
        doc_map.insert(k.clone(), v.clone());
    }
    let merged = serde_json::to_vec(&doc)
        .map_err(|e| FetchError::Config(format!("re-serialize vendor body: {e}")))?;
    Ok(Some(Bytes::from(merged)))
}

/// Parse content **and** extract cost from one response, deserializing the body's JSON
/// **at most once** — a `JsonPath` parse spec paired with a `JsonPath`/`JsonlFirst` cost
/// spec used to deserialize the same bytes twice.
///
/// Content-parse failures are hard errors (the caller explicitly asked for that field);
/// cost-extract failures collapse to `None` — missing cost data is normal, never fatal.
// The 2-tuple return reads naturally as `(content, cost)` at call sites and
// is one of two places it's used; a type alias would obscure more than it
// helps. Clippy's threshold flags it but it's deliberate.
#[allow(clippy::type_complexity)]
pub fn extract_content_and_cost(
    parse: &ResponseParse,
    cost: Option<&CostExtract>,
    headers: &[(String, String)],
    body: &Bytes,
) -> Result<(Option<Bytes>, Option<(f64, String)>), FetchError> {
    // Build only the JSON views the two specs actually need, each exactly once.
    let need_full = matches!(parse, ResponseParse::JsonPath { .. })
        || matches!(cost, Some(CostExtract::JsonPath { .. }));
    let need_first = matches!(parse, ResponseParse::JsonlFirst { .. })
        || matches!(cost, Some(CostExtract::JsonlFirst { .. }));

    let full = if need_full {
        parse_full_json(body)
    } else {
        None
    };
    let first = if need_first {
        parse_first_record(body)
    } else {
        None
    };

    let content = parse_content_with(parse, body, full.as_ref(), first.as_ref())?;
    let cost_out = extract_cost_with(cost, headers, full.as_ref(), first.as_ref());
    Ok((content, cost_out))
}

/// Parse content per the [`ResponseParse`] spec, over already-deserialized JSON views —
/// `full` for `JsonPath`, `first` for `JsonlFirst`. A `None` view for a JSON spec means
/// the body wasn't valid JSON.
///
/// Returns the extracted content as [`Bytes`] (or None for `RawBytes`). For the
/// utf8-passthrough variants the returned Bytes is a **refcount-bumped clone of
/// `body`** — no second copy of large HTML pages. For `JsonPath` and `JsonlFirst`
/// the returned Bytes is a fresh small allocation holding the extracted field.
fn parse_content_with(
    parse: &ResponseParse,
    body: &Bytes,
    full: Option<&serde_json::Value>,
    first: Option<&serde_json::Value>,
) -> Result<Option<Bytes>, FetchError> {
    match parse {
        ResponseParse::RawText | ResponseParse::Html | ResponseParse::Markdown => {
            // Share the body's allocation — clone is a refcount bump, not a memcpy.
            Ok(Some(body.clone()))
        }
        ResponseParse::RawBytes => Ok(None),
        ResponseParse::JsonPath { path } => {
            let v = full
                .ok_or_else(|| FetchError::Parse("json: response body is not valid JSON".into()))?;
            let ptr = dotted_to_pointer(path);
            value_at(v, &ptr)
                .map(|s| Some(Bytes::from(s.into_bytes())))
                .ok_or_else(|| FetchError::Parse(format!("no value at path {path}")))
        }
        ResponseParse::JsonlFirst { path } => {
            let v = first
                .ok_or_else(|| FetchError::Parse("no valid JSON line in JSONL body".into()))?;
            let ptr = dotted_to_pointer(path);
            value_at(v, &ptr)
                .map(|s| Some(Bytes::from(s.into_bytes())))
                .ok_or_else(|| FetchError::Parse(format!("no value at path {path}")))
        }
        // JsonlEach is the parse spec for streaming-many crawl routes — the
        // many-stream adapter walks records itself and never lands here. Fall
        // back to first-record behavior so a misconfigured single-page route
        // still produces *something* sensible instead of erroring.
        ResponseParse::JsonlEach { path } => {
            let v = first
                .ok_or_else(|| FetchError::Parse("no valid JSON line in JSONL body".into()))?;
            let ptr = dotted_to_pointer(path);
            value_at(v, &ptr)
                .map(|s| Some(Bytes::from(s.into_bytes())))
                .ok_or_else(|| FetchError::Parse(format!("no value at path {path}")))
        }
    }
}

/// Deserialize the whole body as one JSON document. `None` on malformed input.
fn parse_full_json(body: &[u8]) -> Option<serde_json::Value> {
    serde_json::from_slice(body).ok()
}

/// Deserialize the first JSONL record, unwrapping a single-element array wrapper
/// (Spider sometimes wraps the record). `None` if no parseable record is found.
fn parse_first_record(body: &[u8]) -> Option<serde_json::Value> {
    let line = first_jsonl_record(body)?;
    let parsed: serde_json::Value = serde_json::from_slice(line).ok()?;
    Some(match parsed {
        serde_json::Value::Array(arr) => arr.into_iter().next().unwrap_or(serde_json::Value::Null),
        other => other,
    })
}

/// Race a request future against the [`CancelToken`]. Returns Cancelled on outer cancel,
/// otherwise the underlying result (mapped to a FetchError).
pub async fn send_with_cancel(
    builder: RequestBuilder,
    cancel: &CancelToken,
) -> Result<HttpOutcome, FetchError> {
    let response = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(FetchError::Cancelled),
        r = builder.send() => r.map_err(classify_reqwest_err)?,
    };
    let status = response.status().as_u16();
    // Capture headers BEFORE consuming the body — needed for cost extraction (Zr-Cost,
    // Spb-Cost, etc.). Lowercase the keys so case-insensitive matching is trivial.
    let headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|s| (k.as_str().to_ascii_lowercase(), s.to_string()))
        })
        .collect();
    let body = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(FetchError::Cancelled),
        r = response.bytes() => r.map_err(classify_reqwest_err)?,
    };
    Ok(HttpOutcome {
        status,
        headers,
        body,
    })
}

/// Extract per-request cost from the vendor's response per the route's [`CostExtract`]
/// spec. Returns `(value, unit_label)` on success; `None` when the spec is absent or the
/// vendor didn't include the expected field this time. Never panics, never errors —
/// missing cost data is normal and shouldn't block a successful response.
/// Extract per-request cost from the vendor's response per the route's [`CostExtract`]
/// spec, over already-deserialized JSON views — `full` for `JsonPath`, `first` for
/// `JsonlFirst`. Shares one parse with [`parse_content_with`] via [`extract_content_and_cost`].
///
/// Returns `(value, unit_label)` on success; `None` when the spec is absent or the vendor
/// didn't include the expected field. Never panics, never errors — missing cost data is
/// normal and shouldn't block a successful response.
fn extract_cost_with(
    spec: Option<&CostExtract>,
    headers: &[(String, String)],
    full: Option<&serde_json::Value>,
    first: Option<&serde_json::Value>,
) -> Option<(f64, String)> {
    let spec = spec?;
    match spec {
        CostExtract::Header {
            name,
            unit,
            multiplier,
        } => {
            let target = name.to_ascii_lowercase();
            for (k, v) in headers {
                if k == &target {
                    return v
                        .trim()
                        .parse::<f64>()
                        .ok()
                        .map(|n| (n * multiplier, unit.clone()));
                }
            }
            None
        }
        CostExtract::JsonPath {
            path,
            unit,
            multiplier,
        } => extract_f64_at_path(full?, path).map(|n| (n * multiplier, unit.clone())),
        CostExtract::JsonlFirst {
            path,
            unit,
            multiplier,
        } => extract_f64_at_path(first?, path).map(|n| (n * multiplier, unit.clone())),
    }
}

fn extract_f64_at_path(v: &serde_json::Value, path: &str) -> Option<f64> {
    let ptr = dotted_to_pointer(path);
    let target = if ptr.is_empty() { v } else { v.pointer(&ptr)? };
    target
        .as_f64()
        .or_else(|| target.as_str().and_then(|s| s.parse::<f64>().ok()))
}

/// Map a `reqwest::Error` into a gottem [`FetchError`] preserving timeout/network semantics.
pub fn classify_reqwest_err(e: reqwest::Error) -> FetchError {
    if e.is_timeout() {
        FetchError::Timeout(Duration::from_secs(0))
    } else if e.is_connect() {
        FetchError::Network(format!("connect: {e}"))
    } else if let Some(status) = e.status() {
        FetchError::Status(status.as_u16())
    } else {
        FetchError::Network(e.to_string())
    }
}

// ---- internals --------------------------------------------------------------

fn first_jsonl_record(body: &[u8]) -> Option<&[u8]> {
    for line in body.split(|&b| b == b'\n') {
        let trimmed = trim_ws(line);
        if trimmed.is_empty() {
            continue;
        }
        if matches!(trimmed[0], b'{' | b'[') {
            return Some(trimmed);
        }
    }
    None
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

fn value_at(v: &serde_json::Value, ptr: &str) -> Option<String> {
    let target = if ptr.is_empty() { v } else { v.pointer(ptr)? };
    Some(match target {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    })
}

/// Convert dotted/bracket JSONPath-ish syntax to RFC 6901 JSON Pointer.
///
/// `$.data.markdown`       -> `/data/markdown`
/// `$.results[0].html`     -> `/results/0/html`
/// `$.field[0][1].deep`    -> `/field/0/1/deep`
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotted_to_pointer_simple() {
        assert_eq!(dotted_to_pointer("$.data.markdown"), "/data/markdown");
        assert_eq!(dotted_to_pointer("$.results[0].html"), "/results/0/html");
        assert_eq!(dotted_to_pointer("$.field[0][1].deep"), "/field/0/1/deep");
        assert_eq!(dotted_to_pointer("$"), "");
    }

    #[test]
    fn first_jsonl_skips_blank_lines() {
        let body = b"\n\n  \n{\"content\":\"hello\"}\n{\"content\":\"world\"}";
        let line = first_jsonl_record(body).unwrap();
        assert_eq!(line, br#"{"content":"hello"}"#);
    }

    /// Run just the content side of [`extract_content_and_cost`] (no cost spec),
    /// returning the extracted content as a `String` for inexpensive equality assertions.
    fn parse_only(parse: &ResponseParse, body: &[u8]) -> Result<Option<String>, FetchError> {
        let body = Bytes::copy_from_slice(body);
        extract_content_and_cost(parse, None, &[], &body)
            .map(|(content, _)| content.map(|b| String::from_utf8(b.to_vec()).unwrap()))
    }

    #[test]
    fn parse_content_jsonpath_extracts_field() {
        let body = br##"{"data":{"markdown":"# heading"}}"##;
        let parse = ResponseParse::JsonPath {
            path: "$.data.markdown".into(),
        };
        let out = parse_only(&parse, body).unwrap().unwrap();
        assert_eq!(out, "# heading");
    }

    #[test]
    fn parse_content_jsonl_first_extracts_field() {
        let body = b"{\"content\":\"line one\"}\n{\"content\":\"line two\"}";
        let parse = ResponseParse::JsonlFirst {
            path: "$.content".into(),
        };
        let out = parse_only(&parse, body).unwrap().unwrap();
        assert_eq!(out, "line one");
    }

    #[test]
    fn parse_content_jsonl_unwraps_array_wrapper() {
        let body = br#"[{"content":"wrapped"}]"#;
        let parse = ResponseParse::JsonlFirst {
            path: "$.content".into(),
        };
        let out = parse_only(&parse, body).unwrap().unwrap();
        assert_eq!(out, "wrapped");
    }

    #[test]
    fn html_passthrough_content_shares_body_allocation() {
        // For `Html`/`Markdown`/`RawText` the returned content Bytes must be the
        // *same* underlying buffer as `body` — same data ptr and same len means it's
        // a refcount-bumped clone, not a fresh copy. This is the core memory win:
        // large HTML pages exist in memory exactly once.
        let body = Bytes::from(b"<html>hello world</html>".to_vec());
        let (content, _) =
            extract_content_and_cost(&ResponseParse::Html, None, &[], &body).unwrap();
        let content = content.expect("html parse always produces content");
        assert_eq!(content.as_ptr(), body.as_ptr(), "expected shared buffer");
        assert_eq!(content.len(), body.len());
    }

    #[test]
    fn json_body_parsed_once_for_content_and_cost() {
        // A JsonPath parse spec + JsonPath cost spec both drill into the same body —
        // extract_content_and_cost deserializes it a single time and feeds both.
        let body = Bytes::from_static(br##"{"data":{"markdown":"# hi"},"meta":{"credits":3}}"##);
        let parse = ResponseParse::JsonPath {
            path: "$.data.markdown".into(),
        };
        let cost = CostExtract::JsonPath {
            path: "$.meta.credits".into(),
            unit: "credits".into(),
            multiplier: 2.0,
        };
        let (content, cost_out) =
            extract_content_and_cost(&parse, Some(&cost), &[], &body).unwrap();
        assert_eq!(content.unwrap().as_ref(), b"# hi");
        assert_eq!(cost_out, Some((6.0, "credits".into())));
    }
}
