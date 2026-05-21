//! Shared helpers for HTTP adapters: client builder, auth, body templating, response parsing.

use std::time::Duration;

use bytes::Bytes;
use gottem_core::{
    templating, AuthSpec, BodyTemplate, CancelToken, FetchError, HttpMethod, ResponseParse,
    ScrapeRequest,
};
use reqwest::{Client, Method, RequestBuilder};

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

/// Apply [`AuthSpec`] to a `RequestBuilder`. Reads credentials from environment variables
/// declared in the route. Missing env vars surface as [`FetchError::Auth`].
pub fn apply_auth(
    builder: RequestBuilder,
    auth: &AuthSpec,
) -> Result<RequestBuilder, FetchError> {
    match auth {
        AuthSpec::None => Ok(builder),
        AuthSpec::Bearer { env } => {
            let token = std::env::var(env)
                .map_err(|_| FetchError::Auth(format!("missing env var: {env}")))?;
            Ok(builder.bearer_auth(token))
        }
        AuthSpec::ApiKey { header, prefix, env } => {
            let raw = std::env::var(env)
                .map_err(|_| FetchError::Auth(format!("missing env var: {env}")))?;
            let value = match prefix.as_deref() {
                Some(p) => format!("{p}{raw}"),
                None => raw,
            };
            Ok(builder.header(header.as_str(), value))
        }
        AuthSpec::Basic { user_env, pass_env } => {
            let user = std::env::var(user_env)
                .map_err(|_| FetchError::Auth(format!("missing env var: {user_env}")))?;
            let pass = match pass_env.as_deref() {
                Some(p) => Some(
                    std::env::var(p)
                        .map_err(|_| FetchError::Auth(format!("missing env var: {p}")))?,
                ),
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
pub fn render_body(
    body: &BodyTemplate,
    req: &ScrapeRequest,
) -> Result<Option<Bytes>, FetchError> {
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

/// Parse the upstream response according to the route's [`ResponseParse`] spec.
///
/// Returns the extracted content as a string (or None for `RawBytes`). For `JsonPath`
/// and `JsonlFirst`, drills into the parsed JSON at a dotted-path like `$.data.markdown`
/// or `$.results[0].html` (converted to RFC 6901 JSON Pointer internally).
pub fn parse_content(
    parse: &ResponseParse,
    body: &[u8],
) -> Result<Option<String>, FetchError> {
    use serde_json::Value;
    match parse {
        ResponseParse::RawText | ResponseParse::Html | ResponseParse::Markdown => {
            Ok(Some(String::from_utf8_lossy(body).into_owned()))
        }
        ResponseParse::RawBytes => Ok(None),
        ResponseParse::JsonPath { path } => {
            let v: Value = serde_json::from_slice(body)
                .map_err(|e| FetchError::Parse(format!("json: {e}")))?;
            let ptr = dotted_to_pointer(path);
            value_at(&v, &ptr)
                .map(Some)
                .ok_or_else(|| FetchError::Parse(format!("no value at path {path}")))
        }
        ResponseParse::JsonlFirst { path } => {
            let line = first_jsonl_record(body)
                .ok_or_else(|| FetchError::Parse("no JSON line in JSONL body".into()))?;
            let parsed: Value = serde_json::from_slice(line)
                .map_err(|e| FetchError::Parse(format!("jsonl line: {e}")))?;
            // Spider Cloud sometimes wraps the record as a single-element array.
            let target = match &parsed {
                Value::Array(arr) => arr.first().cloned().unwrap_or(Value::Null),
                _ => parsed,
            };
            let ptr = dotted_to_pointer(path);
            value_at(&target, &ptr)
                .map(Some)
                .ok_or_else(|| FetchError::Parse(format!("no value at path {path}")))
        }
    }
}

/// Race a request future against the [`CancelToken`]. Returns Cancelled on outer cancel,
/// otherwise the underlying result (mapped to a FetchError).
pub async fn send_with_cancel(
    builder: RequestBuilder,
    cancel: &CancelToken,
) -> Result<(u16, Bytes), FetchError> {
    let response = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(FetchError::Cancelled),
        r = builder.send() => r.map_err(classify_reqwest_err)?,
    };
    let status = response.status().as_u16();
    let body = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(FetchError::Cancelled),
        r = response.bytes() => r.map_err(classify_reqwest_err)?,
    };
    Ok((status, body))
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

    #[test]
    fn parse_content_jsonpath_extracts_field() {
        let body = br##"{"data":{"markdown":"# heading"}}"##;
        let parse = ResponseParse::JsonPath { path: "$.data.markdown".into() };
        let out = parse_content(&parse, body).unwrap().unwrap();
        assert_eq!(out, "# heading");
    }

    #[test]
    fn parse_content_jsonl_first_extracts_field() {
        let body = b"{\"content\":\"line one\"}\n{\"content\":\"line two\"}";
        let parse = ResponseParse::JsonlFirst { path: "$.content".into() };
        let out = parse_content(&parse, body).unwrap().unwrap();
        assert_eq!(out, "line one");
    }

    #[test]
    fn parse_content_jsonl_unwraps_array_wrapper() {
        let body = br#"[{"content":"wrapped"}]"#;
        let parse = ResponseParse::JsonlFirst { path: "$.content".into() };
        let out = parse_content(&parse, body).unwrap().unwrap();
        assert_eq!(out, "wrapped");
    }
}
