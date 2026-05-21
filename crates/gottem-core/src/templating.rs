//! Template rendering for route endpoints and request bodies.
//!
//! Supported placeholders:
//!
//! - `{{url}}`           — the request URL. Percent-encoded for endpoint templates,
//!                          raw for body templates (so JSON bodies look natural).
//! - `{{method}}`        — the request method as a string (`GET`, `POST`, ...).
//! - `{{env:NAME}}`      — environment variable lookup; missing env → [`FetchError::Auth`].
//!                          Percent-encoded for endpoint templates, raw for body templates.
//!
//! # Why two render modes
//!
//! - **Endpoint templates** go into a URL. Special characters in the substituted value
//!   would break the URL grammar, so we percent-encode by default (e.g. `https://x/?u={{url}}`).
//! - **Body templates** typically embed values in JSON. Percent-encoding would corrupt
//!   the URL inside a JSON string, so we leave values raw and trust the caller's quoting.

use crate::{error::FetchError, request::ScrapeRequest};

/// Render an endpoint template — placeholders are percent-encoded.
pub fn render_endpoint(template: &str, req: &ScrapeRequest) -> Result<String, FetchError> {
    if !has_placeholder(template) {
        return Ok(template.to_string());
    }
    let s = template.replace("{{url}}", &percent_encode(req.url.as_str()));
    let s = s.replace("{{method}}", req.method.as_str());
    substitute_env(&s, /* encode = */ true)
}

/// Render a body template — placeholders are NOT encoded.
/// Caller is expected to embed the result inside JSON / form / whatever quoting context.
pub fn render_body(template: &str, req: &ScrapeRequest) -> Result<String, FetchError> {
    if !has_placeholder(template) {
        return Ok(template.to_string());
    }
    let s = template.replace("{{url}}", req.url.as_str());
    let s = s.replace("{{method}}", req.method.as_str());
    substitute_env(&s, /* encode = */ false)
}

/// Whether a template string contains any `{{...}}` placeholder.
pub fn has_placeholder(template: &str) -> bool {
    template.contains("{{")
}

// ---- internals --------------------------------------------------------------

fn substitute_env(s: &str, encode: bool) -> Result<String, FetchError> {
    if !s.contains("{{env:") {
        return Ok(s.to_string());
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("{{env:") {
        out.push_str(&rest[..start]);
        let after_start = &rest[start + 6..];
        let end = after_start
            .find("}}")
            .ok_or_else(|| FetchError::Config("unterminated {{env:...}}".into()))?;
        let env_name = &after_start[..end];
        let val = std::env::var(env_name)
            .map_err(|_| FetchError::Auth(format!("missing env var: {env_name}")))?;
        if encode {
            out.push_str(&percent_encode(&val));
        } else {
            out.push_str(&val);
        }
        rest = &after_start[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

fn percent_encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ScrapeRequest;
    use url::Url;

    fn req(url: &str) -> ScrapeRequest {
        ScrapeRequest::get(Url::parse(url).unwrap())
    }

    #[test]
    fn endpoint_no_placeholder_is_passthrough() {
        let out = render_endpoint("https://example.com/", &req("https://x.test/")).unwrap();
        assert_eq!(out, "https://example.com/");
    }

    #[test]
    fn endpoint_url_is_percent_encoded() {
        let out = render_endpoint(
            "https://api.zenrows.com/?url={{url}}",
            &req("https://example.com/page?x=1"),
        )
        .unwrap();
        assert!(
            out.starts_with("https://api.zenrows.com/?url=https%3A%2F%2Fexample.com%2Fpage"),
            "got {out}"
        );
    }

    #[test]
    fn endpoint_env_is_percent_encoded() {
        std::env::set_var("GOTTEM_TPL_ENV_TEST", "key with spaces");
        let out = render_endpoint(
            "https://x/?k={{env:GOTTEM_TPL_ENV_TEST}}",
            &req("https://example.com/"),
        )
        .unwrap();
        assert!(out.contains("key+with+spaces") || out.contains("key%20with%20spaces"), "got {out}");
    }

    #[test]
    fn endpoint_missing_env_is_auth_error() {
        let err = render_endpoint(
            "https://x/?k={{env:GOTTEM_DEFINITELY_NOT_SET_XYZ}}",
            &req("https://example.com/"),
        )
        .unwrap_err();
        assert!(matches!(err, FetchError::Auth(_)), "got {err:?}");
    }

    #[test]
    fn body_url_is_raw() {
        let out = render_body(
            r#"{"url":"{{url}}","method":"{{method}}"}"#,
            &req("https://example.com/page"),
        )
        .unwrap();
        assert_eq!(out, r#"{"url":"https://example.com/page","method":"GET"}"#);
    }

    #[test]
    fn body_env_is_raw() {
        std::env::set_var("GOTTEM_TPL_BODY_TEST", "tok-123");
        let out = render_body(
            r#"{"token":"{{env:GOTTEM_TPL_BODY_TEST}}"}"#,
            &req("https://example.com/"),
        )
        .unwrap();
        assert_eq!(out, r#"{"token":"tok-123"}"#);
    }

    #[test]
    fn unterminated_env_placeholder_errors() {
        let err = render_body("hello {{env:NOPE", &req("https://x/")).unwrap_err();
        assert!(matches!(err, FetchError::Config(_)));
    }
}
