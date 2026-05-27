use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use url::Url;

use crate::{route::RouteId, tier::Tier};

#[derive(Debug, Clone)]
pub struct ScrapeResponse {
    pub url: Url,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    /// Parsed content per the route's `parse` spec (markdown, plain text, JSON field).
    ///
    /// Stored as [`Bytes`] (not `String`) so the utf8-passthrough cases — `Html`,
    /// `Markdown`, `RawText` — can share the **same allocation** as [`Self::body`]
    /// via a refcount bump. Reading as `&str` is one inexpensive utf8 check on each call;
    /// use [`Self::content_str`] / [`Self::content_str_lossy`] for that.
    pub content: Option<Bytes>,
    pub route_id: RouteId,
    pub tier: Tier,
    /// Static cost from the route (milli-cents — 10 = $0.001). Always present.
    /// Reflects the *expected* cost, not what the vendor actually billed.
    pub cost_milli: u64,
    /// Per-request cost reported by the vendor itself, extracted per the route's
    /// `cost_extract` spec. `None` when the route doesn't declare extraction or the
    /// vendor didn't include the field in this response. Raw numeric value — unit is
    /// in [`cost_actual_unit`](Self::cost_actual_unit).
    pub cost_actual_units: Option<f64>,
    /// Unit label for [`cost_actual_units`](Self::cost_actual_units) — "credits",
    /// "milli_cents", "dollars", etc. Same lifetime as the field above.
    pub cost_actual_unit: Option<String>,
    pub elapsed: Duration,
    pub attempt: u32,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl ScrapeResponse {
    /// Length of the parsed content if present, otherwise raw body length.
    pub fn content_len(&self) -> usize {
        self.content
            .as_ref()
            .map(Bytes::len)
            .unwrap_or(self.body.len())
    }

    /// Borrow [`Self::content`] as `&str`. Returns `None` when there is no content
    /// **or** when the bytes aren't valid UTF-8 (e.g. a route that intentionally
    /// emits binary content via `ResponseParse::RawBytes`).
    pub fn content_str(&self) -> Option<&str> {
        std::str::from_utf8(self.content.as_ref()?).ok()
    }

    /// Borrow [`Self::content`] as a UTF-8 string, replacing invalid sequences with
    /// the replacement character. Cheaper than `to_string()`: yields `Cow::Borrowed`
    /// when the bytes are already valid UTF-8.
    pub fn content_str_lossy(&self) -> Option<Cow<'_, str>> {
        self.content.as_ref().map(|b| String::from_utf8_lossy(b))
    }
}
