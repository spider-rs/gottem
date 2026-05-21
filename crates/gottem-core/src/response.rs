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
    pub content: Option<String>,
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
            .as_deref()
            .map(str::len)
            .unwrap_or(self.body.len())
    }
}
