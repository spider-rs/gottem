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
    /// Cost charged for this response in milli-cents (10 = $0.001).
    pub cost_milli: u64,
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
