use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::capabilities::Capabilities;

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
        }
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
